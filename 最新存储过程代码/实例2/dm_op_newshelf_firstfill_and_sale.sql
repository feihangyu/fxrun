CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_newshelf_firstfill_and_sale`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate, 1);
SET @month_start := SUBDATE(@sub_1, DAY(@sub_1) - 1);
SET @month_id := DATE_FORMAT(@sub_1, '%Y-%m');	   
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.`dm_op_newshelf_firstfill_and_sale` WHERE month_id = @month_id;
        
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp(PRIMARY KEY (shelf_id))
SELECT s.business_name,
       s.shelf_id,
       s.shelf_type,      
       DATE(s.activate_time)activate_time,
       IF(DATEDIFF(@sdate,DATE(s.activate_time)) < 7,DATEDIFF(@sdate,DATE(s.activate_time)),7)7_day, -- 激活7日内运营天数
       DATEDIFF(@sdate,DATE(s.activate_time))active_days, -- 当月运营天数
       p.package_id,
       p.package_name
FROM fe_dwd.dwd_shelf_base_day_all s
LEFT JOIN fe_dm.dm_op_package_shelf p ON s.shelf_id = p.shelf_id AND p.stat_date = @sdate
WHERE s.activate_time >= @month_start
AND s.activate_time < @sdate
AND s.shelf_status IN(2,5) -- 已激活\已注销
AND s.revoke_status = 1    -- 正在运营
AND s.shelf_type IN (1,2,3,8);
-- 货架首次申请补货订单信息（补货类型:初始商品包补货）
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_firsrfill_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_firsrfill_tmp(PRIMARY KEY (shelf_id))
SELECT t.shelf_id,
       t.sku first_fill_sku,
       t.actual_sign_num,
       t.firstfill
FROM fe_dm.dm_op_shelf_firstfill t 
JOIN fe_dm.shelf_tmp s ON t.shelf_id = s.shelf_id
WHERE t.order_id IS NOT NULL;
-- 货架有库存sku及有库存sku标配
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_sku_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_sku_tmp(PRIMARY KEY (shelf_id))
SELECT d.shelf_id,
       COUNT(CASE WHEN stock_quantity > 0 THEN product_id END)sto_sku,
       COUNT(CASE WHEN shelf_fill_flag = 1 THEN product_id END)fill_sku,
       SUM(CASE WHEN stock_quantity > 0 THEN max_quantity END)sto_max_quantity,
       SUM(CASE WHEN shelf_fill_flag = 1 THEN max_quantity END)fill_quantity
FROM fe_dwd.dwd_shelf_product_day_all d
JOIN fe_dm.shelf_tmp p ON d.shelf_id = p.shelf_id
GROUP BY d.shelf_id;
-- 货架gmv
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_sale_tmp(PRIMARY KEY (shelf_id))
SELECT i.shelf_id,
       SUM(CASE WHEN i.sdate >= s.activate_time AND i.sdate < ADDDATE(s.activate_time,7) THEN gmv END)7_gmv,
       SUM(CASE WHEN i.sdate >= s.activate_time AND i.sdate < ADDDATE(s.activate_time,7) THEN gmv END) / s.7_day avg_7_gmv,
       SUM(CASE WHEN i.sdate >= @month_start AND i.sdate < @sdate THEN gmv END)gmv,
       SUM(CASE WHEN i.sdate >= @month_start AND i.sdate < @sdate THEN gmv END) / active_days avg_gmv
FROM fe_dwd.dwd_shelf_day_his i  -- 货架每日gmv
JOIN fe_dm.shelf_tmp s ON i.shelf_id = s.shelf_id
WHERE i.sdate >= @month_start
AND i.sdate < @sdate
GROUP BY i.shelf_id;
INSERT INTO fe_dm.dm_op_newshelf_firstfill_and_sale
(month_id
,business_name
,shelf_id
,package_id
,package_name
,active_days
,first_fill_sku
,is_sku_lack
,actual_sign_num
,is_fillnum_lack
,fill_sku
,fill_quantity
,sto_sku
,sto_max_quantity
,7_gmv
,avg_7_gmv
,gmv
,avg_gmv
,load_time
)
SELECT @month_id month_id,
       s.business_name,
       s.shelf_id,
       s.package_id,
       s.package_name,
       s.active_days,
       f.first_fill_sku,
       CASE WHEN (s.shelf_type IN (1,3,8) AND f.first_fill_sku < 25) OR (s.shelf_type = 2 AND f.first_fill_sku < 10) THEN 1 ELSE 0 END AS is_sku_lack,
       f.actual_sign_num,
       CASE WHEN (s.shelf_type IN (1,3,8) AND f.actual_sign_num < 180) OR (s.shelf_type = 2 AND f.actual_sign_num < 110) THEN 1 ELSE 0 END AS is_fillnum_lack,
       t.fill_sku,
       t.fill_quantity,
       t.sto_sku,
       t.sto_max_quantity,
       sale.7_gmv,
       sale.avg_7_gmv,
       sale.gmv,
       sale.avg_gmv,
       @timestamp AS load_time
FROM fe_dm.shelf_tmp s
LEFT JOIN fe_dm.shelf_firsrfill_tmp f ON s.shelf_id = f.shelf_id
LEFT JOIN fe_dm.shelf_sku_tmp t ON s.shelf_id = t.shelf_id
LEFT JOIN fe_dm.shelf_sale_tmp sale ON s.shelf_id = sale.shelf_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_newshelf_firstfill_and_sale',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_newshelf_firstfill_and_sale','dm_op_newshelf_firstfill_and_sale','唐进（朱星华）');
 
END