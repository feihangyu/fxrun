CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_shelf_cover`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @monday := ADDDATE(SUBDATE(@sdate,WEEKDAY(@sdate) + 1),1);
SET @one_month_date := SUBDATE(@sdate,INTERVAL 1 MONTH);
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT business_name,
       shelf_id,
       whether_close
FROM fe_dwd.`dwd_shelf_base_day_all`
WHERE shelf_type IN (1,2,3,4,5,8)
AND shelf_status = 2
AND revoke_status = 1
AND shelf_name NOT LIKE '%测试%';
-- 地区激活货架数
DROP TEMPORARY TABLE IF EXISTS fe_dm.business_shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.business_shelf_tmp AS
SELECT business_name,
       COUNT(shelf_id)active_shelf,-- 激活货架数
       COUNT(CASE WHEN whether_close = 2 THEN shelf_id END)noclose_active_shelf -- 激活非关闭货架数
FROM fe_dm.shelf_tmp
GROUP BY business_name;
-- 商品覆盖货架情况
DROP TEMPORARY TABLE IF EXISTS fe_dm.business_product_tmp;
CREATE TEMPORARY TABLE fe_dm.business_product_tmp (PRIMARY KEY (business_name,product_id))
SELECT s.business_name,
       d.product_id,
       COUNT(CASE WHEN stock_quantity > 0 THEN d.shelf_id END)sto_shelf,-- 有库存货架数
       COUNT(CASE WHEN shelf_fill_flag = 1 THEN d.shelf_id END)fill_shelf,-- 可补货货架数
       COUNT(CASE WHEN stock_quantity > 0 AND s.whether_close = 2 THEN d.shelf_id END)sto_noclose_shelf,-- 有库存非关闭货架数
       COUNT(CASE WHEN shelf_fill_flag = 1 AND s.whether_close = 2 THEN d.shelf_id END)fill_noclose_shelf -- 可补货非关闭货架数
FROM fe_dwd.dwd_shelf_product_day_all d
JOIN fe_dm.shelf_tmp s ON d.shelf_id = s.shelf_id
GROUP BY s.business_name,d.product_id;
-- 大仓昨日库存
DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_tmp;
CREATE TEMPORARY TABLE fe_dm.stock_tmp (PRIMARY KEY (business_area,product_code2))
SELECT a.business_area,
       sku_no product_code2,
       SUM(storage_amount)stock
FROM fe_dwd.dwd_sc_bdp_warehouse_stock_daily d
JOIN fe_dwd.dwd_pub_warehouse_business_area a ON d.warehouse_name = a.warehouse_name
WHERE sdate = SUBDATE(@sdate,1)
AND storage_amount > 0
GROUP BY a.business_area,sku_no;
-- 前置仓昨日库存
DROP TEMPORARY TABLE IF EXISTS fe_dm.pre_stock_tmp;
CREATE TEMPORARY TABLE fe_dm.pre_stock_tmp (PRIMARY KEY (business_area,product_id))
SELECT business_area,
       product_id,
       IFNULL(SUM(available_stock),0)pre_stock
FROM fe_dm.dm_prewarehouse_stock_detail  --  pj_prewarehouse_stock_detail
WHERE check_date = SUBDATE(@sdate,1)
AND available_stock > 0
GROUP BY business_area,product_id;
-- 地区商品覆盖情况，需每日更新并截存，保留一个月的数据
DELETE FROM fe_dm.dm_op_area_product_shelf_cover WHERE sdate = @sdate OR sdate < @one_month_date;
INSERT INTO fe_dm.dm_op_area_product_shelf_cover
(sdate
,business_name
,product_id
,product_type
,sale_level
,active_shelf
,noclose_active_shelf
,sto_shelf
,fill_shelf
,sto_noclose_shelf
,fill_noclose_shelf
,stock
,pre_stock
)
SELECT @sdate sdate,
       p.business_area,
       p.product_id,
       p.product_type,
       f.sale_level,
       s.active_shelf,
       s.noclose_active_shelf,
       b.sto_shelf,
       b.fill_shelf,
       b.sto_noclose_shelf,
       b.fill_noclose_shelf,
       st.stock,
       pre.pre_stock
FROM fe_dwd.dwd_pub_product_dim_sserp p  -- zs_product_dim_sserp
LEFT JOIN fe_dm.business_shelf_tmp s ON p.business_area = s.business_name
LEFT JOIN fe_dm.business_product_tmp b ON p.business_area = b.business_name AND p.product_id = b.product_id
LEFT JOIN fe_dm.stock_tmp st ON p.business_area = st.business_area AND p.product_fe = st.product_code2
LEFT JOIN fe_dm.pre_stock_tmp pre ON p.business_area = pre.business_area AND p.product_id = pre.product_id
LEFT JOIN fe_dm.dm_area_product_sale_flag f ON p.business_area = f.business_area AND p.product_id = f.product_id AND f.sdate = @monday;  -- zs_area_product_sale_flag
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_shelf_cover',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_shelf_cover','dm_op_area_product_shelf_cover','朱星华');
  COMMIT;	
END