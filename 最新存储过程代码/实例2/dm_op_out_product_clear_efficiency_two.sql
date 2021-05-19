CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_out_product_clear_efficiency_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate, 1);
SET @month_start := SUBDATE(@sub_1, DAY(@sub_1) - 1);
SET @month_id := DATE_FORMAT(@sub_1, '%Y-%m');
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 最新商品清单中的地区淘汰品id
DROP TEMPORARY TABLE IF EXISTS fe_dm.dim_tmp;
CREATE TEMPORARY TABLE fe_dm.dim_tmp (PRIMARY KEY(business_name,product_id))
SELECT business_area business_name,
       product_id,
       product_type,
       out_date,
       h.version
FROM fe_dwd.dwd_pub_product_dim_sserp h
WHERE product_type IN ('停补','停补（替补）','淘汰','淘汰（替补）','退出');
-- 淘汰品历史销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.sal_tmp;
CREATE TEMPORARY TABLE fe_dm.sal_tmp (PRIMARY KEY(business_name,product_id))
SELECT t.business_name,
       t.product_id,
       SUM(t.qty_sal) cum_qty_sal, -- 历史销量
       SUM(t.gmv) cum_gmv,         -- 历史gmv
       IFNULL(SUM(t.gmv)/SUM(t.qty_sal),0) sale_price,
       SUM(CASE WHEN sdate >= @month_start AND sdate < @sdate THEN t.qty_sal END)qty_sal,-- 统计月销量
       SUM(CASE WHEN sdate >= @month_start AND sdate < @sdate THEN t.gmv END)gmv,        -- 统计月gmv
       SUM(CASE WHEN sdate >= @month_start AND sdate < @sdate THEN t.discount END)discount -- 统计月折扣
FROM fe_dm.dm_area_product_dgmv t  -- 商品地区每日gmv
JOIN fe_dm.dim_tmp d ON t.business_name = d.business_name AND t.product_id = d.product_id
WHERE t.sdate < @sdate
GROUP BY t.business_name,t.product_id;
-- 淘汰品历史补货
DROP TEMPORARY TABLE IF EXISTS fe_dm.fil_tmp;
CREATE TEMPORARY TABLE fe_dm.fil_tmp (PRIMARY KEY(business_name,product_id))
SELECT t.business_name,
       t.product_id,
       SUM(t.qty_fill) cum_qty_fil, -- 历史补货量
       SUM(CASE WHEN sdate >= @month_start AND sdate < @sdate THEN t.qty_fill END)qty_fill,-- 统计月补货量
       SUM(CASE WHEN sdate >= @month_start AND sdate < @sdate THEN t.val_fill END)val_fill -- 统计月补货金额
FROM fe_dm.dm_op_area_product_dfill t  -- 地区商品每日补货
JOIN fe_dm.dim_tmp d ON t.business_name = d.business_name AND t.product_id = d.product_id
WHERE t.sdate < @sdate
GROUP BY t.business_name,t.product_id;
-- 淘汰品历史盘点数据(3min56)
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_product_check;
CREATE TEMPORARY TABLE fe_dm.shelf_product_check (PRIMARY KEY(business_name,product_id))
SELECT t.business_name,
       m.product_id,
       SUM(qty_check_error)qty_check_error, -- 盘点总差异量
       SUM(qty_check_error1)qty_check_error1,-- 破损原因差异
       SUM(qty_check_error2)qty_check_error2,-- 过期原因差异
       SUM(qty_check_error4)qty_check_error4 -- 质量原因差异
FROM fe_dm.dm_op_product_shelf_dam_month m
JOIN fe_dwd.dwd_shelf_base_day_all t ON m.shelf_id = t.shelf_id
WHERE qty_check_error != 0
GROUP BY t.business_name,m.product_id;
-- 淘汰品期初期末库存金额
DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_tmp;
CREATE TEMPORARY TABLE fe_dm.sto_tmp ( PRIMARY KEY (business_name, product_id) ) AS 
SELECT a.business_name,
       m.product_id,
       SUM(qty_start) * p.sale_price start_stock,
       SUM(qty_end) * p.sale_price end_stock
FROM fe_dm.dm_op_product_shelf_dam_month m
JOIN fe_dwd.dwd_shelf_base_day_all a ON m.shelf_id = a.shelf_id
JOIN fe_dm.dim_tmp d ON a.business_name = d.business_name AND m.product_id = d.product_id
LEFT JOIN fe_dwd.dwd_shelf_product_day_all p ON m.shelf_id = p.shelf_id AND m.product_id = p.product_id
WHERE month_id = @month_id
GROUP BY a.business_name,m.product_id;
-- 淘汰品售罄率  每日更新截存
DELETE FROM fe_dm.dm_op_out_product_sale_through_rate WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_op_out_product_sale_through_rate
(`sdate`
,`business_name`
,`product_id`
,`product_type`
,`version`
,`out_date`
,`cum_gmv`
,`cum_fill`
,`cum_miss_total`
,`cum_damage_total`
,`load_time`
)
SELECT @sdate sdate,
       d.business_name,
       d.product_id,
       d.product_type,
       d.version,
       d.out_date,
       IFNULL(s.cum_gmv,0)cum_gmv,
       IFNULL(f.cum_qty_fil * s.sale_price,0)cum_fill,
       IFNULL(lost.qty_check_error,0) * s.sale_price  cum_miss_total, -- 2020/06/09增加,累计盘点差异金额
       IFNULL(lost.damage_amount,0) * s.sale_price cum_damage_total,  -- 累计盗损金额
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.dim_tmp d
LEFT JOIN fe_dm.sal_tmp s ON d.business_name = s.business_name AND d.product_id = s.product_id
LEFT JOIN fe_dm.fil_tmp f ON d.business_name = f.business_name AND d.product_id = f.product_id
LEFT JOIN
(SELECT c.business_name,
        c.product_id,
        qty_check_error,
        qty_check_error1,
        qty_check_error2,
        qty_check_error4,
        IF((qty_check_error - (qty_check_error1 + qty_check_error2 + qty_check_error4)) < 0 ,- (qty_check_error - (qty_check_error1 + qty_check_error2 + qty_check_error4)),0)damage_amount -- 盘点差异量会存在盘盈的情况（盘点差异量>0),所以当>0时记0，小于0时取绝对值
FROM fe_dm.shelf_product_check c
)lost ON lost.business_name = d.business_name AND lost.product_id = d.product_id;
-- 淘汰品清货效率  每日更新，每月1日截存上月数据
DELETE FROM fe_dm.dm_op_out_product_clear_efficiency WHERE month_id = @month_id;
INSERT INTO fe_dm.dm_op_out_product_clear_efficiency
(`month_id`
,`business_name`
,`product_id`
,`product_type`
,`version`
,`out_date`
,`qty_sal`
,`gmv`
,`discount`
,`qty_fill`
,`val_fill`
,`start_stock`
,`end_stock`
,`load_time`
)
SELECT @month_id month_id,
       t.business_name,
       t.product_id,
       t.product_type,
       t.version,
       t.out_date,
       sal.qty_sal,
       sal.gmv,
       sal.discount,
       f.qty_fill,
       f.val_fill,
       sto.start_stock,
       sto.end_stock,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.dim_tmp t
LEFT JOIN fe_dm.sto_tmp sto ON t.business_name = sto.business_name AND t.product_id = sto.product_id
LEFT JOIN fe_dm.sal_tmp sal ON t.business_name = sal.business_name AND t.product_id = sal.product_id
LEFT JOIN fe_dm.fil_tmp f ON t.business_name = f.business_name AND t.product_id = f.product_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_out_product_clear_efficiency_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_out_product_sale_through_rate','dm_op_out_product_clear_efficiency_two','唐进（朱星华）');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_out_product_clear_efficiency','dm_op_out_product_clear_efficiency_two','唐进（朱星华）');
 
END