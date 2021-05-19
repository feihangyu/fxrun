CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_flags_res_area_two`( IN in_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate := in_sdate, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
SET @week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1);
SET @add_day := ADDDATE(@week_end, 1);
SET @week_start := SUBDATE(@week_end, 6);
SET @last_week_end := SUBDATE(@week_end, 7);
SET @last_fill_day := SUBDATE(@week_end, 6+14);
SET @y_m_add := DATE_FORMAT(@add_day, '%Y-%m');
SET @y_m_start := DATE_FORMAT(@week_start, '%Y-%m');
-- 期末库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sto_tmp;
CREATE TEMPORARY TABLE fe_dwd.sto_tmp
(PRIMARY KEY (shelf_id, product_id)) 
AS 
SELECT
        shelf_id,
        product_id,
        stock_quantity AS sto_qty_e,
        sales_flag AS sales_flag_e
FROM
        fe_dwd.`dwd_shelf_product_day_all_recent_32`
WHERE sdate = @week_end
        AND !ISNULL(shelf_id) 
        AND !ISNULL(product_id)
        AND stock_quantity > 0

;
-- 期初库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.ssto_tmp;
CREATE TEMPORARY TABLE fe_dwd.ssto_tmp
(PRIMARY KEY (shelf_id, product_id))
AS  
SELECT
        shelf_id,
        product_id,
        stock_quantity AS sto_qty_s,
        sales_flag AS sales_flag_s
FROM 
        fe_dwd.`dwd_shelf_product_day_all_recent_32`
WHERE sdate = @last_week_end
        AND !ISNULL(shelf_id) 
        AND !ISNULL(product_id) 
        AND stock_quantity > 0

;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sal_tmp;
CREATE TEMPORARY TABLE fe_dwd.sal_tmp (PRIMARY KEY (shelf_id, product_id)) AS
SELECT
        t.shelf_id, 
        t.product_id, 
        SUM(t.quantity_act) sal_qty, 
        SUM(t.quantity_act * t.sale_price) gmv
FROM
        fe_dwd.`dwd_pub_order_item_recent_one_month` t
WHERE t.pay_date >= @week_start
        AND t.pay_date < @add_day
        AND ! ISNULL(t.shelf_id)
        AND ! ISNULL(t.product_id)
GROUP BY t.shelf_id, t.product_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.fill_tmp (PRIMARY KEY (shelf_id, product_id)) AS
SELECT
shelf_id, product_id, SUM(actual_sign_num) actual_sign_num
FROM
fe_dwd.`dwd_fill_day_inc_recent_two_month` 
WHERE order_status IN (3, 4)
        AND fill_time >= @week_start
        AND fill_time < @add_day
        AND ! ISNULL(shelf_id)
        AND ! ISNULL(product_id)
GROUP BY shelf_id, product_id
HAVING actual_sign_num != 0
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.detail_tmp;
CREATE TEMPORARY TABLE fe_dwd.detail_tmp (PRIMARY KEY (shelf_id, product_id)) AS
SELECT
        t.shelf_id, 
        t.product_id, 
        t.sale_price, 
        IFNULL(t.first_fill_time >= @last_fill_day, 0) first_fill_flag
FROM
        fe_dwd.`dwd_shelf_product_day_all` t
WHERE  ! ISNULL(t.shelf_id)
        AND ! ISNULL(t.product_id)
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY (shelf_id)) AS
SELECT
        s.shelf_id, 
        s.region_name, 
        s.business_name
FROM
        fe_dwd.`dwd_shelf_base_day_all` s
WHERE ! ISNULL(s.shelf_id);
DELETE FROM fe_dm.dm_op_flags_area_product WHERE week_end = @week_end OR week_end < SUBDATE(@add_day,INTERVAL 1 YEAR);
INSERT INTO fe_dm.dm_op_flags_area_product (
    week_end, 
    region_name, 
    business_name, 
    second_type_id, 
    product_id, 
    first_fill_flag, 
    sales_flag_s, 
    sales_flag_e, 
    sku_num, 
    sal_qty, 
    gmv, 
    sto_qty_s, 
    sto_val_s, 
    sto_qty_e, 
    sto_val_e, 
    actual_sign_num, 
    actual_sign_val, 
    add_user
  )
SELECT
    @week_end, 
    s.region_name, 
    s.business_name, 
    p.second_type_id, 
    f.product_id, 
    f.first_fill_flag, 
    IFNULL(sales_flag_s, 0), 
    IFNULL(sales_flag_e, 0), 
    COUNT(*), 
    IFNULL(SUM(sal.sal_qty), 0), 
    IFNULL(SUM(sal.gmv), 0), 
    IFNULL(SUM(ssto.sto_qty_s), 0), 
    IFNULL(SUM(ssto.sto_qty_s * f.sale_price), 0), 
    IFNULL(SUM(sto.sto_qty_e), 0), 
    IFNULL(SUM(sto.sto_qty_e * f.sale_price), 0), 
    IFNULL(SUM(fill.actual_sign_num), 0), 
    IFNULL(SUM(fill.actual_sign_num * f.sale_price), 0), 
    @add_user
  FROM
    fe_dwd.detail_tmp f
    JOIN fe_dwd.shelf_tmp s
      ON f.shelf_id = s.shelf_id
    JOIN fe_dwd.`dwd_product_base_day_all` p
      ON f.product_id = p.product_id
    LEFT JOIN fe_dwd.sal_tmp sal
      ON f.shelf_id = sal.shelf_id
      AND f.product_id = sal.product_id
    LEFT JOIN fe_dwd.ssto_tmp ssto
      ON f.shelf_id = ssto.shelf_id
      AND f.product_id = ssto.product_id
    LEFT JOIN fe_dwd.sto_tmp sto
      ON f.shelf_id = sto.shelf_id
      AND f.product_id = sto.product_id
    LEFT JOIN fe_dwd.fill_tmp fill
      ON f.shelf_id = fill.shelf_id
      AND f.product_id = fill.product_id
  GROUP BY s.business_name, f.product_id, first_fill_flag, sales_flag_s, sales_flag_e;
 
DELETE FROM fe_dm.dm_op_flags_res_area WHERE week_end = @week_end  OR week_end < SUBDATE(@add_day,INTERVAL 1 YEAR);
INSERT INTO fe_dm.dm_op_flags_res_area (
week_end, region_name, business_name, first_fill_flag, sales_flag_s, sales_flag_e, sku_num, sal_qty, gmv, sto_qty_s, sto_val_s, sto_qty_e, sto_val_e, actual_sign_num, actual_sign_val, add_user
)
SELECT
@week_end, t.region_name, t.business_name, t.first_fill_flag, t.sales_flag_s, t.sales_flag_e, SUM(t.sku_num), SUM(t.sal_qty), SUM(t.gmv), SUM(t.sto_qty_s), SUM(t.sto_val_s), SUM(t.sto_qty_e), SUM(t.sto_val_e), SUM(t.actual_sign_num), SUM(t.actual_sign_val), @add_user
FROM
fe_dm.dm_op_flags_area_product t
WHERE t.week_end = @week_end
GROUP BY t.business_name, t.first_fill_flag, t.sales_flag_s, t.sales_flag_e;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_flags_res_area_two',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('宋英南@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_flags_area_product','dm_op_flags_res_area_two','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_flags_res_area','dm_op_flags_res_area_two','宋英南');
END