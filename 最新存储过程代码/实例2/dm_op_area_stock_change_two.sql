CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_stock_change_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate:=CURRENT_DATE;
SET @sub_day := SUBDATE(@sdate,1);
SET @pre_day2 := SUBDATE(@sdate,2);
-- 前天的数据 27s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.pre_day2_shelf_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.pre_day2_shelf_product_tmp
(KEY idx_shelf_id_product_id(shelf_id,product_id)) 
SELECT
        shelf_id,
        product_id,
        danger_flag,
        stock_quantity,
        sale_price
FROM
        fe_dwd.`dwd_shelf_product_day_all_recent_32`
WHERE sdate = @pre_day2 AND stock_quantity>0 
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.detail_tmp;
CREATE TEMPORARY TABLE fe_dwd.detail_tmp 
SELECT 
    @sub_day sdate, 
    s.business_name, 
    CASE
        WHEN shelf_type = 1 THEN '四层标准货架'
        WHEN shelf_type = 2 THEN '冰箱'
        WHEN shelf_type = 3 THEN '五层防鼠货架'
        WHEN shelf_type = 4 THEN '虚拟货架'
        WHEN shelf_type = 5 THEN '冰柜'
        WHEN shelf_type = 6 THEN '智能货架'
        WHEN shelf_type = 7 THEN '自动贩卖机'
        WHEN shelf_type = 8 THEN '校园货架'
        WHEN shelf_type = 9 THEN '前置仓'
    END AS shelf_type, 
    t.danger_flag, 
    ts.danger_flag danger_flag1, 
    COUNT(*)sku, 
    IFNULL(SUM(t.stock_quantity>IFNULL(ts.stock_quantity,0)),0)sku_add, 
    IFNULL(SUM(t.stock_quantity<ts.stock_quantity),0)sku_sub, 
    IFNULL(SUM(t.stock_quantity),0)stock_quantity, 
    IFNULL(SUM(ts.stock_quantity),0)stock_quantity1, 
    IFNULL(SUM(IF(t.stock_quantity>IFNULL(ts.stock_quantity,0),t.stock_quantity-IFNULL(ts.stock_quantity,0),0)),0)stock_quantity_add, 
    IFNULL(SUM(IF(t.stock_quantity<ts.stock_quantity,ts.stock_quantity-t.stock_quantity,0)),0)stock_quantity_sub, 
    IFNULL(SUM(t.stock_quantity*t.sale_price),0)stock_val, 
    IFNULL(SUM(ts.stock_quantity*ts.sale_price),0)stock_val1, 
    IFNULL(SUM(IF(t.stock_quantity*t.sale_price>IFNULL(ts.stock_quantity*ts.sale_price,0),t.stock_quantity*t.sale_price-IFNULL(ts.stock_quantity*ts.sale_price,0),0)),0)stock_val_add, 
    IFNULL(SUM(IF(t.stock_quantity*t.sale_price<ts.stock_quantity*ts.sale_price,ts.stock_quantity*ts.sale_price-t.stock_quantity*t.sale_price,0)),0)stock_val_sub 
FROM 
    fe_dwd.`dwd_shelf_product_day_all` t 
    JOIN fe_dwd.pre_day2_shelf_product_tmp ts 
        ON t.shelf_id=ts.shelf_id 
        AND t.product_id = ts.product_id
    JOIN fe_dwd.`dwd_shelf_base_day_all` s 
        ON t.shelf_id=s.shelf_id 
WHERE t.stock_quantity>0 
GROUP BY s.business_name, s.shelf_type, t.danger_flag, ts.danger_flag;
 
DELETE FROM fe_dm.dm_op_area_stock_change_detail WHERE sdate = @sub_day OR sdate < SUBDATE(@sdate,30);
INSERT INTO fe_dm.dm_op_area_stock_change_detail
(
    sdate, 
    business_name, 
    shelf_type, 
    danger_flag, 
    danger_flag1, 
    sku, 
    sku_add, 
    sku_sub, 
    stock_quantity, 
    stock_quantity1, 
    stock_quantity_add, 
    stock_quantity_sub, 
    stock_val, 
    stock_val1, 
    stock_val_add, 
    stock_val_sub 
)
SELECT * FROM fe_dwd.detail_tmp
;
DELETE FROM fe_dm.dm_op_area_stock_change WHERE sdate = @sub_day OR sdate < SUBDATE(@sdate,30);
INSERT INTO fe_dm.dm_op_area_stock_change
(
    sdate,
    business_name,
    stock_val,
    stock_val1,
    sub_part,
    add_part
)
SELECT 
    sdate,
    business_name,
    SUM(IF(danger_flag IN(4,5),stock_val,0))stock_val,
    SUM(IF(danger_flag1 IN(4,5),stock_val1,0))stock_val1,
    SUM(IF(danger_flag1 IN(4,5),IF(danger_flag IN(4,5),stock_val_sub,stock_val1),0))sub_part,
    SUM(IF(danger_flag IN(4,5),IF(danger_flag1 IN(4,5),stock_val_add,stock_val),0))add_part
FROM fe_dwd.detail_tmp 
GROUP BY sdate,business_name;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_stock_change_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_stock_change_detail','dm_op_area_stock_change_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_stock_change','dm_op_area_stock_change_two','宋英南');
COMMIT;
	END