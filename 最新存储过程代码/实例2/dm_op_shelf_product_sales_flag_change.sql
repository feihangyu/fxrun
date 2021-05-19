CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_product_sales_flag_change`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @pre_2day := SUBDATE(CURDATE(),2);
-- 期初
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`start_shelf_product_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.start_shelf_product_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        shelf_id,
        product_id,
        sales_flag,
        stock_quantity,
        stock_quantity * sale_price AS stock_value
FROM
        fe_dwd.`dwd_shelf_product_day_all_recent_32`
WHERE sdate = @pre_2day 
        AND shelf_fill_flag = 1
        AND sales_flag IN (4,5)
;
DELETE FROM fe_dm.dm_op_shelf_product_sales_flag_change WHERE stat_date = @stat_date OR stat_date < SUBDATE(@stat_date,INTERVAL 1 YEAR);
INSERT INTO fe_dm.dm_op_shelf_product_sales_flag_change
(
        stat_date,
        business_name,
        shelf_id,
        shelf_name,
        SHELF_CODE,
        shelf_type,
        SHELF_STATUS,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        product_id,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        start_sales_flag,
        start_stock_quantity,
        start_stock_value,
        end_sales_flag,
        end_stock_quantity,
        end_stock_value
)
SELECT
        @stat_date AS stat_date,
        c.`business_name`,
        a.shelf_id,
        c.`shelf_name`,
        c.`SHELF_CODE`,
        c.`shelf_type`,
        c.`SHELF_STATUS`,
        c.`REVOKE_STATUS`,
        c.`WHETHER_CLOSE`,
        a.product_id,
        d.`PRODUCT_CODE2`,
        d.`PRODUCT_NAME`,
        a.sales_flag AS start_sales_flag,
        a.stock_quantity AS start_stock_quantity,
        a.stock_value AS start_stock_value,
        b.`SALES_FLAG` AS end_sales_flag,
        b.`STOCK_QUANTITY` AS end_stock_quantity,
        b.`STOCK_QUANTITY` * b.sale_price AS end_stock_value
FROM
        fe_dwd.start_shelf_product_tmp a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b
                ON a.shelf_id = b.`SHELF_ID`
                AND a.product_id = b.`PRODUCT_ID`
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.shelf_id = c.`shelf_id`
        JOIN fe_dwd.`dwd_product_base_day_all` d
                ON a.product_id = d.`PRODUCT_ID`
WHERE b.sales_flag IN (1,2,3) AND b.`STOCK_QUANTITY`  <= 0
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_product_sales_flag_change',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_product_sales_flag_change','dm_op_shelf_product_sales_flag_change','宋英南');
END