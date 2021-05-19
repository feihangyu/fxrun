CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_warehouse_monitor`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := CURDATE();
SET @ydate := SUBDATE(@stat_date,INTERVAL 1 DAY);
SET @pre_date15 := SUBDATE(@stat_date,INTERVAL 15 DAY);
SET @time_4 := CURRENT_TIMESTAMP(); 
-- 覆盖货架近15天的总销售（不含八折以下的折扣销售）17s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_sale15_discount`; 
CREATE TEMPORARY TABLE fe_dwd.shelf_product_sale15_discount(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        a.shelf_id,
        a.product_id,
        SUM(QUANTITY) AS QUANTITY,
        SUM(IF(a.DISCOUNT_AMOUNT / a.SALE_PRICE <= 0.2,QUANTITY,0)) AS sale_qty15_discount
FROM
        `fe_dwd`.`dwd_pub_order_item_recent_one_month` a
WHERE a.PAY_DATE >= @pre_date15 
        AND a.PAY_DATE < @stat_date
GROUP BY a.shelf_id,a.product_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_warehouse_monitor","@time_4--@time_5",@time_4,@time_5);
SET @time_8 := CURRENT_TIMESTAMP();
--  大仓覆盖货架 2s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`warehouse_shelf`;
CREATE TEMPORARY TABLE fe_dwd.warehouse_shelf(
        KEY idx_WAREHOUSE_NUMBER_shelf_id(WAREHOUSE_NUMBER,shelf_id)
) AS
SELECT 
        t1.business_name AS BUSINESS_AREA,
        t3.WAREHOUSE_NUMBER,
        t3.warehouse_name,
        t1.shelf_id
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` t1
        JOIN fe_dwd.`dwd_pub_warehouse_business_area` t3
                ON t1.business_name = t3.business_area
                AND t3.data_flag = 1
                AND t1.SHELF_STATUS = 2
                AND t1.REVOKE_STATUS = 1
                AND t1.shelf_id NOT IN
                (
                        SELECT 
                                t2.shelf_id
                        FROM
                                `fe_dwd`.`dwd_shelf_base_day_all` t1
                                JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` t2
                                        ON t1.SHELF_ID = t2.prewarehouse_id
                                        AND t1.shelf_type = 9
                                        AND t1.REVOKE_STATUS = 1
                                        AND t1.SHELF_STATUS = 2
                )
;
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_warehouse_monitor","@time_8--@time_9",@time_8,@time_9);
SET @time_11 := CURRENT_TIMESTAMP();
# 大仓覆盖货架商品 8s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.warehouse_shelf_cover_temp;
CREATE TEMPORARY TABLE fe_dwd.warehouse_shelf_cover_temp(
        KEY idx_WAREHOUSE_NUMBER_product_id(WAREHOUSE_NUMBER,product_id)
)  AS
SELECT
        t1.`BUSINESS_AREA`,
        t1.WAREHOUSE_NUMBER,
        t3.product_id,
        e.PRODUCT_CODE2,
        e.PRODUCT_NAME,
        f.PRODUCT_TYPE
FROM
        fe_dwd.warehouse_shelf t1
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` t3
                ON t1.shelf_id = t3.shelf_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` e
                ON t3.`PRODUCT_ID` = e.`PRODUCT_ID`
        JOIN fe_dwd.dwd_pub_product_dim_sserp f
                ON t1.`BUSINESS_AREA` = f.`business_area`
                AND f.`PRODUCT_ID` = t3.`PRODUCT_ID`
GROUP BY t1.WAREHOUSE_NUMBER,t3.product_id
;  
 SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_warehouse_monitor","@time_11--@time_12",@time_11,@time_12);
  SET @time_13 := CURRENT_TIMESTAMP();
-- 货架近15天的总销售 2s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`warehouse_product_sale15`;
CREATE TEMPORARY TABLE fe_dwd.warehouse_product_sale15(
        KEY idx_WAREHOUSE_NUMBER_product_id(WAREHOUSE_NUMBER,product_id)
) AS 
SELECT 
        t1.WAREHOUSE_NUMBER,
        t4.product_id,
        SUM(t4.QUANTITY) AS sale_qty15 ,
        SUM(sale_qty15_discount) AS sale_qty15_discount,
        COUNT(DISTINCT t1.shelf_id) AS shelf_qty
FROM 
        fe_dwd.warehouse_shelf t1
        LEFT JOIN fe_dwd.shelf_product_sale15_discount t4
                ON t1.shelf_id = t4.shelf_id
GROUP BY t1.WAREHOUSE_NUMBER,t4.product_id
;
SET @time_14 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_warehouse_monitor","@time_13--@time_14",@time_13,@time_14);
DELETE FROM fe_dm.dm_op_warehouse_monitor WHERE stat_date <= SUBDATE(@ydate,INTERVAL 15 DAY) OR stat_date = @ydate;
INSERT INTO fe_dm.dm_op_warehouse_monitor
(
        stat_date,
        BUSINESS_AREA,
        warehouse_code,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        PRODUCT_TYPE,
        shelf_qty,
        sale_qty15,
        sale_qty15_discount
)
SELECT 
        @ydate AS stat_date,
        a.`BUSINESS_AREA`,
        a.WAREHOUSE_NUMBER AS warehouse_code,
        a.product_id,
        a.PRODUCT_CODE2 AS PRODUCT_FE,
        a.PRODUCT_NAME,
        a.PRODUCT_TYPE,
        k.shelf_qty,
        k.sale_qty15,
        k.sale_qty15_discount
FROM 
        fe_dwd.warehouse_shelf_cover_temp a  
        LEFT JOIN fe_dwd.`warehouse_product_sale15` k
                ON  k.WAREHOUSE_NUMBER = a.WAREHOUSE_NUMBER
                AND k.product_id = a.product_id
;
SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_warehouse_monitor","@time_14--@time_15",@time_14,@time_15);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_warehouse_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_warehouse_monitor','dm_op_warehouse_monitor','宋英南');
END