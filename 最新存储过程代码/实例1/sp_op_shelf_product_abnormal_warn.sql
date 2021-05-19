CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_product_abnormal_warn`()
    SQL SECURITY INVOKER
BEGIN
    DECLARE l_test VARCHAR(1);
        DECLARE l_row_cnt INT;
        DECLARE CODE CHAR(5) DEFAULT '00000';
        DECLARE done INT;
        DECLARE l_table_owner   VARCHAR(64);
        DECLARE l_city          VARCHAR(64);
        DECLARE l_task_name     VARCHAR(64);
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
                DECLARE EXIT HANDLER FOR SQLEXCEPTION
                BEGIN
                        GET DIAGNOSTICS CONDITION 1
                        CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
                        CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
                END; 
        SET l_task_name = 'sp_op_shelf_product_abnormal_warn';
SET @sdate := SUBDATE(CURDATE(),INTERVAL 1 DAY);
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 1-当日出单量大于前2周日出单量3倍 
TRUNCATE feods.d_op_fill_order_large_than_3times;
INSERT INTO feods.d_op_fill_order_large_than_3times
        (  
        REGION_AREA,
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_NAME,
        SHELF_TYPE,
        MANAGER_ID,
        MANAGER_NAME,
        DEPT_ID,
        DEPT_NAME,
        warehouse_id,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        CATEGORY_NAME,
        FILL_MODEL,
        APPLY_TIME,
        FILL_AUDIT_TIME,
        FILL_TYPE,
        ORDER_STATUS,
        SUPPLIER_TYPE,
        ACTUAL_APPLY_NUM,
        ACTUAL_APPLY_VALUE,
        2WEEK_APPLY_NUM_DAY,
        STOCK_NUM,
        STOCK_VALUE,
        pre_stock_turnover,
        post_stock_turnover
        )    
SELECT 
        c.region_name AS REGION_AREA,
        c.business_name AS BUSINESS_AREA,
        a.`SHELF_ID`,
        c.SHELF_NAME,
        c.SHELF_TYPE,
        c.MANAGER_ID,
        c.MANAGER_NAME,
        c.branch_code AS DEPT_ID,
        c.branch_name AS DEPT_NAME,
        e.prewarehouse_id AS warehouse_id,
        g.product_id,
        g.PRODUCT_CODE2 AS PRODUCT_FE,
        g.PRODUCT_NAME,
        g.CATEGORY_NAME,
        g.FILL_MODEL,
        a.APPLY_TIME,
        a.FILL_AUDIT_TIME,
        a.FILL_TYPE,
        a.ORDER_STATUS,
        a.SUPPLIER_TYPE,
        a.ACTUAL_APPLY_NUM,
        a.ACTUAL_APPLY_NUM * f.SALE_PRICE AS ACTUAL_APPLY_VALUE,
        h.ACTUAL_APPLY_NUM / h.order_qty AS 2WEEK_APPLY_NUM_DAY,
        a.STOCK_NUM,
        a.STOCK_NUM * f.SALE_PRICE AS STOCK_VALUE,
        a.STOCK_NUM / (a.WEEK_SALE_NUM/7) AS pre_stock_turnover,
        (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / (a.WEEK_SALE_NUM/7) AS post_stock_turnover
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON a.`SHELF_ID` = c.SHELF_ID
                AND a.FILL_TYPE = 2
                AND a.ORDER_STATUS IN (1,2)
                AND a.apply_time < CURDATE()
                AND a.apply_time >= @sdate
                AND c.SHELF_STATUS = 2
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` e
                ON a.shelf_id = e.shelf_id
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` f
                ON a.`SHELF_ID` = f.SHELF_ID
                AND a.`PRODUCT_ID` = f.PRODUCT_ID
        JOIN `fe_dwd`.`dwd_product_base_day_all` g
                ON a.`PRODUCT_ID` = g.`PRODUCT_ID`
        JOIN 
                (
                        SELECT
                                a.SHELF_ID,
                                a.PRODUCT_ID,
                                SUM(a.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
                                COUNT(*) AS order_qty
                        FROM 
                                `fe_dwd`.`dwd_fill_day_inc` a
                        WHERE a.FILL_TYPE = 2
                                AND a.apply_time > SUBDATE(CURDATE(),INTERVAL 2 WEEK)
                        GROUP BY shelf_id,product_id
                ) h
                ON a.`SHELF_ID` = h.SHELF_ID
                AND a.`PRODUCT_ID` = h.PRODUCT_ID
WHERE (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / (a.WEEK_SALE_NUM/7) > 45 AND a.STOCK_NUM > 2 AND a.ACTUAL_APPLY_NUM > 2
;
-- 2-货架在途准确性抽查（丙丁有在途未验收5天，甲乙新3天）
DROP TEMPORARY TABLE IF EXISTS feods.`sale_tmp2`;
CREATE TEMPORARY TABLE feods.sale_tmp2 (
        KEY idx_shelf_id_product_id(shelf_id,product_id),
        KEY idx_product_id(product_id)
        ) AS
SELECT 
        c.region_name AS REGION_AREA,
        c.business_name AS BUSINESS_AREA,
        a.`SHELF_ID`,
        c.SHELF_NAME,
        c.SHELF_TYPE,
        c.MANAGER_ID,
        c.MANAGER_NAME,
        c.branch_code AS DEPT_ID,
        c.branch_name AS DEPT_NAME,
        e.prewarehouse_id AS warehouse_id,
        a.product_id,
        a.APPLY_TIME,
        a.FILL_AUDIT_TIME,
        a.FILL_TYPE,
        a.ORDER_STATUS,
        a.SUPPLIER_TYPE,
        c.shelf_level,
        a.ACTUAL_APPLY_NUM,
        a.ACTUAL_APPLY_NUM * f.SALE_PRICE AS ACTUAL_APPLY_VALUE,
        a.STOCK_NUM,
        a.STOCK_NUM * f.SALE_PRICE AS STOCK_VALUE
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON a.`SHELF_ID` = c.SHELF_ID
                AND a.FILL_TYPE IN (1,2,3,7,8,9)
                AND a.ORDER_STATUS IN (1,2)
                AND a.apply_time >= SUBDATE(@sdate,INTERVAL 1 MONTH)
                AND c.shelf_level IN (1,2,3,4,5)
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` e
                ON a.shelf_id = e.shelf_id
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` f
                ON a.`SHELF_ID` = f.SHELF_ID
                AND a.`PRODUCT_ID` = f.PRODUCT_ID
;
TRUNCATE feods.d_op_shelf_product_onload_check;
INSERT INTO feods.d_op_shelf_product_onload_check
        (  
        REGION_AREA,
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_NAME,
        SHELF_TYPE,
        MANAGER_ID,
        MANAGER_NAME,
        DEPT_ID,
        DEPT_NAME,
        warehouse_id,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        CATEGORY_NAME,
        APPLY_TIME,
        FILL_AUDIT_TIME,
        FILL_TYPE,
        ORDER_STATUS,
        SUPPLIER_TYPE,
        ACTUAL_APPLY_NUM,
        ACTUAL_APPLY_VALUE,
        STOCK_NUM,
        STOCK_VALUE
        )    
SELECT 
        a.REGION_AREA,
        a.BUSINESS_AREA,
        a.`SHELF_ID`,
        a.SHELF_NAME,
        a.SHELF_TYPE,
        a.MANAGER_ID,
        a.MANAGER_NAME,
        a.DEPT_ID,
        a.DEPT_NAME,
        a.warehouse_id,
        a.product_id,
        g.PRODUCT_CODE2 AS PRODUCT_FE,
        g.PRODUCT_NAME,
        g.CATEGORY_NAME,
        a.APPLY_TIME,
        a.FILL_AUDIT_TIME,
        a.FILL_TYPE,
        a.ORDER_STATUS,
        a.SUPPLIER_TYPE,
        a.ACTUAL_APPLY_NUM,
        a.ACTUAL_APPLY_VALUE,
        a.STOCK_NUM,
        a.STOCK_VALUE
FROM feods.`sale_tmp2` a
        JOIN `fe_dwd`.`dwd_product_base_day_all` g
                ON a.`PRODUCT_ID` = g.`PRODUCT_ID`
WHERE (a.shelf_level IN (4,5) AND (DATEDIFF(@sdate , DATE(a.apply_time)) > 5)) 
        OR (a.shelf_level IN (1,2,3) AND (DATEDIFF(@sdate , DATE(a.apply_time)) > 3)) 
;
-- 3-单品出单量大于MAX(45天总销量，标配)
DROP TEMPORARY TABLE IF EXISTS feods.`sale_tmp`;
CREATE TEMPORARY TABLE feods.sale_tmp (
        PRIMARY KEY (shelf_id,product_id)
        ) AS
SELECT 
        SHELF_ID,
        PRODUCT_ID,
        SUM(QUANTITY) AS sale_qty_45
FROM `fe_dwd`. `dwd_order_item_refund_day` 
WHERE PAY_DATE >= SUBDATE(@sdate,INTERVAL 45 DAY)
GROUP BY SHELF_ID,PRODUCT_ID
;
TRUNCATE feods.d_op_shelf_product_fill_45;
INSERT INTO feods.d_op_shelf_product_fill_45
        (  
        REGION_AREA,
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_NAME,
        SHELF_TYPE,
        MANAGER_ID,
        MANAGER_NAME,
        DEPT_ID,
        DEPT_NAME,
        warehouse_id,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        CATEGORY_NAME,
        APPLY_TIME,
        FILL_AUDIT_TIME,
        FILL_TYPE,
        ORDER_STATUS,
        SUPPLIER_TYPE,
        ACTUAL_APPLY_NUM,
        ACTUAL_APPLY_VALUE,
        sale_qty_45,
        ALARM_QUANTITY,
        STOCK_NUM,
        STOCK_VALUE,
        pre_stock_turnover,
        post_stock_turnover
        )
SELECT 
        c.region_name AS REGION_AREA,
        c.business_name AS BUSINESS_AREA,
        a.`SHELF_ID`,
        c.SHELF_NAME,
        c.SHELF_TYPE,
        c.MANAGER_ID,
        c.MANAGER_NAME,
        c.branch_code AS DEPT_ID,
        c.branch_name AS DEPT_NAME,
        e.prewarehouse_id AS warehouse_id,
        g.product_id,
        g.PRODUCT_CODE2 AS PRODUCT_FE,
        g.PRODUCT_NAME,
        g.CATEGORY_NAME,
        a.APPLY_TIME,
        a.FILL_AUDIT_TIME,
        a.FILL_TYPE,
        a.ORDER_STATUS,
        a.SUPPLIER_TYPE,
        a.ACTUAL_APPLY_NUM,
        a.ACTUAL_APPLY_NUM * f.SALE_PRICE AS ACTUAL_APPLY_VALUE,
        h.sale_qty_45,
        i.ALARM_QUANTITY,
        a.STOCK_NUM,
        a.STOCK_NUM * f.SALE_PRICE AS STOCK_VALUE,
        a.STOCK_NUM / (a.WEEK_SALE_NUM/7) AS pre_stock_turnover,
        (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / (a.WEEK_SALE_NUM/7) AS post_stock_turnover
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON a.`SHELF_ID` = c.SHELF_ID
                AND a.FILL_TYPE IN (1,2,3,7,8,9)
                AND a.ORDER_STATUS IN (1,2)
                AND a.apply_time >= @sdate
                AND c.SHELF_STATUS = 2
                AND c.REVOKE_STATUS = 1
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` e
                ON a.shelf_id = e.shelf_id
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` f
                ON a.`SHELF_ID` = f.SHELF_ID
                AND a.`PRODUCT_ID` = f.PRODUCT_ID
        JOIN `fe_dwd`.`dwd_product_base_day_all` g
                ON a.`PRODUCT_ID` = g.`PRODUCT_ID`
        LEFT JOIN feods.sale_tmp h
                ON a.`SHELF_ID` = h.SHELF_ID
                AND a.`PRODUCT_ID` = h.PRODUCT_ID
        JOIN fe.`sf_package_item` i
                ON i.ITEM_ID= f.ITEM_ID
WHERE a.ACTUAL_APPLY_NUM > h.sale_qty_45
        AND a.ACTUAL_APPLY_NUM > i.ALARM_QUANTITY
        AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / (a.WEEK_SALE_NUM/7)  >30
;
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_shelf_product_abnormal_warn',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
    END