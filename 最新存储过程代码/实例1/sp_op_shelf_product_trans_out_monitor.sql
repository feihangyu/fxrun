CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_product_trans_out_monitor`()
BEGIN
        SET @run_date := CURRENT_DATE();
        SET @user := CURRENT_USER();
        SET @timestamp := CURRENT_TIMESTAMP();
        SET @cur_date := CURDATE();
        SET @stat_date := SUBDATE(CURDATE(),1);
        SET @pre_day2 := SUBDATE(CURDATE(),2);
DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
CREATE TEMPORARY TABLE feods.fill_tmp(      
        KEY idx_shelf_id_product_id(shelf_id,product_id)
 ) AS 
SELECT
        a.shelf_id,
        a.product_id,
        a.order_status,
        a.FILL_TIME,
        a.ACTUAL_FILL_NUM,
        CASE
                WHEN a.order_status = 4 THEN 1
                WHEN a.order_status = 3 THEN 2
                WHEN a.order_status = 2 THEN 3
                WHEN a.order_status = 1 THEN 4
                WHEN a.order_status = 9 THEN 5
        END AS order_status_flag,
        a.CANCEL_REMARK
FROM
        fe_dwd.`dwd_fill_day_inc` a
WHERE a.apply_time >= @stat_date
        AND a.apply_time < @cur_date 
        AND a.FILL_TYPE = 11
        AND a.ADD_USER_ID = 0
;
-- 自贩机基础信息
DROP TEMPORARY TABLE IF EXISTS feods.machine_tmp;
CREATE TEMPORARY TABLE feods.machine_tmp(      
        KEY idx_shelf_id_product_id(shelf_id,product_id)
 ) AS 
 SELECT
        a.shelf_id,
        a.product_id,
        c.prewarehouse_id AS warehouse_id,
        d.PRODUCT_CODE2,
        d.PRODUCT_NAME,
        a.SALES_FLAG,
        a.SALE_PRICE,
        d.FILL_MODEL
 FROM
        fe_dwd.`dwd_shelf_product_day_all` a
        JOIN fe_dwd.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
                AND b.shelf_type = 7
        LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` c
                ON a.shelf_id = c.shelf_id
        JOIN fe_dwd.`dwd_product_base_day_all` d
                ON a.product_id = d.product_id
;
        
-- 货架商品维度
DELETE FROM fe_dm.dm_op_shelf_product_trans_out_monitor WHERE stat_date = @stat_date OR stat_date < SUBDATE(@stat_date,INTERVAL 2 MONTH);
INSERT INTO fe_dm.dm_op_shelf_product_trans_out_monitor
(
        stat_date,
        business_name,
        shelf_id,
        shelf_code,
        shelf_type,
        shelf_name,
        warehouse_id,
        SF_CODE,
        REAL_NAME,
        manager_type,
        order_status,
        FILL_TIME,
        product_id,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        SALES_FLAG,
        SALE_PRICE,
        FILL_MODEL,
        yday_Q,
        tday_Q,
        remain_qty,
        remain_value,
        ACTUAL_FILL_NUM,
        trans_out_value,
        CANCEL_REMARK
)
SELECT
        @stat_date AS stat_date,
        c.business_name,
        a.shelf_id,
        c.shelf_code,
        c.shelf_type,
        c.shelf_name,
        IFNULL(e.warehouse_id,f.warehouse_id) AS warehouse_id,
        c.SF_CODE,
        c.REAL_NAME,
        c.manager_type,
        SUBSTRING_INDEX(GROUP_CONCAT(a.order_status ORDER BY a.order_status_flag),',',1) AS order_status,
        SUBSTRING_INDEX(GROUP_CONCAT(a.FILL_TIME ORDER BY a.order_status_flag),',',1) AS FILL_TIME,
        a.product_id,
        IFNULL(e.PRODUCT_CODE2,f.PRODUCT_CODE2) AS PRODUCT_CODE2,
        IFNULL(e.PRODUCT_NAME,f.PRODUCT_NAME) AS PRODUCT_NAME,
        IFNULL(e.SALES_FLAG,f.SALES_FLAG) AS SALES_FLAG,
        IFNULL(e.SALE_PRICE,f.SALE_PRICE) AS SALE_PRICE,
        IFNULL(e.FILL_MODEL,f.FILL_MODEL) AS FILL_MODEL,
        e.yday_Q,
        e.tday_Q,
        e.remain_qty,
        e.remain_qty * IFNULL(e.SALE_PRICE,f.SALE_PRICE) AS remain_value,
        SUBSTRING_INDEX(GROUP_CONCAT(a.ACTUAL_FILL_NUM ORDER BY a.order_status_flag),',',1) AS ACTUAL_FILL_NUM,
        SUBSTRING_INDEX(GROUP_CONCAT(a.ACTUAL_FILL_NUM ORDER BY a.order_status_flag),',',1) * IFNULL(e.SALE_PRICE,f.SALE_PRICE) AS trans_out_value,
        CANCEL_REMARK
FROM
        feods.fill_tmp a
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.shelf_id = c.shelf_id
        LEFT JOIN fe_dm.`dm_op_shelf_product_trans_out_his` e
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id
                AND e.cdate = @stat_date
        LEFT JOIN feods.machine_tmp f
                ON a.shelf_id = f.shelf_id
                AND a.product_id = f.product_id           
GROUP BY a.shelf_id,a.product_id
;
-- 更新前天的数据
DROP TEMPORARY TABLE IF EXISTS feods.fill_pre_day2;
CREATE TEMPORARY TABLE feods.fill_pre_day2(      
        KEY idx_shelf_id_product_id(shelf_id,product_id)
 ) AS 
SELECT
        a.shelf_id,
        a.product_id,
        a.order_status,
        a.FILL_TIME,
        a.ACTUAL_FILL_NUM,
        CASE
                WHEN a.order_status = 4 THEN 1
                WHEN a.order_status = 3 THEN 2
                WHEN a.order_status = 2 THEN 3
                WHEN a.order_status = 1 THEN 4
                WHEN a.order_status = 9 THEN 5
        END AS order_status_flag,
        a.CANCEL_REMARK
FROM
        fe_dwd.`dwd_fill_day_inc` a
WHERE a.apply_time >= @pre_day2
        AND a.apply_time < @stat_date 
        AND a.FILL_TYPE = 11
        AND a.ADD_USER_ID = 0
;
DROP TEMPORARY TABLE IF EXISTS feods.shelf_product_pre_day2;
CREATE TEMPORARY TABLE feods.shelf_product_pre_day2(      
        KEY idx_shelf_id_product_id(shelf_id,product_id)
 ) AS 
SELECT
        a.shelf_id,
        a.product_id,
        SUBSTRING_INDEX(GROUP_CONCAT(a.order_status ORDER BY a.order_status_flag),',',1) AS order_status,
        SUBSTRING_INDEX(GROUP_CONCAT(a.FILL_TIME ORDER BY a.order_status_flag),',',1) AS FILL_TIME,
        SUBSTRING_INDEX(GROUP_CONCAT(a.ACTUAL_FILL_NUM ORDER BY a.order_status_flag),',',1) AS ACTUAL_FILL_NUM,
        SUBSTRING_INDEX(GROUP_CONCAT(a.CANCEL_REMARK ORDER BY a.order_status_flag),',',1) AS CANCEL_REMARK
FROM
        feods.fill_pre_day2 a
GROUP BY a.shelf_id,a.product_id
;
UPDATE
        fe_dm.dm_op_shelf_product_trans_out_monitor a
        JOIN feods.shelf_product_pre_day2 b
                ON a.stat_date = @pre_day2
                AND a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
SET a.order_status = b.order_status,
        a.FILL_TIME = b.FILL_TIME,
        a.ACTUAL_FILL_NUM = b.ACTUAL_FILL_NUM,
        a.trans_out_value = b.ACTUAL_FILL_NUM * a.SALE_PRICE,
        a.CANCEL_REMARK = b.CANCEL_REMARK
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_op_shelf_product_trans_out_monitor',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('宋英南@', @user, @timestamp)
  );
COMMIT;
END