CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_cancel_fill_order`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/04/08
-- Modify date: 
-- Description:	
-- 	补货订单取消原因明细表（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @cur_date := CURDATE();
SET @pre_2day := SUBDATE(CURDATE(),2);
-- 当天取消订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        DISTINCT
        a.shelf_id,
        a.order_id,
        a.FILL_TYPE,
        a.order_status,
        IFNULL(c.ITEM_NAME,'其他') AS cancel_result,
        b.remark,
        a.last_update_time AS cancel_time
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` a
        LEFT JOIN fe_dwd.`dwd_sf_product_fill_order_extend` b
                ON a.order_id = b.order_id
                AND a.apply_time >= @stat_date
                AND a.apply_time < @cur_date
                AND a.FILL_TYPE IN (1,2)
                AND a.order_status = 9
        JOIN fe_dwd.`dwd_pub_dictionary` c
                ON b.`cancel_result` = c.ITEM_VALUE 
                AND c.DICTIONARY_ID = 416 
;
DELETE FROM fe_dm.dm_op_cancel_fill_order WHERE stat_date = @stat_date OR stat_date < SUBDATE(@cur_date,INTERVAL 2 YEAR); 
INSERT INTO fe_dm.dm_op_cancel_fill_order
(
        stat_date,
        region_name,
        business_name,
        shelf_id,
        order_id,
        FILL_TYPE,
        order_status,
        MANAGER_ID,
        REAL_NAME,
        manager_type,
        cancel_result,
        remark,
        cancel_time
)
SELECT 
        @stat_date AS stat_date,
        a.region_name,
        a.`business_name`,
        a.`shelf_id`,
        b.order_id,
        b.FILL_TYPE,
        b.order_status,
        a.MANAGER_ID,
        a.REAL_NAME,
        a.manager_type,
        b.cancel_result,
        b.remark,
        b.cancel_time
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.fill_tmp b
                ON a.`shelf_id` = b.`SHELF_ID`
                AND a.SHELF_STATUS = 2
;
-- ======================================================================================
-- 第二天取消订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_day2_fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.pre_day2_fill_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        DISTINCT
        a.shelf_id,
        a.order_id,
        a.FILL_TYPE,
        a.order_status,
        IFNULL(c.ITEM_NAME,'其他') AS cancel_result,
        b.remark,
        a.last_update_time AS cancel_time
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` a
        LEFT JOIN fe_dwd.`dwd_sf_product_fill_order_extend` b
                ON a.order_id = b.order_id
                AND a.apply_time >= @pre_2day
                AND a.apply_time < @stat_date
                AND a.FILL_TYPE IN (1,2)
                AND a.order_status = 9
        JOIN fe_dwd.`dwd_pub_dictionary` c
                ON b.`cancel_result` = c.ITEM_VALUE 
                AND c.DICTIONARY_ID = 416 
;
-- 新增的取消订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_day2_new_fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.pre_day2_new_fill_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.*
FROM
        fe_dwd.pre_day2_fill_tmp a
        LEFT JOIN fe_dm.dm_op_cancel_fill_order b
                ON a.order_id = b.order_id
                AND b.stat_date = @pre_2day
WHERE b.order_id IS NULL
;
INSERT INTO fe_dm.dm_op_cancel_fill_order
(
        stat_date,
        region_name,
        business_name,
        shelf_id,
        order_id,
        FILL_TYPE,
        order_status,
        MANAGER_ID,
        REAL_NAME,
        manager_type,
        cancel_result,
        remark,
        cancel_time
)
SELECT 
        @pre_2day AS stat_date,
        a.region_name,
        a.`business_name`,
        a.`shelf_id`,
        b.order_id,
        b.FILL_TYPE,
        b.order_status,
        a.MANAGER_ID,
        a.REAL_NAME,
        a.manager_type,
        b.cancel_result,
        b.remark,
        b.cancel_time
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.pre_day2_new_fill_tmp b
                ON a.`shelf_id` = b.`SHELF_ID`
                AND a.SHELF_STATUS = 2
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_cancel_fill_order',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_cancel_fill_order','dm_op_cancel_fill_order','宋英南');
END