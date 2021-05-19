CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_fill_type_monitor`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/05/15
-- Modify date: 
-- Description:	
-- 	补货类型分布监控明细表（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @cur_date := CURDATE();
SET @pre_1year := SUBDATE(@stat_date,INTERVAL 1 YEAR);
SET @cur_month01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
-- 补货类型分布监控
DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.fill_tmp(      
        KEY idx_shelf_id_fill_type(shelf_id,fill_type)
 ) AS 
SELECT
        DATE(FILL_TIME) AS stat_date,
        fill_type,
        shelf_id,
        PRODUCT_NUM,
        TOTAL_PRICE,
        SUPPLIER_TYPE
FROM
        fe_dwd.`dwd_fill_day_inc`
WHERE FILL_TIME >= @stat_date
        AND FILL_TIME < @cur_date
        AND fill_type IN (1,2,3,4,7,8,9)
        AND `SUPPLIER_TYPE` IN (1,2,9)
        AND ORDER_STATUS IN (3,4)
GROUP BY stat_date,fill_type,shelf_id
;
DELETE FROM fe_dm.dm_op_fill_type_monitor WHERE stat_date = @stat_date OR stat_date < @pre_1year;
INSERT INTO fe_dm.dm_op_fill_type_monitor
(
        stat_date,
        business_name,
        shelf_id,
        SHELF_TYPE,
        fill_type,
        PRODUCT_NUM,
        TOTAL_PRICE,
        is_prewarehouse_cover,
        manager_type,
        SHELF_STATUS,
        WHETHER_CLOSE,
        REVOKE_STATUS,
        grade,
        SUPPLIER_TYPE,
        prewarehouse_code,
        prewarehouse_name
)
SELECT 
        a.stat_date,
        b.business_name,
        a.shelf_id,
        b.SHELF_TYPE,
        a.fill_type,
        a.PRODUCT_NUM,
        a.TOTAL_PRICE,
        b.is_prewarehouse_cover,
        b.manager_type,
        b.SHELF_STATUS,
        b.WHETHER_CLOSE,
        b.REVOKE_STATUS,
        b.`grade`,
        a.SUPPLIER_TYPE,
        c.`prewarehouse_code`,
        c.`prewarehouse_name`
FROM
        fe_dwd.fill_tmp a
        JOIN fe_dwd.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.`shelf_id`
        LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` c
                ON a.shelf_id = c.`shelf_id`
WHERE b.SHELF_TYPE IN (1,2,3,5,6,7)
;
DELETE FROM fe_dm.dm_op_fill_type_monitor_total WHERE month_id = @month_id OR month_id < @pre_1year;
INSERT INTO fe_dm.dm_op_fill_type_monitor_total
(
        month_id,
        business_name,
        SHELF_TYPE,
        fill_type,
        manager_type,
        order_qty
)
SELECT
        DATE_FORMAT(stat_date,'%Y-%m') AS month_id,
        business_name,
        SHELF_TYPE,
        fill_type,
        manager_type,
        COUNT(*) AS order_qty
FROM
        fe_dm.dm_op_fill_type_monitor
WHERE stat_date >= @cur_month01
GROUP BY month_id,
        business_name,
        SHELF_TYPE,
        fill_type,
        manager_type
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_fill_type_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_type_monitor','dm_op_fill_type_monitor','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_type_monitor_total','dm_op_fill_type_monitor','宋英南');
END