CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_manual_fill_stat`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/04/17
-- Modify date: 
-- Description:	
-- 	补货人工申请订单异常统计表（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
TRUNCATE TABLE  fe_dm.dm_op_manual_fill_stat;
INSERT INTO fe_dm.dm_op_manual_fill_stat
(
        stat_date,
        business_name,
        order_qty,
        surplus_order_qty,
        abnormal_order_qty,
        surplus_order_ratio,
        abnormal_order_ratio
)
SELECT
        DATE(apply_time) AS stat_date,
        business_name,
        COUNT(DISTINCT order_id) AS order_qty,
        COUNT(DISTINCT IF(is_surplus = 1,order_id,NULL)) AS surplus_order_qty,
        COUNT(DISTINCT IF(error_type IS NOT NULL,order_id,NULL)) AS abnormal_order_qty,
        ROUND(COUNT(DISTINCT IF(is_surplus = 1,order_id,NULL)) / COUNT(DISTINCT order_id),2) AS surplus_order_ratio,
        ROUND(COUNT(DISTINCT IF(error_type IS NOT NULL,order_id,NULL)) / COUNT(DISTINCT order_id),2) AS abnormal_order_ratio
FROM
        fe_dm.`dm_op_manual_fill_monitor` 
GROUP BY DATE(apply_time),business_name
;
TRUNCATE TABLE  fe_dm.dm_op_manual_fill_stat_total;
INSERT INTO fe_dm.dm_op_manual_fill_stat_total
(
        month_id,
        business_name,
        order_qty,
        surplus_order_qty,
        abnormal_order_qty,
        surplus_order_ratio,
        abnormal_order_ratio
)
SELECT
        DATE_FORMAT(stat_date,'%Y-%m') AS month_id,
        business_name,
        SUM(order_qty) AS order_qty,
        SUM(surplus_order_qty) AS surplus_order_qty,
        SUM(abnormal_order_qty) AS abnormal_order_qty,
        SUM(surplus_order_qty) / SUM(order_qty) AS surplus_order_ratio,
        SUM(abnormal_order_qty) / SUM(order_qty) AS abnormal_order_ratio
FROM
        fe_dm.dm_op_manual_fill_stat
GROUP BY DATE_FORMAT(stat_date,'%Y-%m'),business_name
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_manual_fill_stat',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_manual_fill_stat','dm_op_manual_fill_stat','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_manual_fill_stat_total','dm_op_manual_fill_stat','宋英南');
END