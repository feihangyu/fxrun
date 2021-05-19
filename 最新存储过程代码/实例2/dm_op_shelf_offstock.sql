CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_offstock`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/04/30
-- Modify date: 
-- Description:	
-- 	货架维度缺货率统计（所有货架类型当月汇总）（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
SET @cur_month := DATE_FORMAT(@stat_date,'%Y-%m');
SET @pre_6month := DATE_FORMAT(SUBDATE(@stat_date,INTERVAL 6 MONTH),'%Y-%m');
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`offstock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.offstock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS        
SELECT 
        shelf_id, 
        SUM(IF(ifsto = 0, ct, 0)) AS offstock_ct,
        SUM(ct) AS ct,
        SUM(offstock_val)  AS offstock_val
FROM
  fe_dm.`dm_op_s_offstock` 
WHERE sdate >= @cur_month_01
GROUP BY shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf7_offstock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf7_offstock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS        
SELECT
        shelf_id,
        SUM(slots) - SUM(slots_sto) AS offstock_slots,
        SUM(slots) AS slots,
        SUM(miss_val) AS offstock_val
FROM
        fe_dm.`dm_op_offstock_s7`
WHERE sdate >= @cur_month_01
GROUP BY shelf_id
;
DELETE FROM fe_dm.dm_op_shelf_offstock WHERE month_id = @cur_month OR month_id < @pre_6month;
INSERT INTO fe_dm.dm_op_shelf_offstock
(
        month_id,
        shelf_id, 
        shelf_name,
        shelf_code,
        shelf_type,
        offstock_ct,
        ct,
        offstock_slots,
        slots,
        offstock_rate,
        offstock_val
)
SELECT
        @cur_month AS month_id,
        a.shelf_id, 
        b.shelf_name,
        b.shelf_code,
        b.shelf_type,
        a.offstock_ct,
        a.ct,
        a.offstock_slots,
        a.slots,
        a.offstock_rate,
        a.offstock_val
FROM
(
        SELECT
                shelf_id, 
                offstock_ct,
                ct,
                NULL AS offstock_slots,
                NULL AS slots,
                ROUND(offstock_ct / ct,2) AS offstock_rate,
                ROUND(offstock_val,2) AS offstock_val
        FROM
                fe_dwd.offstock_tmp
        UNION
        SELECT
                shelf_id, 
                NULL AS offstock_ct,
                NULL AS ct,
                offstock_slots,
                slots,
                ROUND(offstock_slots / slots,2) AS offstock_rate,
                ROUND(offstock_val,2) AS offstock_val
        FROM
                fe_dwd.shelf7_offstock_tmp
) a
JOIN fe_dwd.`dwd_shelf_base_day_all` b
        ON a.shelf_id = b.shelf_id 
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_offstock',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_offstock','dm_op_shelf_offstock','宋英南');
END