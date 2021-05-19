CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelfs_area_areaversion_two`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SELECT
@sdate := SUBDATE(CURRENT_DATE, 1), @month_start := SUBDATE(@sdate, DAY(@sdate) - 1);
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp_areaversion;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp_areaversion (PRIMARY KEY (shelf_id))
    SELECT
        t.shelf_id, t.business_name, t.shelf_type, t.revoke_status = 1 revoke_status1, t.revoke_status = 2 revoke_status2
        , t.whether_close = 2 || t.close_type = 12 close_flag
        ,t.relation_flag
        , IFNULL(t.activate_time < @month_start, 0) act_month_flag
    FROM fe_dwd.dwd_shelf_base_day_all t
    WHERE t.shelf_status = 2 AND t.shelf_type IN (1, 2, 3, 5);
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_relation_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_relation_tmp (PRIMARY KEY (shelf_id))
    SELECT *
    FROM fe_dm.shelf_tmp_areaversion
    WHERE relation_flag;
INSERT INTO fe_dm.shelf_tmp_areaversion
SELECT t.shelf_id, t.business_name, t.shelf_type, s.revoke_status1, s.revoke_status2, s.close_flag, s.relation_flag, s.act_month_flag
FROM fe_dwd.dwd_shelf_base_day_all t
JOIN fe_dm.shelf_relation_tmp s ON t.main_shelf_id = s.shelf_id
WHERE t.SHELF_STATUS<>2
AND t.shelf_handle_status = 9;
DELETE FROM fe_dm.dm_op_shelfs_dstat_areaversion WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_op_shelfs_dstat_areaversion  (
    sdate, business_name, shelf_type, revoke_status1, revoke_status2, close_flag, if_relation, act_month_flag, shelfs
  )
SELECT
    @sdate sdate, t.business_name, t.shelf_type, t.revoke_status1, t.revoke_status2, t.close_flag, t.relation_flag, t.act_month_flag, COUNT(*) shelfs
FROM fe_dm.shelf_tmp_areaversion t
GROUP BY t.business_name, t.shelf_type, t.revoke_status1, t.revoke_status2, t.close_flag, t.relation_flag, t.act_month_flag;
DELETE FROM fe_dm.dm_op_shelfs_area_areaversion WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_op_shelfs_area_areaversion (
    sdate, business_name, shelfs_act13, shelfs_rem13, shelfs_act2, shelfs_rem2, shelfs_act5, shelfs_rem5, shelfs_act_relation, shelfs_rem_relation, shelfs_act, shelfs_rem
)
  SELECT
    t.sdate, t.business_name, SUM(
      IF(
        ! t.if_relation && t.shelf_type IN (1, 3) && t.close_flag && t.revoke_status1 && t.act_month_flag, t.shelfs, 0
      )
    ) shelfs_act13, SUM(
      IF(
        ! t.if_relation && t.shelf_type IN (1, 3) && (
          t.revoke_status1 || t.revoke_status2
        ), t.shelfs, 0
      )
    ) shelfs_rem13, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 2 && t.close_flag && t.revoke_status1 && t.act_month_flag, t.shelfs, 0
      )
    ) shelfs_act2, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 2 && (
          t.revoke_status1 || t.revoke_status2
        ), t.shelfs, 0
      )
    ) shelfs_rem2, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 5 && t.close_flag && t.revoke_status1 && t.act_month_flag, t.shelfs, 0
      )
    ) shelfs_act5, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 5 && (
          t.revoke_status1 || t.revoke_status2
        ), t.shelfs, 0
      )
    ) shelfs_rem5, SUM(
      IF(
        t.if_relation && t.close_flag && t.revoke_status1 && t.act_month_flag, t.shelfs, 0
      )
    ) shelfs_act_relation, SUM(
      IF(
        t.if_relation && (
          t.revoke_status1 || t.revoke_status2
        ), t.shelfs, 0
      )
    ) shelfs_rem_relation, SUM(
      IF(
        t.close_flag && t.revoke_status1 && t.act_month_flag, t.shelfs, 0
      )
    ) shelfs_act, SUM(
      IF(
        t.revoke_status1 || t.revoke_status2, t.shelfs, 0
      )
    ) shelfs_rem
FROM fe_dm.dm_op_shelfs_dstat_areaversion t
WHERE t.sdate = @sdate
GROUP BY t.business_name;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_shelfs_area_areaversion_two',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelfs_dstat_areaversion','dm_op_shelfs_area_areaversion_two','纪伟铨');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelfs_area_areaversion','dm_op_shelfs_area_areaversion_two','纪伟铨');
END