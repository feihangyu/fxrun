CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelfs_dstat_areaversion`()
BEGIN
  SELECT
    @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @month_start := SUBDATE(@sdate, DAY(@sdate) - 1);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp_areaversion;
  CREATE TEMPORARY TABLE feods.shelf_tmp_areaversion (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, b.business_name, t.shelf_type, t.revoke_status = 1 revoke_status1, t.revoke_status = 2 revoke_status2, t.whether_close = 2 || t.close_type = 12 close_flag, ! ISNULL(sr.shelf_id) if_relation, IFNULL(t.activate_time < @month_start, 0) act_month_flag
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN
      (SELECT
        t.main_shelf_id shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9
      UNION
      SELECT
        t.secondary_shelf_id shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9) sr
      ON t.shelf_id = sr.shelf_id
  WHERE t.data_flag = 1
    AND t.shelf_status = 2
    AND t.shelf_type IN (1, 2, 3, 5)
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_relation_tmp;
  CREATE TEMPORARY TABLE feods.shelf_relation_tmp (PRIMARY KEY (shelf_id))
  SELECT
    *
  FROM
    feods.shelf_tmp_areaversion
  WHERE if_relation
    AND ! ISNULL(shelf_id);
  INSERT INTO feods.shelf_tmp_areaversion
  SELECT
    t.secondary_shelf_id shelf_id, s.business_name, s.shelf_type, s.revoke_status1, s.revoke_status2, s.close_flag, s.if_relation, s.act_month_flag
  FROM
    fe.sf_shelf_relation_record t
    JOIN feods.shelf_relation_tmp s
      ON t.main_shelf_id = s.shelf_id
    JOIN fe.sf_shelf sf
      ON t.secondary_shelf_id = sf.shelf_id
      AND sf.data_flag = 1
      AND sf.shelf_status != 2
  WHERE t.data_flag = 1
    AND t.shelf_handle_status = 9;
  DELETE
  FROM
    feods.d_op_shelfs_dstat_areaversion
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_shelfs_dstat_areaversion (
    sdate, business_name, shelf_type, revoke_status1, revoke_status2, close_flag, if_relation, act_month_flag, shelfs, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_type, t.revoke_status1, t.revoke_status2, t.close_flag, t.if_relation, t.act_month_flag, COUNT(*) shelfs, @add_user add_user
  FROM
    feods.shelf_tmp_areaversion t
  GROUP BY t.business_name, t.shelf_type, t.revoke_status1, t.revoke_status2, t.close_flag, t.if_relation, t.act_month_flag;
 /* DELETE
  FROM
    feods.d_op_shelfs_area_areaversion
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_shelfs_area_areaversion (
    sdate, business_name, shelfs_act13, shelfs_rem13, shelfs_act2, shelfs_rem2, shelfs_act5, shelfs_rem5, shelfs_act_relation, shelfs_rem_relation, shelfs_act, shelfs_rem, add_user
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
    ) shelfs_rem, @add_user add_user
  FROM
    feods.d_op_shelfs_dstat_areaversion t
  WHERE t.sdate = @sdate
  GROUP BY t.business_name;*/
  CALL feods.sp_task_log (
    'sp_op_shelfs_dstat_areaversion', @sdate, CONCAT(
      'fjr_d_015cd0cd3c1d666c4c7a5c2ce6e98db6', @timestamp, @add_user
    )
  );
  COMMIT;
END