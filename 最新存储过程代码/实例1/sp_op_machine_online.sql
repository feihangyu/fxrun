CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_machine_online`()
BEGIN
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  TRUNCATE feods.d_op_machine_online_shelf;
  INSERT INTO feods.d_op_machine_online_shelf (
    shelf_id, records, min_change_time, max_change_time, max_change_time_on, max_change_time_off, add_user
  )
  SELECT
    shelf_id, COUNT(*) records, MIN(change_time) min_change_time, MAX(change_time) max_change_time, MAX(
      IF(online_status, change_time, NULL)
    ) max_change_time_on, MAX(
      IF(online_status, NULL, change_time)
    ) max_change_time_off, @add_user add_user
  FROM
    fe_ana_data.sf_shelf_machine_online_status_record
  WHERE data_flag = 1
    AND ! ISNULL(shelf_id)
    AND change_time < @add_day
  GROUP BY shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.record_tmp;
  CREATE TEMPORARY TABLE feods.record_tmp (KEY (shelf_id))
  SELECT
    t.shelf_id, t.min_change_time change_time, 0 online_status
  FROM
    feods.d_op_machine_online_shelf t
    JOIN fe_ana_data.sf_shelf_machine_online_status_record r
      ON t.shelf_id = r.shelf_id
      AND t.min_change_time = r.change_time
      AND r.data_flag = 1
      AND r.online_status = 1;
  INSERT INTO feods.record_tmp
  SELECT
    t.shelf_id, @add_day, 0
  FROM
    feods.d_op_machine_online_shelf t
    JOIN fe_ana_data.sf_shelf_machine_online_status_record r
      ON t.shelf_id = r.shelf_id
      AND t.max_change_time = r.change_time
      AND r.data_flag = 1
      AND r.online_status = 1;
  INSERT INTO feods.record_tmp
  SELECT
    shelf_id, @add_day, 1
  FROM
    feods.d_op_machine_online_shelf;
  INSERT INTO feods.record_tmp
  SELECT
    shelf_id, change_time, online_status
  FROM
    fe_ana_data.sf_shelf_machine_online_status_record
  WHERE data_flag = 1
    AND ! ISNULL(shelf_id)
    AND change_time < @add_day;
  DROP TEMPORARY TABLE IF EXISTS feods.flag_tmp;
  SET @shelf_id := NULL, @online_status := NULL;
  CREATE TEMPORARY TABLE feods.flag_tmp (KEY (shelf_id))
  SELECT
    @shelf_id = shelf_id && @online_status = online_status flag, @shelf_id := shelf_id shelf_id, change_time, @online_status := online_status online_status
  FROM
    (SELECT
      *
    FROM
      feods.record_tmp
    ORDER BY shelf_id, change_time, online_status) t;
  TRUNCATE feods.d_op_machine_online_detail;
  SET @shelf_id := NULL, @change_time := NULL;
  INSERT INTO feods.d_op_machine_online_detail (
    change_time_start, shelf_id, change_time_end, online_status, add_user
  )
  SELECT
    IF(
      @shelf_id = shelf_id, @change_time, NULL
    ) change_time_start, @shelf_id := shelf_id shelf_id, @change_time := change_time change_time_end, 1- online_status online_status, @add_user add_user
  FROM
    (SELECT
      *
    FROM
      feods.flag_tmp
    WHERE flag = 0
    ORDER BY shelf_id, change_time, online_status) t;
  DELETE
  FROM
    feods.d_op_machine_online_detail
  WHERE ISNULL(change_time_start)
    OR change_time_start = change_time_end;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_sdate_tmp;
  CREATE TEMPORARY TABLE feods.shelf_sdate_tmp (PRIMARY KEY (shelf_id, sdate))
  SELECT DISTINCT
    t.shelf_id, w.sdate, ADDDATE(w.sdate, 1) sdate1, DATE_FORMAT(w.sdate, '%Y-%m-%d %08:%00:%00') sdate08, DATE_FORMAT(w.sdate, '%Y-%m-%d %20:%00:%00') sdate20
  FROM
    feods.d_op_machine_online_shelf t
    JOIN feods.fjr_work_days w
      ON w.sdate BETWEEN DATE(t.min_change_time)
      AND @sdate;
  TRUNCATE feods.d_op_machine_online_stat;
  INSERT INTO feods.d_op_machine_online_stat (
    change_id, sdate, shelf_id, online_status, change_time_start, change_time_end, duration, duration820, add_user
  )
  SELECT
    t.row_id change_id, ss.sdate, t.shelf_id, t.online_status, t.change_time_start, t.change_time_end, TIMESTAMPDIFF(
      SECOND, GREATEST(t.change_time_start, ss.sdate), LEAST(t.change_time_end, ss.sdate1)
    ) / 24 / 3600 duration, TIMESTAMPDIFF(
      SECOND, GREATEST(t.change_time_start, ss.sdate08), LEAST(t.change_time_end, ss.sdate20)
    ) / 24 / 3600 duration820, @add_user add_user
  FROM
    feods.d_op_machine_online_detail t
    JOIN feods.shelf_sdate_tmp ss
      ON t.shelf_id = ss.shelf_id
      AND ss.sdate BETWEEN DATE(t.change_time_start)
      AND DATE(t.change_time_end);
  CALL feods.sp_task_log (
    'sp_op_machine_online', @sdate, CONCAT(
      'fjr_d_c5fae1bb00bf06a973d8affc1ba61076', @timestamp, @add_user
    )
  );
  COMMIT;
END