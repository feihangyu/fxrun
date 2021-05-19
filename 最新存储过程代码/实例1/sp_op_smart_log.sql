CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_smart_log`()
BEGIN
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @operation_time := NULL, @shelf_id := NULL, @operation_type := NULL, @user_id := NULL;
  SET @time_gap := 100;
  TRUNCATE feods.d_op_smart_log;
  INSERT INTO feods.d_op_smart_log (
    kid, mflag, shelf_id, user_id, operation_type, operation_result, operation_time, operation_remark, transaction_id, add_user
  )
  SELECT
    kid, @shelf_id = shelf_id && @user_id = user_id && @operation_type = operation_type && IF(
      operation_time > ADDDATE(@operation_time, 1), 0, TIMEDIFF(operation_time, @operation_time) < @time_gap
    ) mflag, @shelf_id := shelf_id shelf_id, @user_id := user_id user_id, @operation_type := operation_type operation_type, operation_result, @operation_time := operation_time operation_time, operation_remark, transaction_id, @add_user add_user
  FROM
    (SELECT
      t.kid, t.shelf_id, t.operation_type, t.operation_result, t.operation_time, t.operation_remark, t.transaction_id, IFNULL(tr.user_id, 0) user_id
    FROM
      fe.sf_shelf_smart_log t
      LEFT JOIN fe.sf_shelf_transactions tr
        ON t.transaction_id = tr.transactions_id
        AND tr.data_flag = 1
    WHERE t.data_flag = 1
      and ! ISNULL(t.shelf_id)
      AND t.operation_time < @add_day
      AND t.operation_type IN (1, 2)
    ORDER BY t.shelf_id, IFNULL(tr.user_id, 0), t.operation_type, t.operation_time) t;
  CALL feods.sp_task_log (
    'sp_op_smart_log', @sdate, CONCAT(
      'fjr_d_e80879377ff5622350479051661f2459', @timestamp, @add_user
    )
  );
  COMMIT;
END