CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_smart_log`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @operation_time := NULL, @shelf_id := NULL, @operation_type := NULL, @user_id := NULL;
  SET @time_gap := 100;
  
  TRUNCATE fe_dm.dm_op_smart_log;
  INSERT INTO fe_dm.dm_op_smart_log (
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
      fe_dwd.dwd_sf_shelf_smart_log t  -- 待唐进同步完成之后可部署 0520
      LEFT JOIN fe_dwd.dwd_sf_shelf_transactions tr
        ON t.transaction_id = tr.transactions_id
    WHERE  ! ISNULL(t.shelf_id)
      AND t.operation_time < @add_day
      AND t.operation_type IN (1, 2)
    ORDER BY t.shelf_id, IFNULL(tr.user_id, 0), t.operation_type, t.operation_time) t;
  
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_smart_log',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_smart_log','dm_op_smart_log','李世龙');
COMMIT;
    END