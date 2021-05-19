CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_slot_change_record_two`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  
  INSERT INTO fe_dm.dm_op_slot_change_record (
    slot_id, shelf_id, product_id, add_user
  )
  SELECT
    t.slot_id, t.shelf_id, t.product_id, @add_user add_user
  FROM
    fe_dwd.dwd_shelf_machine_slot_type t
    LEFT JOIN fe_dm.dm_op_shelf_machine_slot sl
      ON t.slot_id = sl.slot_id
      AND t.product_id = sl.product_id
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
    AND ISNULL(sl.slot_id);
	
  TRUNCATE fe_dm.dm_op_shelf_machine_slot;
  INSERT INTO fe_dm.dm_op_shelf_machine_slot (
    slot_id, shelf_id, product_id, add_user
  )
  SELECT
    slot_id, shelf_id, product_id, @add_user add_user
  FROM
    fe_dwd.dwd_shelf_machine_slot_type
  WHERE ! ISNULL(shelf_id)
    AND ! ISNULL(product_id);
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_slot_change_record_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_machine_slot','dm_op_slot_change_record_two','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_slot_change_record','dm_op_slot_change_record_two','李世龙');
COMMIT;
    END