CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_slot_change_record`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  INSERT INTO feods.d_op_slot_change_record (
    slot_id, shelf_id, product_id, add_user
  )
  SELECT
    t.slot_id, t.shelf_id, t.product_id, @add_user add_user
  FROM
    fe.sf_shelf_machine_slot t
    LEFT JOIN feods.d_op_shelf_machine_slot sl
      ON t.slot_id = sl.slot_id
      and t.product_id = sl.product_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
    AND ISNULL(sl.slot_id);
  TRUNCATE feods.d_op_shelf_machine_slot;
  INSERT INTO feods.d_op_shelf_machine_slot (
    slot_id, shelf_id, product_id, add_user
  )
  SELECT
    slot_id, shelf_id, product_id, @add_user add_user
  FROM
    fe.sf_shelf_machine_slot
  WHERE data_flag = 1
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id);
  CALL feods.sp_task_log (
    'sp_slot_change_record', @sdate, CONCAT(
      'fjr_h_09e7e269a419f21fcd74f3542d113ccd', @timestamp, @add_user
    )
  );
  COMMIT;
END