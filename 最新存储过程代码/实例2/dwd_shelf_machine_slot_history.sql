CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_machine_slot_history`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  DELETE
  FROM
    fe_dwd.`dwd_shelf_machine_slot_history`
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 7 DAY);
  
  DELETE
  FROM
    fe_dwd.`dwd_shelf_machine_slot_history`
  WHERE sdate = CURDATE();
  INSERT INTO fe_dwd.`dwd_shelf_machine_slot_history` (
    sdate,
    shelf_id,
    product_id,
    manufacturer_slot_code,
    slot_status,
    stock_num,
    slot_capacity_limit
  )
  SELECT
    CURDATE() AS sdate,
    shelf_id,
    product_id,
    manufacturer_slot_code,
    slot_status,
    stock_num,
    slot_capacity_limit
  FROM
    fe_dwd.dwd_shelf_machine_slot_type;
	
	
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_machine_slot_history',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_machine_slot_history','dwd_shelf_machine_slot_history','李世龙');
COMMIT;
    END