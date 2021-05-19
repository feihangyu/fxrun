CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sf_shelf_last62`()
begin
  set @sdate := current_date, @add_user := current_user, @timestamp := current_timestamp ;
  set @first_day := subdate(@sdate, 61) ;
  delete 
  from
    feods.d_op_sf_shelf_last62 
  where (
      sdate < @first_day 
      and sdate != last_day(sdate) 
      and day(sdate) != 1
    ) 
    or sdate = @sdate ;
  insert into feods.d_op_sf_shelf_last62 (
    sdate, shelf_id, shelf_name, shelf_code, shelf_random_code, company_id, shelf_type, machine_model, shelf_model, shelf_status, activate_time, exploit_type, province, city, district, area_address, address, bd_id, bd_name, manager_id, manager_name, shelf_picture, revoke_status, revoke_time, revoke_trace, old_shelf_code, valid_chance, add_time, add_user_id, last_update_user_id, last_update_time, data_flag, commission_status, stock_rate, last_select_goods_time, is_select_goods, close_type, close_remark, close_time, whether_close, invalid_reason, group_url, inside_company_id, prewarehouse_dept_id, shelf_level, operate_shelf_type, old_manager_id, asset_barcode, commission_status_temp, contact_name, contact_phone
  ) 
  select 
    @sdate sdate, shelf_id, shelf_name, shelf_code, shelf_random_code, company_id, shelf_type, machine_model, shelf_model, shelf_status, activate_time, exploit_type, province, city, district, area_address, address, bd_id, bd_name, manager_id, manager_name, shelf_picture, revoke_status, revoke_time, revoke_trace, old_shelf_code, valid_chance, add_time, add_user_id, last_update_user_id, last_update_time, data_flag, commission_status, stock_rate, last_select_goods_time, is_select_goods, close_type, close_remark, close_time, whether_close, invalid_reason, group_url, inside_company_id, prewarehouse_dept_id, shelf_level, operate_shelf_type, old_manager_id, asset_barcode, commission_status_temp, contact_name, contact_phone 
  from
    fe.sf_shelf 
  where data_flag = 1 ;
  insert into feods.d_op_sp_bak (
    routine_schema, routine_name, routine_type, routine_definition, routine_definer
  ) 
  select 
    t.routine_schema, t.routine_name, t.routine_type, t.routine_definition, t.definer 
  from
    information_schema.routines t 
  where t.last_altered >= subdate(current_date, 1) ;
  call feods.sp_task_log (
    'sp_op_sf_shelf_last62', @sdate, concat(
      'fjr_d_84be7f1febc78e15f62b641d9ed2f4a0', @timestamp, @add_user
    )
  ) ;
  commit ;
end