CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_info_month`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP ;
SET @sub_day := SUBDATE(@sdate, 1) ;
SET @d := DAY(@sdate) ;
SET @month_start := SUBDATE(@sdate, @d - 1) ;
SET @month_end := LAST_DAY(@sdate) ;
 DELETE FROM fe_dm.dm_op_shelf_info_month 
 WHERE sdate = @sdate OR (sdate > @month_start AND sdate < @month_end) ;
 
 INSERT INTO fe_dm.dm_op_shelf_info_month (
    sdate, region_name, business_name, city_name, address, shelf_id, shelf_code, shelf_name, shelf_type, shelf_status, revoke_status, whether_close, activate_time, revoke_time, shlef_add_time, mobile_phone, sf_code, real_name, shelfs, shelfs6, shelfs7, branch_name, branch_code, fulltime_falg, sf_code_bd, real_name_bd, bdtype, company_name, prewh_falg, warehouse_id, warehouse_name, rel_flag, main_shelf_id, loss_pro_flag, last_revoke_time, lastrevoke_status, inner_flag, machine_type, product_template_id, template_name, online_status, firstfill, add_user
  ) 
  SELECT 
    @sdate sdate, region_name, business_name, city_name, address, shelf_id, shelf_code, shelf_name, shelf_type, shelf_status, revoke_status, whether_close, activate_time, revoke_time, shlef_add_time, mobile_phone, sf_code, real_name, shelfs, shelfs6, shelfs7, branch_name, branch_code, fulltime_falg, sf_code_bd, real_name_bd, bdtype, company_name, prewh_falg, warehouse_id, warehouse_name, rel_flag, main_shelf_id, loss_pro_flag, last_revoke_time, lastrevoke_status, inner_flag, machine_type, product_template_id, template_name, online_status, firstfill, @add_user add_user 
  FROM fe_dm.dm_op_shelf_info ;
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_info_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_info_month','dm_op_shelf_info_month','李世龙');
COMMIT;
    END