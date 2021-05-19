CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_project_excute_status`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 每天更新任务的执行状态
replace into fe_dwd.dwd_project_excute_status(sdate,process_name)
values(current_date,'dm_op_area_product_mgmv_six')
,(current_date,'dwd_fill_day_inc_recent_two_month')
,(current_date,'dwd_op_dim_date_three')
,(current_date,'dwd_shelf_product_sto_sal_30_days')
,(current_date,'dm_op_out_product_sto_and_sale')
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_project_excute_status',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进@', @user),
@stime);
 
END