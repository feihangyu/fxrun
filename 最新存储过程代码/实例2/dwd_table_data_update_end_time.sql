CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_table_data_update_end_time`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- datax同步的表
REPLACE INTO fe_dwd.dwd_table_data_update_end_time
SELECT b.sdate,a.table_name_two,b.end_time FROM (
SELECT datax_project_name,table_name_one,table_name_two FROM fe_dwd.`dwd_datax_table_mapping_info` WHERE delete_flag=1
) a
JOIN (SELECT sdate,datax_project_name,datax_table_name,MIN(end_time) AS end_time FROM fe_dwd.dwd_datax_excute_info_detective WHERE sdate=CURRENT_DATE GROUP BY datax_project_name,datax_table_name) b
ON SUBSTRING_INDEX(a.table_name_one,'.',-1)=b.datax_table_name;
-- azkaban调度
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_last60_buy_users' as table_name,max(add_time) as update_end_time from fe_dm.dm_en_last60_buy_users ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_order_item_60' as table_name,max(add_time) as update_end_time from fe_dm.dm_en_order_item_60 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_wastage_item' as table_name,max(add_time) as update_end_time from fe_dm.dm_en_wastage_item ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_third_user_balance_his' as table_name,max(add_time) as update_end_time from fe_dm.dm_en_third_user_balance_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_fill_order_efficiency' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_fill_order_efficiency ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_finance_statement_log' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_finance_statement_log ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_lead_warehouse_temp_table_main' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_lead_warehouse_temp_table_main ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_lo_campus_manager_level' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_lo_campus_manager_level ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_lo_manager_performance_report_everyday_for_month' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_lo_manager_performance_report_everyday_for_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_lo_shelf_fill_timeliness_detail' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_lo_shelf_fill_timeliness_detail ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_manager_shelf_performance_label' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_manager_shelf_performance_label ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_manager_shelf_statistic_result' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_manager_shelf_statistic_result ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_area_daily' as table_name,max(add_time) as update_end_time from fe_dm.dm_ma_area_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_new_product_daily_statistics' as table_name,max(add_time) as update_end_time from fe_dm.dm_ma_new_product_daily_statistics ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_area_dashboard_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_area_dashboard_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_area_history_sales_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_area_history_sales_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_area_sale_hourly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_area_sale_hourly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_coupon_use_stat_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_coupon_use_stat_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_high_gross' as table_name,max(add_time) as update_end_time from fe_dm.dm_ma_high_gross ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_paytype_dashboard_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_paytype_dashboard_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_paytype_sale_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_paytype_sale_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_paytype_sale_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_paytype_sale_monthly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_product_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_product_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_sale_2week' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_sale_2week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_sale_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_sale_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_sale_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_sale_monthly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_sale_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_sale_weekly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_monitor' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_monitor_area' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_monitor_area ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_user_stat' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_user_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_unsalable' as table_name,max(add_time) as update_end_time from fe_dm.dm_ma_unsalable ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_usertype_sale_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_usertype_sale_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_user_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_user_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_user_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_user_monthly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_user_sale_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_user_sale_weekly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_user_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_user_weekly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_daily_shelf_stock_stag' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_daily_shelf_stock_stag ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_daily_shelf_stock_stag_detail' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_daily_shelf_stock_stag_detail ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_finance_month_income_result' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_finance_month_income_result ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_shelf_stat_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_shelf_stat_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_mp_week_kpi_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_mp_week_kpi_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_new_product_gmv' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_new_product_gmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_order_over100' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_abnormal_order_over100 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_order_product_qty' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_abnormal_order_product_qty ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_order_shelf_product' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_abnormal_order_shelf_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_order_user' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_abnormal_order_user ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_package' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_abnormal_package ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_package_product' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_abnormal_package_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_high_stock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_high_stock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_high_stock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_high_stock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_high_stock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_high_stock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_out_product_purchase' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_area_out_product_purchase ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_avg_price' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_area_product_avg_price ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_dfill' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_area_product_dfill ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_shelftype_dfill' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_shelftype_dfill ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_area_product_dgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_area_product_dgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_mgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_area_product_mgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_wgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_area_product_wgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_shelftype_dgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_shelftype_dgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_shelftype_mgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_shelftype_mgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_shelftype_wgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_shelftype_wgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_pub_shelf_dgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_pub_shelf_dgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_project_excute_status' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_project_excute_status ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_product_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_shelf_cover' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_area_product_shelf_cover ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_stat_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_area_product_stat_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_stock_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_area_product_stock_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_product_unsale_io_ratio' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_product_unsale_io_ratio ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_abnormal_shelf' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_abnormal_shelf ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_operate_quality' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_area_operate_quality ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_lowstorate_area_product' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_lowstorate_area_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_outtime_active_shelf' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_outtime_active_shelf ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_save_revoke_shelf' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_save_revoke_shelf ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_shelf_open_close_times' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_area_shelf_open_close_times ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_stock_change' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_stock_change ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_stock_change_detail' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_stock_change_detail ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_avgqty_fill_dayst' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_avgqty_fill_dayst ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_avgqty_fill_dayst_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_avgqty_fill_dayst_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_cal_fill_days' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_cal_fill_days ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_cal_fill_reasonable' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_cal_fill_reasonable ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_cal_fill_reasonable_month' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_cal_fill_reasonable_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_cancel_fill_order' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_cancel_fill_order ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_ds7p_sal_fil' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_ds7p_sal_fil ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_effective_stock' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_effective_stock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_tot_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_tot_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_gmv_change_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_fill_gmv_change_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_not_push_order' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_fill_not_push_order ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_not_push_order_stat' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_fill_not_push_order_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_area_product_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_fill_area_product_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_shelf_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_fill_shelf_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_shelftype_fill_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_shelftype_fill_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_shelftype_order_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_shelftype_order_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_type_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_fill_type_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_fill_type_monitor_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_fill_type_monitor_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_flag5_product_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_flag5_product_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_flag5_shelf_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_flag5_shelf_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_flags_area_product' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_flags_area_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_flags_res_area' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_flags_res_area ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_area_product_satis_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_area_product_satis_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_area_top10_uprate_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_area_top10_uprate_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_area_top10_uprate_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_area_top10_uprate_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_np_success_rate_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_np_success_rate_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_outlet_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_outlet_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_product_new_out_sto_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_product_new_out_sto_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_sale_vs_stock_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_sale_vs_stock_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_sale_vs_stock_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_sale_vs_stock_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi2_shelf_level_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi2_shelf_level_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_monitor' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_stock_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_stock_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_slot_sale_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_slot_sale_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_slot_stock_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_slot_stock_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_fill_nday' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_fill_nday ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_sale_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_sale_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_sale_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_sale_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_sale_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_sale_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_stat_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_stat_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_stat_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_stat_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_stat_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_stat_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_shelf_stock_his' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_shelf_stock_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_product_sale_stock_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_product_sale_stock_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_product_sale_stock_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_product_sale_stock_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi3_shelf7_product_sale_stock_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi3_shelf7_product_sale_stock_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_area_product_sat_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_area_product_sat_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_gmv_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_gmv_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_np_gmv_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_np_gmv_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_np_sal_sto_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_np_sal_sto_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_np_flag5_sto' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_np_flag5_sto ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_gmv_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_gmv_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_np_gmv_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_np_gmv_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_np_sal_sto_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_np_sal_sto_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_np_out_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_np_out_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_shelf_nps' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_shelf_nps ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_sto_val_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_sto_val_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_kpi_unsku' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_kpi_unsku ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_machine_online_detail' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_machine_online_detail ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_machine_online_shelf' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_machine_online_shelf ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_machine_online_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_machine_online_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_shelf_product_unsale_flag' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_shelf_product_unsale_flag ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_machine_unsale_flag' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_machine_unsale_flag ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_manager_product_trans_list' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_manager_product_trans_list ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_manager_product_trans_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_manager_product_trans_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_manual_fill_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_manual_fill_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_manual_fill_stat' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_manual_fill_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_manual_fill_stat_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_manual_fill_stat_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_newshelf_firstfill_and_sale' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_newshelf_firstfill_and_sale ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_area7' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock_area7 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_m7' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock_m7 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_s7' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock_s7 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_s7p' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock_s7p ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_s7_key' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock_s7_key ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_slot' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock_slot ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_offstock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_p_offstock' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_p_offstock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_sp_offstock' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_sp_offstock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_sp_offstock_his' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_sp_offstock_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_s_offstock' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_s_offstock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_not_push_order' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_offstock_not_push_order ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_high_growth' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_offstock_high_growth ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_offstock_top10' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_offstock_top10 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_order_fill_ratio' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_order_fill_ratio ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_order_fill_ratio_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_order_fill_ratio_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_out_product_clear_efficiency' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_out_product_clear_efficiency ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_out_product_sale_through_rate' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_out_product_sale_through_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_out_product_intime_rate' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_out_product_intime_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_out_product_suggest' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_out_product_suggest ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_out_product_sto_and_sale' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_out_product_sto_and_sale ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_package_config' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_package_config ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_package_shelf' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_package_shelf ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_price_sensitive_stat_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_price_sensitive_stat_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_price_sensitive_stat_month_nation' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_price_sensitive_stat_month_nation ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_price_sensitive_stat_nation' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_price_sensitive_stat_nation ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_price_sensitive_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_price_sensitive_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_user_miser_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_user_miser_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_disrate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_disrate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_sp_disrate' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_sp_disrate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_dim_product_area_normal' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_dim_product_area_normal ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_sal_month_large' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_sal_month_large ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_area_stat_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_area_stat_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_shelf_dam_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_shelf_dam_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_shelf_sal_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_shelf_sal_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_shelf_sal_month_large' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_shelf_sal_month_large ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_shelf_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_shelf_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_shelf_sto_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_shelf_sto_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_list_manager_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_list_manager_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_dc_reqsto' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_dc_reqsto ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_pwh_reqsto' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_pwh_reqsto ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_area_product_sale_day' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf7_area_product_sale_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_area_product_sale_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf7_area_product_sale_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_area_product_sale_week' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf7_area_product_sale_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_area_product_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf7_area_product_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_offstock_reason' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf7_offstock_reason ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_product_offstock_reason' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf7_product_offstock_reason ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf7_slot_analysis' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf7_slot_analysis ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelfs_area_areaversion' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelfs_area_areaversion ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelfs_dstat_areaversion' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelfs_dstat_areaversion ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_active_week' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_active_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_gmv_split' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_gmv_split ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_his' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_info_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf_info_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_label_month' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_label_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_offstock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_offstock ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_level_offstock_loss' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_product_level_offstock_loss ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_level_offstock_loss_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_product_level_offstock_loss_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_type_offstock_loss' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_product_type_offstock_loss ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_type_offstock_loss_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_product_type_offstock_loss_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_offstock_loss' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_offstock_loss ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_offstock_loss_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_offstock_loss_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_offstock_reason' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_offstock_reason ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_price_sensitivity' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_price_sensitivity ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_fill_last_time' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_product_fill_last_time ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_fill_update_his' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_fill_update_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_sales_flag_change' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_sales_flag_change ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_trans_out_his' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_trans_out_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_trans_out_list' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_trans_out_list ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_trans_out_monitor' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf_product_trans_out_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_sku_situation' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_sku_situation ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_type_flag' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_type_flag ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_type_product_sale' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_type_product_sale ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_month_sale' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_month_sale ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_week_sale' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_week_sale ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_week_product_stock_detail_tmp' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_week_product_stock_detail_tmp ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_machine_slot' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf_machine_slot ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_slot_change_record' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_slot_change_record ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_s7p_detail' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_s7p_detail ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_s7p_nslot' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_s7p_nslot ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_slot_his' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_slot_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_smart_log' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_smart_log ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_sp_avgsal_recent_week' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_sp_avgsal_recent_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_stock_reach_ratio' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_stock_reach_ratio ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_stock_area' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_stock_area ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_stock_product' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_stock_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_stock_shelf' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_stock_shelf ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_month_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_month_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_shelfcross_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_shelfcross_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_shelf_month_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_shelf_month_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_s_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_s_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_uptolm_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_uptolm_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_user_month_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_user_month_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_su_u_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_su_u_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_type_revoke_active_num' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_type_revoke_active_num ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_type_revoke_close_day' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_type_revoke_close_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_users_day_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_users_day_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_valid_danger_flag' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_valid_danger_flag ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_warehouse_monitor' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_warehouse_monitor ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_zone_new_product_gmv' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_zone_new_product_gmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_prewarehouse_coverage_rate' as table_name,max(add_time) as update_end_time from fe_dm.dm_prewarehouse_coverage_rate ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_prewarehouse_stock_detail' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_prewarehouse_stock_detail ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_prewarehouse_stock_detail_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_prewarehouse_stock_detail_monthly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_prewarehouse_stock_detail_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_prewarehouse_stock_detail_weekly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_preware_sales_fifteen' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_preware_sales_fifteen ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_preware_sales_seven' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_preware_sales_seven ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_preware_shelf_sales_thirty' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_preware_shelf_sales_thirty ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_sales_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_sales_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_shelf_sales_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_shelf_sales_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_pub_shelf_grade' as table_name,max(load_time) as update_end_time from fe_dm.dm_pub_shelf_grade ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_current_dynamic_purchase_price' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_current_dynamic_purchase_price ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_monthly_manual_purchase_price_insert' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_monthly_manual_purchase_price_insert ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_oms_stock_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_oms_stock_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_poorderlist_day' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_poorderlist_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_onload' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_onload ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_balance' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_balance ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_daily_report' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_daily_report ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_fill_seven_day' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_fill_seven_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_outbound_seven_day' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_outbound_seven_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_outbound_three_day' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_outbound_three_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_preware_fill_daily' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_preware_fill_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_preware_outbound_daily' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_preware_outbound_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_preware_outbound_forteen_day' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_preware_outbound_forteen_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_preware_outbound_monthly' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_preware_outbound_monthly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_preware_outbound_weekly' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_preware_outbound_weekly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_kpi' as table_name,max(add_time) as update_end_time from fe_dm.dm_sc_preware_kpi ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_sku_satisfy' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_sku_satisfy ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_fill_frequency' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_fill_frequency ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_wave_cycle' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_wave_cycle ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_profit_monthly_shelf_product' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_profit_monthly_shelf_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_balance' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_balance ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_preware_stock_outbound' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_preware_stock_outbound ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_sku_shelf_cnt' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_sku_shelf_cnt ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_outbound_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_outbound_daily ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_outbound_forteen' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_outbound_forteen ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_outbound_forteen_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_outbound_forteen_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_outbound_monthly_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_outbound_monthly_total ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_stock_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_stock_monthly ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_shelf_day_avg_gmv' as table_name,max(load_time) as update_end_time from fe_dm.dm_shelf_day_avg_gmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_shelf_mgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_shelf_mgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_shelf_wgmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_shelf_wgmv ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_product_price_salqty' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_product_price_salqty ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_subtype_price_salqty' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_subtype_price_salqty ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_subtype_price_stat' as table_name,max(add_time) as update_end_time from fe_dm.dm_subtype_price_stat ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_warehouse_product_presence' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_warehouse_product_presence ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_check_base_day_inc' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_check_base_day_inc ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_fillorder_requirement_information_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_fillorder_requirement_information_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_fill_day_inc_recent_two_month' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_fill_day_inc_recent_two_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_fill_day_inc' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_fill_day_inc ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_group_order_refound_address_day' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_group_order_refound_address_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_lo_node_monitor_data_after_cleanout' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_lo_node_monitor_data_after_cleanout ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_lo_prewarehouse_fill_order_item_month' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_lo_prewarehouse_fill_order_item_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_lo_school_order_item' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_lo_school_order_item ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_product_dim_sserp' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_pub_product_dim_sserp ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_op_shelf6_should_add_template' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_op_shelf6_should_add_template ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_op_shelf6_should_start_fill_item' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_op_shelf6_should_start_fill_item ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_op_unstock_area_product_week' as table_name,max(add_time) as update_end_time from fe_dwd.dwd_op_unstock_area_product_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_op_unstock_detail_week' as table_name,max(add_time) as update_end_time from fe_dwd.dwd_op_unstock_detail_week ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_order_item_refund_day' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_order_item_refund_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pj_outstock2_day' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_pj_outstock2_day ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_prc_project_relationship_detail_info' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_prc_project_relationship_detail_info ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_activity_order_shelf_product' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_activity_order_shelf_product ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_order_item_recent_one_month' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_order_item_recent_one_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_order_item_recent_two_month ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_day_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_day_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_east_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_east_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_north_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_north_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_south_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_south_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_west_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_west_his ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_price_tag' as table_name,max(add_time) as update_end_time from fe_dwd.dwd_shelf_product_price_tag ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_sto_sal_30_days' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_sto_sal_30_days ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_sto_sal_month_start_end' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_sto_sal_month_start_end ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_sto_sal_day30' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_sto_sal_day30 ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_user_day_inc' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_user_day_inc ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_user_perfect_product' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_user_perfect_product ;
-- 以下是表中没有具体的时间信息表的处理，统一以其对应的存储过程的执行结束时间作为更新时间
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_fill_shelf_action_history' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_fill_shelf_action_total_history_two') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_fill_shelf_action_total_history' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_fill_shelf_action_total_history_two') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_ma_shelf_product_temp' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_ma_shelf_product_temp') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_firstday_month_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_users_dayct_month_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_users_dayct_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_users_dayct_week_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_users_dayct_year_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_user_firstday_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_user_firstday_week_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_user_firstday_year_tran' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_op_users_day_stat_nine') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_product_fill_number_sorting' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_product_fill_number_sorting') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_shelf_manager_monitor_result' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dm_shelf_manager_monitor_result') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_op_dim_date' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dwd_op_dim_date_three') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_pub_product_dim_sserp_his' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dwd_op_dim_date_three') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_shelf_machine_slot_history' AS table_name,(SELECT MAX(end_time) FROM fe_dwd.dwd_sf_dw_task_log WHERE task_name='dwd_shelf_machine_slot_history') AS update_end_time  ; 
 
 
CALL sh_process.`sp_sf_dw_task_log` ('dwd_table_data_update_end_time',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('唐进@', @user), @stime);
END