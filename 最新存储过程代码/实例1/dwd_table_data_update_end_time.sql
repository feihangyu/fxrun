CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_table_data_update_end_time`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 更新表的最近一次的更新时间
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_db_machine_shelf_gmv' as table_name,max(add_time) as update_end_time from fe_dm.dm_db_machine_shelf_gmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_group_distribute_dept_rank' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_en_group_distribute_dept_rank ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_emp_distribute_zxwh_mid' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_en_emp_distribute_zxwh_mid ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_en_group_distribute_emp_rank' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_en_group_distribute_emp_rank ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_area_shelfType_kpi_daily' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_area_shelfType_kpi_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_area_shelfType_kpi_weekly' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_area_shelfType_kpi_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_coupon_shelf_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_coupon_shelf_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_discount_activity_shelf_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_discount_activity_shelf_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_group_product_flag' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_group_product_flag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_derived_data_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_derived_data_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_users_all_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_users_all_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_user_stat_info' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_user_stat_info ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_autoshelf_stock_and_sale' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_autoshelf_stock_and_sale ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_new_shelf_suggest_list' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_new_shelf_suggest_list ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_price_sensitivity' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_price_sensitivity ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_fill_last_time' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_product_fill_last_time ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_sku_situation' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_sku_situation ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_type_product_sale_month' as table_name,max(load_time) as update_end_time from fe_dm.dm_op_shelf_type_product_sale_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_pub_area_product_stat' as table_name,max(load_time) as update_end_time from fe_dm.dm_pub_area_product_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_shelf_add_mgmv' as table_name,max(load_time) as update_end_time from fe_dm.dm_shelf_add_mgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_activity_invitation_information' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_activity_invitation_information ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_automachine_slot_product_template' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_automachine_slot_product_template ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_check_base_day_inc' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_check_base_day_inc ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_en_combined_payment_order' as table_name,max(add_time) as update_end_time from fe_dwd.dwd_en_combined_payment_order ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_en_distribute_detail_fx' as table_name,max(add_time_detail) as update_end_time from fe_dwd.dwd_en_distribute_detail_fx ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_en_fx_distribute_to_bdp' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_en_fx_distribute_to_bdp ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_fillorder_requirement_information' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_fillorder_requirement_information ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_group_emp_user_day' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_group_emp_user_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_group_order_coupon_day' as table_name,max(add_time) as update_end_time from fe_dwd.dwd_group_order_coupon_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_group_order_refound_address_day' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_group_order_refound_address_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_group_product_base_day' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_group_product_base_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_group_wallet_log_business' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_group_wallet_log_business ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_lsl_shelf_product_abnormal' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_lsl_shelf_product_abnormal ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_order_item_recent_one_month' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_order_item_recent_one_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_order_item_refund_day' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_order_item_refund_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_order_item_recent_two_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_op_order_and_item_shelf7' as table_name,max(add_time) as update_end_time from fe_dwd.dwd_op_order_and_item_shelf7 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_order_item_refund_real_time' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_order_item_refund_real_time ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_order_refund_item' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_order_refund_item ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_package_information' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_package_information ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_product_base_day_all' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_product_base_day_all ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_product_label_all' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_product_label_all ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_activity_order_shelf_product' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_activity_order_shelf_product ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_order_shelf_product_yht' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_order_shelf_product_yht ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_school_shelf_infornation' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_school_shelf_infornation ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_shelf_first_order_info' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_pub_shelf_first_order_info ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_fill_day_inc_recent_two_month' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_fill_day_inc_recent_two_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_fill_day_inc' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_fill_day_inc ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_base_day_all' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_base_day_all ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_day_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_day_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_machine_fault' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_machine_fault ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_machine_info' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_machine_info ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_machine_slot_type' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_machine_slot_type ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_all' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_all ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_north_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_north_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_west_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_west_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_east_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_east_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_product_day_south_his' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_product_day_south_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_scope_area_all' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_scope_area_all ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_shelf_smart_product_template_information' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_shelf_smart_product_template_information ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_pub_dictionary' as table_name,max(last_update_time) as update_end_time from fe_dwd.dwd_pub_dictionary ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dwd.dwd_user_day_inc' as table_name,max(load_time) as update_end_time from fe_dwd.dwd_user_day_inc ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_weixin_payment' as table_name,max(last_update_time) as update_end_time from feods.d_mp_weixin_payment ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op2_shelf_day_avg_gmv' as table_name,max(load_time) as update_end_time from feods.d_op2_shelf_day_avg_gmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_grade' as table_name,max(load_time) as update_end_time from feods.d_op_shelf_grade ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_area_shelf_open_close_times' as table_name,max(load_time) as update_end_time from feods.d_op_area_shelf_open_close_times ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_active_week' as table_name,max(load_time) as update_end_time from feods.d_op_shelf_active_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_type_flag' as table_name,max(load_time) as update_end_time from feods.d_op_shelf_type_flag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_type_product_sale' as table_name,max(load_time) as update_end_time from feods.d_op_shelf_type_product_sale ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_avgsal7' as table_name,max(load_time) as update_end_time from feods.d_op_sp_avgsal7 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_type_revoke_active_num' as table_name,max(load_time) as update_end_time from feods.d_op_type_revoke_active_num ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_type_revoke_close_day' as table_name,max(load_time) as update_end_time from feods.d_op_type_revoke_close_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_kpi' as table_name,max(add_time) as update_end_time from feods.d_sc_preware_kpi ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_sku_satisfy' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_sku_satisfy ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_shelf_stock_thirty' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_shelf_stock_thirty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_shelf_sale_thirty' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_shelf_sale_thirty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.wt_order_item_twomonth_temp' as table_name,max(last_update_time) as update_end_time from feods.wt_order_item_twomonth_temp ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.op_shelf_week_product_stock_detail_tmp' as table_name,max(load_time) as update_end_time from feods.op_shelf_week_product_stock_detail_tmp ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_warehouse_onload' as table_name,max(last_update_time) as update_end_time from feods.d_sc_warehouse_onload ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_poorderlist_day' as table_name,max(last_update_time) as update_end_time from feods.pj_poorderlist_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_shelf_stock_daily' as table_name,max(last_update_time) as update_end_time from feods.d_sc_shelf_stock_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_prewarehouse_coverage_rate' as table_name,max(add_time) as update_end_time from feods.pj_prewarehouse_coverage_rate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_shelf_level_ab' as table_name,max(last_update_time) as update_end_time from feods.pj_shelf_level_ab ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_shelf_level_ab_df2' as table_name,max(last_update_time) as update_end_time from feods.pj_shelf_level_ab_df2 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_user_week_sale' as table_name,max(last_update_time) as update_end_time from feods.zs_user_week_sale ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.sf_order_item_temp' as table_name,max(last_update_time) as update_end_time from feods.sf_order_item_temp ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.PJ_OUTSTOCK2_DAY' as table_name,max(last_update_time) as update_end_time from feods.PJ_OUTSTOCK2_DAY ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_third_user_balance_his' as table_name,max(add_time) as update_end_time from feods.d_en_third_user_balance_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_area_shelfType_kpi_monthly' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_area_shelfType_kpi_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_AutoShelf_SalesFlag_kpi_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_AutoShelf_SalesFlag_kpi_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_autoshelf_kpi_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_autoshelf_kpi_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_coupon_bi_daily' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_coupon_bi_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_discount_activity_bi_daily' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_discount_activity_bi_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_group_kpi_day' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_group_kpi_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_HighProfit_list_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_HighProfit_list_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_kpi_data_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_kpi_data_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_MarketingTools_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_MarketingTools_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_MarketingTools_weekly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_MarketingTools_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_sectype_kpi_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_sectype_kpi_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_sectype_kpi_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_sectype_kpi_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_shelfInfo_extend' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_shelfInfo_extend ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_shelf_info_daily' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_ma_shelf_info_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_shelf_kpi_detail_monthly' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_shelf_kpi_detail_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.dm_ma_shelf_kpi_weekly' as table_name,max(last_update_time) as update_end_time from feods.dm_ma_shelf_kpi_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_user_suspect' as table_name,max(add_time) as update_end_time from fe_dm.dm_user_suspect ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_fx_daily_num_user_balance' as table_name,max(add_time) as update_end_time from feods.d_en_fx_daily_num_user_balance ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_fx_balance' as table_name,max(add_time) as update_end_time from feods.d_en_fx_balance ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_fx_new_user_daily_balance' as table_name,max(add_time) as update_end_time from feods.d_en_fx_new_user_daily_balance ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_gross_margin_rate_order_month' as table_name,max(last_update_time) as update_end_time from feods.d_en_gross_margin_rate_order_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_gross_margin_rate_user_month' as table_name,max(last_update_time) as update_end_time from feods.d_en_gross_margin_rate_user_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_gross_margin_rate_order_week' as table_name,max(last_update_time) as update_end_time from feods.d_en_gross_margin_rate_order_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_gross_margin_rate_user_week' as table_name,max(last_update_time) as update_end_time from feods.d_en_gross_margin_rate_user_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_last60_buy_users' as table_name,max(add_time) as update_end_time from feods.d_en_last60_buy_users ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_wastage_item' as table_name,max(add_time) as update_end_time from feods.d_en_wastage_item ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_en_order_item_60' as table_name,max(add_time) as update_end_time from feods.d_en_order_item_60 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.sf_third_user_balance_day' as table_name,max(add_time) as update_end_time from feods.sf_third_user_balance_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_high_gross' as table_name,max(add_time) as update_end_time from feods.d_ma_high_gross ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_ma_area_daily' as table_name,max(add_time) as update_end_time from fe_dm.dm_ma_area_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_new_product_daily_statistics' as table_name,max(add_time) as update_end_time from feods.d_ma_new_product_daily_statistics ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_shelf_sale_daily' as table_name,max(last_update_time) as update_end_time from feods.d_ma_shelf_sale_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_shelf_sale_weekly' as table_name,max(last_update_time) as update_end_time from feods.d_ma_shelf_sale_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_shelf_sale_monthly' as table_name,max(last_update_time) as update_end_time from feods.d_ma_shelf_sale_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_tag_num' as table_name,max(add_time) as update_end_time from feods.d_ma_tag_num ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_ma_unsalable' as table_name,max(add_time) as update_end_time from feods.d_ma_unsalable ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_shelf_member_flag' as table_name,max(last_update_time) as update_end_time from feods.zs_shelf_member_flag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_machine_fill_update' as table_name,max(last_update_time) as update_end_time from feods.d_op_machine_fill_update ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_product_fill_update' as table_name,max(last_update_time) as update_end_time from feods.d_op_shelf_product_fill_update ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_new_product_gmv' as table_name,max(last_update_time) as update_end_time from feods.zs_new_product_gmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.shelf_product_14days_stock' as table_name,max(load_time) as update_end_time from feods.shelf_product_14days_stock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.shelf_product_stock_14days' as table_name,max(load_time) as update_end_time from feods.shelf_product_stock_14days ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.shelf_sku_stock_7days_tmp' as table_name,max(load_time) as update_end_time from feods.shelf_sku_stock_7days_tmp ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_current_dynamic_purchase_price' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_current_dynamic_purchase_price ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.wt_monthly_manual_purchase_price' as table_name,max(last_update_time) as update_end_time from feods.wt_monthly_manual_purchase_price ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_shelf_member_flag_history' as table_name,max(last_update_time) as update_end_time from feods.zs_shelf_member_flag_history ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.preware_outbound_monthly' as table_name,max(last_update_time) as update_end_time from feods.preware_outbound_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_outbound_three_day' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_outbound_three_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.preware_outbound_daily' as table_name,max(last_update_time) as update_end_time from feods.preware_outbound_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_outbound_seven_day' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_outbound_seven_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.preware_outbound_weekly' as table_name,max(last_update_time) as update_end_time from feods.preware_outbound_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.preware_fill_daily' as table_name,max(last_update_time) as update_end_time from feods.preware_fill_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.preware_outbound_forteen_day' as table_name,max(last_update_time) as update_end_time from feods.preware_outbound_forteen_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_fill_seven_day' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_fill_seven_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_sales_daily' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_sales_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_preware_sales_fifteen' as table_name,max(last_update_time) as update_end_time from feods.pj_preware_sales_fifteen ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_preware_shelf_sales_thirty' as table_name,max(last_update_time) as update_end_time from feods.pj_preware_shelf_sales_thirty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_shelf_sales_daily' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_shelf_sales_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_preware_sales_seven' as table_name,max(last_update_time) as update_end_time from feods.pj_preware_sales_seven ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_prewarehouse_stock_detail_weekly' as table_name,max(last_update_time) as update_end_time from feods.pj_prewarehouse_stock_detail_weekly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_prewarehouse_stock_detail_monthly' as table_name,max(last_update_time) as update_end_time from feods.pj_prewarehouse_stock_detail_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_product_flag' as table_name,max(last_update_time) as update_end_time from feods.zs_product_flag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_nsale_shelf_product' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_nsale_shelf_product ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_order_over100' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_order_over100 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_order_shelf_product' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_order_shelf_product ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_order_product_qty' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_order_product_qty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_order_user' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_order_user ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_package_product' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_package_product ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_abnormal_package' as table_name,max(add_time) as update_end_time from feods.fjr_abnormal_package ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_zs_add_shelf_damaged' as table_name,max(update_time) as update_end_time from feods.pj_zs_add_shelf_damaged ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_disrate' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_disrate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_shelftype_dgmv' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_shelftype_dgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_dfill' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_dfill ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_shelftype_dfill' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_shelftype_dfill ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_disrate' as table_name,max(add_time) as update_end_time from feods.d_op_sp_disrate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_dgmv' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_dgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_month' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_stock_rate' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_stock_rate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.area_product_user' as table_name,max(add_time) as update_end_time from feods.area_product_user ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.area_product_countuser' as table_name,max(add_time) as update_end_time from feods.area_product_countuser ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_association_analysis' as table_name,max(add_time) as update_end_time from feods.fjr_association_analysis ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_avgqty_fill_dayst' as table_name,max(add_time) as update_end_time from feods.fjr_avgqty_fill_dayst ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_avgqty_fill_dayst_stat' as table_name,max(add_time) as update_end_time from feods.fjr_avgqty_fill_dayst_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_lo_shelf_manager_line_day' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_lo_shelf_manager_line_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_preware_monthly_kpi' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_preware_monthly_kpi ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_area_fulltime_reached_index_statistics' as table_name,max(last_update_time) as update_end_time from feods.D_LO_area_fulltime_reached_index_statistics ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_campus_manager_level' as table_name,max(last_update_time) as update_end_time from feods.D_LO_campus_manager_level ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_fulltime_manager_history_record' as table_name,max(last_update_time) as update_end_time from feods.D_LO_fulltime_manager_history_record ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_manager_performance_report_everyday_for_month' as table_name,max(last_update_time) as update_end_time from feods.D_LO_manager_performance_report_everyday_for_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_node_monitor_data_after_cleanout' as table_name,max(last_update_time) as update_end_time from feods.D_LO_node_monitor_data_after_cleanout ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_prewarehouse_fill_order_item_month' as table_name,max(last_update_time) as update_end_time from feods.D_LO_prewarehouse_fill_order_item_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_school_order_item' as table_name,max(last_update_time) as update_end_time from feods.D_LO_school_order_item ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_shelf_check_for_month_label' as table_name,max(last_update_time) as update_end_time from feods.D_LO_shelf_check_for_month_label ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_shelf_check_schedule_query' as table_name,max(last_update_time) as update_end_time from feods.D_LO_shelf_check_schedule_query ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_shelf_fill_timeliness_detail' as table_name,max(last_update_time) as update_end_time from feods.D_LO_shelf_fill_timeliness_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_LO_shelf_manager_info_history_balance' as table_name,max(last_update_time) as update_end_time from feods.D_LO_shelf_manager_info_history_balance ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MA_area_product_sales_data_daily' as table_name,max(last_update_time) as update_end_time from feods.D_MA_area_product_sales_data_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_boss_data_vending_machine' as table_name,max(add_time) as update_end_time from feods.d_mp_boss_data_vending_machine ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_day_sale_vending_machine' as table_name,max(add_time) as update_end_time from feods.d_mp_day_sale_vending_machine ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_user_vending_machine' as table_name,max(add_time) as update_end_time from feods.d_mp_user_vending_machine ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_boss_data_shelf6' as table_name,max(add_time) as update_end_time from feods.d_mp_boss_data_shelf6 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_day_sale_shelf6' as table_name,max(add_time) as update_end_time from feods.d_mp_day_sale_shelf6 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_user_shelf6' as table_name,max(add_time) as update_end_time from feods.d_mp_user_shelf6 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_daily_shelf_stock_stag_detail' as table_name,max(last_update_time) as update_end_time from feods.d_mp_daily_shelf_stock_stag_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_daily_shelf_stock_stag' as table_name,max(last_update_time) as update_end_time from feods.d_mp_daily_shelf_stock_stag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MP_finance_month_income_result' as table_name,max(last_update_time) as update_end_time from feods.D_MP_finance_month_income_result ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MP_shelf_system_temp_table_main' as table_name,max(last_update_time) as update_end_time from feods.D_MP_shelf_system_temp_table_main ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MP_finance_statement_log' as table_name,max(last_update_time) as update_end_time from feods.D_MP_finance_statement_log ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_shelf_monitor' as table_name,max(last_update_time) as update_end_time from feods.d_mp_shelf_monitor ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MP_CMBC_payment' as table_name,max(last_update_time) as update_end_time from feods.D_MP_CMBC_payment ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_ssf_payment' as table_name,max(last_update_time) as update_end_time from feods.d_mp_ssf_payment ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MP_epay_shelf_detail' as table_name,max(last_update_time) as update_end_time from feods.D_MP_epay_shelf_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_mp_week_kpi_monitor' as table_name,max(last_update_time) as update_end_time from feods.d_mp_week_kpi_monitor ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_active_result' as table_name,max(last_update_time) as update_end_time from feods.d_sc_active_result ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_balance' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_balance ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_daily_report' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_daily_report ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_fill_frequency' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_fill_frequency ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_preware_wave_cycle' as table_name,max(last_update_time) as update_end_time from feods.d_sc_preware_wave_cycle ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_profit_monthly_shelf_product' as table_name,max(last_update_time) as update_end_time from feods.d_sc_profit_monthly_shelf_product ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_shelf_packages_onsale' as table_name,max(last_update_time) as update_end_time from feods.d_sc_shelf_packages_onsale ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_shelf_packages' as table_name,max(last_update_time) as update_end_time from feods.d_sc_shelf_packages ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_shelf_promote_result' as table_name,max(last_update_time) as update_end_time from feods.d_sc_shelf_promote_result ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_warehouse_outbound_forteen' as table_name,max(last_update_time) as update_end_time from feods.d_sc_warehouse_outbound_forteen ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_outbound_monthly_total' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_outbound_monthly_total ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_warehouse_outbound_daily' as table_name,max(last_update_time) as update_end_time from feods.d_sc_warehouse_outbound_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_warehouse_outbound_forteen_total' as table_name,max(last_update_time) as update_end_time from feods.d_sc_warehouse_outbound_forteen_total ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_sc_warehouse_stock_monthly' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_sc_warehouse_stock_monthly ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_warehouse_preware_stock_outbound' as table_name,max(last_update_time) as update_end_time from feods.d_sc_warehouse_preware_stock_outbound ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_warehouse_sku_shelf_cnt' as table_name,max(last_update_time) as update_end_time from feods.d_sc_warehouse_sku_shelf_cnt ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_sc_oms_stock_daily' as table_name,max(last_update_time) as update_end_time from feods.d_sc_oms_stock_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_enterprice_gmv_daily' as table_name,max(update_time) as update_end_time from feods.pj_enterprice_gmv_daily ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_fill_order_efficiency' as table_name,max(last_update_time) as update_end_time from feods.pj_fill_order_efficiency ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.D_MP_Lead_warehouse_temp_table_main' as table_name,max(last_update_time) as update_end_time from feods.D_MP_Lead_warehouse_temp_table_main ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_flag5_product' as table_name,max(add_time) as update_end_time from feods.fjr_flag5_product ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_flag5_shelf' as table_name,max(add_time) as update_end_time from feods.fjr_flag5_shelf ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_area_product_satrate' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_area_product_satrate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_monitor_area' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_monitor_area ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_monitor' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_monitor ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_area_top10_uprate_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_area_top10_uprate_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_area_top10_uprate_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_area_top10_uprate_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_new_out_storate' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_new_out_storate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_np_success_rate_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_np_success_rate_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_outlet_rate' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_outlet_rate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_sale_vs_stock_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_sale_vs_stock_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_sale_vs_stock_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_sale_vs_stock_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_shelf_gmv_uprate_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_shelf_gmv_uprate_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_shelf_gmv_uprate_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_shelf_gmv_uprate_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi2_unsku_shelf' as table_name,max(add_time) as update_end_time from feods.fjr_kpi2_unsku_shelf ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_stat_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_stat_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_stock_his' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_stock_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_monitor' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_monitor ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_sale_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_sale_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_stat_day' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_stat_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_stat_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_stat_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_fill_nday' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_fill_nday ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_sale_day' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_sale_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_sale_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_sale_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_slot_sale_day' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_slot_sale_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock_m7' as table_name,max(add_time) as update_end_time from feods.d_op_offstock_m7 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock_s7p' as table_name,max(add_time) as update_end_time from feods.d_op_offstock_s7p ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock_slot' as table_name,max(add_time) as update_end_time from feods.d_op_offstock_slot ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_shelf_stock_day' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_shelf_stock_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock_area7' as table_name,max(add_time) as update_end_time from feods.d_op_offstock_area7 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_slot_stock_day' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_slot_stock_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock_s7' as table_name,max(add_time) as update_end_time from feods.d_op_offstock_s7 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock_s7_key' as table_name,max(add_time) as update_end_time from feods.d_op_offstock_s7_key ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_product_sale_stock_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_product_sale_stock_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_product_sale_stock_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_product_sale_stock_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi3_shelf7_product_sale_stock_day' as table_name,max(add_time) as update_end_time from feods.fjr_kpi3_shelf7_product_sale_stock_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_area_product_sat_rate' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_area_product_sat_rate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_avggmv_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_avggmv_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_ns_avggmv_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_ns_avggmv_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_avggmv_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_avggmv_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_ns_avggmv_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_ns_avggmv_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_monitor' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_monitor ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_np_flag5_sto' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_np_flag5_sto ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_np_gmv_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_np_gmv_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_gmv_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_gmv_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_np_sal_sto_month' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_np_sal_sto_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_gmv_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_gmv_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_np_sal_sto_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_np_sal_sto_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_np_gmv_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_np_gmv_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_np_out_week' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_np_out_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_shelf_nps' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_shelf_nps ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_sto_val_rate' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_sto_val_rate ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_kpi_unsku' as table_name,max(add_time) as update_end_time from feods.fjr_kpi_unsku ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_manager_shelf_statistic_result' as table_name,max(last_update_time) as update_end_time from feods.pj_manager_shelf_statistic_result ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_newshelf_quality' as table_name,max(add_time) as update_end_time from feods.fjr_newshelf_quality ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_newshelf_stat' as table_name,max(add_time) as update_end_time from feods.fjr_newshelf_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_cal_fill_days' as table_name,max(last_update_time) as update_end_time from feods.d_op_cal_fill_days ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_product_dim_sserp' as table_name,max(last_update_time) as update_end_time from feods.zs_product_dim_sserp ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_ds7p_sal_fil' as table_name,max(add_time) as update_end_time from feods.d_op_ds7p_sal_fil ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_effective_stock' as table_name,max(add_time) as update_end_time from feods.d_op_effective_stock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_tot_stat' as table_name,max(add_time) as update_end_time from feods.d_op_tot_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_false_stock_danger_level' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_false_stock_danger_level ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_fill_day_sale_qty' as table_name,max(last_update_time) as update_end_time from feods.d_op_fill_day_sale_qty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_valid_sto_sal_day30' as table_name,max(last_update_time) as update_end_time from feods.d_op_valid_sto_sal_day30 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_machine_online_detail' as table_name,max(add_time) as update_end_time from feods.d_op_machine_online_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_machine_online_stat' as table_name,max(add_time) as update_end_time from feods.d_op_machine_online_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_machine_online_shelf' as table_name,max(add_time) as update_end_time from feods.d_op_machine_online_shelf ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offshelf' as table_name,max(add_time) as update_end_time from feods.d_op_offshelf ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_p_offstock' as table_name,max(add_time) as update_end_time from feods.d_op_p_offstock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_offstock_his' as table_name,max(add_time) as update_end_time from feods.d_op_sp_offstock_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_dc_reqsto' as table_name,max(add_time) as update_end_time from feods.d_op_dc_reqsto ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_pwh_reqsto' as table_name,max(add_time) as update_end_time from feods.d_op_pwh_reqsto ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_offstock' as table_name,max(add_time) as update_end_time from feods.d_op_sp_offstock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_s_offstock' as table_name,max(add_time) as update_end_time from feods.d_op_s_offstock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_offstock' as table_name,max(add_time) as update_end_time from feods.d_op_offstock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_stock_detail' as table_name,max(add_time) as update_end_time from feods.d_op_sp_stock_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_package_shelf' as table_name,max(last_update_time) as update_end_time from feods.d_op_package_shelf ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_package_config' as table_name,max(last_update_time) as update_end_time from feods.d_op_package_config ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_dim_product_area_normal' as table_name,max(add_time) as update_end_time from feods.d_op_dim_product_area_normal ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_stat_month' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_stat_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_shelf_sal_month' as table_name,max(add_time) as update_end_time from feods.d_op_product_shelf_sal_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_shelf_stat' as table_name,max(add_time) as update_end_time from feods.d_op_product_shelf_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_sal_month_large' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_sal_month_large ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_shelf_dam_month' as table_name,max(add_time) as update_end_time from feods.d_op_product_shelf_dam_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_shelf_sal_month_large' as table_name,max(add_time) as update_end_time from feods.d_op_product_shelf_sal_month_large ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_shelf_sto_month' as table_name,max(add_time) as update_end_time from feods.d_op_product_shelf_sto_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sf_fillorder_requirement_item_his' as table_name,max(add_time) as update_end_time from feods.d_op_sf_fillorder_requirement_item_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sf_fillorder_requirement_his' as table_name,max(add_time) as update_end_time from feods.d_op_sf_fillorder_requirement_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf7_area_product_sale_month' as table_name,max(add_time) as update_end_time from feods.d_op_shelf7_area_product_sale_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf7_area_product_sale_day' as table_name,max(add_time) as update_end_time from feods.d_op_shelf7_area_product_sale_day ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf7_area_product_sale_week' as table_name,max(add_time) as update_end_time from feods.d_op_shelf7_area_product_sale_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf7_area_product_stat' as table_name,max(add_time) as update_end_time from feods.d_op_shelf7_area_product_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_gmv_analysis' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_gmv_analysis ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_firstfill' as table_name,max(add_time) as update_end_time from feods.d_op_shelf_firstfill ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_info_month' as table_name,max(add_time) as update_end_time from feods.d_op_shelf_info_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_fill3_detail' as table_name,max(add_time) as update_end_time from feods.d_op_fill3_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_info' as table_name,max(add_time) as update_end_time from feods.d_op_shelf_info ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_product_fill_update_his' as table_name,max(last_update_time) as update_end_time from feods.d_op_shelf_product_fill_update_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_smart_shelf_fill_update_his' as table_name,max(last_update_time) as update_end_time from feods.d_op_smart_shelf_fill_update_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_auto_push_fill_date_his' as table_name,max(last_update_time) as update_end_time from feods.d_op_auto_push_fill_date_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_smart_shelf_fill_update' as table_name,max(last_update_time) as update_end_time from feods.d_op_smart_shelf_fill_update ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_auto_push_fill_date' as table_name,max(last_update_time) as update_end_time from feods.d_op_auto_push_fill_date ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_high_stock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_high_stock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_high_stock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_high_stock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_area_high_stock' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_area_high_stock ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_trans_out_his' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_trans_out_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_trans_out_list' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_shelf_product_trans_out_list ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_stock_real_time_monitor' as table_name,max(last_update_time) as update_end_time from feods.d_op_shelf_stock_real_time_monitor ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_s_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_s_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_user_month_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_user_month_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_shelfcross_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_shelfcross_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_uptolm_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_uptolm_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_u_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_u_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_month_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_month_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_su_shelf_month_stat' as table_name,max(add_time) as update_end_time from feods.d_op_su_shelf_month_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_s7p_detail' as table_name,max(add_time) as update_end_time from feods.d_op_s7p_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_slot_his' as table_name,max(add_time) as update_end_time from feods.d_op_slot_his ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_s7p_nslot' as table_name,max(add_time) as update_end_time from feods.d_op_s7p_nslot ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_smart_log' as table_name,max(add_time) as update_end_time from feods.d_op_smart_log ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_sale_detail' as table_name,max(add_time) as update_end_time from feods.d_op_sp_sale_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_sal_sto_detail' as table_name,max(add_time) as update_end_time from feods.d_op_sp_sal_sto_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_shelf7_stock3' as table_name,max(add_time) as update_end_time from feods.d_op_sp_shelf7_stock3 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sp_stock_detail_after' as table_name,max(add_time) as update_end_time from feods.d_op_sp_stock_detail_after ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_sto_cdays' as table_name,max(update_time) as update_end_time from feods.d_op_sto_cdays ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_valid_danger_flag' as table_name,max(last_update_time) as update_end_time from fe_dm.dm_op_valid_danger_flag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_prewarehouse_stock_detail' as table_name,max(last_update_time) as update_end_time from feods.pj_prewarehouse_stock_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_price_sensitive_stat_month_nation' as table_name,max(add_time) as update_end_time from feods.fjr_price_sensitive_stat_month_nation ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_price_sensitive_stat_month' as table_name,max(add_time) as update_end_time from feods.fjr_price_sensitive_stat_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_user_miser_stat' as table_name,max(add_time) as update_end_time from feods.fjr_user_miser_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_price_sensitive_stat_nation' as table_name,max(add_time) as update_end_time from feods.fjr_price_sensitive_stat_nation ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_price_sensitive_stat' as table_name,max(add_time) as update_end_time from feods.fjr_price_sensitive_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_product_list_manager_week' as table_name,max(add_time) as update_end_time from feods.fjr_product_list_manager_week ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_archives' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_archives ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelfs_dstat' as table_name,max(add_time) as update_end_time from feods.d_op_shelfs_dstat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_board' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_board ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelfs_area' as table_name,max(add_time) as update_end_time from feods.d_op_shelfs_area ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_board_month' as table_name,max(add_time) as update_end_time from feods.d_op_shelf_board_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_shelftype_mgmv' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_shelftype_mgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_mgmv' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_mgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_dgmv' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_dgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_wgmv' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_wgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_product_area_shelftype_wgmv' as table_name,max(add_time) as update_end_time from feods.d_op_product_area_shelftype_wgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_wgmv' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_wgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_mgmv' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_mgmv ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'fe_dm.dm_op_shelf_product_danger_month' as table_name,max(add_time) as update_end_time from fe_dm.dm_op_shelf_product_danger_month ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_area_stock_detail' as table_name,max(add_time) as update_end_time from feods.zs_area_stock_detail ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_product_price_tag' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_product_price_tag ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_sal_base' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_sal_base ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_profile' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_profile ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_shelf_user_duration' as table_name,max(add_time) as update_end_time from feods.fjr_shelf_user_duration ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_shelf_machine_slot' as table_name,max(add_time) as update_end_time from feods.d_op_shelf_machine_slot ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.d_op_slot_change_record' as table_name,max(add_time) as update_end_time from feods.d_op_slot_change_record ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_product_price_salqty' as table_name,max(add_time) as update_end_time from feods.fjr_product_price_salqty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_subtype_price_stat' as table_name,max(add_time) as update_end_time from feods.fjr_subtype_price_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_area_product_pq4' as table_name,max(add_time) as update_end_time from feods.fjr_area_product_pq4 ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_subtype_price_salqty' as table_name,max(add_time) as update_end_time from feods.fjr_subtype_price_salqty ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.fjr_users_day_stat' as table_name,max(add_time) as update_end_time from feods.fjr_users_day_stat ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.pj_warehouse_product_presence' as table_name,max(last_update_time) as update_end_time from feods.pj_warehouse_product_presence ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_manager_shelf_performance_label' as table_name,max(last_update_time) as update_end_time from feods.zs_manager_shelf_performance_label ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_shelf_manager_check_monitor_point' as table_name,max(last_update_time) as update_end_time from feods.zs_shelf_manager_check_monitor_point ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_shelf_manager_suspect_problem_label' as table_name,max(last_update_time) as update_end_time from feods.zs_shelf_manager_suspect_problem_label ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
select current_date,'feods.zs_shelf_product_flag' as table_name,max(last_update_time) as update_end_time from feods.zs_shelf_product_flag ; 
-- 以下是表中没有具体的时间信息表的处理，统一以其对应的存储过程的执行结束时间作为更新时间
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_shelf_product_fill_suggest_label' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='dm_op_shelf_product_fill_suggest_label') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dm.dm_op_shelf_product_start_fill_label' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='dm_op_shelf_product_start_fill_label') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_group_activity_order_day' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='dwd_group_activity_order_day') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_shelf_machine_second_info' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='dwd_shelf_machine_second_info') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_staff_score_distribute_result' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='dwd_staff_score_distribute_detail') AS update_end_time  ; 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'fe_dwd.dwd_staff_score_distribute_detail' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='dwd_staff_score_distribute_detail') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_area_sale_dashboard_history' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_area_sale_dashboard') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_area_sale_dashboard' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_area_sale_dashboard') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_buhuo_shelf_action_history' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_buhuo_shelf_action') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_buhuo_shelf_action_total_history' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_buhuo_shelf_action') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_city_unsalable_his' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_city_unsalable_his') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_loss_value' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_loss_value') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_shelf_check_month' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_check_month') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_shelf_level_ab_day' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_level_ab') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_shelf_level_ab_week' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_level_ab') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_shelf_machine_slot_history' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_machine_slot') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_shelf_machine_sale_total' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_machine_slot') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_zs_goods_damaged' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_zs_goods_damaged') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_shelf_grade' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_zs_shelf_grade') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_en_new_user_balance' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='prc_d_en_order_user') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_en_user_channle_first' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='prc_d_en_order_user') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_en_order_user' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='prc_d_en_order_user') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_en_org_area' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='prc_d_en_org_area') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_ma_next_week_birthday' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='prc_d_ma_next_week_birthday') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.shelf_product_stock_7days_tmp' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='shelf_sku_stock_7days_tmp') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.user_research_day' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_member_research_crr_2') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.user_research' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_member_research_crr_4') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.D_MA_area_history_sales_data_daily' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_D_MA_area_history_sales_data_daily') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_kpi_avggmv_month_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_kpi_monitor') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_kpi_avggmv_week_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_kpi_monitor') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_op_dim_date' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_op_dim_date') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_product_dim_sserp_his' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_op_dim_date') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_op_sp_bak' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_op_sf_shelf_last62') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_op_sf_shelf_last62' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_op_sf_shelf_last62') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.d_op_sp_avgsal30' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_op_sp_avgsal30') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_boss_operation_kpi_day' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_pj_boss_operation_kpi_day') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.pj_boss_operation_kpi_stock_detail_day' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_pj_boss_operation_kpi_stock_detail_day') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.csl_prewarehouse_check_detail' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_prewarehouse_manager_salary_scheme') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.csl_prewarehouse_order_detail' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_prewarehouse_manager_salary_scheme') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_user_firstday_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_user_firstday_year_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_users_dayct_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_users_dayct_year_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_user_firstday_month_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_user_firstday_week_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_users_dayct_month_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.fjr_users_dayct_week_tran' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_users_day_stat') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_fill_operation_kpi_for_management' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_zs_fill_operation_kpi_for_management') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_product_fill_number_sorting' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_zs_product_fill_number_sorting') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_shelf_manager_monitor_result' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sp_zs_shelf_manager_monitor_result') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_area_product_sale_flag' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_area_product_sale_flag') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_shelf_flag' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_flag') AS update_end_time  ; 
 
replace into fe_dwd.dwd_table_data_update_end_time(sdate,table_name,update_end_time) 
SELECT CURRENT_DATE,'feods.zs_shelf_flag_his' AS table_name,(SELECT MAX(createtime) FROM feods.sf_dw_task_log WHERE task_name='sh_shelf_flag') AS update_end_time  ; 
 
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_table_data_update_end_time',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('唐进@', @user, @timestamp));
    END