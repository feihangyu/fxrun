CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_datax_table_check_rows_num2_not_fe`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 全量同步表 实例1  3sec
REPLACE INTO fe_dwd.`dwd_datax_table_check_rows_num_not_fe`(sdate,table_name,nums)
-- 30sec 全量
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_bill_check' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_bill_check                                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_db_machine_shelf_gmv' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_db_machine_shelf_gmv                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_new_user_balance' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_new_user_balance                          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_order_user' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_order_user                                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_user_channle_first' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_user_channle_first                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_area_product_type_sku_limit_insert' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_area_product_type_sku_limit_insert        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_false_stock_danger_level' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_false_stock_danger_level                  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_product_fill_update2' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_shelf_product_fill_update2                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_add_mgmv' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_add_mgmv                               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_auto_shelf_template' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_auto_shelf_template                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_auto_shelf_undock_gmv_insert' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_auto_shelf_undock_gmv_insert               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_city_business' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_city_business                              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_count_process_aim_table_size_from_one' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_count_process_aim_table_size_from_one      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_csm_product_vote_submit_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_csm_product_vote_submit_all                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_en_org_area' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_en_org_area                                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_group_emp_user_day' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_emp_user_day                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_group_exchange_card' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_exchange_card                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_group_product_base_day' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_product_base_day                     UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_order_refund_item' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_order_refund_item                          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_package_information' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_package_information                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_prewarehouse_base_day' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_prewarehouse_base_day                      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_product_base_day_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_product_base_day_all                       UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_product_label_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_product_label_all                          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_auto_shelf_undock_insert' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_auto_shelf_undock_insert               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_comb_pay_without_weixin_result' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_comb_pay_without_weixin_result         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_dictionary' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_dictionary                             UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_school_shelf_infornation' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_school_shelf_infornation               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_shelf_first_order_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_shelf_first_order_info                 UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_supplier_machine_bill_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_supplier_machine_bill_all              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_warehouse_business_area' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_warehouse_business_area                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all     UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sc_bdp_warehouse_receive_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sc_bdp_warehouse_receive_detail            UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_base_day_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_base_day_all                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_machine_fault' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_machine_fault                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_machine_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_machine_info                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_machine_second_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_machine_second_info                  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_machine_slot_type' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_machine_slot_type                    UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_product_day_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_product_day_all                      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_product_weeksales_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_product_weeksales_detail WHERE year_id='2021' UNION ALL  -- 实例1只保留2020年数据
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_smart_product_template_information' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_smart_product_template_information   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_transaction_exception_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_transaction_exception_info           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_gross_margin_rate_order_month' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_gross_margin_rate_order_month             UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_gross_margin_rate_order_week' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_gross_margin_rate_order_week              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_gross_margin_rate_user_month' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_gross_margin_rate_user_month              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_gross_margin_rate_user_week' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_en_gross_margin_rate_user_week               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_en_org_address_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_en_org_address_info                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_ma_tag_num' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_ma_tag_num                                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_mp_purchase_sell_stock_summary' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_mp_purchase_sell_stock_summary               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_fill_day_sale_qty' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_fill_day_sale_qty                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_op_load_dim' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_op_load_dim                                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pub_shelfs_area' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pub_shelfs_area                              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_info' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_shelf_info                                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_sp_avgsal30' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_sp_avgsal30                               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_sp_shelf7_stock3' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_sp_shelf7_stock3                          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_abnormal_nsale_shelf_product' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_abnormal_nsale_shelf_product              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pub_shelf_archives' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pub_shelf_archives                           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pub_shelf_board' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pub_shelf_board                              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_materialgroup_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_materialgroup_l                 UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_material' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_material                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_material_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_material_l                      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_stockstatus_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_stockstatus_l                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_stock' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_stock                           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_stock_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_stock_l                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_supplierbase' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_supplierbase                    UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_supplier_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_supplier_l                      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_org_organizations_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_org_organizations_l                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_mrappentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_mrappentry                     UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_mrapp' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_mrapp                          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_mrbentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_mrbentry                       UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_mrb' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_mrb                            UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_poorderentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_poorderentry                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_poorderentry_f' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_poorderentry_f                 UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_poorder' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_poorder                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_pricelistentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_pricelistentry                 UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_pricelist' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_pricelist                      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_receiveentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_receiveentry                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_pur_receive' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_pur_receive                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_instockentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_instockentry                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_instock' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_instock                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_inventory' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_inventory                      UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_outstockapply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_outstockapply                  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_stktransferappentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_stktransferappentry            UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_stktransferappentry_e' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_stktransferappentry_e          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_stktransferapp' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_stktransferapp                 UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_v_bd_buyer_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_v_bd_buyer_l                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bd_supplier' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bd_supplier                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_bgj_stockfqty' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bgj_stockfqty  WHERE load_time>=CURRENT_DATE UNION ALL  -- 实例2是每天结存
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_area_product_sale_flag' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_area_product_sale_flag                       UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_group_phone_area' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_phone_area                           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_flag' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_flag                                   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_member_flag' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_member_flag                            UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_product_flag' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_product_flag                          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_check_extend_recent_62' AS table_name,  COUNT(1) AS nums FROM fe_dwd.dwd_check_extend_recent_62                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_valid_danger_flag' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_op_valid_danger_flag                        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_product_area_pool_item' AS table_name,  COUNT(1) AS nums FROM fe_dwd.dwd_pub_product_area_pool_item           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_su_s_stat' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_op_su_s_stat           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_su_u_stat' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_op_su_u_stat           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_city_weather_day' AS table_name,  COUNT(1) AS nums FROM fe_dwd.dwd_shelf_city_weather_day           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_city_weather_day_hour' AS table_name,  COUNT(1) AS nums FROM fe_dwd.dwd_shelf_city_weather_day_hour           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_supplier_machine_apply_settle_bill' AS table_name,  COUNT(1) AS nums FROM fe_dwd.dwd_pub_supplier_machine_apply_settle_bill           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_price_sensitivity' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_op_shelf_price_sensitivity           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_sku_situation' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_op_shelf_sku_situation   UNION ALL 
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_product_sales_30day' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_en_product_sales_30day   UNION ALL 
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_user_product_label' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_en_user_product_label   UNION ALL 
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_en_product_label' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_en_product_label   UNION ALL 
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_su_stat' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_op_su_stat          ;
-- 增量同步表 实例1  3sec
REPLACE INTO fe_dwd.`dwd_datax_table_check_rows_num_not_fe`(sdate,table_name,nums)
-- 增量  3min
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_lo_area_performance_report_everyday' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_lo_area_performance_report_everyday  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_lo_fill_for_month_label' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_lo_fill_for_month_label              UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_ma_highprofit_list_monthly' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_ma_highprofit_list_monthly           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_ma_shelfInfo_extend' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_ma_shelfInfo_extend                  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_ma_sp_plc' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_ma_sp_plc                            UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_ma_sp_stopfill' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_ma_sp_stopfill                       UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_auto_push_fill_date2_his' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_auto_push_fill_date2_his          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_new_shelf_suggest_list' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_new_shelf_suggest_list            UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_order_sku_relation' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_order_sku_relation                UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_product_fill_suggest_label' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_shelf_product_fill_suggest_label  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_product_start_fill_label' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_shelf_product_start_fill_label    UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pub_area_product_stat' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pub_area_product_stat   WHERE sdate>= SUBDATE(CURDATE(),62)   UNION ALL  -- 实例1保留62天数据
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pub_third_user_balance_day' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pub_third_user_balance_day           UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_mgmv' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_mgmv   WHERE month_id>='2020-09' UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_wgmv' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_wgmv    WHERE sdate>='2020-09-01' UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_user_suspect' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_user_suspect                         UNION ALL
-- select 'fe_dwd.dwd_activity_invitation_information' as table_name, count(1) as nums from fe_dwd.dwd_activity_invitation_information    union all
-- select 'fe_temp.dwd_check_base_day_inc' as table_name, count(1) as nums from fe_temp.dwd_check_base_day_inc                union all
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_en_combined_payment_order' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_en_combined_payment_order          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_en_distribute_detail_fx' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_en_distribute_detail_fx            UNION ALL
-- select 'fe_temp.dwd_fill_day_inc' as table_name, count(1) as nums from fe_temp.dwd_fill_day_inc                      union all
-- select 'fe_temp.dwd_fill_day_inc_recent_two_month' as table_name, count(1) as nums from fe_temp.dwd_fill_day_inc_recent_two_month     union all
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_group_order_coupon_day' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_order_coupon_day             UNION ALL
-- select 'fe_temp.dwd_group_order_refound_address_day' as table_name, count(1) as nums from fe_temp.dwd_group_order_refound_address_day   union all
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_group_wallet_log_business' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_wallet_log_business  WHERE load_time>=SUBDATE(CURRENT_DATE,7)        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_lo_order_logistics_task_base_all' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_lo_order_logistics_task_base_all   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_op_out_of_system_order_yht' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_op_out_of_system_order_yht         UNION ALL
-- select 'fe_temp.dwd_order_item_refund_day' as table_name, count(1) as nums from fe_temp.dwd_order_item_refund_day             union all
-- select 'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name, count(1) as nums from fe_dwd.dwd_pub_order_item_recent_two_month    union all
-- select 'fe_temp.dwd_pub_activity_order_shelf_product' as table_name, count(1) as nums from fe_temp.dwd_pub_activity_order_shelf_product  union all
-- select 'fe_temp.dwd_pub_order_item_recent_two_month' as table_name, count(1) as nums from fe_temp.dwd_pub_order_item_recent_two_month   union all
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_order_shelf_product_yht' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_order_shelf_product_yht        UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sc_bdp_warehouse_shipment_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sc_bdp_warehouse_shipment_detail   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sc_bdp_warehouse_stock_daily' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sc_bdp_warehouse_stock_daily       UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_check_recent62' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_check_recent62               UNION ALL
-- select 'fe_temp.dwd_shelf_day_his' as table_name, count(1) as nums from fe_temp.dwd_shelf_day_his                     union all
-- select 'fe_temp.dwd_user_day_inc' as table_name, count(1) as nums from fe_temp.dwd_user_day_inc                      union all
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_dv_emp_org' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_dv_emp_org                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_en_emp_org' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_en_emp_org                         UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_mp_cmbc_payment' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_mp_cmbc_payment                    UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_mp_epay_shelf_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_mp_epay_shelf_detail               UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_mp_ssf_payment' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_mp_ssf_payment                     UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pub_shelf_board_month' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pub_shelf_board_month  UNION ALL  
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_product_stock_detail_after' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_product_stock_detail_after  WHERE month_id >=DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m')  UNION ALL    -- 实例1只保留两个月份数据
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_shelf_product_stock_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_shelf_product_stock_detail  WHERE month_id >=DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m') UNION ALL   -- 实例1只保留两个月份数据
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_mongo_shelf_manager_behavior_log' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_mongo_shelf_manager_behavior_log   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_area_sale_dashboard' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_area_sale_dashboard WHERE sdate>=SUBDATE(CURDATE(),40) UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_pj_zs_goods_damaged' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_pj_zs_goods_damaged                  UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sap_pmp_hos_emp_base_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sap_pmp_hos_emp_base_info          UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_sserp_t_stk_outstockapplyentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_outstockapplyentry     UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_flag_his' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_flag_his                       UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_manager_check_monitor_point' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_manager_check_monitor_point    UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_shelf_manager_suspect_problem_label' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_manager_suspect_problem_label  UNION ALL 
SELECT CURRENT_DATE AS sdate, 'fe_dm.dm_sc_warehouse_preware_stock_outbound' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_sc_warehouse_preware_stock_outbound where sdate>=subdate(current_date,7) UNION ALL -- 实例1只保留近7天的数据
SELECT CURRENT_DATE AS sdate, 'fe_dwd.dwd_group_dictionary' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_group_dictionary    UNION ALL
SELECT CURRENT_DATE AS sdate, 'fe_dwd.dwd_sserp_t_bas_billtype' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bas_billtype    UNION ALL
SELECT CURRENT_DATE AS sdate, 'fe_dwd.dwd_sserp_t_bas_billtype_l' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_bas_billtype_l    UNION ALL
SELECT CURRENT_DATE AS sdate, 'fe_dwd.dwd_sserp_t_stk_misdelivery' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_misdelivery    UNION ALL
SELECT CURRENT_DATE AS sdate, 'fe_dwd.dwd_sserp_t_stk_misdeliveryentry' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sserp_t_stk_misdeliveryentry    UNION ALL
SELECT CURRENT_DATE AS sdate, 'fe_dm.dm_pub_shelf_grade' AS table_name,  COUNT(1) AS nums FROM fe_dm.dm_pub_shelf_grade          union all
select current_date as sdate, 'fe_dwd.dwd_mp_auto_shelf_transactions' as table_name,count(1) as nums from fe_dwd.dwd_mp_auto_shelf_transactions  union all
select current_date as sdate, 'fe_dm.dm_sc_preware_daily_report' as table_name,count(1) as nums from fe_dm.dm_sc_preware_daily_report  where sdate>=subdate(current_date,10) union all  -- 实例1只保留近30天数据，但是数据量很大只统计近10天数据
select current_date as sdate, 'fe_dwd.dwd_inv_lot_att_from_bdp' as table_name,count(1) as nums from fe_dwd.dwd_inv_lot_att_from_bdp  union all
select current_date as sdate, 'fe_dwd.dwd_inv_lot_loc_id_from_bdp' as table_name,count(1) as nums from fe_dwd.dwd_inv_lot_loc_id_from_bdp  union all
select current_date as sdate, 'fe_dwd.dwd_pub_discount_card_refund' as table_name,count(1) as nums from fe_dwd.dwd_pub_discount_card_refund  union all
select current_date as sdate, 'fe_dm.dm_op_area_product_level' as table_name,count(1) as nums from fe_dm.dm_op_area_product_level  union all
select current_date as sdate, 'fe_dm.dm_op_business_product_max_source' as table_name,count(1) as nums from fe_dm.dm_op_business_product_max_source  union all
select current_date as sdate, 'fe_dwd.dwd_pub_out_of_system_shelf_day_his' as table_name,count(1) as nums from fe_dwd.dwd_pub_out_of_system_shelf_day_his  union all
select current_date as sdate, 'fe_dm.dm_lo_apply_shelf_mid' as table_name,count(1) as nums from fe_dm.dm_lo_apply_shelf_mid  union all
select  current_date as sdate,'fe_dm.dm_lo_zone_daily_data' as table_name, count(1) as nums from fe_dm.dm_lo_zone_daily_data                  union all
select  current_date as sdate,'fe_dm.dm_op_out_product_stock_item' as table_name, count(1) as nums from fe_dm.dm_op_out_product_stock_item           union all
select  current_date as sdate,'fe_dm.dm_op_shelf_high_stock' as table_name, count(1) as nums from fe_dm.dm_op_shelf_high_stock                 union all
select  current_date as sdate,'fe_dm.dm_op_shelf_product_confirm_risk' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_confirm_risk       union all
select  current_date as sdate,'fe_dm.dm_op_shelf_product_offstock2_detail' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_offstock2_detail   union all
select  current_date as sdate,'fe_dm.dm_op_slot_offstock2_detail' as table_name, count(1) as nums from fe_dm.dm_op_slot_offstock2_detail            union all
select  current_date as sdate,'fe_dm.dm_pub_shelf_product_sale_sum_90' as table_name, count(1) as nums from fe_dm.dm_pub_shelf_product_sale_sum_90       union all
select  current_date as sdate,'fe_dm.sf_sham_assign_record' as table_name, count(1) as nums from fe_dm.sf_sham_assign_record                  union all
select  current_date as sdate,'fe_dm.dm_op_shelf_product_start_fill_label' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_start_fill_label   union all
SELECT  CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_product_trans_remain' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_shelf_product_trans_remain   UNION ALL
SELECT  CURRENT_DATE AS sdate,'fe_dm.dm_shelf_product_high_danger_zero_stock' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_shelf_product_high_danger_zero_stock   UNION ALL
SELECT  CURRENT_DATE AS sdate,'fe_dwd.dwd_lo_shelf_source_log' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_lo_shelf_source_log   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_pub_shelf_manager' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_shelf_manager UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dm.dm_op_shelf_product_confirm_risk_status' AS table_name, COUNT(1) AS nums FROM fe_dm.dm_op_shelf_product_confirm_risk_status   UNION ALL
SELECT CURRENT_DATE AS sdate,'fe_dwd.dwd_lo_node_monitor_data_after_cleanout' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_lo_node_monitor_data_after_cleanout UNION ALL
select current_date as sdate, 'fe_dm.dm_op_shelf_product_risk_stock' as table_name,count(1) as nums from fe_dm.dm_op_shelf_product_risk_stock where month_id=date_format(current_date,'%Y-%m');
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_datax_table_check_rows_num2_not_fe',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user), @timestamp
  );
COMMIT;
    END