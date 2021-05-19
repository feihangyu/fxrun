CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_datax_table_check_rows_num2`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 全量同步表 实例2
REPLACE INTO fe_dwd.`dwd_datax_table_check_rows_num`(sdate,table_name,nums)
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_machine_slot_template_item' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_machine_slot_template_item       UNION ALL        
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_area_city_relation' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_area_city_relation             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_up_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_up_record                UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_risk_production_date_source' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_risk_production_date_source            UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_product_package' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_product_package                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_product_shelf_type' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_product_shelf_type                        UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_product_status_change_log' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_product_status_change_log                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_supplier_product_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_supplier_product_detail                UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_pub_manager' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_pub_manager                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_supplier' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_supplier                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_smart_product_template_item' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_smart_product_template_item      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_smart_product_template' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_smart_product_template           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_relation_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_relation_record                  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_activity' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_activity                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_survey_question' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_survey_question                        UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_type_black_catagory' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_type_black_catagory      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_fill_flag_apply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_fill_flag_apply          UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_package_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_package_detail                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_company_protocol_apply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_company_protocol_apply                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_product_area_pool' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_product_area_pool                         UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_logistics_task_install' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_logistics_task_install           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_coupon_model' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_coupon_model                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_machine_product_change_apply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_machine_product_change_apply              UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_line_relation' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_line_relation                    UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_department' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_department                             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_group_contract' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_group_contract                         UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_group_contract_shelf' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_group_contract_shelf                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_group_dictionary_item' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_group_dictionary_item                  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_check_detail_extend_old_snapshot' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_check_detail_extend_old_snapshot UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_group_supply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_group_supply                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_logistics_supplier_line_config_branch' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_logistics_supplier_line_config_branch  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_machines_apply_gradient_bonus' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_machines_apply_gradient_bonus          UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_change_apply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_change_apply                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_machines_apply_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_machines_apply_record                  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_machines_apply_record_extend' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_machines_apply_record_extend           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_area_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_area_info                        UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_apply_visit_company' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_apply_visit_company              UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_material' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_material                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_material_shelf_relation' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_material_shelf_relation                UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_material_transfer_order' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_material_transfer_order                UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_apply_addition_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_apply_addition_info              UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_sham_upgoods_assign_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_sham_upgoods_assign_record             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_foundation_advertion_position' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_foundation_advertion_position             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_code' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_code                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_business_area' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_business_area                  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_logistics_task_change' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_logistics_task_change            UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_foundation_advertion' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_foundation_advertion                      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prewarehouse_delivery_date_config' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_delivery_date_config      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prewarehouse_dept_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_dept_detail               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prewarehouse_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_info                      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prewarehouse_supplier_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_supplier_detail           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prewarehouse_stock_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_stock_detail              UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_order_logistics_task_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_order_logistics_task_record            UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_machines_apply_operation' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_machines_apply_operation               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_fill_day_config' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_fill_day_config                  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_logistics_task' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_logistics_task                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_machine_product_change' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_machine_product_change           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_supply_info' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_supply_info              UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_revoke' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_revoke                           UNION ALL
SELECT CURRENT_DATE, 'fe_dwd.dwd_sf_prewarehouse_apply' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_apply   UNION ALL
SELECT CURRENT_DATE, 'fe_dwd.dwd_sf_shelf_logistics_task_revoke' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_logistics_task_revoke   UNION ALL
SELECT CURRENT_DATE, 'fe_dwd.dwd_sf_shelf_logistics_task_move' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_logistics_task_move   UNION ALL
SELECT CURRENT_DATE, 'fe_dwd.dwd_sf_prewarehouse_apply_bind_shelf' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_apply_bind_shelf    UNION ALL
SELECT CURRENT_DATE, 'fe_dwd.dwd_sf_shelf_revoke_bak' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_revoke_bak    UNION ALL
SELECT CURRENT_DATE, 'fe_dwd.dwd_sf_channel_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_channel_record ;
-- 增量同步表 实例2  36.460 sec
REPLACE INTO fe_dwd.`dwd_datax_table_check_rows_num`(sdate,table_name,nums)
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_activity_item' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_activity_item WHERE stat_date>=SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_check_audit_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_check_audit_record                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prewarehouse_product_detail' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_prewarehouse_product_detail                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_prize_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_prize_record                                                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_pay_requirement' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_pay_requirement                                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_order_yht_item' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_order_yht_item                                             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_order_yht' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_order_yht                                                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_order_timeout_follow_result_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_order_timeout_follow_result_record     UNION ALL
-- select current_date,'fe_dwd.dwd_sf_product_fill_order_recent32' as table_name,count(1) as nums from fe_dwd.dwd_sf_product_fill_order_recent32                   union all
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_fill_order_extend' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_fill_order_extend                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_fill_order_item_data_flag_2' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_fill_order_item_data_flag_2                         UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_order_overstock_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_order_overstock_record                             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_sale_channel_spec' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_sale_channel_spec                                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_operate_result' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_operate_result                                             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_apply' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_apply                                                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_apply_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_apply_log                                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_apply_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_apply_record                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_material_detail' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_material_detail                                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_manager_operate_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_manager_operate_log                                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_check_detail_extend' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_check_detail_extend                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_group_order_third_rela' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_group_order_third_rela                             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_check_production_date' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_check_production_date                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_goods_transfer' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_goods_transfer                                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_info_flag' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_info_flag                                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_inspection_survey_answer' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_inspection_survey_answer             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_inspection_task' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_inspection_task                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_inspection_task_operation' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_inspection_task_operation           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_coupon_use' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_coupon_use                                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_coupon_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_coupon_record                                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_company_visit_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_company_visit_log                                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_logistics_task_operation' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_logistics_task_operation             UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_log                                                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_machine_command_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_machine_command_log                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_machine_fault' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_machine_fault                                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_machine_online_status_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_machine_online_status_record     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_machine_slot' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_machine_slot                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_company_visit_info' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_company_visit_info                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_manager_score_detail' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_manager_score_detail                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_company' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_company                                                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_company_customer_info' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_company_customer_info                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_log                                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_status_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_status_log                         UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_user_present' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_user_present                                                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_activity_user_integral_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_activity_user_integral_record               UNION ALL
-- select current_date,'fe_dwd.dwd_pub_user_integral_record' as table_name,count(1) as nums from fe_dwd.dwd_pub_user_integral_record                               union all
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_scope_detail' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_scope_detail                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_smart_log' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_smart_log                                           UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_pub_user_integral_growth' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_pub_user_integral_growth                               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_transactions' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_transactions                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_transfer_apply' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_transfer_apply                                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_transfer_shelf_info' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_transfer_shelf_info                       UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_statistics_product_inventory' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_statistics_product_inventory                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_statistics_shelf_sale' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_statistics_shelf_sale                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_pub_member_level_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_pub_member_level_record                                 UNION ALL
-- select current_date,'fe_dwd.dwd_pub_import_shelf_product' as table_name,count(1) as nums from fe_dwd.dwd_pub_import_shelf_product                               union all
SELECT CURRENT_DATE,'fe_dwd.dwd_product_area_pool_item' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_product_area_pool_item                                   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_third_user_balance' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_third_user_balance                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_user_present_exchange_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_user_present_exchange_record                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_activity_user_join' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_activity_user_join                                     UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_activity_user_join_item' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_activity_user_join_item                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_group_product_audit' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_group_product_audit                 UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_user_member_wallet' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_user_member_wallet               UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_revoke_specific_reason' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_revoke_specific_reason  UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_smart_breach_order'  AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_smart_breach_order            UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_activity'  AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_activity              UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_coupon_scope_delivery'  AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_coupon_scope_delivery         UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_smart_transaction_check_log'  AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_smart_transaction_check_log      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_mall_product_specs'  AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_mall_product_specs      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_loss_bill' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_loss_bill      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_product_counted_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_product_counted_record      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_supplier_relation' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_supplier_relation      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_after_payment' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_after_payment      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_label_detail' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_label_detail      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_pub_user_integral_record' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_pub_user_integral_record  WHERE from_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_pub_import_shelf_product' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_pub_import_shelf_product  WHERE last_update_time >= SUBDATE(CURRENT_DATE(), INTERVAL 1 DAY)   UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_shelf_workbench_follow_result_record' AS table_name, COUNT(1) AS nums FROM fe_dwd.dwd_sf_shelf_workbench_follow_result_record      UNION ALL
SELECT CURRENT_DATE,'fe_dwd.dwd_sf_product_machine_slot' AS table_name,COUNT(1) AS nums FROM fe_dwd.dwd_sf_product_machine_slot ;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_datax_table_check_rows_num2',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user), @timestamp
  );
COMMIT;
    END