
-- 实例1数据核对

SELECT c.sdate,c.table_name AS '实例1表名',c.nums AS '实例1表数据量',c.load_time AS '实例1数据统计时间',c.table_name_two AS '实例2表名',
d.nums AS '实例2表数据量',d.load_time AS '实例2数据统计时间',c.nums-d.nums AS '数据差异量（实例1-实例2）',e.last_start_time AS '同步到实例2开始时间',e.last_end_time AS '同步到实例2结束时间',e.last_run_time  AS '同步耗时（秒）',
e.erp_frequency AS '同步类型',e.remark AS '同步说明'
FROM (
     SELECT a.*,b.table_name_two FROM (
     SELECT * FROM fe_dwd.`dwd_datax_table_check_rows_num` WHERE sdate=CURRENT_DATE AND data_base=1) a
     LEFT JOIN (SELECT table_name_one,table_name_two FROM fe_dwd.`dwd_datax_table_mapping_info` WHERE delete_flag=1) b
     ON a.table_name=b.table_name_one
     ) c
JOIN (
     SELECT * FROM fe_dwd.`dwd_datax_table_check_rows_num` WHERE sdate=CURRENT_DATE AND data_base=2
     ) d
ON c.table_name_two=d.table_name
JOIN (
     SELECT 
     CONCAT(b.job_desc,'_erp') AS datax_project_name, a.table_name_one,a.table_name_two,a.erp_frequency,a.remark,
     MAX(b.trigger_time) AS last_start_time,
     MAX(b.handle_time) AS last_end_time,
     TIMESTAMPDIFF(SECOND,b.trigger_time,b.handle_time) AS last_run_time
     FROM fe_dwd.`dwd_datax_table_mapping_info` a
     JOIN fe_datax.job_log b
     ON SUBSTRING_INDEX(a.table_name_one,'.',-1)=b.job_desc
     AND  b.handle_code=200  #b.trigger_time>=CURRENT_DATE AND
     #AND a.table_name_one='fe_ana_data.sf_shelf_machine_online_status_record'  -- 实例2表名
     AND a.delete_flag=1 AND a.table_name_one  NOT LIKE 'feng1.%'
     GROUP BY CONCAT(b.job_desc,'_erp')
     ) e
ON c.table_name=e.table_name_one
ORDER BY c.nums-d.nums DESC
;

-- 实例2数据核对

SELECT c.sdate,c.table_name AS '实例1表名',c.nums AS '实例1表数据量',c.load_time AS '实例1数据统计时间',c.table_name_two AS '实例2表名',
d.nums AS '实例2表数据量',d.load_time AS '实例2数据统计时间',c.nums-d.nums AS '数据差异量（实例1-实例2）',e.last_start_time AS '同步到实例2开始时间',e.last_end_time AS '同步到实例2结束时间',e.last_run_time  AS '同步耗时（秒）',
e.erp_frequency AS '同步类型',e.remark AS '同步说明'
FROM (
     SELECT a.*,b.table_name_two FROM (
     SELECT * FROM fe_dwd.`dwd_datax_table_check_rows_num_not_fe` WHERE sdate=CURRENT_DATE AND data_base=1) a
     LEFT JOIN (SELECT table_name_one,table_name_two FROM fe_dwd.`dwd_datax_table_mapping_info` WHERE delete_flag=1) b
     ON a.table_name=b.table_name_one
     ) c
JOIN (
     SELECT * FROM fe_dwd.`dwd_datax_table_check_rows_num_not_fe` WHERE sdate=CURRENT_DATE AND data_base=2
     ) d
ON c.table_name_two=d.table_name
JOIN (
     SELECT 
     CONCAT(b.job_desc,'_erp') AS datax_project_name, a.table_name_one,a.table_name_two,a.erp_frequency,a.remark,
     MAX(b.trigger_time) AS last_start_time,
     MAX(b.handle_time) AS last_end_time,
     TIMESTAMPDIFF(SECOND,b.trigger_time,b.handle_time) AS last_run_time
     FROM fe_dwd.`dwd_datax_table_mapping_info` a
     JOIN fe_datax.job_log b
     ON SUBSTRING_INDEX(a.table_name_one,'.',-1)=b.job_desc
     AND  b.handle_code=200  #b.trigger_time>=CURRENT_DATE AND
     #AND a.table_name_one='fe_ana_data.sf_shelf_machine_online_status_record'  -- 实例2表名
     AND a.delete_flag=1 AND a.table_name_one  NOT LIKE 'feng1.%'
     GROUP BY CONCAT(b.job_desc,'_erp')
     ) e
ON c.table_name=e.table_name_one
ORDER BY c.nums-d.nums DESC
;






























drop TABLE if exists fe_dwd.`dwd_datax_table_check_rows_num`;
CREATE TABLE fe_dwd.`dwd_datax_table_check_rows_num` (
  `sdate` date NOT NULL  COMMENT '日期',
  `table_name` varchar(128) NOT NULL COMMENT '表名',
  `data_base` tinyint(1) DEFAULT '1' COMMENT '数据所属实例(1:实例1，2:实例2)',
  `nums` bigint(20) DEFAULT NULL COMMENT '数据量',
  `load_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '数据加载时间',
  PRIMARY KEY (`sdate`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='datax同步表数据核对';


-- 全量同步表 实例1  3sec
replace into fe_dwd.`dwd_datax_table_check_rows_num`(sdate,table_name,nums)
select current_date,'fe.sf_shelf_machine_slot_template_item' as table_name, count(1) as nums from fe.sf_shelf_machine_slot_template_item  where data_flag=1 union all
select current_date,'fe.sf_product_area_city_relation' as table_name, count(1) as nums from fe.sf_product_area_city_relation where data_flag=1 union all
select current_date,'fe.sf_shelf_product_up_record' as table_name, count(1) as nums from fe.sf_shelf_product_up_record where data_flag=1 union all
select current_date,'fe.sf_risk_production_date_source' as table_name, count(1) as nums from (SELECT a.* FROM fe.`sf_risk_production_date_source` a JOIN fe.`sf_shelf` b ON a.`shelf_id` = b.`SHELF_ID` AND b.`DATA_FLAG` = 1 AND b.`SHELF_TYPE` = 9) a union all
select current_date, 'fe.product_package' as table_name, count(1) as nums from fe.product_package                            where data_flag=1 union all                              
select current_date, 'fe.product_shelf_type' as table_name, count(1) as nums from fe.product_shelf_type                         where data_flag=1 union all
select current_date, 'fe.product_status_change_log' as table_name, count(1) as nums from fe.product_status_change_log                  where data_flag=1 union all
select current_date, 'fe.sf_supplier_product_detail' as table_name, count(1) as nums from fe.sf_supplier_product_detail                 where data_flag=1 union all
select current_date, 'fe.pub_manager' as table_name, count(1) as nums from fe.pub_manager                                where data_flag=1 union all
select current_date, 'fe.sf_supplier' as table_name, count(1) as nums from fe.sf_supplier                                where data_flag=1 union all
select current_date, 'fe.sf_shelf_smart_product_template_item' as table_name, count(1) as nums from fe.sf_shelf_smart_product_template_item       where data_flag=1 union all
select current_date, 'fe.sf_shelf_smart_product_template' as table_name, count(1) as nums from fe.sf_shelf_smart_product_template            where data_flag=1 union all
select current_date, 'fe.sf_shelf_relation_record' as table_name, count(1) as nums from fe.sf_shelf_relation_record                   where data_flag=1 union all
select current_date, 'fe_activity.sf_activity' as table_name, count(1) as nums from fe_activity.sf_activity                       where data_flag=1 union all
select current_date, 'fe.sf_survey_question' as table_name, count(1) as nums from fe.sf_survey_question                         where data_flag=1 union all
select current_date, 'fe.sf_shelf_product_type_black_catagory' as table_name, count(1) as nums from fe.sf_shelf_product_type_black_catagory       where data_flag=1 union all
select current_date, 'fe.sf_shelf_product_fill_flag_apply' as table_name, count(1) as nums from fe.sf_shelf_product_fill_flag_apply           where data_flag=1 union all
select current_date, 'fe.sf_shelf_package_detail' as table_name, count(1) as nums from fe.sf_shelf_package_detail                    where data_flag=1 union all
select current_date, 'fe.sf_company_protocol_apply' as table_name, count(1) as nums from fe.sf_company_protocol_apply                  where data_flag=1 union all
select current_date, 'fe.product_area_pool' as table_name, count(1) as nums from fe.product_area_pool                          where data_flag=1 union all
select current_date, 'fe.sf_shelf_logistics_task_install' as table_name, count(1) as nums from fe.sf_shelf_logistics_task_install            where data_flag=1 union all
select current_date, 'fe.sf_coupon_model' as table_name, count(1) as nums from fe.sf_coupon_model                            where data_flag=1 union all
select current_date, 'fe.machine_product_change_apply' as table_name, count(1) as nums from fe.machine_product_change_apply               where data_flag=1 union all
select current_date, 'fe.sf_shelf_line_relation' as table_name, count(1) as nums from fe.sf_shelf_line_relation                     where data_flag=1 union all
select current_date, 'fe.sf_department' as table_name, count(1) as nums from fe.sf_department        union all
select current_date, 'fe_group.sf_group_contract' as table_name, count(1) as nums from fe_group.sf_group_contract                    where data_flag=1 union all
select current_date, 'fe_group.sf_group_contract_shelf' as table_name, count(1) as nums from fe_group.sf_group_contract_shelf              where data_flag=1 union all
select current_date, 'fe_group.sf_group_dictionary_item' as table_name, count(1) as nums from fe_group.sf_group_dictionary_item             where data_flag=1 union all
select current_date, 'fe.sf_shelf_check_detail_extend_old_snapshot' as table_name, count(1) as nums from fe.sf_shelf_check_detail_extend_old_snapshot  where data_flag=1 union all
select current_date, 'fe_group.sf_group_supply' as table_name, count(1) as nums from fe_group.sf_group_supply                      where data_flag=1 union all
select current_date, 'fe.sf_logistics_supplier_line_config_branch' as table_name, count(1) as nums from fe.sf_logistics_supplier_line_config_branch   where data_flag=1 union all
select current_date, 'fe.sf_machines_apply_gradient_bonus' as table_name, count(1) as nums from fe.sf_machines_apply_gradient_bonus           where data_flag=1 union all
select current_date, 'fe.sf_shelf_change_apply' as table_name, count(1) as nums from fe.sf_shelf_change_apply                      where data_flag=1 union all
select current_date, 'fe.sf_machines_apply_record' as table_name, count(1) as nums from fe.sf_machines_apply_record                   where data_flag=1 union all
select current_date, 'fe.sf_machines_apply_record_extend' as table_name, count(1) as nums from fe.sf_machines_apply_record_extend            where data_flag=1 union all
select current_date, 'fe.sf_shelf_area_info' as table_name, count(1) as nums from fe.sf_shelf_area_info                         where data_flag=1 union all
select current_date, 'fe.sf_shelf_apply_visit_company' as table_name, count(1) as nums from fe.sf_shelf_apply_visit_company               where data_flag=1 union all
select current_date, 'fe.sf_material' as table_name, count(1) as nums from fe.sf_material                                where data_flag=1 union all
select current_date, 'fe.sf_material_shelf_relation' as table_name, count(1) as nums from fe.sf_material_shelf_relation                 where data_flag=1 union all
select current_date, 'fe.sf_material_transfer_order' as table_name, count(1) as nums from fe.sf_material_transfer_order                 where data_flag=1 union all
select current_date, 'fe.sf_shelf_apply_addition_info' as table_name, count(1) as nums from fe.sf_shelf_apply_addition_info               where data_flag=1 union all
select current_date, 'fe.sf_sham_upgoods_assign_record' as table_name, count(1) as nums from fe.sf_sham_upgoods_assign_record              where data_flag=1 union all
select current_date, 'fe.foundation_advertion_position' as table_name, count(1) as nums from fe.foundation_advertion_position              where data_flag=1 union all
select current_date, 'fe.sf_product_code' as table_name, count(1) as nums from fe.sf_product_code                            where data_flag=1 union all
select current_date, 'fe.sf_product_business_area' as table_name, count(1) as nums from fe.sf_product_business_area                   where data_flag=1 union all
select current_date, 'fe.sf_shelf_logistics_task_change' as table_name, count(1) as nums from fe.sf_shelf_logistics_task_change             where data_flag=1 union all
select current_date, 'fe.foundation_advertion' as table_name, count(1) as nums from fe.foundation_advertion                       where data_flag=1 union all
select current_date, 'fe.sf_prewarehouse_delivery_date_config' as table_name, count(1) as nums from fe.sf_prewarehouse_delivery_date_config       where data_flag=1 union all
select current_date, 'fe.sf_prewarehouse_dept_detail' as table_name, count(1) as nums from fe.sf_prewarehouse_dept_detail                where data_flag=1 union all
select current_date, 'fe.sf_prewarehouse_info' as table_name, count(1) as nums from fe.sf_prewarehouse_info                       where data_flag=1 union all
select current_date, 'fe.sf_prewarehouse_supplier_detail' as table_name, count(1) as nums from fe.sf_prewarehouse_supplier_detail            where data_flag=1 union all
select current_date, 'fe.sf_prewarehouse_stock_detail' as table_name, count(1) as nums from fe.sf_prewarehouse_stock_detail               where data_flag=1 union all
select current_date, 'fe.sf_order_logistics_task_record' as table_name, count(1) as nums from fe.sf_order_logistics_task_record             where data_flag=1 union all
select current_date, 'fe.sf_machines_apply_operation' as table_name, count(1) as nums from fe.sf_machines_apply_operation                where data_flag=1 union all
select current_date, 'fe.sf_shelf_fill_day_config' as table_name, count(1) as nums from fe.sf_shelf_fill_day_config                   where data_flag=1 union all
select current_date, 'fe.sf_shelf_logistics_task' as table_name, count(1) as nums from fe.sf_shelf_logistics_task                    where data_flag=1 union all
select current_date, 'fe.sf_shelf_machine_product_change' as table_name, count(1) as nums from fe.sf_shelf_machine_product_change            where data_flag=1 union all
select current_date, 'fe.sf_shelf_product_supply_info' as table_name, count(1) as nums from fe.sf_shelf_product_supply_info               where data_flag=1 union all
select current_date, 'fe.sf_shelf_revoke' as table_name, count(1) as nums from fe.sf_shelf_revoke                            where data_flag=1 union all
select current_date, 'fe.pub_shelf_manager' as table_name, count(1) as nums from fe.pub_shelf_manager                          where data_flag=1 ;




-- 增量同步表 实例1  45.984 sec
replace into fe_dwd.`dwd_datax_table_check_rows_num`(sdate,table_name,nums)

select current_date,'fe.sf_product_activity_item' as table_name,count(1) as nums from fe.sf_product_activity_item  where stat_date>=subdate(current_date,interval 1 day) union all
select current_date,'fe.sf_check_audit_record' as table_name,count(1) as nums from fe.sf_check_audit_record                           union all
select current_date,'fe.sf_prewarehouse_product_detail' as table_name,count(1) as nums from fe.sf_prewarehouse_product_detail                  union all
select current_date,'fe_activity.sf_prize_record' as table_name,count(1) as nums from fe_activity.sf_prize_record                        union all
select current_date,'fe_pay.sf_pay_requirement' as table_name,count(1) as nums from fe_pay.sf_pay_requirement                          union all
select current_date,'fe.sf_order_yht_item' as table_name,count(1) as nums from fe.sf_order_yht_item                               union all
select current_date,'fe.sf_order_yht' as table_name,count(1) as nums from fe.sf_order_yht                                    union all
select current_date,'fe.sf_order_timeout_follow_result_record' as table_name,count(1) as nums from fe.sf_order_timeout_follow_result_record           union all
-- select current_date,'fe.sf_product_fill_order' as table_name,count(1) as nums from fe.sf_product_fill_order                           union all
select current_date,'fe.sf_product_fill_order_extend' as table_name,count(1) as nums from fe.sf_product_fill_order_extend                    union all
select current_date,'fe.sf_product_fill_order_item' as table_name,count(1) as nums from fe.sf_product_fill_order_item  where last_update_time>=SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) AND data_flag=2 union all
select current_date,'fe_order.sf_order_overstock_record' as table_name,count(1) as nums from fe_order.sf_order_overstock_record                 union all
select current_date,'fe_goods.sf_sale_channel_spec' as table_name,count(1) as nums from fe_goods.sf_sale_channel_spec                      union all
select current_date,'fe.sf_operate_result' as table_name,count(1) as nums from fe.sf_operate_result                               union all
select current_date,'fe.sf_shelf_apply' as table_name,count(1) as nums from fe.sf_shelf_apply                                  union all
select current_date,'fe.sf_shelf_apply_log' as table_name,count(1) as nums from fe.sf_shelf_apply_log                              union all
select current_date,'fe.sf_shelf_apply_record' as table_name,count(1) as nums from fe.sf_shelf_apply_record                           union all
select current_date,'fe.sf_material_detail' as table_name,count(1) as nums from fe.sf_material_detail                              union all
select current_date,'fe.sf_manager_operate_log' as table_name,count(1) as nums from fe.sf_manager_operate_log                          union all
select current_date,'fe.sf_shelf_check_detail_extend' as table_name,count(1) as nums from fe.sf_shelf_check_detail_extend                    union all
select current_date,'fe_goods.sf_group_order_third_rela' as table_name,count(1) as nums from fe_goods.sf_group_order_third_rela                 union all
select current_date,'fe.sf_shelf_check_production_date' as table_name,count(1) as nums from fe.sf_shelf_check_production_date                  union all
select current_date,'fe.sf_shelf_goods_transfer' as table_name,count(1) as nums from fe.sf_shelf_goods_transfer                         union all
select current_date,'fe.sf_shelf_info_flag' as table_name,count(1) as nums from fe.sf_shelf_info_flag                              union all
select current_date,'fe.sf_shelf_inspection_survey_answer' as table_name,count(1) as nums from fe.sf_shelf_inspection_survey_answer               union all
select current_date,'fe.sf_shelf_inspection_task' as table_name,count(1) as nums from fe.sf_shelf_inspection_task                        union all
select current_date,'fe.sf_shelf_inspection_task_operation' as table_name,count(1) as nums from fe.sf_shelf_inspection_task_operation              union all
select current_date,'fe.sf_coupon_use' as table_name,count(1) as nums from fe.sf_coupon_use                                   union all
select current_date,'fe.sf_coupon_record' as table_name,count(1) as nums from fe.sf_coupon_record                                union all
select current_date,'fe.sf_company_visit_log' as table_name,count(1) as nums from fe.sf_company_visit_log                            union all
select current_date,'fe.sf_shelf_logistics_task_operation' as table_name,count(1) as nums from fe.sf_shelf_logistics_task_operation               union all
select current_date,'fe.sf_shelf_log' as table_name,count(1) as nums from fe.sf_shelf_log                                    union all
select current_date,'fe.sf_shelf_machine_command_log' as table_name,count(1) as nums from fe.sf_shelf_machine_command_log                    union all
select current_date,'fe.sf_shelf_machine_fault' as table_name,count(1) as nums from fe.sf_shelf_machine_fault                          union all
select current_date,'fe_ana_data.sf_shelf_machine_online_status_record' as table_name,count(1) as nums from fe_ana_data.sf_shelf_machine_online_status_record  union all
select current_date,'fe.sf_shelf_machine_slot' as table_name,count(1) as nums from fe.sf_shelf_machine_slot                           union all
select current_date,'fe.sf_company_visit_info' as table_name,count(1) as nums from fe.sf_company_visit_info                           union all
select current_date,'fe.sf_shelf_manager_score_detail' as table_name,count(1) as nums from fe.sf_shelf_manager_score_detail                   union all
select current_date,'fe.sf_company' as table_name,count(1) as nums from fe.sf_company                                      union all
select current_date,'fe.sf_company_customer_info' as table_name,count(1) as nums from fe.sf_company_customer_info                        union all
select current_date,'fe.sf_shelf_product_log' as table_name,count(1) as nums from fe.sf_shelf_product_log                            union all
select current_date,'fe.sf_shelf_product_status_log' as table_name,count(1) as nums from fe.sf_shelf_product_status_log                     union all
select current_date,'fe.sf_user_present' as table_name,count(1) as nums from fe.sf_user_present                                 union all
select current_date,'fe_activity.sf_activity_user_integral_record' as table_name,count(1) as nums from fe_activity.sf_activity_user_integral_record       union all
-- select current_date,'fe.pub_user_integral_record' as table_name,count(1) as nums from fe.pub_user_integral_record                        union all
select current_date,'fe.sf_shelf_scope_detail' as table_name,count(1) as nums from fe.sf_shelf_scope_detail                           union all
select current_date,'fe.sf_shelf_smart_log' as table_name,count(1) as nums from fe.sf_shelf_smart_log                              union all
select current_date,'fe.pub_user_integral_growth' as table_name,count(1) as nums from fe.pub_user_integral_growth                        union all
select current_date,'fe.sf_shelf_transactions' as table_name,count(1) as nums from fe.sf_shelf_transactions                           union all
select current_date,'fe.sf_shelf_transfer_apply' as table_name,count(1) as nums from fe.sf_shelf_transfer_apply                         union all
select current_date,'fe.sf_shelf_transfer_shelf_info' as table_name,count(1) as nums from fe.sf_shelf_transfer_shelf_info                    union all
select current_date,'fe.sf_statistics_product_inventory' as table_name,count(1) as nums from fe.sf_statistics_product_inventory                 union all
select current_date,'fe.sf_statistics_shelf_sale' as table_name,count(1) as nums from fe.sf_statistics_shelf_sale where create_date >='2019-12-01'  union all
select current_date,'fe.pub_member_level_record' as table_name,count(1) as nums from fe.pub_member_level_record                         union all
-- select current_date,'fe.pub_import_shelf_product' as table_name,count(1) as nums from fe.pub_import_shelf_product                        union all
select current_date,'fe.product_area_pool_item' as table_name,count(1) as nums from fe.product_area_pool_item                          union all
select current_date,'fe_goods.sf_third_user_balance' as table_name,count(1) as nums from fe_goods.sf_third_user_balance                     union all
select current_date,'fe.sf_user_present_exchange_record' as table_name,count(1) as nums from fe.sf_user_present_exchange_record                 ;



-- 全量同步表 实例2
replace into fe_dwd.`dwd_datax_table_check_rows_num`(sdate,table_name,nums)
select current_date,'fe_dwd.dwd_sf_shelf_machine_slot_template_item' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_machine_slot_template_item       union all        
select current_date,'fe_dwd.dwd_sf_product_area_city_relation' as table_name, count(1) as nums from fe_dwd.dwd_sf_product_area_city_relation             union all
select current_date,'fe_dwd.dwd_sf_shelf_product_up_record' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_product_up_record                union all
select current_date,'fe_dwd.dwd_sf_risk_production_date_source' as table_name, count(1) as nums from fe_dwd.dwd_sf_risk_production_date_source            union all
select current_date,'fe_dwd.dwd_product_package' as table_name, count(1) as nums from fe_dwd.dwd_product_package                           union all
select current_date,'fe_dwd.dwd_product_shelf_type' as table_name, count(1) as nums from fe_dwd.dwd_product_shelf_type                        union all
select current_date,'fe_dwd.dwd_product_status_change_log' as table_name, count(1) as nums from fe_dwd.dwd_product_status_change_log                 union all
select current_date,'fe_dwd.dwd_sf_supplier_product_detail' as table_name, count(1) as nums from fe_dwd.dwd_sf_supplier_product_detail                union all
select current_date,'fe_dwd.dwd_pub_manager' as table_name, count(1) as nums from fe_dwd.dwd_pub_manager                               union all
select current_date,'fe_dwd.dwd_sf_supplier' as table_name, count(1) as nums from fe_dwd.dwd_sf_supplier                               union all
select current_date,'fe_dwd.dwd_sf_shelf_smart_product_template_item' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_smart_product_template_item      union all
select current_date,'fe_dwd.dwd_sf_shelf_smart_product_template' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_smart_product_template           union all
select current_date,'fe_dwd.dwd_sf_shelf_relation_record' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_relation_record                  union all
select current_date,'fe_dwd.dwd_sf_activity' as table_name, count(1) as nums from fe_dwd.dwd_sf_activity                               union all
select current_date,'fe_dwd.dwd_sf_survey_question' as table_name, count(1) as nums from fe_dwd.dwd_sf_survey_question                        union all
select current_date,'fe_dwd.dwd_sf_shelf_product_type_black_catagory' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_product_type_black_catagory      union all
select current_date,'fe_dwd.dwd_sf_shelf_product_fill_flag_apply' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_product_fill_flag_apply          union all
select current_date,'fe_dwd.dwd_sf_shelf_package_detail' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_package_detail                   union all
select current_date,'fe_dwd.dwd_sf_company_protocol_apply' as table_name, count(1) as nums from fe_dwd.dwd_sf_company_protocol_apply                 union all
select current_date,'fe_dwd.dwd_product_area_pool' as table_name, count(1) as nums from fe_dwd.dwd_product_area_pool                         union all
select current_date,'fe_dwd.dwd_sf_shelf_logistics_task_install' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_logistics_task_install           union all
select current_date,'fe_dwd.dwd_sf_coupon_model' as table_name, count(1) as nums from fe_dwd.dwd_sf_coupon_model                           union all
select current_date,'fe_dwd.dwd_machine_product_change_apply' as table_name, count(1) as nums from fe_dwd.dwd_machine_product_change_apply              union all
select current_date,'fe_dwd.dwd_sf_shelf_line_relation' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_line_relation                    union all
select current_date,'fe_dwd.dwd_sf_department' as table_name, count(1) as nums from fe_dwd.dwd_sf_department                             union all
select current_date,'fe_dwd.dwd_sf_group_contract' as table_name, count(1) as nums from fe_dwd.dwd_sf_group_contract                         union all
select current_date,'fe_dwd.dwd_sf_group_contract_shelf' as table_name, count(1) as nums from fe_dwd.dwd_sf_group_contract_shelf                   union all
select current_date,'fe_dwd.dwd_sf_group_dictionary_item' as table_name, count(1) as nums from fe_dwd.dwd_sf_group_dictionary_item                  union all
select current_date,'fe_dwd.dwd_sf_shelf_check_detail_extend_old_snapshot' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_check_detail_extend_old_snapshot union all
select current_date,'fe_dwd.dwd_sf_group_supply' as table_name, count(1) as nums from fe_dwd.dwd_sf_group_supply                           union all
select current_date,'fe_dwd.dwd_sf_logistics_supplier_line_config_branch' as table_name, count(1) as nums from fe_dwd.dwd_sf_logistics_supplier_line_config_branch  union all
select current_date,'fe_dwd.dwd_sf_machines_apply_gradient_bonus' as table_name, count(1) as nums from fe_dwd.dwd_sf_machines_apply_gradient_bonus          union all
select current_date,'fe_dwd.dwd_sf_shelf_change_apply' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_change_apply                     union all
select current_date,'fe_dwd.dwd_sf_machines_apply_record' as table_name, count(1) as nums from fe_dwd.dwd_sf_machines_apply_record                  union all
select current_date,'fe_dwd.dwd_sf_machines_apply_record_extend' as table_name, count(1) as nums from fe_dwd.dwd_sf_machines_apply_record_extend           union all
select current_date,'fe_dwd.dwd_sf_shelf_area_info' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_area_info                        union all
select current_date,'fe_dwd.dwd_sf_shelf_apply_visit_company' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_apply_visit_company              union all
select current_date,'fe_dwd.dwd_sf_material' as table_name, count(1) as nums from fe_dwd.dwd_sf_material                               union all
select current_date,'fe_dwd.dwd_sf_material_shelf_relation' as table_name, count(1) as nums from fe_dwd.dwd_sf_material_shelf_relation                union all
select current_date,'fe_dwd.dwd_sf_material_transfer_order' as table_name, count(1) as nums from fe_dwd.dwd_sf_material_transfer_order                union all
select current_date,'fe_dwd.dwd_sf_shelf_apply_addition_info' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_apply_addition_info              union all
select current_date,'fe_dwd.dwd_sf_sham_upgoods_assign_record' as table_name, count(1) as nums from fe_dwd.dwd_sf_sham_upgoods_assign_record             union all
select current_date,'fe_dwd.dwd_foundation_advertion_position' as table_name, count(1) as nums from fe_dwd.dwd_foundation_advertion_position             union all
select current_date,'fe_dwd.dwd_sf_product_code' as table_name, count(1) as nums from fe_dwd.dwd_sf_product_code                           union all
select current_date,'fe_dwd.dwd_sf_product_business_area' as table_name, count(1) as nums from fe_dwd.dwd_sf_product_business_area                  union all
select current_date,'fe_dwd.dwd_sf_shelf_logistics_task_change' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_logistics_task_change            union all
select current_date,'fe_dwd.dwd_foundation_advertion' as table_name, count(1) as nums from fe_dwd.dwd_foundation_advertion                      union all
select current_date,'fe_dwd.dwd_sf_prewarehouse_delivery_date_config' as table_name, count(1) as nums from fe_dwd.dwd_sf_prewarehouse_delivery_date_config      union all
select current_date,'fe_dwd.dwd_sf_prewarehouse_dept_detail' as table_name, count(1) as nums from fe_dwd.dwd_sf_prewarehouse_dept_detail               union all
select current_date,'fe_dwd.dwd_sf_prewarehouse_info' as table_name, count(1) as nums from fe_dwd.dwd_sf_prewarehouse_info                      union all
select current_date,'fe_dwd.dwd_sf_prewarehouse_supplier_detail' as table_name, count(1) as nums from fe_dwd.dwd_sf_prewarehouse_supplier_detail           union all
select current_date,'fe_dwd.dwd_sf_prewarehouse_stock_detail' as table_name, count(1) as nums from fe_dwd.dwd_sf_prewarehouse_stock_detail              union all
select current_date,'fe_dwd.dwd_sf_order_logistics_task_record' as table_name, count(1) as nums from fe_dwd.dwd_sf_order_logistics_task_record            union all
select current_date,'fe_dwd.dwd_sf_machines_apply_operation' as table_name, count(1) as nums from fe_dwd.dwd_sf_machines_apply_operation               union all
select current_date,'fe_dwd.dwd_sf_shelf_fill_day_config' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_fill_day_config                  union all
select current_date,'fe_dwd.dwd_sf_shelf_logistics_task' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_logistics_task                   union all
select current_date,'fe_dwd.dwd_sf_shelf_machine_product_change' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_machine_product_change           union all
select current_date,'fe_dwd.dwd_sf_shelf_product_supply_info' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_product_supply_info              union all
select current_date,'fe_dwd.dwd_sf_shelf_revoke' as table_name, count(1) as nums from fe_dwd.dwd_sf_shelf_revoke                           union all
select current_date,'fe_dwd.dwd_pub_shelf_manager' as table_name, count(1) as nums from fe_dwd.dwd_pub_shelf_manager ;


-- 增量同步表 实例2  36.460 sec
replace into fe_dwd.`dwd_datax_table_check_rows_num`(sdate,table_name,nums)

select current_date,'fe_temp.sf_product_activity_item' as table_name,count(1) as nums from fe_temp.sf_product_activity_item                                     union all
select current_date,'fe_dwd.dwd_sf_check_audit_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_check_audit_record                                     union all
select current_date,'fe_dwd.dwd_sf_prewarehouse_product_detail' as table_name,count(1) as nums from fe_dwd.dwd_sf_prewarehouse_product_detail                   union all
select current_date,'fe_dwd.dwd_sf_prize_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_prize_record                                                 union all
select current_date,'fe_dwd.dwd_sf_pay_requirement' as table_name,count(1) as nums from fe_dwd.dwd_sf_pay_requirement                                           union all
select current_date,'fe_dwd.dwd_sf_order_yht_item' as table_name,count(1) as nums from fe_dwd.dwd_sf_order_yht_item                                             union all
select current_date,'fe_dwd.dwd_sf_order_yht' as table_name,count(1) as nums from fe_dwd.dwd_sf_order_yht                                                       union all
select current_date,'fe_dwd.dwd_sf_order_timeout_follow_result_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_order_timeout_follow_result_record     union all
-- select current_date,'fe_dwd.dwd_sf_product_fill_order_recent32' as table_name,count(1) as nums from fe_dwd.dwd_sf_product_fill_order_recent32                   union all
select current_date,'fe_dwd.dwd_sf_product_fill_order_extend' as table_name,count(1) as nums from fe_dwd.dwd_sf_product_fill_order_extend                       union all
select current_date,'fe_dwd.dwd_fill_order_item_data_flag_2' as table_name,count(1) as nums from fe_dwd.dwd_fill_order_item_data_flag_2                         union all
select current_date,'fe_dwd.dwd_sf_order_overstock_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_order_overstock_record                             union all
select current_date,'fe_dwd.dwd_sf_sale_channel_spec' as table_name,count(1) as nums from fe_dwd.dwd_sf_sale_channel_spec                                       union all
select current_date,'fe_dwd.dwd_sf_operate_result' as table_name,count(1) as nums from fe_dwd.dwd_sf_operate_result                                             union all
select current_date,'fe_dwd.dwd_sf_shelf_apply' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_apply                                                   union all
select current_date,'fe_dwd.dwd_sf_shelf_apply_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_apply_log                                           union all
select current_date,'fe_dwd.dwd_sf_shelf_apply_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_apply_record                                     union all
select current_date,'fe_dwd.dwd_sf_material_detail' as table_name,count(1) as nums from fe_dwd.dwd_sf_material_detail                                           union all
select current_date,'fe_dwd.dwd_sf_manager_operate_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_manager_operate_log                                   union all
select current_date,'fe_dwd.dwd_sf_shelf_check_detail_extend' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_check_detail_extend                       union all
select current_date,'fe_dwd.dwd_sf_group_order_third_rela' as table_name,count(1) as nums from fe_dwd.dwd_sf_group_order_third_rela                             union all
select current_date,'fe_dwd.dwd_sf_shelf_check_production_date' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_check_production_date                   union all
select current_date,'fe_dwd.dwd_sf_shelf_goods_transfer' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_goods_transfer                                 union all
select current_date,'fe_dwd.dwd_sf_shelf_info_flag' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_info_flag                                           union all
select current_date,'fe_dwd.dwd_sf_shelf_inspection_survey_answer' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_inspection_survey_answer             union all
select current_date,'fe_dwd.dwd_sf_shelf_inspection_task' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_inspection_task                               union all
select current_date,'fe_dwd.dwd_sf_shelf_inspection_task_operation' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_inspection_task_operation           union all
select current_date,'fe_dwd.dwd_sf_coupon_use' as table_name,count(1) as nums from fe_dwd.dwd_sf_coupon_use                                                     union all
select current_date,'fe_dwd.dwd_sf_coupon_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_coupon_record                                               union all
select current_date,'fe_dwd.dwd_sf_company_visit_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_company_visit_log                                       union all
select current_date,'fe_dwd.dwd_sf_shelf_logistics_task_operation' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_logistics_task_operation             union all
select current_date,'fe_dwd.dwd_sf_shelf_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_log                                                       union all
select current_date,'fe_dwd.dwd_sf_shelf_machine_command_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_machine_command_log                       union all
select current_date,'fe_dwd.dwd_sf_shelf_machine_fault' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_machine_fault                                   union all
select current_date,'fe_dwd.dwd_sf_shelf_machine_online_status_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_machine_online_status_record     union all
select current_date,'fe_dwd.dwd_sf_shelf_machine_slot' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_machine_slot                                     union all
select current_date,'fe_dwd.dwd_sf_company_visit_info' as table_name,count(1) as nums from fe_dwd.dwd_sf_company_visit_info                                     union all
select current_date,'fe_dwd.dwd_sf_shelf_manager_score_detail' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_manager_score_detail                     union all
select current_date,'fe_dwd.dwd_sf_company' as table_name,count(1) as nums from fe_dwd.dwd_sf_company                                                           union all
select current_date,'fe_dwd.dwd_sf_company_customer_info' as table_name,count(1) as nums from fe_dwd.dwd_sf_company_customer_info                               union all
select current_date,'fe_dwd.dwd_sf_shelf_product_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_product_log                                       union all
select current_date,'fe_dwd.dwd_sf_shelf_product_status_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_product_status_log                         union all
select current_date,'fe_dwd.dwd_sf_user_present' as table_name,count(1) as nums from fe_dwd.dwd_sf_user_present                                                 union all
select current_date,'fe_dwd.dwd_sf_activity_user_integral_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_activity_user_integral_record               union all
-- select current_date,'fe_dwd.dwd_pub_user_integral_record' as table_name,count(1) as nums from fe_dwd.dwd_pub_user_integral_record                               union all
select current_date,'fe_dwd.dwd_sf_shelf_scope_detail' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_scope_detail                                     union all
select current_date,'fe_dwd.dwd_sf_shelf_smart_log' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_smart_log                                           union all
select current_date,'fe_dwd.dwd_pub_user_integral_growth' as table_name,count(1) as nums from fe_dwd.dwd_pub_user_integral_growth                               union all
select current_date,'fe_dwd.dwd_sf_shelf_transactions' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_transactions                                     union all
select current_date,'fe_dwd.dwd_sf_shelf_transfer_apply' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_transfer_apply                                 union all
select current_date,'fe_dwd.dwd_sf_shelf_transfer_shelf_info' as table_name,count(1) as nums from fe_dwd.dwd_sf_shelf_transfer_shelf_info                       union all
select current_date,'fe_dwd.dwd_sf_statistics_product_inventory' as table_name,count(1) as nums from fe_dwd.dwd_sf_statistics_product_inventory                 union all
select current_date,'fe_dwd.dwd_statistics_shelf_sale' as table_name,count(1) as nums from fe_dwd.dwd_statistics_shelf_sale                                     union all
select current_date,'fe_dwd.dwd_pub_member_level_record' as table_name,count(1) as nums from fe_dwd.dwd_pub_member_level_record                                 union all
-- select current_date,'fe_dwd.dwd_pub_import_shelf_product' as table_name,count(1) as nums from fe_dwd.dwd_pub_import_shelf_product                               union all
select current_date,'fe_dwd.dwd_product_area_pool_item' as table_name,count(1) as nums from fe_dwd.dwd_product_area_pool_item                                   union all
select current_date,'fe_dwd.dwd_sf_third_user_balance' as table_name,count(1) as nums from fe_dwd.dwd_sf_third_user_balance                                     union all
select current_date,'fe_dwd.dwd_sf_user_present_exchange_record' as table_name,count(1) as nums from fe_dwd.dwd_sf_user_present_exchange_record                ;






--------------------------------------------------------------------------------------------------------  非开发库数据同步

-- 30sec 全量
select 'fe_dm.dm_bill_check' as table_name,  count(1) as nums from fe_dm.dm_bill_check                                 union all
select 'fe_dm.dm_db_machine_shelf_gmv' as table_name,  count(1) as nums from fe_dm.dm_db_machine_shelf_gmv                       union all
select 'fe_dm.dm_en_new_user_balance' as table_name,  count(1) as nums from fe_dm.dm_en_new_user_balance                        union all
select 'fe_dm.dm_en_order_user' as table_name,  count(1) as nums from fe_dm.dm_en_order_user                              union all
select 'fe_dm.dm_en_user_channle_first' as table_name,  count(1) as nums from fe_dm.dm_en_user_channle_first                      union all
select 'fe_dm.dm_op_area_product_type_sku_limit_insert' as table_name,  count(1) as nums from fe_dm.dm_op_area_product_type_sku_limit_insert      union all
select 'fe_dm.dm_op_false_stock_danger_level' as table_name,  count(1) as nums from fe_dm.dm_op_false_stock_danger_level                union all
select 'fe_dm.dm_op_shelf_product_fill_update2' as table_name,  count(1) as nums from fe_dm.dm_op_shelf_product_fill_update2              union all
select 'fe_dm.dm_shelf_add_mgmv' as table_name,  count(1) as nums from fe_dm.dm_shelf_add_mgmv                             union all
select 'fe_dwd.dwd_auto_shelf_template' as table_name,  count(1) as nums from fe_dwd.dwd_auto_shelf_template                      union all
select 'fe_dwd.dwd_auto_shelf_undock_gmv_insert' as table_name,  count(1) as nums from fe_dwd.dwd_auto_shelf_undock_gmv_insert             union all
select 'fe_dwd.dwd_city_business' as table_name,  count(1) as nums from fe_dwd.dwd_city_business                            union all
select 'fe_dwd.dwd_count_process_aim_table_size' as table_name,  count(1) as nums from fe_dwd.dwd_count_process_aim_table_size             union all
select 'fe_dwd.dwd_csm_product_vote_submit_all' as table_name,  count(1) as nums from fe_dwd.dwd_csm_product_vote_submit_all              union all
select 'fe_dwd.dwd_en_org_area' as table_name,  count(1) as nums from fe_dwd.dwd_en_org_area                              union all
select 'fe_dwd.dwd_group_emp_user_day' as table_name,  count(1) as nums from fe_dwd.dwd_group_emp_user_day                       union all
select 'fe_dwd.dwd_group_exchange_card' as table_name,  count(1) as nums from fe_dwd.dwd_group_exchange_card                      union all
select 'fe_dwd.dwd_group_product_base_day' as table_name,  count(1) as nums from fe_dwd.dwd_group_product_base_day                   union all
select 'fe_dwd.dwd_order_refund_item' as table_name,  count(1) as nums from fe_dwd.dwd_order_refund_item                        union all
select 'fe_dwd.dwd_package_information' as table_name,  count(1) as nums from fe_dwd.dwd_package_information                      union all
select 'fe_dwd.dwd_prewarehouse_base_day' as table_name,  count(1) as nums from fe_dwd.dwd_prewarehouse_base_day                    union all
select 'fe_dwd.dwd_product_base_day_all' as table_name,  count(1) as nums from fe_dwd.dwd_product_base_day_all                     union all
select 'fe_dwd.dwd_product_label_all' as table_name,  count(1) as nums from fe_dwd.dwd_product_label_all                        union all
select 'fe_dwd.dwd_pub_auto_shelf_undock_insert' as table_name,  count(1) as nums from fe_dwd.dwd_pub_auto_shelf_undock_insert             union all
select 'fe_dwd.dwd_pub_comb_pay_without_weixin_result' as table_name,  count(1) as nums from fe_dwd.dwd_pub_comb_pay_without_weixin_result       union all
select 'fe_dwd.dwd_pub_dictionary' as table_name,  count(1) as nums from fe_dwd.dwd_pub_dictionary                           union all
select 'fe_dwd.dwd_pub_school_shelf_infornation' as table_name,  count(1) as nums from fe_dwd.dwd_pub_school_shelf_infornation             union all
select 'fe_dwd.dwd_pub_shelf_first_order_info' as table_name,  count(1) as nums from fe_dwd.dwd_pub_shelf_first_order_info               union all
select 'fe_dwd.dwd_pub_supplier_machine_bill_all' as table_name,  count(1) as nums from fe_dwd.dwd_pub_supplier_machine_bill_all            union all
select 'fe_dwd.dwd_pub_warehouse_business_area' as table_name,  count(1) as nums from fe_dwd.dwd_pub_warehouse_business_area              union all
select 'fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all' as table_name,  count(1) as nums from fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all   union all
select 'fe_dwd.dwd_sc_bdp_warehouse_receive_detail' as table_name,  count(1) as nums from fe_dwd.dwd_sc_bdp_warehouse_receive_detail          union all
select 'fe_dwd.dwd_shelf_base_day_all' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_base_day_all                       union all
select 'fe_dwd.dwd_shelf_machine_fault' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_machine_fault                      union all
select 'fe_dwd.dwd_shelf_machine_info' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_machine_info                       union all
select 'fe_dwd.dwd_shelf_machine_second_info' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_machine_second_info                union all
select 'fe_dwd.dwd_shelf_machine_slot_type' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_machine_slot_type                  union all
select 'fe_dwd.dwd_shelf_product_day_all' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_product_day_all                    union all
select 'fe_dwd.dwd_shelf_product_weeksales_detail' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_product_weeksales_detail           union all
select 'fe_dwd.dwd_shelf_smart_product_template_information' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_smart_product_template_information union all
select 'fe_dwd.dwd_shelf_transaction_exception_info' as table_name,  count(1) as nums from fe_dwd.dwd_shelf_transaction_exception_info         union all
select 'feods.d_en_fx_daily_num_user_balance' as table_name,  count(1) as nums from feods.d_en_fx_daily_num_user_balance                union all
select 'feods.d_en_gross_margin_rate_order_month' as table_name,  count(1) as nums from feods.d_en_gross_margin_rate_order_month            union all
select 'feods.d_en_gross_margin_rate_order_week' as table_name,  count(1) as nums from feods.d_en_gross_margin_rate_order_week             union all
select 'feods.d_en_gross_margin_rate_user_month' as table_name,  count(1) as nums from feods.d_en_gross_margin_rate_user_month             union all
select 'feods.d_en_gross_margin_rate_user_week' as table_name,  count(1) as nums from feods.d_en_gross_margin_rate_user_week              union all
select 'feods.d_en_org_address_info' as table_name,  count(1) as nums from feods.d_en_org_address_info                         union all
select 'feods.d_ma_tag_num' as table_name,  count(1) as nums from feods.d_ma_tag_num                                  union all
select 'feods.d_mp_purchase_sell_stock_summary' as table_name,  count(1) as nums from feods.d_mp_purchase_sell_stock_summary              union all
select 'feods.d_op_fill_day_sale_qty' as table_name,  count(1) as nums from feods.d_op_fill_day_sale_qty                        union all
select 'feods.d_op_load_dim' as table_name,  count(1) as nums from feods.d_op_load_dim                                 union all
select 'feods.d_op_shelfs_area' as table_name,  count(1) as nums from feods.d_op_shelfs_area                              union all
select 'feods.d_op_shelf_firstfill' as table_name,  count(1) as nums from feods.d_op_shelf_firstfill                          union all
select 'feods.d_op_shelf_info' as table_name,  count(1) as nums from feods.d_op_shelf_info                               union all
select 'feods.d_op_sp_avgsal30' as table_name,  count(1) as nums from feods.d_op_sp_avgsal30                              union all
select 'feods.d_op_sp_shelf7_stock3' as table_name,  count(1) as nums from feods.d_op_sp_shelf7_stock3                         union all
select 'feods.fjr_abnormal_nsale_shelf_product' as table_name,  count(1) as nums from feods.fjr_abnormal_nsale_shelf_product              union all
select 'feods.fjr_shelf_archives' as table_name,  count(1) as nums from feods.fjr_shelf_archives                            union all
select 'feods.fjr_shelf_board' as table_name,  count(1) as nums from feods.fjr_shelf_board                               union all
select 'sserp.T_BD_MATERIALGROUP_L' as table_name,  count(1) as nums from sserp.T_BD_MATERIALGROUP_L                          union all
select 'sserp.T_BD_MATERIAL' as table_name,  count(1) as nums from sserp.T_BD_MATERIAL                                 union all
select 'sserp.T_BD_MATERIAL_L' as table_name,  count(1) as nums from sserp.T_BD_MATERIAL_L                               union all
select 'sserp.T_BD_STOCKSTATUS_L' as table_name,  count(1) as nums from sserp.T_BD_STOCKSTATUS_L                            union all
select 'sserp.T_BD_STOCK' as table_name,  count(1) as nums from sserp.T_BD_STOCK                                    union all
select 'sserp.T_BD_STOCK_L' as table_name,  count(1) as nums from sserp.T_BD_STOCK_L                                  union all
select 'sserp.T_BD_SUPPLIERBASE' as table_name,  count(1) as nums from sserp.T_BD_SUPPLIERBASE                             union all
select 'sserp.T_BD_SUPPLIER_L' as table_name,  count(1) as nums from sserp.T_BD_SUPPLIER_L                               union all
select 'sserp.T_ORG_ORGANIZATIONS_L' as table_name,  count(1) as nums from sserp.T_ORG_ORGANIZATIONS_L                         union all
select 'sserp.T_PUR_MRAPPENTRY' as table_name,  count(1) as nums from sserp.T_PUR_MRAPPENTRY                              union all
select 'sserp.T_PUR_MRAPP' as table_name,  count(1) as nums from sserp.T_PUR_MRAPP                                   union all
select 'sserp.T_PUR_MRBENTRY' as table_name,  count(1) as nums from sserp.T_PUR_MRBENTRY                                union all
select 'sserp.T_PUR_MRB' as table_name,  count(1) as nums from sserp.T_PUR_MRB                                     union all
select 'sserp.T_PUR_POORDERENTRY' as table_name,  count(1) as nums from sserp.T_PUR_POORDERENTRY                            union all
select 'sserp.T_PUR_POORDERENTRY_F' as table_name,  count(1) as nums from sserp.T_PUR_POORDERENTRY_F                          union all
select 'sserp.T_PUR_POORDER' as table_name,  count(1) as nums from sserp.T_PUR_POORDER                                 union all
select 'sserp.T_PUR_PRICELISTENTRY' as table_name,  count(1) as nums from sserp.T_PUR_PRICELISTENTRY                          union all
select 'sserp.T_PUR_PRICELIST' as table_name,  count(1) as nums from sserp.T_PUR_PRICELIST                               union all
select 'sserp.T_PUR_RECEIVEENTRY' as table_name,  count(1) as nums from sserp.T_PUR_RECEIVEENTRY                            union all
select 'sserp.T_PUR_RECEIVE' as table_name,  count(1) as nums from sserp.T_PUR_RECEIVE                                 union all
select 'sserp.T_STK_INSTOCKENTRY' as table_name,  count(1) as nums from sserp.T_STK_INSTOCKENTRY                            union all
select 'sserp.T_STK_INSTOCK' as table_name,  count(1) as nums from sserp.T_STK_INSTOCK                                 union all
select 'sserp.T_STK_INVENTORY' as table_name,  count(1) as nums from sserp.T_STK_INVENTORY                               union all
select 'sserp.T_STK_OUTSTOCKAPPLY' as table_name,  count(1) as nums from sserp.T_STK_OUTSTOCKAPPLY                           union all
select 'sserp.T_STK_STKTRANSFERAPPENTRY' as table_name,  count(1) as nums from sserp.T_STK_STKTRANSFERAPPENTRY                     union all
select 'sserp.T_STK_STKTRANSFERAPPENTRY_E' as table_name,  count(1) as nums from sserp.T_STK_STKTRANSFERAPPENTRY_E                   union all
select 'sserp.T_STK_STKTRANSFERAPP' as table_name,  count(1) as nums from sserp.T_STK_STKTRANSFERAPP                          union all
select 'sserp.V_BD_BUYER_L' as table_name,  count(1) as nums from sserp.V_BD_BUYER_L                                  union all
select 'feods.zs_area_product_sale_flag' as table_name,  count(1) as nums from feods.zs_area_product_sale_flag                     union all
select 'feods.zs_qy_phone_area' as table_name,  count(1) as nums from feods.zs_qy_phone_area                              union all
select 'feods.zs_shelf_flag' as table_name,  count(1) as nums from feods.zs_shelf_flag                                 union all
select 'feods.zs_shelf_member_flag' as table_name,  count(1) as nums from feods.zs_shelf_member_flag                          union all
select 'feods.zs_shelf_product_flag' as table_name,  count(1) as nums from feods.zs_shelf_product_flag                 union all 
select 'fe_dwd.dwd_check_extend_recent_62' as table_name,  count(1) as nums from fe_dwd.dwd_check_extend_recent_62                         union all
select 'fe_dm.dm_op_valid_danger_flag' as table_name,  count(1) as nums from fe_dm.dm_op_valid_danger_flag                        union all
select 'fe_dwd.dwd_pub_product_area_pool_item' as table_name,  count(1) as nums from fe_dwd.dwd_pub_product_area_pool_item           union all
select 'fe_dm.dm_op_su_s_stat' as table_name,  count(1) as nums from fe_dm.dm_op_su_s_stat           union all
select 'fe_dm.dm_op_su_u_stat' as table_name,  count(1) as nums from fe_dm.dm_op_su_u_stat           union all
select 'fe_dm.dm_op_su_stat' as table_name,  count(1) as nums from fe_dm.dm_op_su_stat          ;


-- 增量  3min
select 'fe_dm.dm_lo_area_performance_report_everyday' as table_name, count(1) as nums from fe_dm.dm_lo_area_performance_report_everyday    union all     
select 'fe_dm.dm_lo_fill_for_month_label' as table_name, count(1) as nums from fe_dm.dm_lo_fill_for_month_label                union all
select 'fe_dm.dm_ma_HighProfit_list_monthly' as table_name, count(1) as nums from fe_dm.dm_ma_HighProfit_list_monthly             union all
select 'feods.dm_ma_shelfInfo_extend' as table_name, count(1) as nums from feods.dm_ma_shelfInfo_extend                    union all
select 'fe_dm.dm_ma_sp_plc' as table_name, count(1) as nums from fe_dm.dm_ma_sp_plc                              union all
select 'fe_dm.dm_ma_sp_stopfill' as table_name, count(1) as nums from fe_dm.dm_ma_sp_stopfill                         union all
select 'fe_dm.dm_op_auto_push_fill_date2_his' as table_name, count(1) as nums from fe_dm.dm_op_auto_push_fill_date2_his            union all
select 'fe_dm.dm_op_new_shelf_suggest_list' as table_name, count(1) as nums from fe_dm.dm_op_new_shelf_suggest_list              union all
select 'fe_dm.dm_op_order_sku_relation' as table_name, count(1) as nums from fe_dm.dm_op_order_sku_relation                  union all
select 'fe_dm.dm_op_shelf_product_fill_suggest_label' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_fill_suggest_label    union all
select 'fe_dm.dm_op_shelf_product_start_fill_label' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_start_fill_label      union all
select 'fe_dm.dm_pub_area_product_stat' as table_name, count(1) as nums from fe_dm.dm_pub_area_product_stat                  union all
select 'fe_dm.dm_pub_third_user_balance_day' as table_name, count(1) as nums from fe_dm.dm_pub_third_user_balance_day             union all
select 'fe_dm.dm_shelf_mgmv' as table_name, count(1) as nums from fe_dm.dm_shelf_mgmv   where month_id>='2020-09' union all  -- 9月份开始从实例2同步到实例1
select 'fe_dm.dm_shelf_wgmv' as table_name, count(1) as nums from fe_dm.dm_shelf_wgmv   where sdate>='2020-09-01' union all  -- 9月份开始从实例2同步到实例1
select 'fe_dm.dm_user_suspect' as table_name, count(1) as nums from fe_dm.dm_user_suspect                           union all
-- select 'fe_dwd.dwd_activity_invitation_information' as table_name, count(1) as nums from fe_dwd.dwd_activity_invitation_information      union all
-- select 'fe_dwd.dwd_check_base_day_inc' as table_name, count(1) as nums from fe_dwd.dwd_check_base_day_inc                   union all
select 'fe_dwd.dwd_en_combined_payment_order' as table_name, count(1) as nums from fe_dwd.dwd_en_combined_payment_order            union all
select 'fe_dwd.dwd_en_distribute_detail_fx' as table_name, count(1) as nums from fe_dwd.dwd_en_distribute_detail_fx              union all
-- select 'fe_dwd.dwd_fill_day_inc' as table_name, count(1) as nums from fe_dwd.dwd_fill_day_inc                         union all
-- select 'fe_dwd.dwd_fill_day_inc_recent_two_month' as table_name, count(1) as nums from fe_dwd.dwd_fill_day_inc_recent_two_month        union all
select 'fe_dwd.dwd_group_order_coupon_day' as table_name, count(1) as nums from fe_dwd.dwd_group_order_coupon_day               union all
-- select 'fe_dwd.dwd_group_order_refound_address_day' as table_name, count(1) as nums from fe_dwd.dwd_group_order_refound_address_day      union all
select 'fe_dwd.dwd_group_wallet_log_business' as table_name, count(1) as nums from fe_dwd.dwd_group_wallet_log_business            union all
select 'fe_dwd.dwd_lo_order_logistics_task_base_all' as table_name, count(1) as nums from fe_dwd.dwd_lo_order_logistics_task_base_all     union all
select 'fe_dwd.dwd_op_out_of_system_order_yht' as table_name, count(1) as nums from fe_dwd.dwd_op_out_of_system_order_yht           union all
-- select 'fe_dwd.dwd_order_item_refund_day' as table_name, count(1) as nums from fe_dwd.dwd_order_item_refund_day                union all
-- select 'fe_dwd.dwd_order_item_refund_real_time' as table_name, count(1) as nums from fe_dwd.dwd_order_item_refund_real_time          union all
-- select 'fe_dwd.dwd_pub_activity_order_shelf_product' as table_name, count(1) as nums from fe_dwd.dwd_pub_activity_order_shelf_product     union all
-- select 'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name, count(1) as nums from fe_dwd.dwd_pub_order_item_recent_two_month      union all
select 'fe_dwd.dwd_pub_order_shelf_product_yht' as table_name, count(1) as nums from fe_dwd.dwd_pub_order_shelf_product_yht          union all
select 'fe_dwd.dwd_sc_bdp_warehouse_shipment_detail' as table_name, count(1) as nums from fe_dwd.dwd_sc_bdp_warehouse_shipment_detail     union all
select 'fe_dwd.dwd_sc_bdp_warehouse_stock_daily' as table_name, count(1) as nums from fe_dwd.dwd_sc_bdp_warehouse_stock_daily         union all
select 'fe_dwd.dwd_shelf_check_recent62' as table_name, count(1) as nums from fe_dwd.dwd_shelf_check_recent62                 union all
-- select 'fe_dwd.dwd_shelf_day_his' as table_name, count(1) as nums from fe_dwd.dwd_shelf_day_his                        union all
-- select 'fe_dwd.dwd_user_day_inc' as table_name, count(1) as nums from fe_dwd.dwd_user_day_inc                         union all
select 'feods.d_dv_emp_org' as table_name, count(1) as nums from feods.d_dv_emp_org                              union all
select 'feods.d_en_emp_org' as table_name, count(1) as nums from feods.d_en_emp_org                              union all
-- select 'feods.d_en_fx_balance' as table_name, count(1) as nums from feods.d_en_fx_balance                           union all
select 'feods.d_en_fx_new_user_daily_balance' as table_name, count(1) as nums from feods.d_en_fx_new_user_daily_balance            union all
select 'feods.D_MP_CMBC_payment' as table_name, count(1) as nums from feods.D_MP_CMBC_payment                         union all
select 'feods.D_MP_epay_shelf_detail' as table_name, count(1) as nums from feods.D_MP_epay_shelf_detail                    union all
select 'feods.d_mp_ssf_payment' as table_name, count(1) as nums from feods.d_mp_ssf_payment                          union all
select 'feods.d_mp_weixin_payment' as table_name, count(1) as nums from feods.d_mp_weixin_payment  where sdate >= '2020-09-01' union all  -- 数据量太大，选择2020-09-01之后的数据
select 'feods.d_op_fill3_detail' as table_name, count(1) as nums from feods.d_op_fill3_detail                         union all
select 'feods.d_op_shelf_board_month' as table_name, count(1) as nums from feods.d_op_shelf_board_month                    union all
select 'feods.d_op_sp_sal_sto_detail' as table_name, count(1) as nums from feods.d_op_sp_sal_sto_detail                    union all
select 'feods.d_op_sp_stock_detail_after' as table_name, count(1) as nums from feods.d_op_sp_stock_detail_after                union all
select 'feods.d_op_sp_stock_detail' as table_name, count(1) as nums from feods.d_op_sp_stock_detail                      union all
select 'feods.fjr_newshelf_stat' as table_name, count(1) as nums from feods.fjr_newshelf_stat                         union all
select 'feods.mongo_shelf_manager_behavior_log' as table_name, count(1) as nums from feods.mongo_shelf_manager_behavior_log where logTimeDate >= subdate(current_date,interval 120 day) union all
select 'feods.pj_area_sale_dashboard' as table_name, count(1) as nums from feods.pj_area_sale_dashboard                    union all
select 'feods.pj_zs_goods_damaged' as table_name, count(1) as nums from feods.pj_zs_goods_damaged                       union all
select 'feods.sap_pmp_hos_emp_base_info' as table_name, count(1) as nums from feods.sap_pmp_hos_emp_base_info                 union all
select 'sserp.T_STK_OUTSTOCKAPPLYENTRY ' as table_name, count(1) as nums from sserp.T_STK_OUTSTOCKAPPLYENTRY                  union all
select 'feods.zs_shelf_flag_his' as table_name, count(1) as nums from feods.zs_shelf_flag_his  where sdate<current_date  union all  -- 同步到实例2的时间是晚上11点多
select 'feods.zs_shelf_manager_check_monitor_point' as table_name, count(1) as nums from feods.zs_shelf_manager_check_monitor_point      union all
select 'feods.zs_shelf_manager_suspect_problem_label' as table_name, count(1) as nums from feods.zs_shelf_manager_suspect_problem_label    ;











-- 23sec 全量
select 'fe_dm.dm_bill_check' as table_name, count(1) as nums from fe_dm.dm_bill_check                                   union all
select 'fe_dm.dm_db_machine_shelf_gmv' as table_name, count(1) as nums from fe_dm.dm_db_machine_shelf_gmv                         union all
select 'fe_dm.dm_en_new_user_balance' as table_name, count(1) as nums from fe_dm.dm_en_new_user_balance                          union all
select 'fe_dm.dm_en_order_user' as table_name, count(1) as nums from fe_dm.dm_en_order_user                                union all
select 'fe_dm.dm_en_user_channle_first' as table_name, count(1) as nums from fe_dm.dm_en_user_channle_first                        union all
select 'fe_dm.dm_op_area_product_type_sku_limit_insert' as table_name, count(1) as nums from fe_dm.dm_op_area_product_type_sku_limit_insert        union all
select 'fe_dm.dm_op_false_stock_danger_level' as table_name, count(1) as nums from fe_dm.dm_op_false_stock_danger_level                  union all
select 'fe_dm.dm_op_shelf_product_fill_update2' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_fill_update2                union all
select 'fe_dm.dm_shelf_add_mgmv' as table_name, count(1) as nums from fe_dm.dm_shelf_add_mgmv                               union all
select 'fe_dwd.dwd_auto_shelf_template' as table_name, count(1) as nums from fe_dwd.dwd_auto_shelf_template                        union all
select 'fe_dwd.dwd_auto_shelf_undock_gmv_insert' as table_name, count(1) as nums from fe_dwd.dwd_auto_shelf_undock_gmv_insert               union all
select 'fe_dwd.dwd_city_business' as table_name, count(1) as nums from fe_dwd.dwd_city_business                              union all
select 'fe_dwd.dwd_count_process_aim_table_size_from_one' as table_name, count(1) as nums from fe_dwd.dwd_count_process_aim_table_size_from_one      union all
select 'fe_dwd.dwd_csm_product_vote_submit_all' as table_name, count(1) as nums from fe_dwd.dwd_csm_product_vote_submit_all                union all
select 'fe_dwd.dwd_en_org_area' as table_name, count(1) as nums from fe_dwd.dwd_en_org_area                                union all
select 'fe_dwd.dwd_group_emp_user_day' as table_name, count(1) as nums from fe_dwd.dwd_group_emp_user_day                         union all
select 'fe_dwd.dwd_group_exchange_card' as table_name, count(1) as nums from fe_dwd.dwd_group_exchange_card                        union all
select 'fe_dwd.dwd_group_product_base_day' as table_name, count(1) as nums from fe_dwd.dwd_group_product_base_day                     union all
select 'fe_dwd.dwd_order_refund_item' as table_name, count(1) as nums from fe_dwd.dwd_order_refund_item                          union all
select 'fe_dwd.dwd_package_information' as table_name, count(1) as nums from fe_dwd.dwd_package_information                        union all
select 'fe_dwd.dwd_prewarehouse_base_day' as table_name, count(1) as nums from fe_dwd.dwd_prewarehouse_base_day                      union all
select 'fe_dwd.dwd_product_base_day_all' as table_name, count(1) as nums from fe_dwd.dwd_product_base_day_all                       union all
select 'fe_dwd.dwd_product_label_all' as table_name, count(1) as nums from fe_dwd.dwd_product_label_all                          union all
select 'fe_dwd.dwd_pub_auto_shelf_undock_insert' as table_name, count(1) as nums from fe_dwd.dwd_pub_auto_shelf_undock_insert               union all
select 'fe_dwd.dwd_pub_comb_pay_without_weixin_result' as table_name, count(1) as nums from fe_dwd.dwd_pub_comb_pay_without_weixin_result         union all
select 'fe_dwd.dwd_pub_dictionary' as table_name, count(1) as nums from fe_dwd.dwd_pub_dictionary                             union all
select 'fe_dwd.dwd_pub_school_shelf_infornation' as table_name, count(1) as nums from fe_dwd.dwd_pub_school_shelf_infornation               union all
select 'fe_dwd.dwd_pub_shelf_first_order_info' as table_name, count(1) as nums from fe_dwd.dwd_pub_shelf_first_order_info                 union all
select 'fe_dwd.dwd_pub_supplier_machine_bill_all' as table_name, count(1) as nums from fe_dwd.dwd_pub_supplier_machine_bill_all              union all
select 'fe_dwd.dwd_pub_warehouse_business_area' as table_name, count(1) as nums from fe_dwd.dwd_pub_warehouse_business_area                union all
select 'fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all' as table_name, count(1) as nums from fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all     union all
select 'fe_dwd.dwd_sc_bdp_warehouse_receive_detail' as table_name, count(1) as nums from fe_dwd.dwd_sc_bdp_warehouse_receive_detail            union all
select 'fe_dwd.dwd_shelf_base_day_all' as table_name, count(1) as nums from fe_dwd.dwd_shelf_base_day_all                         union all
select 'fe_dwd.dwd_shelf_machine_fault' as table_name, count(1) as nums from fe_dwd.dwd_shelf_machine_fault                        union all
select 'fe_dwd.dwd_shelf_machine_info' as table_name, count(1) as nums from fe_dwd.dwd_shelf_machine_info                         union all
select 'fe_dwd.dwd_shelf_machine_second_info' as table_name, count(1) as nums from fe_dwd.dwd_shelf_machine_second_info                  union all
select 'fe_dwd.dwd_shelf_machine_slot_type' as table_name, count(1) as nums from fe_dwd.dwd_shelf_machine_slot_type                    union all
select 'fe_dwd.dwd_shelf_product_day_all' as table_name, count(1) as nums from fe_dwd.dwd_shelf_product_day_all                      union all
select 'fe_dwd.dwd_shelf_product_weeksales_detail' as table_name, count(1) as nums from fe_dwd.dwd_shelf_product_weeksales_detail             union all
select 'fe_dwd.dwd_shelf_smart_product_template_information' as table_name, count(1) as nums from fe_dwd.dwd_shelf_smart_product_template_information   union all
select 'fe_dwd.dwd_shelf_transaction_exception_info' as table_name, count(1) as nums from fe_dwd.dwd_shelf_transaction_exception_info           union all
select 'fe_dm.dm_en_fx_daily_num_user_balance' as table_name, count(1) as nums from fe_dm.dm_en_fx_daily_num_user_balance                 union all
select 'fe_dm.dm_en_gross_margin_rate_order_month' as table_name, count(1) as nums from fe_dm.dm_en_gross_margin_rate_order_month             union all
select 'fe_dm.dm_en_gross_margin_rate_order_week' as table_name, count(1) as nums from fe_dm.dm_en_gross_margin_rate_order_week              union all
select 'fe_dm.dm_en_gross_margin_rate_user_month' as table_name, count(1) as nums from fe_dm.dm_en_gross_margin_rate_user_month              union all
select 'fe_dm.dm_en_gross_margin_rate_user_week' as table_name, count(1) as nums from fe_dm.dm_en_gross_margin_rate_user_week               union all
select 'fe_dwd.dwd_en_org_address_info' as table_name, count(1) as nums from fe_dwd.dwd_en_org_address_info                        union all
select 'fe_dm.dm_ma_tag_num' as table_name, count(1) as nums from fe_dm.dm_ma_tag_num                                   union all
select 'fe_dm.dm_mp_purchase_sell_stock_summary' as table_name, count(1) as nums from fe_dm.dm_mp_purchase_sell_stock_summary               union all
select 'fe_dm.dm_op_fill_day_sale_qty' as table_name, count(1) as nums from fe_dm.dm_op_fill_day_sale_qty                         union all
select 'fe_dwd.dwd_op_load_dim' as table_name, count(1) as nums from fe_dwd.dwd_op_load_dim                                union all
select 'fe_dm.dm_pub_shelfs_area' as table_name, count(1) as nums from fe_dm.dm_pub_shelfs_area                              union all
select 'fe_dm.dm_op_shelf_firstfill' as table_name, count(1) as nums from fe_dm.dm_op_shelf_firstfill                           union all
select 'fe_dm.dm_op_shelf_info' as table_name, count(1) as nums from fe_dm.dm_op_shelf_info                                union all
select 'fe_dm.dm_op_sp_avgsal30' as table_name, count(1) as nums from fe_dm.dm_op_sp_avgsal30                               union all
select 'fe_dm.dm_op_sp_shelf7_stock3' as table_name, count(1) as nums from fe_dm.dm_op_sp_shelf7_stock3                          union all
select 'fe_dm.dm_op_abnormal_nsale_shelf_product' as table_name, count(1) as nums from fe_dm.dm_op_abnormal_nsale_shelf_product              union all
select 'fe_dm.dm_pub_shelf_archives' as table_name, count(1) as nums from fe_dm.dm_pub_shelf_archives                           union all
select 'fe_dm.dm_pub_shelf_board' as table_name, count(1) as nums from fe_dm.dm_pub_shelf_board                              union all
select 'fe_dwd.dwd_sserp_t_bd_materialgroup_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_materialgroup_l                 union all
select 'fe_dwd.dwd_sserp_t_bd_material' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_material                        union all
select 'fe_dwd.dwd_sserp_t_bd_material_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_material_l                      union all
select 'fe_dwd.dwd_sserp_t_bd_stockstatus_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_stockstatus_l                   union all
select 'fe_dwd.dwd_sserp_t_bd_stock' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_stock                           union all
select 'fe_dwd.dwd_sserp_t_bd_stock_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_stock_l                         union all
select 'fe_dwd.dwd_sserp_t_bd_supplierbase' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_supplierbase                    union all
select 'fe_dwd.dwd_sserp_t_bd_supplier_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_bd_supplier_l                      union all
select 'fe_dwd.dwd_sserp_t_org_organizations_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_org_organizations_l                union all
select 'fe_dwd.dwd_sserp_t_pur_mrappentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_mrappentry                     union all
select 'fe_dwd.dwd_sserp_t_pur_mrapp' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_mrapp                          union all
select 'fe_dwd.dwd_sserp_t_pur_mrbentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_mrbentry                       union all
select 'fe_dwd.dwd_sserp_t_pur_mrb' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_mrb                            union all
select 'fe_dwd.dwd_sserp_t_pur_poorderentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_poorderentry                   union all
select 'fe_dwd.dwd_sserp_t_pur_poorderentry_f' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_poorderentry_f                 union all
select 'fe_dwd.dwd_sserp_t_pur_poorder' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_poorder                        union all
select 'fe_dwd.dwd_sserp_t_pur_pricelistentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_pricelistentry                 union all
select 'fe_dwd.dwd_sserp_t_pur_pricelist' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_pricelist                      union all
select 'fe_dwd.dwd_sserp_t_pur_receiveentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_receiveentry                   union all
select 'fe_dwd.dwd_sserp_t_pur_receive' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_pur_receive                        union all
select 'fe_dwd.dwd_sserp_t_stk_instockentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_instockentry                   union all
select 'fe_dwd.dwd_sserp_t_stk_instock' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_instock                        union all
select 'fe_dwd.dwd_sserp_t_stk_inventory' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_inventory                      union all
select 'fe_dwd.dwd_sserp_t_stk_outstockapply' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_outstockapply                  union all
select 'fe_dwd.dwd_sserp_t_stk_stktransferappentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_stktransferappentry            union all
select 'fe_dwd.dwd_sserp_t_stk_stktransferappentry_e' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_stktransferappentry_e          union all
select 'fe_dwd.dwd_sserp_t_stk_stktransferapp' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_stktransferapp                 union all
select 'fe_dwd.dwd_sserp_v_bd_buyer_l' as table_name, count(1) as nums from fe_dwd.dwd_sserp_v_bd_buyer_l                         union all
select 'fe_dm.dm_area_product_sale_flag' as table_name, count(1) as nums from fe_dm.dm_area_product_sale_flag                       union all
select 'fe_dwd.dwd_group_phone_area' as table_name, count(1) as nums from fe_dwd.dwd_group_phone_area                           union all
select 'fe_dm.dm_shelf_flag' as table_name, count(1) as nums from fe_dm.dm_shelf_flag                                   union all
select 'fe_dm.dm_shelf_member_flag' as table_name, count(1) as nums from fe_dm.dm_shelf_member_flag                            union all
select 'fe_dm.dm_shelf_product_flag' as table_name, count(1) as nums from fe_dm.dm_shelf_product_flag                           ;




-- 增量

select 'fe_dm.dm_lo_area_performance_report_everyday' as table_name, count(1) as nums from fe_dm.dm_lo_area_performance_report_everyday  union all
select 'fe_dm.dm_lo_fill_for_month_label' as table_name, count(1) as nums from fe_dm.dm_lo_fill_for_month_label              union all
select 'fe_dm.dm_ma_highprofit_list_monthly' as table_name, count(1) as nums from fe_dm.dm_ma_highprofit_list_monthly           union all
select 'fe_dm.dm_ma_shelfInfo_extend' as table_name, count(1) as nums from fe_dm.dm_ma_shelfInfo_extend                  union all
select 'fe_dm.dm_ma_sp_plc' as table_name, count(1) as nums from fe_dm.dm_ma_sp_plc                            union all
select 'fe_dm.dm_ma_sp_stopfill' as table_name, count(1) as nums from fe_dm.dm_ma_sp_stopfill                       union all
select 'fe_dm.dm_op_auto_push_fill_date2_his' as table_name, count(1) as nums from fe_dm.dm_op_auto_push_fill_date2_his          union all
select 'fe_dm.dm_op_new_shelf_suggest_list' as table_name, count(1) as nums from fe_dm.dm_op_new_shelf_suggest_list            union all
select 'fe_dm.dm_op_order_sku_relation' as table_name, count(1) as nums from fe_dm.dm_op_order_sku_relation                union all
select 'fe_dm.dm_op_shelf_product_fill_suggest_label' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_fill_suggest_label  union all
select 'fe_dm.dm_op_shelf_product_start_fill_label' as table_name, count(1) as nums from fe_dm.dm_op_shelf_product_start_fill_label    union all
select 'fe_dm.dm_pub_area_product_stat' as table_name, count(1) as nums from fe_dm.dm_pub_area_product_stat   where sdate>= SUBDATE(CURDATE(),62)   union all  -- 实例1保留62天数据
select 'fe_dm.dm_pub_third_user_balance_day' as table_name, count(1) as nums from fe_dm.dm_pub_third_user_balance_day           union all
select 'fe_dm.dm_shelf_mgmv' as table_name, count(1) as nums from fe_dm.dm_shelf_mgmv   where month_id>='2020-09' union all
select 'fe_dm.dm_shelf_wgmv' as table_name, count(1) as nums from fe_dm.dm_shelf_wgmv    where sdate>='2020-09-01' union all
select 'fe_dm.dm_user_suspect' as table_name, count(1) as nums from fe_dm.dm_user_suspect                         union all
-- select 'fe_dwd.dwd_activity_invitation_information' as table_name, count(1) as nums from fe_dwd.dwd_activity_invitation_information    union all
-- select 'fe_temp.dwd_check_base_day_inc' as table_name, count(1) as nums from fe_temp.dwd_check_base_day_inc                union all
select 'fe_dwd.dwd_en_combined_payment_order' as table_name, count(1) as nums from fe_dwd.dwd_en_combined_payment_order          union all
select 'fe_dwd.dwd_en_distribute_detail_fx' as table_name, count(1) as nums from fe_dwd.dwd_en_distribute_detail_fx            union all
-- select 'fe_temp.dwd_fill_day_inc' as table_name, count(1) as nums from fe_temp.dwd_fill_day_inc                      union all
-- select 'fe_temp.dwd_fill_day_inc_recent_two_month' as table_name, count(1) as nums from fe_temp.dwd_fill_day_inc_recent_two_month     union all
select 'fe_dwd.dwd_group_order_coupon_day' as table_name, count(1) as nums from fe_dwd.dwd_group_order_coupon_day             union all
-- select 'fe_temp.dwd_group_order_refound_address_day' as table_name, count(1) as nums from fe_temp.dwd_group_order_refound_address_day   union all
select 'fe_dwd.dwd_group_wallet_log_business' as table_name, count(1) as nums from fe_dwd.dwd_group_wallet_log_business  where load_time>=subdate(current_date,7)        union all
select 'fe_dwd.dwd_lo_order_logistics_task_base_all' as table_name, count(1) as nums from fe_dwd.dwd_lo_order_logistics_task_base_all   union all
select 'fe_dwd.dwd_op_out_of_system_order_yht' as table_name, count(1) as nums from fe_dwd.dwd_op_out_of_system_order_yht         union all
-- select 'fe_temp.dwd_order_item_refund_day' as table_name, count(1) as nums from fe_temp.dwd_order_item_refund_day             union all
-- select 'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name, count(1) as nums from fe_dwd.dwd_pub_order_item_recent_two_month    union all
-- select 'fe_temp.dwd_pub_activity_order_shelf_product' as table_name, count(1) as nums from fe_temp.dwd_pub_activity_order_shelf_product  union all
-- select 'fe_temp.dwd_pub_order_item_recent_two_month' as table_name, count(1) as nums from fe_temp.dwd_pub_order_item_recent_two_month   union all
select 'fe_dwd.dwd_pub_order_shelf_product_yht' as table_name, count(1) as nums from fe_dwd.dwd_pub_order_shelf_product_yht        union all
select 'fe_dwd.dwd_sc_bdp_warehouse_shipment_detail' as table_name, count(1) as nums from fe_dwd.dwd_sc_bdp_warehouse_shipment_detail   union all
select 'fe_dwd.dwd_sc_bdp_warehouse_stock_daily' as table_name, count(1) as nums from fe_dwd.dwd_sc_bdp_warehouse_stock_daily       union all
select 'fe_dwd.dwd_shelf_check_recent62' as table_name, count(1) as nums from fe_dwd.dwd_shelf_check_recent62               union all
-- select 'fe_temp.dwd_shelf_day_his' as table_name, count(1) as nums from fe_temp.dwd_shelf_day_his                     union all
-- select 'fe_temp.dwd_user_day_inc' as table_name, count(1) as nums from fe_temp.dwd_user_day_inc                      union all
select 'fe_dwd.dwd_dv_emp_org' as table_name, count(1) as nums from fe_dwd.dwd_dv_emp_org                         union all
select 'fe_dwd.dwd_en_emp_org' as table_name, count(1) as nums from fe_dwd.dwd_en_emp_org                         union all
-- select 'fe_dm.dm_en_fx_balance' as table_name, count(1) as nums from fe_dm.dm_en_fx_balance                        union all
select 'fe_dm.dm_en_fx_new_user_daily_balance' as table_name, count(1) as nums from fe_dm.dm_en_fx_new_user_daily_balance         union all
select 'fe_dwd.dwd_mp_cmbc_payment' as table_name, count(1) as nums from fe_dwd.dwd_mp_cmbc_payment                    union all
select 'fe_dwd.dwd_mp_epay_shelf_detail' as table_name, count(1) as nums from fe_dwd.dwd_mp_epay_shelf_detail               union all
select 'fe_dwd.dwd_mp_ssf_payment' as table_name, count(1) as nums from fe_dwd.dwd_mp_ssf_payment                     union all
select 'fe_dwd.dwd_mp_weixin_payment' as table_name, count(1) as nums from fe_dwd.dwd_mp_weixin_payment    where sdate >= '2020-09-01' union all  -- 数据量太大，选择2020-09-01之后的数据
select 'fe_dm.dm_op_fill3_detail' as table_name, count(1) as nums from fe_dm.dm_op_fill3_detail                      union all
select 'fe_dm.dm_pub_shelf_board_month' as table_name, count(1) as nums from fe_dm.dm_pub_shelf_board_month where sdate >='2020-09-30' union all  -- 实例1只有2020-09-30之后的数据
select 'fe_dm.dm_op_sp_sal_sto_detail' as table_name, count(1) as nums from fe_dm.dm_op_sp_sal_sto_detail where month_id>= DATE_FORMAT(subdate(SUBDATE(CURRENT_DATE,INTERVAL 1 DAY),interval 4 month),'%Y-%m') union all  -- 实例1只保留5个月的数据
select 'fe_dwd.dwd_shelf_product_stock_detail_after' as table_name, count(1) as nums from fe_dwd.dwd_shelf_product_stock_detail_after  where month_id >=date_format(subdate(current_date,interval 1 month),'%Y-%m')  union all    -- 实例1只保留两个月份数据
select 'fe_dwd.dwd_shelf_product_stock_detail' as table_name, count(1) as nums from fe_dwd.dwd_shelf_product_stock_detail  where month_id >=date_format(subdate(current_date,interval 1 month),'%Y-%m') union all   -- 实例1只保留两个月份数据
select 'fe_dm.dm_op_newshelf_stat' as table_name, count(1) as nums from fe_dm.dm_op_newshelf_stat where sdate >=SUBDATE(CURDATE(),INTERVAL 62 DAY) union all
select 'fe_dwd.dwd_mongo_shelf_manager_behavior_log' as table_name, count(1) as nums from fe_dwd.dwd_mongo_shelf_manager_behavior_log   union all
select 'fe_dm.dm_area_sale_dashboard' as table_name, count(1) as nums from fe_dm.dm_area_sale_dashboard where sdate>=SUBDATE(CURDATE(),40) union all
select 'fe_dm.dm_pj_zs_goods_damaged' as table_name, count(1) as nums from fe_dm.dm_pj_zs_goods_damaged                  union all
select 'fe_dwd.dwd_sap_pmp_hos_emp_base_info' as table_name, count(1) as nums from fe_dwd.dwd_sap_pmp_hos_emp_base_info          union all
select 'fe_dwd.dwd_sserp_t_stk_outstockapplyentry' as table_name, count(1) as nums from fe_dwd.dwd_sserp_t_stk_outstockapplyentry     union all
select 'fe_dm.dm_shelf_flag_his' as table_name, count(1) as nums from fe_dm.dm_shelf_flag_his                       union all
select 'fe_dm.dm_shelf_manager_check_monitor_point' as table_name, count(1) as nums from fe_dm.dm_shelf_manager_check_monitor_point    union all
select 'fe_dm.dm_shelf_manager_suspect_problem_label' as table_name, count(1) as nums from fe_dm.dm_shelf_manager_suspect_problem_label  ;





-- 实例2
SELECT 'fe_dwd.dwd_check_base_day_inc' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_check_base_day_inc WHERE OPERATE_TIME>=SUBDATE(SUBDATE(CURDATE(), DAY(CURDATE()) - 1), INTERVAL 3 MONTH) union all -- 实例1动态保留3个月
SELECT 'fe_dwd.dwd_fill_day_inc_recent_two_month' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_fill_day_inc_recent_two_month  union all
SELECT 'fe_dwd.dwd_fill_day_inc' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_fill_day_inc where apply_time>='2019-05-01' union all  -- 实例1保留201905之后的数据  
SELECT 'fe_dwd.dwd_group_order_refound_address_day' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_group_order_refound_address_day union all
SELECT 'fe_dwd.dwd_order_item_refund_day' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_order_item_refund_day  where PAY_DATE>=SUBDATE(SUBDATE(CURDATE(), DAY(CURDATE()) - 1), INTERVAL 4 MONTH) union all -- 实例1动态保留4个月 
SELECT 'fe_dwd.dwd_pub_activity_order_shelf_product' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_pub_activity_order_shelf_product where pay_date>='2019-11-01' union all -- 支付时间在20191101之后的数据 
SELECT 'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_pub_order_item_recent_two_month  union all
SELECT 'fe_dwd.dwd_shelf_day_his' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_shelf_day_his  union all
SELECT 'fe_dwd.dwd_user_day_inc' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_user_day_inc union all
SELECT 'fe_dwd.dwd_activity_invitation_information' as table_name,COUNT(1) as nums FROM fe_dwd.dwd_activity_invitation_information


-- 实例1
SELECT 'fe_dwd.dwd_check_base_day_inc' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_check_base_day_inc`  union all
SELECT 'fe_dwd.dwd_fill_day_inc_recent_two_month' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_fill_day_inc_recent_two_month`  union all
SELECT 'fe_dwd.dwd_fill_day_inc' as table_name,COUNT(1) as nums  FROM fe_dwd.`dwd_fill_day_inc` where apply_time>='2019-05-01' union all  -- 实例1保留201905之后的数据
SELECT 'fe_dwd.dwd_group_order_refound_address_day' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_group_order_refound_address_day` union all
SELECT 'fe_dwd.dwd_order_item_refund_day' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_order_item_refund_day`  where PAY_DATE>=SUBDATE(SUBDATE(CURDATE(), DAY(CURDATE()) - 1), INTERVAL 4 MONTH) union all -- 实例1动态保留4个月
SELECT 'fe_dwd.dwd_pub_activity_order_shelf_product' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_pub_activity_order_shelf_product` where pay_date>='2019-11-01' union all -- 支付时间在20191101之后的数据
SELECT 'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_pub_order_item_recent_two_month` union all 
SELECT 'fe_dwd.dwd_shelf_day_his' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_shelf_day_his`  union all
SELECT 'fe_dwd.dwd_user_day_inc' as table_name,COUNT(1) as nums FROM fe_dwd.`dwd_user_day_inc` union all
SELECT 'fe_dwd.dwd_activity_invitation_information' as table_name,COUNT(1) as nums  FROM fe_dwd.`dwd_activity_invitation_information`


实例1-实例2
dwd_check_base_day_inc:18693437 - 18693543 = -106
dwd_fill_day_inc_recent_two_month:11875905 - 11907510 = -31605
dwd_fill_day_inc:13161289 - 41956432 = -28795143
dwd_group_order_refound_address_day: 10735177 - 10735177 = 0
dwd_order_item_refund_day:30796450 - 30796450 = 0
dwd_pub_activity_order_shelf_product:6971269  - 3380404 = 3590865
dwd_pub_order_item_recent_two_month:14042103 - 14152339 = -110236
dwd_shelf_day_his:13089450 - 13089450 = 0
dwd_user_day_inc:10329590 - 10329590 = 0
dwd_activity_invitation_information:14223609 - 14223989 = -380


-- select 'fe_dwd.dwd_activity_invitation_information' as table_name, count(1) as nums from fe_dwd.dwd_activity_invitation_information      union all
-- select 'fe_dwd.dwd_check_base_day_inc' as table_name, count(1) as nums from fe_dwd.dwd_check_base_day_inc                   union all
-- select 'fe_dwd.dwd_fill_day_inc' as table_name, count(1) as nums from fe_dwd.dwd_fill_day_inc                         union all
-- select 'fe_dwd.dwd_fill_day_inc_recent_two_month' as table_name, count(1) as nums from fe_dwd.dwd_fill_day_inc_recent_two_month        union all
-- select 'fe_dwd.dwd_group_order_refound_address_day' as table_name, count(1) as nums from fe_dwd.dwd_group_order_refound_address_day      union all
-- select 'fe_dwd.dwd_order_item_refund_day' as table_name, count(1) as nums from fe_dwd.dwd_order_item_refund_day                union all
-- select 'fe_dwd.dwd_order_item_refund_real_time' as table_name, count(1) as nums from fe_dwd.dwd_order_item_refund_real_time          union all
-- select 'fe_dwd.dwd_pub_activity_order_shelf_product' as table_name, count(1) as nums from fe_dwd.dwd_pub_activity_order_shelf_product     union all
-- select 'fe_dwd.dwd_pub_order_item_recent_two_month' as table_name, count(1) as nums from fe_dwd.dwd_pub_order_item_recent_two_month      union all
-- select 'fe_dwd.dwd_shelf_day_his' as table_name, count(1) as nums from fe_dwd.dwd_shelf_day_his                        union all
-- select 'fe_dwd.dwd_user_day_inc' as table_name, count(1) as nums from fe_dwd.dwd_user_day_inc                         union all