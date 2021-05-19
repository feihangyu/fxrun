CREATE DEFINER=`feprocess`@`%` PROCEDURE `calculate_wide_table_nums`()
BEGIN
 
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_group_order_refound_address_day','dwd_group_order_refound_address_day','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_order_item_refund_day','dwd_order_item_refund_day_inc','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pub_order_item_recent_one_month','dwd_order_item_refund_day_inc','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pub_order_item_recent_two_month','dwd_order_item_refund_day_inc','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pub_activity_order_shelf_product','dwd_pub_activity_order_shelf_product','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_fill_day_inc','dwd_replenish_day_inc','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_fill_day_inc_recent_two_month','dwd_replenish_day_inc','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_day_his','dwd_shelf_day_his','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_day_east_his','dwd_shelf_product_his','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_day_west_his','dwd_shelf_product_his','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_day_north_his','dwd_shelf_product_his','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_day_south_his','dwd_shelf_product_his','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_check_base_day_inc','dwd_check_base_day_inc','李世龙') ;
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_user_day_inc','dwd_user_day_inc','李世龙') ;
-- 记录kettle同步的表数据量 便于任务监控
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BAS_BILLTYPE','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BAS_BILLTYPE_L','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_STOCKSTATUS','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_UNIT','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BGJ_STOCKNEWFQTY','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_ORG_ORGANIZATIONS','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_ORG_ORGANIZATIONS_L','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_MRBENTRY_F','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_MRBFIN','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_INSTOCKFIN','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_MISCELLANEOUS','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_MISCELLANEOUSENTRY','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_MISDELIVERY','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_MISDELIVERYENTRY','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_STKTRANSFERAPP','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_STKTRANSFERAPPENTRY','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_STKTRANSFERAPPENTRY_E','extraction_erp_csl','唐进（蔡松林）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BAS_ASSISTANTDATAENTRY_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_MATERIAL','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_MATERIALGROUP','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_MATERIALGROUP_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_MATERIAL_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_STOCK','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_STOCKSTATUS_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_STOCK_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_SUPPLIER_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_MRAPP','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_MRAPPENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_MRB','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_MRBENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_POORDER','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_POORDERENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_POORDERENTRY_F','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_RECEIVE','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_RECEIVEENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_SAL_OUTSTOCKENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_INSTOCK','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_INSTOCKENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_INVENTORY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_OUTSTOCKAPPLY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_STK_OUTSTOCKAPPLYENTRY','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.V_BD_BUYER_L','extraction_erp','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_PRICELISTENTRY','extraction_erp_csl','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_PUR_PRICELIST','extraction_erp_csl','唐进（吴婷）') ;
CALL sh_process.dwd_count_process_aim_table_size('sserp.T_BD_SUPPLIERBASE','extraction_erp_csl','唐进（吴婷）') ;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'calculate_wide_table_nums',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('唐进@',@user,@timestamp)
);
COMMIT;
END