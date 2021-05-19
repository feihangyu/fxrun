CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_oms_stock_daily`()
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2019/07/26
-- Modify date: 
-- Description:	
-- 	监控大仓商品oms库存出库量等 - 采购报表
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
#（1）OMS库存每日结存(每日的结余库存，如要每日的起始库存参考可采用前一天的结余库存)
DELETE FROM fe_dm.dm_sc_oms_stock_daily WHERE sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
INSERT INTO fe_dm.dm_sc_oms_stock_daily 
( sdate,
  region_area,
  business_area ,
  supplier_id,
  warehouse_name, 
 PRODUCT_ID,
  product_code2 ,
  product_name,
  product_type,
  purchase_price,
  oms_stock_defective,
  erp_stock_defective,
  oms_stock_quantity,
  erp_stock_quantity,
  stock_update_time
)
SELECT 
DATE_SUB(CURDATE(),INTERVAL 1 DAY) AS sdate,
t3.region_area,
t3.business_area,
t1.SUPPLIER_ID, 
t2.SUPPLIER_NAME AS warehouse_name,
t1.PRODUCT_ID,
p.product_code2,
p.product_name,
t4.product_type,
t1.PURCHASE_PRICE,
t1.oms_stock_defective,
t1.erp_stock_defective,
t1.oms_stock_quantity,
t1.erp_stock_quantity,
t1.LAST_UPDATE_TIME 
FROM fe_dwd.dwd_sf_supplier_product_detail t1
JOIN fe_dwd.dwd_sf_supplier t2
ON t1.SUPPLIER_ID = t2.SUPPLIER_ID
AND t2.SUPPLIER_TYPE = 2 
AND t2.DATA_FLAG = 1
AND t2.STATUS =2 #启用状态
JOIN fe_dwd.dwd_pub_warehouse_business_area t3
ON t3.warehouse_name = t2.supplier_name
AND t3.data_flag = 1
JOIN fe_dwd.dwd_product_base_day_all p
ON t1.product_id =p.product_id
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp t4
ON t4.business_area = t3.business_area
AND t4.product_id = t1.product_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_oms_stock_daily',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_oms_stock_daily','dm_sc_oms_stock_daily','吴婷');
COMMIT;
    END