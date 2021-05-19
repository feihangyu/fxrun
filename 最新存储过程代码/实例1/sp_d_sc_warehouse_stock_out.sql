CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_warehouse_stock_out`()
    SQL SECURITY INVOKER
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2019/07/26
-- Modify date: 
-- Description:	
-- 	监控大仓商品oms库存出库量等 - 采购报表
-- 
-- =============================================
    DECLARE l_test VARCHAR(1);
    DECLARE l_row_cnt INT;
    DECLARE CODE CHAR(5) DEFAULT '00000';
    DECLARE done INT;
    
	DECLARE l_table_owner   VARCHAR(64);
	DECLARE l_city          VARCHAR(64);
    DECLARE l_task_name     VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1
			CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
			CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
		END; 
		
    SET l_task_name = 'sp_d_sc_warehouse_stock_out'; 
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
#（1）OMS库存每日结存(每日的结余库存，如要每日的起始库存参考可采用前一天的结余库存)
DELETE FROM feods.d_sc_oms_stock_daily WHERE sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
INSERT INTO feods.d_sc_oms_stock_daily 
( `sdate`,
  region_area,
  business_area ,
  supplier_id,
  warehouse_name, 
 `PRODUCT_ID`,
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
t1.`SUPPLIER_ID`, 
t2.SUPPLIER_NAME AS warehouse_name,
t1.`PRODUCT_ID`,
t3.product_code2,
t3.product_name,
t4.product_type,
t1.`PURCHASE_PRICE`,
t1.`oms_stock_defective`,
t1.`erp_stock_defective`,
t1.`oms_stock_quantity`,
t1.`erp_stock_quantity`,
t1.`LAST_UPDATE_TIME` 
FROM fe.sf_supplier_product_detail t1
JOIN fe.`sf_supplier` t2
ON t1.`SUPPLIER_ID` = t2.`SUPPLIER_ID`
AND t2.`SUPPLIER_TYPE` = 2 
AND t2.`DATA_FLAG` = 1
AND t2.`STATUS` =2 #启用状态
JOIN feods.`wt_warehouse_business_area` t3
ON t3.warehouse_name = t2.supplier_name
AND t3.data_flag = 1
JOIN fe.`sf_product` t3
ON t1.product_id =t3.product_id
LEFT JOIN feods.`zs_product_dim_sserp` t4
ON t4.business_area = t3.business_area
AND t4.product_id = t1.product_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_sc_warehouse_stock_out',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
 COMMIT;
    END