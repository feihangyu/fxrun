CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_prewarehouse_stock_detail`()
BEGIN
 SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
 SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
 DELETE FROM fe_dm.dm_prewarehouse_stock_detail WHERE DATE(CHECK_DATE)= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
 INSERT INTO fe_dm.dm_prewarehouse_stock_detail
(check_date ,
  region_area ,
  business_area ,
  warehouse_number ,
  warehouse_name ,
  warehouse_id ,
  shelf_code,
  shelf_name,
  product_id ,
  product_code2 ,
  product_name,
  fname,
  purchase_price ,
  freeze_stock ,
  available_stock,
  total_stock
)
SELECT 
SUBDATE(CURDATE(),1) AS check_date
, e.region_area
, e.business_area
, e.warehouse_number
, e.warehouse_name
, t.warehouse_id
, c.shelf_code
, c.shelf_name
, t.product_id
, p.product_code2
, p.product_name
, p.fname_type
, t9.purchase_price
, t.freeze_stock
, t.available_stock
, t.freeze_stock + t.available_stock AS total_stock
FROM fe_dwd.dwd_sf_prewarehouse_stock_detail t
JOIN fe_dwd.dwd_shelf_base_day_all c
ON t.warehouse_id = c.shelf_id
AND t.data_flag = 1
AND  c.DATA_FLAG = 1
JOIN fe_dwd.dwd_pub_warehouse_business_area e   ### 仓库编码
ON c.business_name = e.business_area 
AND e.to_preware = 1
LEFT JOIN fe_dwd.dwd_product_base_day_all p
ON t.product_id = p.product_id
LEFT JOIN fe_dm.dm_sc_current_dynamic_purchase_price t9
ON c.business_name = t9.business_area
AND t.product_id = t9.product_id 
;
   
   
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_prewarehouse_stock_detail',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_prewarehouse_stock_detail','dm_prewarehouse_stock_detail','吴婷');
END