CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_profit_monthly_shelf_product`()
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2019/12/16
-- Modify date: 
-- Description:	
-- 	采购部门-毛利—每月累计毛利
-- 
-- =============================================   	
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();	
# 1、当月GMV统计
DELETE FROM fe_dm.dm_sc_profit_monthly_shelf_product WHERE stat_month = DATE_FORMAT(SUBDATE(CURDATE(),1),"%Y-%m");
INSERT INTO fe_dm.dm_sc_profit_monthly_shelf_product
( stat_month,
  region_area,
  business_area,
  shelf_type,
  product_id,
  product_code2,
  FNAME,
  product_name,
  quantity,
  GMV,
  avg_sale_price,
  purchase_price,
  discount_amount,
  purchase_amount,
  profit,
  profit_rate
)
SELECT 
t1.stat_month,
p.region_area,
t1.business_area,
t1.shelf_type,
t1.product_id,
t3.product_code2,
t3.fname_type, 
t3.product_name,
t1.quantity,
t1.GMV,
sale_price AS avg_sale_price,
t6.purchase_price,
t1.discount_amount,
t1.quantity*t6.purchase_price AS purchase_amount,
t1.gmv - t1.quantity*t6.purchase_price AS profit,
(t1.gmv - t1.quantity*t6.purchase_price)/(t1.sale_price*t1.quantity) AS profit_rate
FROM 
(SELECT 
DATE_FORMAT(SUBDATE(CURDATE(),1),"%Y-%m") AS stat_month
, s.business_name AS business_area
-- , s.city_name
-- , s.branch_name
, s.city
, s.province
, s.shelf_type
, a.product_id
, SUM(sale_price * a.quantity) / SUM(a.quantity) AS sale_price
, SUM(a.quantity) AS quantity
, SUM(a.sale_price * a.quantity) AS gmv
, SUM(discount_amount) AS discount_amount
FROM fe_dwd.dwd_pub_order_item_recent_one_month a
JOIN fe_dwd.dwd_shelf_base_day_all s
ON a.shelf_id = s.shelf_id
WHERE pay_date >= DATE_FORMAT(SUBDATE(CURDATE(),1),"%Y-%m-01") AND pay_date < CURDATE()
GROUP BY 
a.product_id
-- ,DATE(a.order_date)
, s.business_name
-- , s.city_name
-- , s.branch_name
, s.shelf_type
) t1
JOIN fe_dwd.dwd_product_base_day_all t3
ON t1.product_id = t3.product_id
JOIN  fe_dwd.`dwd_sc_business_region` p
ON t1.business_area = p.business_area
LEFT JOIN fe_dm.dm_sc_current_dynamic_purchase_price t6
ON t1.business_area = t6.business_area
AND t1.product_id = t6.product_id
;
 
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_profit_monthly_shelf_product',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_profit_monthly_shelf_product','dm_sc_profit_monthly_shelf_product','吴婷');
COMMIT;
    END