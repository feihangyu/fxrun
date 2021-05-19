CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_dynamic_weighted_purchase_price`()
    SQL SECURITY INVOKER
BEGIN
   
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY); 
SET @sdate1 = DATE_FORMAT(@sdate,"%Y-%m-01"); #本月第一天
SET @sdate2 = DATE_ADD(@sdate1,INTERVAL 1 MONTH); # 下月最后一天
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_monthly_inventory_tmp ;
SET @time_1 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE feods.d_sc_monthly_inventory_tmp
(KEY idx_business_product(business_area,product_code2))
AS 
# 出库方变化
SELECT t.STAT_DATE,t.`BUSINESS_AREA`
, t.`product_id` AS product_code2
, t.`product_name`
, (IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0)) + SUM(t1.sku_qty) AS  inventory
, weighted_average_price2
, weighted_average_price2 * 1.13 AS purchase_price
FROM feods.`csl_finance_instock_sales_outstock_table` t
JOIN 
(SELECT 
 -- t.`sdate`,
 c.`business_area` AS sender_area
, t.`sender_city`
, t.`receiver_city`
, r.business_area AS receiver_area
, t.`sku_no`
, -SUM(t.`sku_qty`) sku_qty
FROM fe_dwd.`dwd_sc_bdp_warehouse_shipment_detail` t
JOIN fe_dwd.`dwd_sc_city_business` c
ON t.`sender_city` = c.`CITY_NAME` 
JOIN fe_dwd.dwd_sc_city_business r
ON t.`receiver_city` = r.`CITY_NAME` 
WHERE t.`order_type` = '调拨订单'
AND t.sdate >= @sdate1  
-- GROUP BY t.`sdate`,t.`sender_city`,t.`receiver_city`,t.`sku_no`
GROUP BY t.`sender_city`,t.`receiver_city`,t.`sku_no`
)  t1 # 出库
ON t.business_area = t1.sender_area
AND t.product_id = t1.sku_no 
WHERE t.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m")
AND t1.`sender_area` != t1.`receiver_area`  # 20200311防备切仓导致数据重复
GROUP BY t1.`sender_area`,t1.sku_no 
UNION
# 入库方变化
SELECT t.`STAT_DATE`
, t1.business_name AS BUSINESS_AREA
, t.`product_id` AS product_code2
, t.`product_name`
, IF(t1.send_area = t1.`business_name`
,(IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0))
,(IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) + SUM(t1.sku_qty)) AS  inventory
-- , ifnull(t.weighted_average_price2,0) weighted_average_price2
-- , ifnull(t.weighted_average_price2 * 1.13,0) AS purchase_price
#入库后的加权价
,IF(t1.send_area = t1.`business_name`
,IFNULL(t.weighted_average_price2,0)
,((IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) * IFNULL(t2.weighted_average_price2,0)
 + SUM(t1.sku_qty * IFNULL(t.weighted_average_price2,0)))
 / (IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0) + SUM(t1.sku_qty))) weighted_average_price2
# 价税价格
, 1.13 * IF(t1.send_area = t1.`business_name`
,IFNULL(t.weighted_average_price2,0)
,((IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) * IFNULL(t2.weighted_average_price2,0)
 + SUM(t1.sku_qty * IFNULL(t.weighted_average_price2,0)))
 / (IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0) + SUM(t1.sku_qty))) purchase_price
FROM 
(
SELECT 
-- t.`sdate` , 
t.warehouse_code
, t.warehouse_name
, s.business_area AS send_area
, c.`business_area` AS business_name
, c.`CITY_NAME`
, t.`sku_no`
, SUM(t.`sku_qty`) sku_qty
FROM fe_dwd.`dwd_sc_bdp_warehouse_shipment_detail` t
JOIN fe_dwd.dwd_sc_city_business c
ON t.`receiver_city` = c.`CITY_NAME` 
JOIN fe_dwd.dwd_sc_city_business s
ON t.sender_city = s.`CITY_NAME` 
WHERE t.`order_type` = '调拨订单'
AND t.sdate >= @sdate1  
-- and t.`sender_city` != t.`receiver_city`
-- GROUP BY t.`sdate`,t.`sender_city`,t.`receiver_city`,t.`sku_no`
GROUP BY t.`sender_city`,t.`receiver_city`,t.`sku_no`
)  t1 # 出库
JOIN feods.`csl_finance_instock_sales_outstock_table` t # 需要使用出库方的价格
ON t.business_area = t1.send_area
AND t.product_id = t1.sku_no 
AND t.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m") 
LEFT JOIN feods.`csl_finance_instock_sales_outstock_table` t2
ON t2.business_area = t1.business_name
AND t2.product_id = t1.sku_no 
AND t2.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m") # 需要使用入库方的库存
GROUP BY t1.business_name,t1.sku_no
UNION
# 其余
SELECT t1.*
FROM 
( 
SELECT t.`STAT_DATE`
, t.`BUSINESS_AREA`
, t.`product_id` AS product_code2
, t.`product_name`
, (IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0)) inventory
, weighted_average_price2
, weighted_average_price2 * 1.13 AS purchase_price
FROM feods.`csl_finance_instock_sales_outstock_table` t
WHERE t.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m")
AND t.weighted_average_price2 IS NOT NULL
) t1 # 上月采购结余价
LEFT JOIN
(# 调拨入库方
SELECT 
DISTINCT c.`BUSINESS_AREA` , t.`sku_no`
FROM fe_dwd.`dwd_sc_bdp_warehouse_shipment_detail` t
JOIN fe_dwd.`dwd_sc_city_business` c
ON t.`receiver_city` = c.`CITY_NAME` 
WHERE t.`order_type` = '调拨订单'
AND t.sdate >= @sdate1  
# 调拨出库方
UNION  
SELECT 
DISTINCT c.`BUSINESS_AREA`,t.`sku_no`
FROM fe_dwd.`dwd_sc_bdp_warehouse_shipment_detail` t
JOIN fe_dwd.`dwd_sc_city_business` c
ON t.`sender_city` = c.`CITY_NAME` 
WHERE t.`order_type` = '调拨订单'
AND t.sdate >= @sdate1  
) t2
ON t1.business_area = t2.business_area
AND t1.product_code2 = t2.sku_no
WHERE ISNULL(t2.sku_no) 
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_dynamic_weighted_purchase_price","@time_1--@time_2",@time_1,@time_2);
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_monthly_inventory_total_tmp ;
CREATE TEMPORARY TABLE feods.d_sc_monthly_inventory_total_tmp 
(KEY idx_business_product(business_area,product_code2))
AS 
SELECT business_area,product_code2
FROM feods.d_sc_monthly_inventory_tmp
WHERE purchase_price IS NOT NULL
UNION 
SELECT business_area,product_code2
FROM feods.pj_poorderlist_day
WHERE instock_time >= @sdate1 AND instock_time < @sdate2
GROUP BY business_area,product_code2
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_dynamic_weighted_purchase_price","@time_2--@time_3",@time_2,@time_3);
DELETE FROM feods.wt_monthly_manual_purchase_price WHERE stat_month = LAST_DAY(@sdate);
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_dynamic_weighted_purchase_price","@time_3--@time_4",@time_3,@time_4);
INSERT INTO feods.wt_monthly_manual_purchase_price
(business_area,
product_id,
product_code2,
area_fecode,
product_name,
purchase_price,
stat_month
)
SELECT 
t.business_area,t3.product_id,t.product_code2,CONCAT(t.business_area,t.product_code2),
t3.product_name,
CASE WHEN t1.inventory >=0 AND t2.instock_qty >=0 THEN ROUND((t1.purchase_price*t1.inventory + t2.purchase_amount)/(t1.inventory + t2.instock_qty),4)
WHEN ISNULL(t2.instock_qty) THEN t1.purchase_price
WHEN ISNULL(t1.inventory) THEN ROUND(t2.purchase_amount/t2.instock_qty,4)
END AS purchase_price,
LAST_DAY(@sdate)  AS stat_month
FROM feods.d_sc_monthly_inventory_total_tmp  t
LEFT JOIN feods.d_sc_monthly_inventory_tmp t1 
ON t.business_area = t1.business_area
AND t.product_code2 = t1.product_code2
LEFT JOIN 
(SELECT a.business_area,a.product_code2,SUM( a.actual_instock_qty*a.purchase_price ) AS purchase_amount, SUM(a.actual_instock_qty) AS instock_qty
FROM feods.pj_poorderlist_day a
WHERE a.instock_time >= @sdate1 AND a.instock_time < @sdate2
GROUP BY a.business_area,a.product_code2) t2
ON t.business_area = t2.business_area
AND t.product_code2 = t2.product_code2
LEFT JOIN fe.`sf_product` t3
ON t.product_code2 = t3.product_code2
AND t3.data_flag =1
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_dynamic_weighted_purchase_price","@time_4--@time_5",@time_4,@time_5);
TRUNCATE fe_dm.dm_sc_current_dynamic_purchase_price;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_dynamic_weighted_purchase_price","@time_5--@time_6",@time_5,@time_6);
INSERT INTO fe_dm.dm_sc_current_dynamic_purchase_price
(sdate,
business_area,
product_id,
product_code2,
product_name,
purchase_price
)
SELECT SUBDATE(CURDATE(),1) AS sdate
, w.`business_area`
, w.product_id
, w.`product_code2`
, w.`product_name`
,SUBSTRING_INDEX(GROUP_CONCAT(w.`purchase_price` ORDER BY w.`stat_month` DESC SEPARATOR ","),",",1) AS purchase_price
FROM feods.wt_monthly_manual_purchase_price w
GROUP BY w.`business_area`,w.`product_code2`
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_dynamic_weighted_purchase_price","@time_6--@time_7",@time_6,@time_7);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_dynamic_weighted_purchase_price',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END