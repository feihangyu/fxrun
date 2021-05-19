CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_current_dynamic_purchase_price_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();  
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY); 
SET @sdate1 = DATE_FORMAT(@sdate,"%Y-%m-01"); #本月第一天
SET @sdate2 = DATE_ADD(@sdate1,INTERVAL 1 MONTH); # 下月最后一天
# 出库方动态结余价变化
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_transfer_out_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_transfer_out_tmp
-- (index idx(business_area,product_code2)
-- )
SELECT 
t.STAT_DATE
,t1.sender_area 
,t1.receiver_area
, t1.sku_no AS product_code2
, t1.sku_name AS product_name
, (IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0)) + SUM(t1.sku_qty) AS  inventory
, weighted_average_price2
, weighted_average_price2 * 1.13 AS purchase_price
, (IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0)) base_qty
, SUM(t1.sku_qty) out_qty
FROM 
(SELECT 
 -- t.`sdate`,
 c.`business_area` AS sender_area
, t.`sender_city`
, t.`receiver_city`
, r.business_area AS receiver_area
, t.`sku_no`
, t.sku_name
, -SUM(t.`sku_qty`) sku_qty
FROM fe_dwd.`dwd_sc_bdp_warehouse_shipment_detail` t
JOIN fe_dwd.`dwd_sc_city_business` c
ON t.`sender_city` = c.`CITY_NAME` 
JOIN fe_dwd.`dwd_sc_city_business` r
ON t.`receiver_city` = r.`CITY_NAME` 
WHERE t.`order_type` = '调拨订单'
AND t.sdate >= @sdate1  
-- GROUP BY t.`sdate`,t.`sender_city`,t.`receiver_city`,t.`sku_no`
-- AND t.sku_no = 'FE0002783'
AND t.sku_no NOT LIKE "WZ%"
GROUP BY t.`sender_city`,t.`receiver_city`,t.`sku_no`
)  t1 # 出库
LEFT JOIN fe_dm.`dm_finance_instock_sales_outstock_table` t
ON t.business_area = t1.sender_area
AND t.product_id = t1.sku_no 
AND t.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m")
WHERE t1.`sender_area` != t1.`receiver_area`  # 20200311防备切仓导致数据重复
GROUP BY t1.sender_area,t1.sku_no
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_transfer_in_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_transfer_in_tmp
# 入库方变化
SELECT t.`STAT_DATE`
, t1.send_area
, t1.business_area AS receiver_area
, t1.sku_no AS product_code2
, t1.sku_name AS product_name
, IF(t1.send_area = t1.`business_area`
,(IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0))
,(IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) + SUM(t1.sku_qty)) AS  inventory
-- , ifnull(t.weighted_average_price2,0) weighted_average_price2
-- , ifnull(t.weighted_average_price2 * 1.13,0) AS purchase_price
#入库后的加权价
,IF(t1.send_area = t1.`business_area`
,IFNULL(t.weighted_average_price2,0)
,((IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) * IFNULL(t2.weighted_average_price2,0)
 + SUM(t1.sku_qty * IFNULL(t.weighted_average_price2,0)))
 / (IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0) + SUM(t1.sku_qty))) weighted_average_price2
# 价税价格
, 1.13 * IF(t1.send_area = t1.`business_area`
,IFNULL(t.weighted_average_price2,0)
,((IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) * IFNULL(t2.weighted_average_price2,0)
 + SUM(t1.sku_qty * IFNULL(t.weighted_average_price2,0)))
 / (IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0) + SUM(t1.sku_qty))) purchase_price
 ,IF(t1.send_area = t1.`business_area`
,(IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0))
,(IFNULL(t2.`shelf_actual_stock_qty`,0) + IFNULL(t2.`warehouse_actual_stock_qty`,0) + IFNULL(t2.`storage_actual_stock_qty`,0)) ) base_qty
,IF(t1.send_area = t1.`business_area`,0,SUM(t1.sku_qty)) in_qty 
FROM 
(
SELECT 
-- t.`sdate` , 
t.warehouse_code
, t.warehouse_name
, s.business_area AS send_area
, c.`business_area` 
, c.`CITY_NAME`
, t.`sku_no`
, t.sku_name
, SUM(t.`sku_qty`) sku_qty
FROM fe_dwd.`dwd_sc_bdp_warehouse_shipment_detail` t
JOIN fe_dwd.`dwd_sc_city_business` c
ON t.`receiver_city` = c.`CITY_NAME` 
JOIN fe_dwd.`dwd_sc_city_business` s
ON t.sender_city = s.`CITY_NAME` 
WHERE t.`order_type` = '调拨订单'
AND t.sdate >= @sdate1  
-- and t.`sender_city` != t.`receiver_city`
-- GROUP BY t.`sdate`,t.`sender_city`,t.`receiver_city`,t.`sku_no`
-- AND t.sku_no = 'FE0002783'
-- AND c.`business_area` = '山西区'
AND t.sku_no NOT LIKE "WZ%"
GROUP BY t.`sender_city`,t.`receiver_city`,t.`sku_no`
)  t1 # 出库
LEFT JOIN fe_dm.`dm_finance_instock_sales_outstock_table` t # 需要使用出库方的价格
ON t1.send_area = t.business_area 
AND  t1.sku_no = t.product_id 
AND t.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m") 
LEFT JOIN fe_dm.`dm_finance_instock_sales_outstock_table` t2
ON t2.business_area = t1.business_area
AND t2.product_id = t1.sku_no 
AND t2.`STAT_DATE` = DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),"%Y%m") # 需要使用入库方的库存
GROUP BY t1.business_area,t1.sku_no
;
# (1) 出库为主的变化,处理的部分调拨差异订单
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_transfer_diff_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_transfer_diff_tmp
SELECT
    t1.stat_date
    , t1.sender_area
    , t1.receiver_area
    , t1.product_code2
    , t1.product_name
    , IFNULL(t1.weighted_average_price2,t2.weighted_average_price2) weighted_average_price2
    , IFNULL(t1.purchase_price,t2.purchase_price) purchase_price
    , t1.base_qty
    , t1.out_qty
    , IFNULL(in_qty,0) in_qty
    , IF(-t1.out_qty  <= t2.in_qty,out_qty + t1.base_qty + t2.in_qty,t1.base_qty+out_qty) AS out_change# 出库方库存变化
FROM fe_dm.dm_sc_transfer_out_tmp t1
JOIN fe_dm.dm_sc_transfer_in_tmp t2
ON t1.sender_area = t2.receiver_area
AND t1.receiver_area = t2.send_area
AND t1.product_code2 = t2.product_code2
;
# 正常出库
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_transfer_out_normal_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_transfer_out_normal_tmp
SELECT   t1.stat_date
    , t1.sender_area AS business_area
    , t1.receiver_area
    , t1.product_code2
    , t1.product_name
    , t1.weighted_average_price2
    , t1.purchase_price
    , t1.base_qty
    , t1.out_qty 
    , t1.inventory
FROM fe_dm.dm_sc_transfer_out_tmp t1
LEFT JOIN fe_dm.dm_sc_transfer_diff_tmp d
ON t1.sender_area = d.sender_area
AND t1.receiver_area = d.receiver_area
AND t1.product_code2 = d.product_code2
WHERE ISNULL(d.sender_area) 
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_transfer_in_normal_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_transfer_in_normal_tmp
# 正常入库
SELECT   t1.stat_date
    , t1.receiver_area AS business_area
    , t1.send_area 
    , t1.product_code2
    , t1.product_name
    , t1.weighted_average_price2
    , t1.purchase_price
    , t1.base_qty
    , t1.in_qty 
    , t1.inventory
FROM fe_dm.dm_sc_transfer_in_tmp t1
LEFT JOIN fe_dm.dm_sc_transfer_diff_tmp d
ON t1.send_area = d.sender_area
AND t1.receiver_area = d.receiver_area
AND t1.product_code2 = d.product_code2
WHERE ISNULL(d.receiver_area) 
;
# 最终上月结余变化
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_monthly_inventory_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_monthly_inventory_tmp
(KEY idx_business_product(business_area,product_code2))
AS 
SELECT  t.stat_date
    , t.sender_area AS business_Area
    , t.product_code2
    , t.product_name
    , t.weighted_average_price2
    , t.purchase_price
    , t.out_change AS inventory
FROM fe_dm.dm_sc_transfer_diff_tmp t
UNION 
SELECT stat_date,business_Area,product_code2,product_name,weighted_average_price2,purchase_price,inventory
FROM fe_dm.dm_sc_transfer_out_normal_tmp
UNION 
SELECT stat_date,business_Area,product_code2,product_name,weighted_average_price2,purchase_price,inventory
FROM fe_dm.dm_sc_transfer_in_normal_tmp
UNION 
SELECT t1.STAT_DATE,t1.BUSINESS_AREA,t1.product_code2,t1.product_name,t1.weighted_average_price2,t1.purchase_price,inventory
FROM 
( 
SELECT t.`STAT_DATE`
, t.`BUSINESS_AREA`
, t.`product_id` AS product_code2
, t.`product_name`
, (IFNULL(t.`shelf_actual_stock_qty`,0) + IFNULL(t.`warehouse_actual_stock_qty`,0) + IFNULL(t.`storage_actual_stock_qty`,0)) inventory
, weighted_average_price2
, weighted_average_price2 * 1.13 AS purchase_price
FROM fe_dm.`dm_finance_instock_sales_outstock_table` t
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
# 所有品，上月结余，本月采购sku
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_monthly_inventory_total_tmp ;
CREATE TEMPORARY TABLE fe_dm.dm_sc_monthly_inventory_total_tmp 
(KEY idx_business_product(business_area,product_code2))
AS 
SELECT business_area,product_code2
FROM fe_dm.dm_sc_monthly_inventory_tmp
WHERE purchase_price IS NOT NULL
UNION 
SELECT business_area,product_code2
FROM fe_dm.dm_sc_poorderlist_day
WHERE instock_time >= @sdate1 AND instock_time < @sdate2
GROUP BY business_area,product_code2
;
DELETE FROM fe_dwd.dwd_monthly_manual_purchase_price_insert WHERE stat_month = LAST_DAY(@sdate);
INSERT INTO fe_dwd.dwd_monthly_manual_purchase_price_insert
(region_area,
business_area,
product_id,
product_code2,
area_fecode,
product_name,
purchase_price,
stat_month
)
SELECT 
t4.region_area,t.business_area,t3.product_id,t.product_code2,CONCAT(t.business_area,t.product_code2),
t3.product_name,
CASE WHEN t1.inventory >=0 AND t2.instock_qty >=0 THEN ROUND((t1.purchase_price*t1.inventory + t2.purchase_amount)/(t1.inventory + t2.instock_qty),4)
WHEN ISNULL(t2.instock_qty) THEN t1.purchase_price
WHEN ISNULL(t1.inventory) THEN ROUND(t2.purchase_amount/t2.instock_qty,4)
END AS purchase_price,
LAST_DAY(@sdate)  AS stat_month
FROM fe_dm.dm_sc_monthly_inventory_total_tmp  t
LEFT JOIN fe_dm.dm_sc_monthly_inventory_tmp t1 
ON t.business_area = t1.business_area
AND t.product_code2 = t1.product_code2
LEFT JOIN 
(SELECT a.business_area,a.product_code2,SUM( a.actual_instock_qty*a.purchase_price ) AS purchase_amount, SUM(a.actual_instock_qty) AS instock_qty
FROM fe_dm.dm_sc_poorderlist_day a
WHERE a.instock_time >= @sdate1 AND a.instock_time < @sdate2
GROUP BY a.business_area,a.product_code2) t2
ON t.business_area = t2.business_area
AND t.product_code2 = t2.product_code2
LEFT JOIN fe_dwd.`dwd_product_base_day_all` t3
ON t.product_code2 = t3.product_code2
JOIN fe_dwd.`dwd_sc_business_region` t4
ON t.business_area = t4.business_area
;
SET @time_6 := CURRENT_TIMESTAMP();
TRUNCATE fe_dm.dm_sc_current_dynamic_purchase_price;
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
FROM fe_dwd.`dwd_monthly_manual_purchase_price_insert` w
GROUP BY w.`business_area`,w.`product_code2`
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_sc_current_dynamic_purchase_price_two","@time_6--@time_7",@time_6,@time_7);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_current_dynamic_purchase_price_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_monthly_manual_purchase_price_insert','dm_sc_current_dynamic_purchase_price_two','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_current_dynamic_purchase_price','dm_sc_current_dynamic_purchase_price_two','吴婷');
END