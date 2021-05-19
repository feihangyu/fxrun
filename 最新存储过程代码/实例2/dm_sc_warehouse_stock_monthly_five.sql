CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_warehouse_stock_monthly_five`(in_sdate DATE)
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2019/08/06
-- Modify date: 
-- Description:	
-- 	监控大仓商品oms库存出库量、近14天出库量等 - 采购报表
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
# 当天出库-采用了BDP中oms库存数据
SET @sdate = DATE_ADD(in_sdate,INTERVAL 1 DAY);
SET @sdate1 = in_sdate;
DELETE FROM fe_dm.dm_sc_warehouse_outbound_daily WHERE sdate = @sdate1;
INSERT INTO fe_dm.dm_sc_warehouse_outbound_daily
(sdate,
  region_area,
  business_area,
  warehouse_number, 
  warehouse_name, 
  destination,
  product_code2,
  product_name,
  product_type,
  FBASEQTY,
  FQTY,
  purchase_price,
  out_amount
)
SELECT 
s.sdate AS '订单时间'
,c.big_area AS '大区'
,c.BUSINESS_AREA AS '区域'
,s.warehouse_code
,s.warehouse_name
,CASE 
WHEN s.order_type  = '销售订单' AND LEFT(erp_order,2) = "XS" THEN "大客户"
WHEN s.order_type  = '销售订单' AND sh.shelf_type = 4  THEN "虚拟货架"
WHEN s.order_type  = '销售订单' THEN "货架"
WHEN s.order_type = 'B2B订单'  THEN "前置仓"
WHEN s.order_type = '调拨订单' AND s.sender_city = s.receiver_city  THEN "调拨出库-同区"
WHEN s.order_type = '调拨订单' AND s.sender_city != s.receiver_city  THEN "调拨出库-跨区"
ELSE s.order_type
END AS destination
,s.sku_no AS '商品二维码'
,s.sku_name AS '商品名称'
,t.PRODUCT_TYPE
-- ,FILL_MODEL '补货箱规'
,F_BGJ_FBOXEDSTANDARDS  AS '箱规'
,SUM(sku_qty) AS '申请数量'
-- ,p.F_BGJ_POPRICE AS '采购价'
-- ,SUM(sku_qty * p.F_BGJ_POPRICE) AS '金额'
, p1.purchase_price AS '采购价'
,SUM(sku_qty * p1.purchase_price) AS '金额'
FROM fe_dwd.dwd_sc_bdp_warehouse_shipment_detail s  # 到货架和大客户订单都在BDP中都属于销售订单
JOIN fe_dwd.dwd_sserp_zs_dc_business_area c
ON s.warehouse_code = c.DC_CODE
-- JOIN fe_dwd.`dwd_sc_business_region` w
-- ON c.`business_name` = w.`business_area`
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp t
ON c.BUSINESS_AREA = t.business_area
AND s.sku_no  = t.PRODUCT_FE
JOIN fe_dwd.dwd_product_base_day_all p
ON s.sku_no = p.PRODUCT_CODE2
LEFT JOIN fe_dm.dm_sc_current_dynamic_purchase_price p1
ON c.business_area = p1.business_area
AND p.product_id = p1.product_id
LEFT JOIN fe_dwd.dwd_fill_day_inc f
ON  f.order_id = RIGHT(s.erp_order,18)
AND f.product_id=p.product_id
LEFT JOIN fe_dwd.dwd_shelf_base_day_all sh
ON f.SHELF_ID = sh.shelf_id
AND sh.data_flag = 1
WHERE s.sdate = @sdate1
GROUP BY s.sdate,s.warehouse_code,s.sku_no,destination
;
# 14天出库 各类型
DELETE FROM fe_dm.dm_sc_warehouse_outbound_forteen WHERE sdate = @sdate1;
INSERT INTO fe_dm.dm_sc_warehouse_outbound_forteen
(sdate,
  region_area,
  business_area,
  warehouse_number, 
  warehouse_name, 
  destination,
  product_code2,
  product_name,
  FBASEQTY,
  FQTY,
  purchase_price,
  out_amount,
  out_days,
  avg_out_amount,
  avg_out_qty
)
SELECT 
  @sdate1,
  s.region_area,
  s.business_area,
  w.warehouse_number, 
  w.warehouse_name, 
  destination,
  product_code2,
  product_name,
  FBASEQTY,
  SUM(FQTY) AS FQTY ,
  purchase_price,
  SUM(out_amount) AS out_amount ,
  COUNT(DISTINCT s.sdate)  AS out_days,
  SUM(out_amount)/COUNT(DISTINCT s.sdate)  AS avg_out_amount,
  SUM(FQTY) / COUNT(DISTINCT s.sdate)  AS avg_out_qty   
FROM fe_dm.dm_sc_warehouse_outbound_daily s
JOIN fe_dwd.dwd_pub_warehouse_business_area w
ON s.business_area = w.business_area
AND w.warehouse_type = 1
WHERE s.sdate >= DATE_SUB(@sdate,INTERVAL 14 DAY ) AND s.sdate < @sdate
GROUP BY s.business_area,s.product_code2,s.destination
;
# 14天总计
DELETE FROM fe_dm.dm_sc_warehouse_outbound_forteen_total WHERE sdate = @sdate1;
INSERT INTO fe_dm.dm_sc_warehouse_outbound_forteen_total
(sdate,
  region_area,
  business_area,
  warehouse_number, 
  warehouse_name, 
  product_code2,
  product_name,
  FBASEQTY,
  FQTY,
  purchase_price,
  out_amount,
  out_days,
  out_day_sp,
  out_qty_sp,
  out_day_shelf,
  out_qty_shelf
)
SELECT 
  @sdate1,
  s.region_area,
  s.business_area,
  w.warehouse_number, 
  w.warehouse_name, 
  product_code2,
  product_name,
  FBASEQTY,
  SUM(FQTY) AS FQTY ,
  purchase_price,
  SUM(out_amount) AS out_amount ,
  COUNT(DISTINCT s.sdate)  AS out_days,
  COUNT(DISTINCT(IF(destination IN ("货架","前置仓"),s.sdate,NULL))) AS out_day_sp,
  SUM(IF(destination IN ("货架","前置仓"),s.FQTY,0)) AS out_qty_sp,
  
  COUNT(DISTINCT(IF(destination = "货架",s.sdate,NULL))) AS out_day_shelf,
  SUM(IF(destination = "货架",s.FQTY,0)) AS out_qty_shelf  
FROM fe_dm.dm_sc_warehouse_outbound_daily s
JOIN fe_dwd.dwd_pub_warehouse_business_area w
ON s.business_area = w.business_area
AND w.warehouse_type = 1
WHERE s.sdate >= DATE_SUB(@sdate,INTERVAL 14 DAY ) AND s.sdate < @sdate
GROUP BY s.business_area,s.product_code2
;
# 月度出库累计 bdp 月度累计出库量
DELETE FROM fe_dm.dm_sc_warehouse_outbound_monthly_total WHERE smonth = DATE_FORMAT(@sdate1,"%Y-%m");
INSERT INTO fe_dm.dm_sc_warehouse_outbound_monthly_total
(smonth ,
  region_area,
  business_area ,
  warehouse_number ,
  warehouse_name ,
  product_code2 ,
  product_name ,
  FBASEQTY ,
  FQTY ,
  purchase_price ,
  out_amount ,
  out_days ,
  out_day_sp ,
  out_qty_sp ,
   out_day_shelf, 
   out_qty_shelf
)
SELECT 
  DATE_FORMAT(s.sdate,"%Y-%m") AS smonth,
  s.region_area,
  s.business_area,
  w.warehouse_number, 
  w.warehouse_name, 
  product_code2,
  product_name,
  FBASEQTY,
  SUM(FQTY) AS FQTY ,
  purchase_price,
  SUM(out_amount) AS out_amount ,
  COUNT(DISTINCT s.sdate)  AS out_days,
  COUNT(DISTINCT(IF(destination IN ("货架","前置仓"),s.sdate,NULL))) AS out_day_sp,
  SUM(IF(destination IN ("货架","前置仓"),s.FQTY,0)) AS out_qty_sp, 
  COUNT(DISTINCT(IF(destination = "货架",s.sdate,NULL))) AS out_day_shelf,
  SUM(IF(destination = "货架",s.FQTY,0)) AS out_qty_shelf  
FROM fe_dm.dm_sc_warehouse_outbound_daily s
JOIN fe_dwd.dwd_pub_warehouse_business_area w
ON s.business_area = w.business_area
AND w.warehouse_type = 1
WHERE s.sdate >= DATE_FORMAT(@sdate1,"%Y-%m-01") AND s.sdate <= LAST_DAY(@sdate1)
GROUP BY DATE_FORMAT(s.sdate,"%Y-%m"),s.business_area,s.product_code2
;
DELETE FROM fe_dm.dm_sc_warehouse_stock_monthly WHERE smonth = DATE_FORMAT(@sdate1,"%Y-%m");
INSERT INTO fe_dm.dm_sc_warehouse_stock_monthly
(   smonth
    , region_area
    , business_area
    , warehouse_number
    , warehouse_name
    , product_code2
    , product_name
    , stock_quantity
)
SELECT DATE_FORMAT(sdate,"%Y-%m") AS smonth
    , b.region_area
    , b.business_area
    , a.warehouse
    , a.warehouse_name
    , sku_no AS product_code2
    , skuname AS product_name
--     , SUM(storage_amount) / DAY(@sdate) AS avg_stock_month 
    , SUM(storage_amount) AS stock_quantity
FROM
    fe_dwd.dwd_sc_bdp_warehouse_stock_daily a 
    JOIN 
    (SELECT DISTINCT region_area,business_area,warehouse_number
     FROM fe_dwd.`dwd_pub_warehouse_business_area` ) b
    ON a.warehouse = b.warehouse_number
    -- AND b.data_flag =1
 WHERE a.sdate >= DATE_FORMAT(@sdate1,"%Y-%m-01") AND a.sdate <= LAST_DAY(@sdate1)
 AND sku_no NOT LIKE "WZ%"
 GROUP BY b.business_area,sku_no
 ;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_warehouse_stock_monthly_five',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_warehouse_outbound_daily','dm_sc_warehouse_stock_monthly_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_warehouse_outbound_forteen','dm_sc_warehouse_stock_monthly_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_warehouse_outbound_forteen_total','dm_sc_warehouse_stock_monthly_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_warehouse_outbound_monthly_total','dm_sc_warehouse_stock_monthly_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_warehouse_stock_monthly','dm_sc_warehouse_stock_monthly_five','吴婷');
END