CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_warehouse_balance`(in_sdate DATE)
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2020/04/23
-- Modify date: 
-- Description:	
-- 	监控大仓商品oms结余(包括库存、出入库各类型)等 - 采购报表
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate = in_sdate;
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_warehouse_balance_out_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_warehouse_balance_out_tmp 
(INDEX idx_warehouse_product(warehouse_number,product_code2))
AS
SELECT t.`sdate`
,t.region_area
,t.business_area
,t.warehouse_number
,t.warehouse_name
,t.product_code2
,SUM(t.`FQTY`) fqty
,SUM(IF(t.`destination` = '货架',t.`FQTY`,0)) 货架
,SUM(IF(t.`destination` = '前置仓',t.`FQTY`,0)) 前置仓
,SUM(IF(t.`destination` = '虚拟货架',t.`FQTY`,0)) 虚拟货架
,SUM(IF(t.`destination` = '大客户',t.`FQTY`,0)) 大客户
,SUM(IF(t.`destination` IN ('调拨出库-同区','调拨出库-跨区'),t.`FQTY`,0)) 调拨订单
,SUM(IF(t.`destination` = '次品订单',t.`FQTY`,0)) 次品订单
,SUM(IF(t.`destination` = '返厂订单',t.`FQTY`,0)) 返厂订单
,SUM(IF(t.`destination` NOT IN ('货架','前置仓','虚拟货架','大客户','调拨出库-同区','调拨出库-跨区','次品订单','返厂订单'),t.`FQTY`,0)) 其他出库
FROM fe_dm.`dm_sc_warehouse_outbound_daily` t
WHERE t.`sdate` = @sdate
GROUP BY t.sdate,t.warehouse_number,t.product_code2
;
# 入库
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_warehouse_balance_in_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_warehouse_balance_in_tmp 
(INDEX idx_warehouse_product(warehouse_number,product_code2))
SELECT 
 DATE(t.`end_time`) sdate
,w.region_area
,w.business_area
,t.`warehouseid` AS warehouse_number
,t.`warehouse` AS warehouse_name
,t.good_id AS product_code2
,t.good_name AS product_name 
,SUM(t.`actual_qty`) qty
,SUM(IF(t.`asn_type` = 'CN调拨入库',t.`actual_qty`,0)) CN调拨入库
,SUM(IF(t.`asn_type` = 'CN采购入库',t.`actual_qty`,0)) CN采购入库
,SUM(IF(t.`asn_type` = '退货入库',t.`actual_qty`,0)) 退货入库
,SUM(IF(t.`asn_type` NOT IN ('CN调拨入库','CN采购入库','退货入库'),t.`actual_qty`,0) ) 其他入库
FROM fe_dwd.`dwd_sc_bdp_warehouse_receive_detail` t
JOIN 
(SELECT DISTINCT region_area,business_area,warehouse_number
FROM fe_dwd.`dwd_pub_warehouse_business_area` 
) w
ON t.`warehouseid` = w.warehouse_number
WHERE t.`end_time` >= @sdate AND t.`end_time` < ADDDATE(@sdate,1)
GROUP BY sdate,w.business_area,t.good_id
HAVING qty > 0
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_warehouse_balance_stock_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_warehouse_balance_stock_tmp 
(INDEX idx_warehouse_product(warehouse_number,product_code2))
SELECT w.region_area
, w.business_area 
, t.`warehouse` AS warehouse_number
, t.`warehouse_name`
, t.`sku_no` AS product_code2
, t.`skuname` AS product_name
, t.`storage_amount` AS stock_qty
FROM fe_dwd.`dwd_sc_bdp_warehouse_stock_daily` t
LEFT JOIN 
(SELECT DISTINCT region_area,business_area,warehouse_number
FROM fe_dwd.`dwd_pub_warehouse_business_area` 
) w
ON t.`warehouse` = w.warehouse_number
WHERE t.`sdate` = @sdate
AND business_area IS NOT NULL
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_warehouse_balance_product_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_warehouse_balance_product_tmp 
SELECT t1.region_area,t1.business_area,t1.warehouse_number,t1.warehouse_name,t1.product_code2
FROM fe_dm.dm_sc_warehouse_balance_stock_tmp t1
WHERE t1.business_area IS NOT NULL # 去除物资类商品
UNION 
SELECT t2.region_area,t2.business_area,t2.warehouse_number,t2.warehouse_name,t2.product_code2
FROM fe_dm.dm_sc_warehouse_balance_out_tmp t2
UNION 
SELECT t3.region_area,t3.business_area,t3.warehouse_number,t3.warehouse_name,t3.product_code2
FROM fe_dm.dm_sc_warehouse_balance_in_tmp t3
;
DELETE FROM fe_dm.dm_sc_warehouse_balance WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_warehouse_balance
(sdate
, region_area
, business_area
, warehouse_number
, warehouse_name
, product_code2
, product_id
, product_name
, fname
, product_type
, stock_qty
, in_qty
, in_qty_transfer
, in_qty_purchase
, in_qty_back
, in_qty_other
, out_qty
, out_qty_shelf
, out_qty_preware
, out_qty_virtual
, out_qty_client
, out_qty_transfer
, out_qty_inferior
, out_qty_factory
, out_qty_other
, purchase_price 
, area_sale_level
)
SELECT @sdate
    , t1.region_area
    , t1.business_area
    , t1.warehouse_number
    , t1.warehouse_name
    , t1.product_code2
    , p.product_id
    , p.`PRODUCT_NAME` 
    , p.fname_type
    , sp.product_type
    , IFNULL(s.stock_qty,0) stock_qty
    , IFNULL(it.qty,0) in_qty
    , IFNULL(it.CN调拨入库,0) in_qty_transfer
    , IFNULL(it.CN采购入库,0) in_qty_purchase
    , IFNULL(it.退货入库,0) in_qty_back
    , IFNULL(it.其他入库,0) in_qty_other
    , IFNULL(ot.fqty,0) out_qty
    , IFNULL(ot.货架,0) out_qty_shelf
    , IFNULL(ot.前置仓,0) out_qty_preware
    , IFNULL(ot.虚拟货架,0) out_qty_virtual
    , IFNULL(ot.大客户,0) out_qty_client
    , IFNULL(ot.调拨订单,0) out_qty_transfer
    , IFNULL(ot.次品订单,0) out_qty_inferior
    , IFNULL(ot.返厂订单,0) out_qty_factory
    , IFNULL(ot.其他出库,0) out_qty_factory
    , pr.`purchase_price`
    , sa.sale_level 
FROM
    fe_dm.dm_sc_warehouse_balance_product_tmp t1 
    LEFT JOIN fe_dm.dm_sc_warehouse_balance_stock_tmp s 
        ON t1.warehouse_number = s.warehouse_number 
        AND t1.product_code2 = s.product_code2 
    LEFT JOIN fe_dm.dm_sc_warehouse_balance_out_tmp ot 
        ON t1.warehouse_number = ot.warehouse_number 
        AND t1.product_code2 = ot.product_code2 
    LEFT JOIN fe_dm.dm_sc_warehouse_balance_in_tmp it 
        ON t1.warehouse_number = it.warehouse_number 
        AND t1.product_code2 = it.product_code2 
    JOIN fe_dwd.dwd_product_base_day_all p 
        ON t1.product_code2 = p.product_code2 
    JOIN fe_dm.dm_sc_current_dynamic_purchase_price pr
        ON t1.business_area = pr.business_area
        AND t1.product_code2 = pr.product_code2
    LEFT JOIN fe_dwd.`dwd_pub_product_dim_sserp` sp
       ON t1.business_area = sp.business_area
       AND t1.product_code2 = sp.product_fe
    LEFT JOIN fe_dm.dm_area_product_sale_flag sa
    ON  pr.business_Area = sa.business_area
    AND pr.product_id = sa.product_id
    AND sa.sdate = SUBDATE(@sdate,WEEKDAY(@sdate)) 
  ;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_warehouse_balance',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_warehouse_balance','dm_sc_warehouse_balance','吴婷');
COMMIT;
    END