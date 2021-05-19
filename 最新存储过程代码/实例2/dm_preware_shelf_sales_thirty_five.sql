CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_preware_shelf_sales_thirty_five`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
DELETE FROM fe_dm.dm_sc_preware_sales_daily WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_sales_daily
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_code2,
product_name,
product_id,
FNAME,
sale_shelf_cnt,
GMV,
quantity
)
SELECT 
@sdate AS sdate,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t1.prewarehouse_id,
t4.shelf_code,
t4.shelf_name,
t3.product_code2,
t3.product_name,
t1.product_id,
t3.FNAME_type,
t1.sale_shelf_cnt,
t1.gmv,
t1.QUANTITY
FROM
( 
SELECT t.prewarehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
FROM
(SELECT sh.prewarehouse_id,sh.shelf_id, sa.PRODUCT_ID,sa.quantity_act AS quantity ,sa.SALE_PRICE,sa.discount_amount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_item_recent_one_month sa
ON sh.shelf_id = sa.SHELF_ID
AND sa.pay_date >= @sdate AND sa.pay_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.prewarehouse_id,sh.shelf_id, yt.product_id, yt.product_count,yt.price,IFNULL(yt.price_2,0) discount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_shelf_product_yht yt
ON sh.shelf_id = yt.shelf_id
AND yt.pay_status = 1 #支付成功
AND yt.payTime >= @sdate AND yt.payTime < CURDATE() 
) t
GROUP BY t.prewarehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.dwd_product_base_day_all t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.dwd_shelf_base_day_all t4        ## 货架名称，编码
ON t1.prewarehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
#前置仓覆盖货架每日销售数据（货架商品级）
DELETE FROM fe_dm.dm_sc_preware_shelf_sales_daily WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_shelf_sales_daily
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
shelf_id,
product_code2,
product_name,
product_id,
FNAME,
GMV,
quantity
)
SELECT 
@sdate AS sdate,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t1.prewarehouse_id,
t4.shelf_code,
t4.shelf_name,
t1.shelf_id,
t3.product_code2,
t3.product_name,
t1.product_id,
t3.FNAME_type,
t1.GMV,
t1.quantity
FROM 
( 
SELECT t.prewarehouse_id
,t.shelf_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
FROM
(SELECT sh.prewarehouse_id,sh.shelf_id, sa.PRODUCT_ID,sa.quantity_act AS QUANTITY,sa.SALE_PRICE,sa.discount_amount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_item_recent_one_month sa
ON sh.shelf_id = sa.SHELF_ID
AND sa.pay_date >= @sdate AND sa.pay_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.prewarehouse_id,sh.shelf_id, yt.product_id, yt.product_count,yt.price,IFNULL(yt.price_2,0) discount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_shelf_product_yht yt
ON sh.shelf_id = yt.shelf_id
AND yt.pay_status = 1 #支付成功
AND yt.payTime >= @sdate AND yt.payTime < CURDATE() 
) t
GROUP BY t.prewarehouse_id,t.shelf_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.dwd_product_base_day_all t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.dwd_shelf_base_day_all t4        ## 货架名称，编码
ON t1.prewarehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;  
  
# 前置仓覆盖货架近7天销量（包括当天）--前置仓/商品级别(从2019-5月份开始)
DELETE FROM fe_dm.dm_preware_sales_seven WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_preware_sales_seven
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_code2,
product_name,
product_id,
FNAME,
sale_shelf_cnt,
GMV,
quantity
)
SELECT 
@sdate AS sdate,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t1.prewarehouse_id,
t4.shelf_code,
t4.shelf_name,
t3.product_code2,
t3.product_name,
t1.product_id,
t3.FNAME_type,
t1.sale_shelf_cnt,
t1.gmv,
t1.QUANTITY
FROM
( 
SELECT t.prewarehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
FROM
(SELECT sh.prewarehouse_id,sh.shelf_id, sa.PRODUCT_ID,sa.quantity_act AS QUANTITY,sa.SALE_PRICE,sa.discount_amount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_item_recent_one_month sa
ON sh.shelf_id = sa.SHELF_ID
AND sa.pay_date >= SUBDATE(CURDATE(),7) AND sa.pay_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.prewarehouse_id,sh.shelf_id, yt.product_id, yt.product_count,yt.price,IFNULL(yt.price_2,0) discount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_shelf_product_yht yt
ON sh.shelf_id = yt.shelf_id
AND yt.pay_status = 1 #支付成功
AND yt.payTime >= SUBDATE(CURDATE(),7) AND yt.payTime < CURDATE() 
) t
GROUP BY t.prewarehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.dwd_product_base_day_all t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.dwd_shelf_base_day_all t4        ## 货架名称，编码
ON t1.prewarehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
 
 
--  前置仓覆盖货架近15天销量（包括当天）--前置仓/商品级别(从2019-5月份开始)
DELETE FROM fe_dm.dm_preware_sales_fifteen WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_preware_sales_fifteen
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_code2,
product_name,
product_id,
FNAME,
sale_shelf_cnt,
GMV,
quantity,
discount_shelf_cnt,
discount_sale_qty,
sale_flag
)
SELECT 
@sdate AS sdate,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t1.prewarehouse_id,
t4.shelf_code,
t4.shelf_name,
t3.product_code2,
t3.product_name,
t1.product_id,
t3.FNAME_type,
t1.sale_shelf_cnt,
t1.gmv,
t1.QUANTITY,
t1.discount_shelf_cnt,
t1.discount_qty,
CASE WHEN t1.discount_qty = 0 THEN "严重滞销"
WHEN t1.discount_qty/t1.discount_shelf_cnt/15 < 0.07 THEN "严重滞销"
WHEN t1.discount_qty/t1.discount_shelf_cnt/15 >= 0.07 AND t1.discount_qty/t1.discount_shelf_cnt/15 < 0.21 THEN "滞销"
WHEN t1.discount_qty/t1.discount_shelf_cnt/15 >= 0.21 AND t1.discount_qty/t1.discount_shelf_cnt/15 < 0.43 THEN "平销"
WHEN t1.discount_qty/t1.discount_shelf_cnt/15 >= 0.43 AND t1.discount_qty/t1.discount_shelf_cnt/15 < 0.71 THEN "畅销"
ELSE "爆款" 
END AS sale_flag 
FROM
( 
SELECT t.prewarehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
,COUNT(DISTINCT(IF(discount_amount < sale_price * QUANTITY * 0.2,shelf_id,NULL))) discount_shelf_cnt
FROM
(SELECT sh.prewarehouse_id,sh.shelf_id, sa.PRODUCT_ID,sa.quantity_act AS QUANTITY,sa.SALE_PRICE,sa.discount_amount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_item_recent_one_month sa
ON sh.shelf_id = sa.SHELF_ID
AND sa.pay_date >= SUBDATE(CURDATE(),15) AND sa.pay_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.prewarehouse_id,sh.shelf_id, yt.product_id, yt.product_count,yt.price,IFNULL(yt.price_2,0) discount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_shelf_product_yht yt
ON sh.shelf_id = yt.shelf_id
AND yt.pay_status = 1 #支付成功
AND yt.payTime >= SUBDATE(CURDATE(),15) AND yt.payTime < CURDATE() 
) t
GROUP BY t.prewarehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.dwd_product_base_day_all t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.dwd_shelf_base_day_all t4        ## 货架名称，编码
ON t1.prewarehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
 
# 前置仓覆盖货架近30天销量（包括当天）--前置仓/商品级别-可用于直接计算前置仓商品销售等级
DELETE FROM fe_dm.dm_preware_shelf_sales_thirty WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_preware_shelf_sales_thirty
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_code2,
product_name,
product_id,
FNAME,
sale_shelf_cnt,
GMV,
quantity,
discount_shelf_cnt,
discount_sale_qty,
sale_flag
)
SELECT 
@sdate AS sdate,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t1.prewarehouse_id,
t4.shelf_code,
t4.shelf_name,
t3.product_code2,
t3.product_name,
t1.product_id,
t3.FNAME_type,
t1.sale_shelf_cnt,
t1.gmv,
t1.QUANTITY,
t1.discount_shelf_cnt,
t1.discount_qty,
CASE WHEN t1.discount_qty15 = 0 THEN "严重滞销"
WHEN ISNULL(t1.discount_qty15)  THEN "严重滞销"
WHEN t1.discount_qty15/t1.discount_shelf_cnt15/15 < 0.07 THEN "严重滞销"
WHEN t1.discount_qty15/t1.discount_shelf_cnt15/15 >= 0.07 AND t1.discount_qty15/t1.discount_shelf_cnt15/15 < 0.21 THEN "滞销"
WHEN t1.discount_qty15/t1.discount_shelf_cnt15/15 >= 0.21 AND t1.discount_qty15/t1.discount_shelf_cnt15/15 < 0.43 THEN "平销"
WHEN t1.discount_qty15/t1.discount_shelf_cnt15/15 >= 0.43 AND t1.discount_qty15/t1.discount_shelf_cnt15/15 < 0.71 THEN "畅销"
ELSE "爆款" 
END AS sale_flag 
FROM
( 
SELECT t.prewarehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,SUM(IF(pay_date >= SUBDATE(CURDATE(),15) AND pay_date < CURDATE() AND discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty15
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
,COUNT(DISTINCT(IF(discount_amount < sale_price * QUANTITY * 0.2,shelf_id,NULL))) discount_shelf_cnt
,COUNT(DISTINCT(IF(pay_date >= SUBDATE(CURDATE(),15) AND pay_date < CURDATE() AND discount_amount < sale_price * QUANTITY * 0.2,shelf_id,NULL))) discount_shelf_cnt15
FROM
(SELECT DATE(sa.pay_date) AS pay_date,sh.prewarehouse_id,sh.shelf_id, sa.PRODUCT_ID,sa.quantity_act AS QUANTITY,sa.SALE_PRICE,sa.discount_amount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_item_recent_one_month sa
ON sh.shelf_id = sa.SHELF_ID
AND sa.pay_date >= SUBDATE(CURDATE(),30) AND sa.pay_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT DATE(yt.payTime) AS pay_date,sh.prewarehouse_id,sh.shelf_id, yt.product_id, yt.product_count,yt.price,IFNULL(yt.price_2,0) discount
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all sh
JOIN fe_dwd.dwd_pub_order_shelf_product_yht yt
ON sh.shelf_id = yt.shelf_id
AND yt.pay_status = 1 #支付成功
AND yt.payTime >= SUBDATE(CURDATE(),30) AND yt.payTime < CURDATE() 
) t
GROUP BY t.prewarehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.dwd_product_base_day_all t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.dwd_shelf_base_day_all t4        ## 货架名称，编码
ON t1.prewarehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_preware_shelf_sales_thirty_five',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_preware_shelf_sales_thirty_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_preware_shelf_sales_thirty_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_preware_shelf_sales_thirty_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_preware_shelf_sales_thirty_five','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_preware_shelf_sales_thirty_five','吴婷');
COMMIT;
    END