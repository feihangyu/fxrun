CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_preware_product_sale`()
    SQL SECURITY INVOKER
BEGIN
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
		
    SET l_task_name = 'sh_preware_product_sale'; 
    
 SET @sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
 SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
#前置仓每日销售
DELETE FROM feods.d_sc_preware_sales_daily WHERE sdate = @sdate;
INSERT INTO feods.d_sc_preware_sales_daily
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
t1.warehouse_id,
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
SELECT t.warehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
FROM
(SELECT sh.`warehouse_id`,sh.`shelf_id`, sa.`PRODUCT_ID`,sa.`QUANTITY`,sa.`SALE_PRICE`,sa.discount_amount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN feods.sf_order_item_temp sa
ON sh.shelf_id = sa.SHELF_ID
AND sh.data_flag = 1
AND sa.order_date >= @sdate AND sa.order_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.`warehouse_id`,sh.`shelf_id`, yi.`goods_id`, yi.`product_count`,yi.price,IFNULL(yt.price_2,0) discount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN fe.`sf_order_yht` yt
ON sh.`shelf_id` = yt.shelf_id
AND sh.data_flag =1
AND yt.data_flag =1
AND yt.`pay_status` = 1 #支付成功
AND yt.`payTime` >= @sdate AND yt.`payTime` < CURDATE() 
JOIN fe.`sf_order_yht_item` yi
ON yt.`order_id` = yi.`order_id`) t
GROUP BY t.warehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.`dwd_product_base_day_all` t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` t4        ## 货架名称，编码
ON t1.warehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.`dwd_pub_warehouse_business_area`  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
#前置仓覆盖货架每日销售数据（货架商品级）
DELETE FROM feods.d_sc_preware_shelf_sales_daily WHERE sdate = @sdate;
INSERT INTO feods.d_sc_preware_shelf_sales_daily
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
t1.warehouse_id,
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
SELECT t.warehouse_id
,t.shelf_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
FROM
(SELECT sh.`warehouse_id`,sh.`shelf_id`, sa.`PRODUCT_ID`,sa.`QUANTITY`,sa.`SALE_PRICE`,sa.discount_amount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN feods.sf_order_item_temp sa
ON sh.shelf_id = sa.SHELF_ID
AND sh.data_flag = 1
AND sa.order_date >= @sdate AND sa.order_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.`warehouse_id`,sh.`shelf_id`, yi.`goods_id`, yi.`product_count`,yi.price,IFNULL(yt.price_2,0) discount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN fe.`sf_order_yht` yt
ON sh.`shelf_id` = yt.shelf_id
AND sh.data_flag =1
AND yt.data_flag =1
AND yt.`pay_status` = 1 #支付成功
AND yt.`payTime` >= @sdate AND yt.`payTime` < CURDATE() 
JOIN fe.`sf_order_yht_item` yi
ON yt.`order_id` = yi.`order_id`) t
GROUP BY t.warehouse_id,t.shelf_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.`dwd_product_base_day_all` t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` t4        ## 货架名称，编码
ON t1.warehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.`dwd_pub_warehouse_business_area`  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;  
  
# 前置仓覆盖货架近7天销量（包括当天）--前置仓/商品级别(从2019-5月份开始)
DELETE FROM feods.pj_preware_sales_seven WHERE sdate = @sdate;
INSERT INTO feods.pj_preware_sales_seven
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
t1.warehouse_id,
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
SELECT t.warehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
FROM
(SELECT sh.`warehouse_id`,sh.`shelf_id`, sa.`PRODUCT_ID`,sa.`QUANTITY`,sa.`SALE_PRICE`,sa.discount_amount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN feods.sf_order_item_temp sa
ON sh.shelf_id = sa.SHELF_ID
AND sh.data_flag = 1
AND sa.order_date >= SUBDATE(CURDATE(),7) AND sa.order_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.`warehouse_id`,sh.`shelf_id`, yi.`goods_id`, yi.`product_count`,yi.price,IFNULL(yt.price_2,0) discount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN fe.`sf_order_yht` yt
ON sh.`shelf_id` = yt.shelf_id
AND sh.data_flag =1
AND yt.data_flag =1
AND yt.`pay_status` = 1 #支付成功
AND yt.`payTime` >= SUBDATE(CURDATE(),7) AND yt.`payTime` < CURDATE() 
JOIN fe.`sf_order_yht_item` yi
ON yt.`order_id` = yi.`order_id`) t
GROUP BY t.warehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.`dwd_product_base_day_all` t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` t4        ## 货架名称，编码
ON t1.warehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.`dwd_pub_warehouse_business_area`  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
 
 
--  前置仓覆盖货架近15天销量（包括当天）--前置仓/商品级别(从2019-5月份开始)
DELETE FROM feods.pj_preware_sales_fifteen WHERE sdate = @sdate;
INSERT INTO feods.pj_preware_sales_fifteen
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
t1.warehouse_id,
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
SELECT t.warehouse_id
,t.PRODUCT_ID
,SUM(QUANTITY) QUANTITY 
,SUM(SALE_PRICE * QUANTITY) gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,sale_price * QUANTITY ,0)) discount_gmv
,SUM(IF(discount_amount < sale_price * QUANTITY * 0.2,QUANTITY ,0)) discount_qty
,COUNT(DISTINCT t.shelf_id) sale_shelf_cnt
,COUNT(DISTINCT(IF(discount_amount < sale_price * QUANTITY * 0.2,shelf_id,NULL))) discount_shelf_cnt
FROM
(SELECT sh.`warehouse_id`,sh.`shelf_id`, sa.`PRODUCT_ID`,sa.`QUANTITY`,sa.`SALE_PRICE`,sa.discount_amount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN feods.sf_order_item_temp sa
ON sh.shelf_id = sa.SHELF_ID
AND sh.data_flag = 1
AND sa.order_date >= SUBDATE(CURDATE(),15) AND sa.order_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT sh.`warehouse_id`,sh.`shelf_id`, yi.`goods_id`, yi.`product_count`,yi.price,IFNULL(yt.price_2,0) discount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN fe.`sf_order_yht` yt
ON sh.`shelf_id` = yt.shelf_id
AND sh.data_flag = 1
AND yt.data_flag =1
AND yt.`pay_status` = 1 #支付成功
AND yt.`payTime` >= SUBDATE(CURDATE(),15) AND yt.`payTime` < CURDATE() 
JOIN fe.`sf_order_yht_item` yi
ON yt.`order_id` = yi.`order_id`) t
GROUP BY t.warehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.`dwd_product_base_day_all` t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` t4        ## 货架名称，编码
ON t1.warehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.`dwd_pub_warehouse_business_area`  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
 
# 前置仓覆盖货架近30天销量（包括当天）--前置仓/商品级别-可用于直接计算前置仓商品销售等级
DELETE FROM feods.pj_preware_shelf_sales_thirty WHERE sdate = @sdate;
INSERT INTO feods.pj_preware_shelf_sales_thirty
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
t1.warehouse_id,
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
SELECT t.warehouse_id
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
(SELECT DATE(sa.order_date) AS pay_date,sh.`warehouse_id`,sh.`shelf_id`, sa.`PRODUCT_ID`,sa.`QUANTITY`,sa.`SALE_PRICE`,sa.discount_amount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN feods.sf_order_item_temp sa
ON sh.shelf_id = sa.SHELF_ID
AND sh.data_flag = 1
AND sa.order_date >= SUBDATE(CURDATE(),30) AND sa.order_date < CURDATE()
UNION ALL    # 自动售卖机未对接系统
SELECT DATE(yt.`payTime`) AS pay_date,sh.`warehouse_id`,sh.`shelf_id`, yi.`goods_id`, yi.`product_count`,yi.price,IFNULL(yt.price_2,0) discount
FROM fe.sf_prewarehouse_shelf_detail sh
JOIN fe.`sf_order_yht` yt
ON sh.`shelf_id` = yt.shelf_id
AND sh.data_flag =1
AND yt.data_flag =1
AND yt.`pay_status` = 1 #支付成功
AND yt.`payTime` >= SUBDATE(CURDATE(),30) AND yt.`payTime` < CURDATE() 
JOIN fe.`sf_order_yht_item` yi
ON yt.`order_id` = yi.`order_id`) t
GROUP BY t.warehouse_id,t.PRODUCT_ID 
) t1
JOIN fe_dwd.`dwd_product_base_day_all` t3   ##product_code2，FE码
ON t1.product_id = t3.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` t4        ## 货架名称，编码
ON t1.warehouse_id = t4.shelf_id
AND t4.DATA_FLAG = 1
JOIN fe_dwd.`dwd_pub_warehouse_business_area`  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_preware_product_sale',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END