CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_pub_area_product_stat`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dm.`dm_pub_area_product_stat_tmp_1`;
CREATE TEMPORARY TABLE fe_dm.dm_pub_area_product_stat_tmp_1
SELECT 
t1.region_name,
t1.business_name,
t1.PRODUCT_ID,
t5.PRODUCT_CODE,
t5.PRODUCT_CODE2,
t5.PRODUCT_NAME,
pd.PRODUCT_TYPE,
t3.gmv,
t1.stock_shelf_cnt,
t3.sale_shelf_cnt,
t2.pre_cover_shelf_cnt,
t1.normal_fill_shelf_cnt,
t3.orders,
t3.users,
t1.stock_cnt,
t1.stock_amount,
t3.sale_cnt,
t3.sale_act_cnt,
t1.avg_sale_price,
t3.discounts,
t1.onsale_rate 
FROM 
(
SELECT 
s.region_name ,
s.`business_name`
,t.`PRODUCT_ID`
,AVG(t.`SALE_PRICE`) avg_sale_price
,COUNT(DISTINCT(IF(t.SHELF_FILL_FLAG = 1,t.`SHELF_ID`,NULL))) normal_fill_shelf_cnt
,COUNT(DISTINCT(IF(t.STOCK_QUANTITY > 0,t.`SHELF_ID`,NULL))) stock_shelf_cnt
,COUNT(DISTINCT(IF(t.SHELF_FILL_FLAG = 1,t.`SHELF_ID`,NULL)))/COUNT(DISTINCT(IF(t.STOCK_QUANTITY > 0,t.`SHELF_ID`,NULL))) onsale_rate
,SUM(t.stock_quantity) stock_cnt
,SUM(t.stock_quantity * t.sale_price) stock_amount
FROM fe_dwd.`dwd_shelf_product_day_all` t
JOIN fe_dwd.`dwd_shelf_base_day_all` s
ON t.`SHELF_ID` = s.`shelf_id`
GROUP BY s.region_name,s.`business_name`,t.`PRODUCT_ID`
) t1
LEFT JOIN
(
SELECT t.`business_area` business_name ,COUNT(DISTINCT t.`shelf_id`) pre_cover_shelf_cnt
FROM fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` t
WHERE t.`shelf_status` = 2
GROUP BY t.`business_area`
) t2
ON t1.business_name = t2.business_name
LEFT JOIN 
(SELECT 
	s.`business_name`,
	a.product_id,
	COUNT(DISTINCT order_id)  orders,
	COUNT(DISTINCT user_id)  users,
	SUM(a.sale_price * a.quantity_act) AS gmv,  -- GMV
	SUM(a.quantity) AS sale_cnt,   -- 销量
SUM(a.REAL_TOTAL_PRICE) REAL_TOTAL_PRICE,  -- 实收
SUM(a.quantity_act) sale_act_cnt,  -- 实际出货量
SUM(a.sale_price * a.quantity_act) AS gmv_shipped ,  -- gmv_shipped
SUM(a.discount_amount)  discounts,
COUNT(DISTINCT a.`shelf_id`) sale_shelf_cnt
FROM 
	`fe_dwd`.`dwd_pub_order_item_recent_one_month` a
	JOIN fe_dwd.`dwd_shelf_base_day_all` s
	ON a.`shelf_id` = s.`shelf_id`
WHERE a.PAY_DATE >= SUBDATE(CURDATE(),1)
AND a.PAY_DATE < CURDATE()	
GROUP BY s.`business_name`,a.product_id
) t3
ON t1.business_name = t3.business_name
AND t1.product_id = t3.product_id  
LEFT JOIN 
feods.zs_product_dim_sserp pd 
ON t1.product_id = pd.product_id 
AND pd.business_area = t1.business_name 
LEFT JOIN 
fe_dwd.dwd_product_base_day_all t5 
ON t1.product_id = t5.product_id
;
delete from fe_dm.`dm_pub_area_product_stat`
WHERE sdate >= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
insert into fe_dm.`dm_pub_area_product_stat`
(
sdate,
region_name,
business_name,
PRODUCT_ID,
PRODUCT_CODE,
PRODUCT_CODE2,
PRODUCT_NAME,
PRODUCT_TYPE,
gmv,
stock_shelf_cnt,
sale_shelf_cnt,
pre_cover_shelf_cnt,
normal_fill_shelf_cnt,
orders,
users,
stock_cnt,
stock_amount,
sale_cnt,
sale_act_cnt,
avg_sale_price,
discounts,
onsale_rate 
)
select 
DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate,
region_name,
business_name,
PRODUCT_ID,
PRODUCT_CODE,
PRODUCT_CODE2,
PRODUCT_NAME,
PRODUCT_TYPE,
gmv,
stock_shelf_cnt,
sale_shelf_cnt,
pre_cover_shelf_cnt,
normal_fill_shelf_cnt,
orders,
users,
stock_cnt,
stock_amount,
sale_cnt,
sale_act_cnt,
avg_sale_price,
discounts,
onsale_rate 
from 
fe_dm.dm_pub_area_product_stat_tmp_1;
  
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_pub_area_product_stat',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('李世龙@', @user, @timestamp));
COMMIT;
    END