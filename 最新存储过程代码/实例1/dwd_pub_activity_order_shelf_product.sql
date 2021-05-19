CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_pub_activity_order_shelf_product`()
BEGIN
-- 替换feods.`d_sc_active_result`
-- 先建立基础宽表，再建立dm层表
   SET @end_date = CURDATE(); 
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_3_date = SUBDATE(@end_date,INTERVAL 90 DAY);
   SET @month_1_date = SUBDATE(@end_date,INTERVAL 30 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @month_id := DATE_FORMAT(SUBDATE(@end_date,INTERVAL 1 MONTH),'%Y-%m');
   SET @timestamp := CURRENT_TIMESTAMP();
 
 DELETE FROM fe_dwd.dwd_pub_activity_order_shelf_product WHERE pay_date >= @start_date;
 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_1_1;
 SET @time_1 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_1_1 (
        KEY idx_order(order_id,product_id)
        ) AS
# 没有商品编号的活动订单，也没有活动号
SELECT
    a.activity_id
    , a.order_id
    , a.shelf_id
    , a.`GOODS_ID` AS product_id
FROM
    fe.`sf_order_activity` a
WHERE a.`data_flag` = 1
    AND a.order_status = 2 #1为已取消
     AND a.pay_date >= @start_date
    AND a.pay_date < @end_date
    AND a.`GOODS_ID` IS NULL;
CREATE INDEX idx_dwd_dwd_lsl_shelf_1_1_1
ON fe_dwd.dwd_lsl_shelf_1_1_1 (order_id);	
-- 没有商品编号的，从订单表里取。而且这种折扣也基本为0 ;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_1;
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_1 (
        KEY idx_order(order_id,product_id)
        ) AS
SELECT distinct 
    a.activity_id
    , a.order_id
    , a.shelf_id		
	,b.product_id
from fe_dwd.dwd_lsl_shelf_1_1_1 a
left join 
fe_dwd.dwd_pub_order_item_recent_one_month b 
on a.order_id = b.order_id;
 SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_1--@time_2",@time_1,@time_2);
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_2;   
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_2 (
        KEY idx_order(order_id,product_id)
        ) AS
#正常显示的订单和商品
SELECT
    a.activity_id
    , a.order_id
    , a.shelf_id
    , a.`GOODS_ID` AS product_id
FROM
    fe.`sf_order_activity` a
WHERE a.`GOODS_ID` IS NOT NULL
    AND a.`data_flag` = 1
    AND a.`GOODS_ID` NOT LIKE "%,%"
    AND a.order_status = 2 #1为已取消
    AND a.pay_date >= @start_date
    AND a.pay_date < @end_date;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_2--@time_3",@time_2,@time_3);
# 商品中逗号的    
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_3;    
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_3 (
        KEY idx_order(order_id,product_id)
        ) AS
SELECT
    a.`ACTIVITY_ID`
    , a.`ORDER_ID`
    , a.`SHELF_ID`
    , SUBSTRING_INDEX(
        SUBSTRING_INDEX(a.`GOODS_ID`, ",", n.`number` + 1)
        , ","
        , - 1
    ) AS product_id
FROM
    fe.`sf_order_activity` a
    JOIN fe_dwd.`dwd_pub_number` n
        ON n.number <= LENGTH(a.`GOODS_ID`) - LENGTH(REPLACE(a.`GOODS_ID`, ",", ""))
WHERE a.`data_flag` = 1
    AND a.order_status = 2 #1为已取消
     AND a.`GOODS_ID` LIKE "%,%"
     AND a.pay_date >= @start_date
    AND a.pay_date < @end_date;
	
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_3--@time_4",@time_3,@time_4);
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1;    
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1 AS	
SELECT * FROM 	fe_dwd.dwd_lsl_shelf_1_1
UNION ALL
SELECT * FROM 	fe_dwd.dwd_lsl_shelf_1_2
UNION ALL
SELECT * FROM 	fe_dwd.dwd_lsl_shelf_1_3;	
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_4--@time_5",@time_4,@time_5);
	
CREATE INDEX idx_dwd_lsl_shelf_1
ON fe_dwd.dwd_lsl_shelf_1 (order_id,product_id);
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_5--@time_6",@time_5,@time_6);		
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2;    
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_2 AS
SELECT
    ao.order_id
    ,oi.order_item_id
    , ao.shelf_id
    , ao.product_id
    , ao.activity_id
    , oi.QUANTITY
    , oi.quantity_act
    , oi.sale_price
    , oi.DISCOUNT_AMOUNT
	, oi.pay_date
    , oi.order_date
FROM fe_dwd.dwd_lsl_shelf_1 ao  #活动结果
 JOIN fe_dwd.dwd_pub_order_item_recent_two_month oi  
ON ao.order_id = oi.order_id
AND ao.product_id = oi.product_id;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_6--@time_7",@time_6,@time_7);
CREATE INDEX idx_dwd_lsl_shelf_2
ON fe_dwd.dwd_lsl_shelf_2 (activity_id);
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_7--@time_8",@time_7,@time_8);
INSERT INTO fe_dwd.dwd_pub_activity_order_shelf_product
(
activity_id
,activity_name
,cost_dept
,platform
,platform_business_type
,discount_type
,discount_name
,discount_value
,activity_type
,start_date
,end_date
,order_id
,ORDER_ITEM_ID
,pay_date
,shelf_id
,product_id
,quantity
,quantity_act
,sale_price
,DISCOUNT_AMOUNT
)
SELECT 
a.`activity_id`,
a.activity_name,
CASE a.`cost_dept`  
  WHEN 1 THEN '市场组'
  WHEN 2 THEN '运营组'
  WHEN 3 THEN '采购组'
  WHEN 4 THEN '大客户组'
  WHEN 5 THEN 'BD组'
  WHEN 6 THEN '经规组'
  END AS cost_dept,
 CASE a.platform   
 WHEN 1 THEN '能量站'   
 WHEN 2 THEN '商城'  
 WHEN 3 THEN '店主'   
 END AS platform,
 CASE a.platform_business_type  
 WHEN 1 THEN '无人货架' 
 WHEN 2 THEN '自动贩卖机' 
 WHEN 3 THEN '智能货架' 
 WHEN 4 THEN '校园货架' 
 END AS 'platform_business_type',
 a.discount_type, 
 CASE a.discount_type 
WHEN 1 THEN '打折'
WHEN 2 THEN '降价'
WHEN 3 THEN '优惠价'
END AS discount_name,
a.discount_value,
a.activity_type,
a.`start_time` start_date,
a.`end_time` end_date,
o.order_id,
o.ORDER_ITEM_ID, 
o.PAY_DATE,
o.`shelf_id`,
o.product_id,
o.QUANTITY,
o.quantity_act,
o.sale_price,
o.DISCOUNT_AMOUNT
FROM 
fe_dwd.dwd_lsl_shelf_2 o
LEFT JOIN fe.sf_product_activity a   #活动生效表
ON o.`activity_id` = a.`activity_id`
AND a.`activity_state` = 2 # 已确认
AND a.`data_flag` =1;
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pub_activity_order_shelf_product","@time_8--@time_9",@time_8,@time_9);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_pub_activity_order_shelf_product',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;	
END