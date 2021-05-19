CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_active_result`(in_date DATE)
BEGIN
   
SET @sdate = in_date;
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
#由于活动订单表数据不规范，因此将订单详情分为3种情况，如下：
DROP TEMPORARY TABLE IF EXISTS feods.active_result_tmp1;
CREATE TEMPORARY TABLE IF NOT EXISTS  feods.active_result_tmp1 (
        KEY idx_order(order_id)
        ) AS
# 没有商品编号的活动订单，也没有活动号
SELECT
    a.activity_id
    , a.order_id
    , a.shelf_id
    , a.`GOODS_ID` AS product_id
    , a.`COMBINED_PRICE`
FROM
    fe.`sf_order_activity` a
WHERE a.`data_flag` = 1
    AND a.order_status = 2 #1为已取消
     AND a.pay_date >= @sdate
    AND a.pay_date < DATE_ADD(@sdate, INTERVAL 1 DAY)
    AND a.`GOODS_ID` IS NULL;
DROP TEMPORARY TABLE IF EXISTS feods.active_result_tmp2;    
CREATE TEMPORARY TABLE IF NOT EXISTS  feods.active_result_tmp2 (
        KEY idx_order(order_id)
        ) AS
#正常显示的订单和商品
SELECT
    a.activity_id
    , a.order_id
    , a.shelf_id
    , a.`GOODS_ID` AS product_id
    , a.`COMBINED_PRICE`
    , 1 AS sense
FROM
    fe.`sf_order_activity` a
WHERE a.`GOODS_ID` IS NOT NULL
    AND a.`data_flag` = 1
    AND a.`GOODS_ID` NOT LIKE "%,%"
    AND a.order_status = 2 #1为已取消
     AND a.pay_date >= @sdate
    AND a.pay_date < DATE_ADD(@sdate, INTERVAL 1 DAY)
UNION ALL
# 商品中逗号的    
SELECT
    a.`ACTIVITY_ID`
    , a.`ORDER_ID`
    , a.`SHELF_ID`
    , SUBSTRING_INDEX(
        SUBSTRING_INDEX(a.`GOODS_ID`, ",", n.`number` + 1)
        , ","
        , - 1
    ) AS product_id
    , a.`COMBINED_PRICE`
    , 2 AS sense
FROM
    fe.`sf_order_activity` a
    JOIN feods.`fjr_number` n
        ON n.number <= LENGTH(a.`GOODS_ID`) - LENGTH(REPLACE(a.`GOODS_ID`, ",", ""))
WHERE a.`data_flag` = 1
    AND a.order_status = 2 #1为已取消
     AND a.`GOODS_ID` LIKE "%,%"
    AND a.pay_date >= @sdate
    AND a.pay_date < DATE_ADD(@sdate, INTERVAL 1 DAY);
#活动结果表：
DELETE FROM feods.`d_sc_active_result` WHERE sdate = @sdate;
INSERT INTO feods.`d_sc_active_result` 
(
sdate,
region_area,
business_area,
warehouse_id,
wshelf_code,
wshelf_name,
activity_id,
activity_name,
start_date,
end_date,
order_id,
order_date,
activity_add_time,
shelf_id,
shelf_code,
shelf_name,
product_id,
product_code2,
product_name,
cost_dept,
platform,
platform_business_type,
discount_type,
discount_name,
discount_value,
activity_type,
quantity,
sale_price,
gmv,
discount_value_active,
gmv_after_discount,
pay_total_amount,
discount_amount_all,
other_discounts
)
SELECT 
@sdate,
c.region_name, 
c.business_name,
ps.warehouse_id,
ws.shelf_code,
ws.shelf_name,
a.`activity_id`,
a.activity_name,
a.`start_time`,
a.`end_time`,
o.order_id,
o.order_date,
o.order_date AS PAY_DATE, 
o.`shelf_id`,
s.shelf_code,
s.shelf_name,
p.product_id,
p.product_code2,
p.product_name,
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
 WHEN 1 THEN '自动贩卖机' 
 WHEN 1 THEN '智能货架' 
 WHEN 1 THEN '校园货架' 
 END AS 'platform_business_type',
 a.discount_type,
 CASE a.discount_type 
WHEN 1 THEN '打折'
WHEN 2 THEN '降价'
WHEN 3 THEN '优惠价'
END AS discount_name,
a.discount_value,
a.activity_type,
o.QUANTITY,
o.sale_price, 
o.sale_price * o.quantity AS gmv,
IF(o.sense = 1,o.COMBINED_PRICE,DISCOUNT_AMOUNT) COMBINED_PRICE , #活动优惠金额
FLOOR((o.real_total_price + o.DISCOUNT_AMOUNT)/o.sale_price)*o.sale_price - IF(o.sense = 1,o.COMBINED_PRICE,DISCOUNT_AMOUNT) AS gmv_after_discount,
-- o.sale_price * o.quantity - IF(o.sense = 1,o.COMBINED_PRICE,DISCOUNT_AMOUNT) AS gmv_after_discount,  
-- IF(o.sale_price * o.quantity - o.DISCOUNT_AMOUNT <0,0,o.sale_price * o.quantity - o.DISCOUNT_AMOUNT )AS REAL_TOTAL_PRICE, #暂时的订单明细表时候有bug，因此以GMV-优惠来计算，等bug更新之后可做调整
o.real_total_price,
o.DISCOUNT_AMOUNT , 
IF(o.DISCOUNT_AMOUNT - IF(o.sense = 1,o.COMBINED_PRICE,DISCOUNT_AMOUNT)<0,0.00,o.DISCOUNT_AMOUNT - IF(o.sense = 1,o.COMBINED_PRICE,DISCOUNT_AMOUNT)) AS other_discounts 
FROM 
(
SELECT
    ao.order_id
    , ao.shelf_id
    , ao.product_id
    , ao.activity_id
    , oi.QUANTITY
    , oi.sale_price
    , oi.DISCOUNT_AMOUNT
    , ao.COMBINED_PRICE
    , oi.sale_price * oi.quantity - ao.COMBINED_PRICE AS gmv_after_discount
    , oi.order_date
    , oi.real_total_price
    , ao.sense
FROM feods.active_result_tmp2 ao  #活动结果
JOIN feods.`wt_order_item_twomonth_temp` oi  #订单明细表，获取实际GMV和销量，实收，总的优惠金额
ON ao.order_id = oi.order_id
AND ao.product_id = oi.product_id
-- AND oi.DISCOUNT_AMOUNT >0
UNION ALL
SELECT
    ao.order_id
    , ao.shelf_id
    , oi.product_id
    , ao.activity_id
    , oi.QUANTITY
    , oi.sale_price
    , oi.DISCOUNT_AMOUNT
    , ao.COMBINED_PRICE
    , oi.sale_price * oi.quantity - oi.DISCOUNT_AMOUNT AS gmv_after_discount
    , oi.order_date
    , oi.real_total_price
    , 3 AS sense
FROM feods.active_result_tmp1 ao  #活动结果
JOIN feods.`wt_order_item_twomonth_temp` oi  #订单明细表，获取实际GMV和销量，实收，总的优惠金额
ON ao.order_id = oi.order_id
-- AND oi.DISCOUNT_AMOUNT >0
) o
LEFT JOIN fe.sf_product_activity a   #活动生效表
ON o.`activity_id` = a.`activity_id`
AND a.`activity_state` = 2 # 已确认
AND a.`data_flag` =1
JOIN fe.`sf_shelf` s  # 货架的编码
ON o.shelf_id = s.shelf_id
AND s.data_flag =1
JOIN feods.`fjr_city_business` c #区域
ON s.city = c.city
LEFT JOIN fe.`sf_product` p    #商品编码
ON o.product_id = p.product_id
AND p.data_flag =1
LEFT JOIN fe.`sf_prewarehouse_shelf_detail` ps  #是否为前置仓覆盖货架
ON s.shelf_id = ps.shelf_id
AND ps.data_flag =1
LEFT JOIN fe.`sf_shelf` ws   #前置仓编码和名称
ON ps.warehouse_id = ws.shelf_id
AND ws.data_flag =1;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_sc_active_result',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END