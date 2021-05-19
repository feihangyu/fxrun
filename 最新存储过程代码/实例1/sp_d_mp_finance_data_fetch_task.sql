CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_finance_data_fetch_task`(in date_in date)
begin
  -- =============================================
-- Author:	财务收入
-- Create date: 2019/10/11
-- Modify date: 
-- Description:	
--    更新财务月度收入供数结果表(每个月第一天的3时43分跑)
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  
set @stadate_top:= date_add(date_sub(date_in,interval 1 day),interval -day(DATE_SUB(date_in,INTERVAL 1 DAY))+1 day);
SET @stadate_end:= date_in;

delete from feods.`D_MP_finance_month_income_result` where sdate = date_format(@stadate_top,'%Y%m');

-- 微信支付-无人货架订单明细以及汇总计算（不计贩卖机、补付款）
DROP TEMPORARY TABLE IF EXISTS feods.sales_order_detail_shelf_temp;
CREATE TEMPORARY TABLE feods.sales_order_detail_shelf_temp AS 
SELECT
  t.order_id,
  t.weixin_pay_id,
  f.`SHELF_ID`,
  s.BUSINESS_name AS BUSINESS_AREA,
  t.pay_amount,
  SUM(m.`QUANTITY` * m.`SALE_PRICE`) AS GMV
FROM (SELECT
  t.order_id,
  t.weixin_pay_id,
  SUM(t.pay_amount) AS pay_amount
FROM feods.`d_mp_weixin_payment` t
WHERE t.shelf_type NOT IN ('自动贩卖机','')
AND t.weixin_pay_type IN ('微信支付','微信委托付款','WeiXinContractPay')
AND t.`business_type` IN ('货架实收(不包括智能货架和退款)','货架收款(智能货柜)'
,'货架退款(之前申请当日到账)','货架退款(当日到账)','自动售卖机退款中','货架收款(智能货柜9)'
,'智能柜首次支付失败现到账')
AND t.`sdate` >= @stadate_top
AND t.`sdate` < @stadate_end
GROUP BY t.`order_id`) t
LEFT JOIN 
fe.`sf_order` r
ON t.order_id = r.order_id
AND r.`DATA_FLAG`=1
-- AND r.pay_date >= @stadate_top
-- AND r.pay_date < @stadate_end
LEFT JOIN
fe.`sf_order_item` m
ON r.order_id = m.order_id
AND m.`DATA_FLAG`=1
LEFT JOIN 
fe.`sf_shelf` f
ON r.shelf_id = f.shelf_id
AND f.`DATA_FLAG`=1
LEFT JOIN
feods.`fjr_city_business` s
ON f.city = s.city
GROUP BY t.`order_id`;

-- 微信支付-自动贩卖机订单明细
DROP TEMPORARY TABLE IF EXISTS feods.sales_order_detail_autoshelf_temp;
CREATE TEMPORARY TABLE feods.sales_order_detail_autoshelf_temp AS 
SELECT
  t.order_id,
  t.weixin_pay_id,
  f.`SHELF_ID`,
  s.business_name,
  t.pay_amount as pay_amount,   -- 已经减去退款的实收
  SUM(IF(r.order_status = 6 and e.refund_amount is not null,m.quantity_shipped,m.`QUANTITY`) * m.`SALE_PRICE`) AS GMV
--   SUM(m.`QUANTITY` * m.`SALE_PRICE`) AS GMV
FROM (SELECT
  t.order_id,
  t.weixin_pay_id,
  SUM(t.pay_amount) AS pay_amount
FROM feods.`d_mp_weixin_payment` t
WHERE t.shelf_type IN ('自动贩卖机')
AND t.weixin_pay_type IN ('微信支付','微信委托付款','WeiXinContractPay')
AND t.`business_type` IN ('货架实收(不包括智能货架和退款)','货架收款(智能货柜)'
,'货架退款(之前申请当日到账)','货架退款(当日到账)','自动售卖机退款中','货架收款(智能货柜9)')
AND t.`sdate` >= @stadate_top
AND t.`sdate` < @stadate_end
GROUP BY t.`order_id`) t
LEFT JOIN 
fe.`sf_order` r
ON t.order_id = r.order_id
AND r.`DATA_FLAG`=1
-- AND r.pay_date >= @stadate_top
-- AND r.pay_date < @stadate_end
left join
fe.`sf_order_refund_order` e
on e.order_id = r.order_id
and e.refund_status = 5
LEFT JOIN
fe.`sf_order_item` m
ON r.order_id = m.order_id
AND m.`DATA_FLAG`=1
LEFT JOIN 
fe.`sf_shelf` f
ON r.shelf_id = f.shelf_id
AND f.`DATA_FLAG`=1
LEFT JOIN
feods.`fjr_city_business` s
ON f.city = s.city
GROUP BY t.`order_id`;

-- 微信支付-货架补付款订单明细
DROP TEMPORARY TABLE IF EXISTS feods.sales_order_detail_after_payment_temp;
CREATE TEMPORARY TABLE feods.sales_order_detail_after_payment_temp AS 
SELECT
  t.order_id,
  t.weixin_pay_id,
  f.`SHELF_ID`,
  s.business_name,
  t.pay_amount
FROM (SELECT
  t.order_id,
  t.weixin_pay_id,
  SUM(t.pay_amount) AS pay_amount
FROM feods.`d_mp_weixin_payment` t
WHERE t.weixin_pay_type IN ('微信支付','微信委托付款','WeiXinContractPay')
AND t.`business_type` IN ('货架补款')
AND t.`sdate` >= @stadate_top
AND t.`sdate` < @stadate_end
GROUP BY t.`order_id`) t
LEFT JOIN 
fe.sf_order_pay p
ON t.order_id = p.order_id
LEFT JOIN 
fe.`sf_shelf` f
ON p.shelf_id = f.shelf_id
AND f.`DATA_FLAG`=1
LEFT JOIN
feods.`fjr_city_business` s
ON f.city = s.city
WHERE p.pay_state = 2
AND p.pay_time >= @stadate_top
AND p.pay_time < @stadate_end
AND p.pay_type IN (1,8,21)
GROUP BY t.`order_id`;

-- 微信支付(无人货架、自动贩卖机、货架补付款)收入汇总
INSERT INTO feods.`D_MP_finance_month_income_result`(
sdate               
,business_type       
,shelf_type          
,BUSINESS_AREA           
,GMV                
,pay_amount
,third_amount)
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  '微信支付' AS business_type,
  '无人货架' AS shelf_type,
  t.`BUSINESS_AREA`,
  SUM(t.GMV) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  0 AS third_amount
FROM
  feods.sales_order_detail_shelf_temp t
GROUP BY t.BUSINESS_AREA
UNION ALL
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  '微信支付' AS business_type,
  '自动贩卖机' AS shelf_type,
  t.business_name as business_area,
  SUM(t.GMV) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  0 AS third_amount
FROM
  feods.sales_order_detail_autoshelf_temp t
GROUP BY t.business_name
UNION ALL
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  '微信支付-补付款' AS business_type,
  '无人货架' AS shelf_type,
  t.business_name as business_area,
  SUM(t.pay_amount) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  0 AS third_amount
FROM
  feods.sales_order_detail_after_payment_temp t
GROUP BY t.business_name;

-- 微信支付-早餐业务（在货架上）
-- DROP TEMPORARY TABLE IF EXISTS feods.sales_order_detail_breakfast_temp;
-- CREATE TEMPORARY TABLE feods.sales_order_detail_breakfast_temp AS 
-- SELECT
--   t.order_id,
--   t.weixin_pay_id,
--   f.`SHELF_ID`,
--   s.`BUSINESS_AREA`,
--   t.pay_amount,
--   SUM(m.`QUANTITY` * m.`SALE_PRICE`) AS GMV
-- FROM (SELECT
--   t.order_id,
--   t.weixin_pay_id,
--   SUM(t.pay_amount) AS pay_amount
-- FROM feods.`d_mp_weixin_payment` t
-- WHERE t.shelf_type NOT IN ('自动贩卖机','')
-- AND t.weixin_pay_type IN ('微信支付','微信委托付款','WeiXinContractPay')
-- AND t.`business_type` IN ('货架实收(不包括智能货架和退款)','货架收款(智能货柜)'
-- ,'货架退款(之前申请当日到账)','货架退款(当日到账)','自动售卖机退款中','货架收款(智能货柜9)')
-- AND t.`sdate` >= @stadate_top
-- AND t.`sdate` < @stadate_end
-- GROUP BY t.`order_id`) t
-- LEFT JOIN 
-- fe.`sf_order` r
-- ON t.order_id = r.order_id
-- AND r.`DATA_FLAG`=1
-- AND r.pay_date >= @stadate_top
-- AND r.pay_date < @stadate_end
-- LEFT JOIN
-- fe.`sf_order_item` m
-- ON r.order_id = m.order_id
-- AND m.`DATA_FLAG`=1
-- LEFT JOIN
-- fe.`sf_product` u
-- ON m.product_id = u.product_id
-- LEFT JOIN 
-- fe.`sf_shelf` f
-- ON r.shelf_id = f.shelf_id
-- AND f.`DATA_FLAG`=1
-- LEFT JOIN
-- fe.`zs_city_business` s
-- ON SUBSTRING_INDEX(SUBSTRING_INDEX(f.`AREA_ADDRESS`,',',2),',',-1)= s.`CITY_NAME`
-- WHERE u.product_code2 LIKE 'ZC%'
-- AND f.shelf_type = 4
-- GROUP BY t.`order_id`
-- ;
-- INSERT INTO feods.`D_MP_finance_month_income_result`(
-- sdate               
-- ,business_type       
-- ,shelf_type          
-- ,BUSINESS_AREA           
-- ,GMV                
-- ,pay_amount
-- ,third_amount)
-- SELECT
--   DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
--   '微信支付-早餐业务' AS business_type,
--   '无人货架' AS shelf_type,
--   t.`BUSINESS_AREA`,
--   SUM(t.GMV) AS GMV,
--   SUM(t.pay_amount) AS pay_amount,
--   0 as third_amount
-- FROM
--   feods.sales_order_detail_breakfast_temp t
-- GROUP BY t.BUSINESS_AREA;

-- 顺手付，建行龙支付 云闪付 升腾支付-无人货架订单明细（不计贩卖机、补付款）
DROP TEMPORARY TABLE IF EXISTS feods.sales_order_detail_shelf_ssf;
CREATE TEMPORARY TABLE feods.sales_order_detail_shelf_ssf AS 
SELECT
  t.order_id,
  t.ssf_pay_id,
  t.ssf_pay_type,
  f.`SHELF_ID`,
  s.business_name business_area,
  t.pay_amount,
  r.third_discount_amount,
  SUM(m.`QUANTITY` * m.`SALE_PRICE`) AS GMV
FROM (SELECT
  t.order_id,
  t.ssf_pay_id,
  t.ssf_pay_type,
  SUM(t.pay_amount) AS pay_amount
FROM feods.`d_mp_ssf_payment` t
WHERE t.shelf_type NOT IN ('自动贩卖机','')
-- AND t.weixin_pay_type IN ('微信支付','微信委托付款','WeiXinContractPay')
AND t.`business_type` IN ('货架实收(不包括智能货架和退款)')
AND t.`sdate` >= @stadate_top
AND t.`sdate` < @stadate_end
GROUP BY t.`order_id`) t
LEFT JOIN 
fe.`sf_order` r
ON t.order_id = r.order_id
AND r.`DATA_FLAG`=1
-- AND r.pay_date >= @stadate_top
-- AND r.pay_date < @stadate_end
LEFT JOIN
fe.`sf_order_item` m
ON r.order_id = m.order_id
AND m.`DATA_FLAG`=1
LEFT JOIN 
fe.`sf_shelf` f
ON r.shelf_id = f.shelf_id
AND f.`DATA_FLAG`=1
LEFT JOIN
feods.`fjr_city_business` s
ON s.city = f.city
GROUP BY t.`order_id`;

-- 顺手付，建行龙支付 云闪付 升腾支付 -自动贩卖机订单明细
DROP TEMPORARY TABLE IF EXISTS feods.sales_order_detail_autoshelf_ssf;
CREATE TEMPORARY TABLE feods.sales_order_detail_autoshelf_ssf AS 
SELECT
  t.order_id,
  t.ssf_pay_id,
  t.ssf_pay_type,
  f.`SHELF_ID`,
  s.business_name business_area,
  t.pay_amount-IFNULL(e.refund_amount,0) AS pay_amount,
  r.third_discount_amount,
  SUM(IF(r.order_status = 6 AND e.refund_amount IS NOT NULL,m.quantity_shipped,m.`QUANTITY`) * m.`SALE_PRICE`) AS GMV
--   SUM(m.`QUANTITY` * m.`SALE_PRICE`) AS GMV
FROM (SELECT
  t.order_id,
  t.ssf_pay_id,
  t.ssf_pay_type,
  SUM(t.pay_amount) AS pay_amount
FROM feods.`d_mp_ssf_payment` t
WHERE t.shelf_type IN ('自动贩卖机')
-- AND t.weixin_pay_type IN ('微信支付','微信委托付款','WeiXinContractPay')
AND t.`business_type` IN ('货架实收(不包括智能货架和退款)')
AND t.`sdate` >= @stadate_top
AND t.`sdate` < @stadate_end
GROUP BY t.`order_id`) t
LEFT JOIN 
fe.`sf_order` r
ON t.order_id = r.order_id
AND r.`DATA_FLAG`=1
-- AND r.pay_date >= @stadate_top
-- AND r.pay_date < @stadate_end
LEFT JOIN
fe.`sf_order_refund_order` e
ON e.order_id = r.order_id
AND e.refund_status = 5
LEFT JOIN
fe.`sf_order_item` m
ON r.order_id = m.order_id
AND m.`DATA_FLAG`=1
LEFT JOIN 
fe.`sf_shelf` f
ON r.shelf_id = f.shelf_id
AND f.`DATA_FLAG`=1
LEFT JOIN
feods.`fjr_city_business` s
ON f.city = s.CITY
GROUP BY t.`order_id`;

-- 顺手付，建行龙支付 云闪付 升腾支付的收入汇总（货架和自贩机）
INSERT INTO feods.`D_MP_finance_month_income_result`(
sdate               
,business_type       
,shelf_type          
,BUSINESS_AREA           
,GMV                
,pay_amount
,third_amount)
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  t.ssf_pay_type AS business_type,
  '无人货架' AS shelf_type,
  t.`BUSINESS_AREA`,
  SUM(t.GMV) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  ifnull(SUM(t.third_discount_amount),0) AS third_amount
FROM
  feods.sales_order_detail_shelf_ssf t
GROUP BY t.ssf_pay_type,t.BUSINESS_AREA
union
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  t.ssf_pay_type AS business_type,
  '自动贩卖机' AS shelf_type,
  t.`BUSINESS_AREA`,
  SUM(t.GMV) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  ifnull(SUM(t.third_discount_amount),0) AS third_amount
FROM
  feods.sales_order_detail_autoshelf_ssf t
GROUP BY t.ssf_pay_type,t.BUSINESS_AREA ;

 -- 招行支付的订单明细（货架和自贩机）
 DROP TEMPORARY TABLE IF EXISTS feods.CMBC_payment_temp;
 CREATE TEMPORARY TABLE feods.CMBC_payment_temp AS 
 SELECT
  t3.order_date,
  t3.pay_date,
  t2.order_id,
  t3.payment_type_name,
  t3.GATEWAY_ORDER_ID,
  t3.shelf_id,
  t6.business_name AS business_area,
  t2.zh_gmv,
  t3.zh_AMOUNT,
  t3.zh_third_AMOUNT,
  IF(t5.item_name IN ('自动贩卖机'),'自动贩卖机','无人货架') AS shelf_type
FROM
  
  (SELECT
    b.`ORDER_ID`,
  SUM(IF(b.order_status = 6 AND e.refund_amount IS NOT NULL,a.quantity_shipped,a.`QUANTITY`) * a.`SALE_PRICE`) AS zh_gmv
--     SUM(a.QUANTITY * a.SALE_PRICE) AS zh_gmv
  FROM
    fe.sf_order_item a
    LEFT JOIN fe.sf_order b
      ON a.`ORDER_ID` = b.`ORDER_ID`
    LEFT JOIN
    fe.`sf_order_refund_order` e
      ON e.order_id = b.order_id
      AND e.refund_status = 5
  WHERE b.pay_date >= @stadate_top
    AND b.pay_date < @stadate_end
    AND b.ORDER_STATUS IN (2, 6, 7)
    AND b.PAYMENT_TYPE_NAME = '招行一卡通'
  GROUP BY b.`ORDER_ID`) t2
  INNER JOIN
    (SELECT
      b.`ORDER_ID`,
      b.order_date,
      b.pay_date,
      b.payment_type_name,
      b.GATEWAY_ORDER_ID,
      b.shelf_id,
      SUM(b.PRODUCT_TOTAL_AMOUNT)-sum(IFNULL(e.refund_amount,0)) AS zh_AMOUNT,
      SUM(b.`third_discount_amount`) AS zh_third_AMOUNT
    FROM
      fe.sf_order b
     LEFT JOIN
      fe.`sf_order_refund_order` e
      ON e.order_id = b.order_id
      AND e.refund_status = 5
    WHERE b.pay_date >= @stadate_top
      AND b.pay_date < @stadate_end
      AND b.ORDER_STATUS IN (2, 6, 7)
      AND b.PAYMENT_TYPE_NAME = '招行一卡通'
    GROUP BY b.`ORDER_ID`) t3
    ON t2.order_id = t3.order_id
  INNER JOIN fe.`sf_shelf` t4
    ON t3.shelf_id = t4.shelf_id
  INNER JOIN feods.`fjr_city_business` t6
    ON t4.`CITY` = t6.`CITY`
  INNER JOIN
    (SELECT
      m.`ITEM_VALUE`,
      m.`ITEM_NAME`
    FROM
      fe.`pub_dictionary_item` m
    WHERE m.`DICTIONARY_ID` IN (8)) t5
    ON t4.shelf_type = t5.item_value;
    
-- 招行支付收入汇总（货架和自贩机）
INSERT INTO feods.`D_MP_finance_month_income_result`(
sdate               
,business_type       
,shelf_type          
,BUSINESS_AREA           
,GMV                
,pay_amount
,third_amount)
  SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  '招行支付' AS business_type,
   t.shelf_type,
   t.business_area,
   SUM(t.zh_gmv) AS '招行GMV',
   SUM(t.zh_AMOUNT) AS '招行实收',
   SUM(t.zh_third_AMOUNT) AS '招行优惠金额'
  FROM
   feods.CMBC_payment_temp t
  GROUP BY t.shelf_type,t.business_area;
  
-- e币支付的订单明细（货架和自贩机）
DROP TEMPORARY TABLE IF EXISTS feods.e_payment_shelf_detail;
CREATE TEMPORARY TABLE feods.e_payment_shelf_detail AS 
SELECT
 n.shelf_id,
 SUM(n.e_gmv) e_gmv,
 SUM(n.e_AMOUNT) e_AMOUNT,
--  t4.shelf_type,
 t5.item_name,
 IF(t4.shelf_type = 7,'自动贩卖机','无人货架') AS shelf_type
FROM
(SELECT
  t2.shelf_id,
  t2.e_gmv,
  t3.e_AMOUNT
FROM
  (SELECT
    b.`SHELF_ID`,
    SUM(IF(b.order_status = 6 AND e.refund_amount IS NOT NULL,a.quantity_shipped,a.`QUANTITY`) * a.`SALE_PRICE`) AS e_gmv
--     SUM(a.QUANTITY * a.SALE_PRICE) AS e_gmv
  FROM
    fe.sf_order_item a
    LEFT JOIN fe.sf_order b
      ON a.`ORDER_ID` = b.`ORDER_ID`
     LEFT JOIN
      fe.`sf_order_refund_order` e
      ON e.order_id = b.order_id
      AND e.refund_status = 5
  WHERE b.pay_date >= @stadate_top   
    AND b.pay_date < @stadate_end
    AND b.ORDER_STATUS IN (2, 6, 7)
    AND b.PAYMENT_TYPE_NAME = 'E币支付'
  GROUP BY b.`SHELF_ID`) t2
  INNER JOIN
    (SELECT
      b.shelf_id,
      SUM(b.PRODUCT_TOTAL_AMOUNT)-SUM(IFNULL(e.refund_amount,0)) AS e_AMOUNT
    FROM
      fe.sf_order b
     LEFT JOIN
      fe.`sf_order_refund_order` e
      ON e.order_id = b.order_id
      AND e.refund_status = 5 
    WHERE b.pay_date >= @stadate_top
      AND b.pay_date < @stadate_end
      AND b.ORDER_STATUS IN (2, 6, 7)
      AND b.PAYMENT_TYPE_NAME = 'E币支付'
    GROUP BY b.`SHELF_ID`) t3
    ON t2.shelf_id = t3.shelf_id 
) n
  INNER JOIN fe.`sf_shelf` t4
    ON n.shelf_id = t4.shelf_id
  INNER JOIN
    (SELECT
      m.`ITEM_VALUE`,
      m.`ITEM_NAME`
    FROM
      fe.`pub_dictionary_item` m
    WHERE m.`DICTIONARY_ID` IN (8)) t5
    ON t4.shelf_type = t5.item_value
GROUP BY n.shelf_id;

-- e币支付的收入汇总（货架、自贩机、货架补付款）
INSERT INTO feods.`D_MP_finance_month_income_result`(
sdate               
,business_type       
,shelf_type          
,BUSINESS_AREA           
,GMV                
,pay_amount
,third_amount)
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  'e币支付' AS business_type,
 t.shelf_type as shelf_type,
 s.`business_name` AS business_area,
 SUM(t.e_gmv) AS shelf_e_gmv,
 SUM(t.e_AMOUNT) AS shelf_e_AMOUNT,
 0 as third_amount
FROM
  feods.e_payment_shelf_detail t,
  fe.`sf_shelf` f,
  feods.`fjr_city_business` s
WHERE t.shelf_id = f.`SHELF_ID`
AND f.`CITY` = s.`CITY`
AND f.`DATA_FLAG` =1
GROUP BY t.shelf_type,s.`business_name`
union
  SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  'e币支付-补付款' AS business_type,
 '无人货架' AS shelf_type,
  s.`business_name` AS business_area,
  SUM(p.PAYMENT_MONEY) AS e_gmv,
  SUM(p.PAYMENT_MONEY) AS e_AMOUNT,
  0 as third_amount
FROM
  fe.sf_after_payment p,
  fe.`sf_shelf` f,
  feods.`fjr_city_business` s
WHERE p.`SHELF_ID` = f.`SHELF_ID`
  AND f.`CITY` = s.`CITY` 
  AND p.PAYMENT_STATUS = 2
  AND p.`PAY_DATE` >= @stadate_top AND p.`PAY_DATE` < @stadate_end
  AND p.PAYMENT_TYPE_NAME = 'EPay'
  AND f.`DATA_FLAG` = 1
  GROUP BY s.`business_name`;
  
-- 企业代购、餐卡支付、小蜜丰积分支付、中国移动和包支付-货架订单明细（未经第三方对账）
DROP TEMPORARY TABLE IF EXISTS feods.other_payment_type_detail;
CREATE TEMPORARY TABLE feods.other_payment_type_detail(key idx_order(order_id),key idx_shelf(shelf_id)) AS 
SELECT
  b.`ORDER_ID`,
  b.PAYMENT_TYPE_NAME,
  b.`SHELF_ID`,
  SUM(a.QUANTITY * a.SALE_PRICE) AS gmv,
  b.PRODUCT_TOTAL_AMOUNT AS pay_amount,
  b.`third_discount_amount` AS third_amount
FROM
  fe.sf_order_item a
  JOIN
  fe.sf_order b
  ON a.`ORDER_ID`= b.`ORDER_ID`
WHERE b.`PAY_DATE` >= @stadate_top
  AND b.`PAY_DATE` <  @stadate_end
  AND b.`DATA_FLAG` = 1
  AND a.`DATA_FLAG` = 1
  AND b.ORDER_STATUS = 2
  AND b.`PAYMENT_TYPE_NAME` IN ('企业代扣','餐卡支付','小蜜蜂积分支付','中国移动和包支付')
GROUP BY b.`ORDER_ID`;

-- 企业代购、餐卡支付、小蜜丰积分支付、中国移动和包支付-自贩机订单明细（未经第三方对账）
DROP TEMPORARY TABLE IF EXISTS feods.other_payment_auto_detail;
CREATE TEMPORARY TABLE feods.other_payment_auto_detail(KEY idx_order(order_id),KEY idx_shelf(shelf_id)) AS 
SELECT
  b.`ORDER_ID`,
  b.PAYMENT_TYPE_NAME,
  b.`SHELF_ID`,  
  SUM(IF(c.refund_amount IS NOT NULL AND b.ORDER_STATUS=6,a.`quantity_shipped`,a.quantity) * a.SALE_PRICE) AS gmv,
  b.PRODUCT_TOTAL_AMOUNT - IFNULL(c.refund_amount, 0) AS pay_amount,
  b.`third_discount_amount` AS third_amount
    FROM
      fe.sf_order_item a
      JOIN fe.sf_order b
        ON a.order_id = b.order_id
      LEFT JOIN fe.sf_order_refund_order c
	ON b.ORDER_ID = c.order_id
	AND c.refund_status = 5
	AND c.`data_flag`= 1
    WHERE b.PAY_DATE >= @stadate_top
      AND b.PAY_DATE < @stadate_end
      AND a.`DATA_FLAG` = 1
      AND b.`DATA_FLAG`= 1
      AND b.ORDER_STATUS IN (6, 7)
      AND b.`PAYMENT_TYPE_NAME` IN ('企业代扣','餐卡支付','小蜜蜂积分支付','中国移动和包支付')
    GROUP BY b.`ORDER_ID`;

-- 企业代购、餐卡支付、小蜜丰积分支付、中国移动和包支付的收入汇总（货架和自贩机）
INSERT INTO feods.`D_MP_finance_month_income_result`(
 sdate                     
,business_type             
,shelf_type                
,BUSINESS_AREA                 
,GMV                       
,pay_amount                
,third_amount)
 SELECT
   DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
   CASE WHEN t1.PAYMENT_TYPE_NAME = '企业代扣' AND aa.`group_name`= '南昌黑鲨科技有限公司'
        THEN '企业代扣-黑鲨'
        WHEN t1.PAYMENT_TYPE_NAME = '企业代扣' AND aa.`group_name`= '湖北亿咖通科技有限公司'
        THEN '企业代扣-亿咖通'
        WHEN t1.PAYMENT_TYPE_NAME = '企业代扣' AND aa.`group_name`= '上海众链科技有限公司'
        THEN '企业代扣-众链'
        ELSE t1.PAYMENT_TYPE_NAME
   END AS business_type,
   '无人货架' AS shelf_type,
   t3.`business_name`,
   SUM(IFNULL(t1.gmv,0)) AS gmv,
   SUM(IFNULL(t1.pay_amount,0)) AS pay_amount,
   SUM(IFNULL(t1.third_amount,0)) AS third_amount
 FROM
  feods.other_payment_type_detail t1
 JOIN fe.`sf_shelf` t2
   ON t1.shelf_id = t2.`SHELF_ID`
 JOIN feods.`fjr_city_business` t3
   ON t2.`CITY` = t3.`CITY`
  LEFT JOIN
  fe_group.sf_group_wallet_log_business g
  ON t1.`ORDER_ID`= g.`business_id`
  AND g.`data_flag`= 1
  AND g.business_type = 4
  LEFT JOIN
  fe_group.sf_group_emp e
  ON g.`add_user_id`= e.`emp_user_id`
  AND e.`data_flag` = 1
  LEFT JOIN
  fe_group.sf_group_customer aa
  ON e.`group_customer_id`= aa.`group_customer_id`
  AND aa.`data_flag`= 1
 GROUP BY t3.`business_name`,t1.PAYMENT_TYPE_NAME,aa.`group_name`
 UNION ALL 
 SELECT
   DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
   CASE WHEN t1.PAYMENT_TYPE_NAME = '企业代扣' AND aa.`group_name`= '南昌黑鲨科技有限公司'
        THEN '企业代扣-黑鲨'
        WHEN t1.PAYMENT_TYPE_NAME = '企业代扣' AND aa.`group_name`= '湖北亿咖通科技有限公司'
        THEN '企业代扣-亿咖通'
        WHEN t1.PAYMENT_TYPE_NAME = '企业代扣' AND aa.`group_name`= '上海众链科技有限公司'
        THEN '企业代扣-众链'
        ELSE t1.PAYMENT_TYPE_NAME
   END AS business_type,
   '自动贩卖机' AS shelf_type,
   t3.`business_name`,
   SUM(IFNULL(t1.gmv,0)) AS gmv,
   SUM(IFNULL(t1.pay_amount,0)) AS pay_amount,
   SUM(IFNULL(t1.third_amount,0)) AS third_amount
 FROM
  feods.other_payment_auto_detail t1
 JOIN fe.`sf_shelf` t2
   ON t1.shelf_id = t2.`SHELF_ID`
 JOIN feods.`fjr_city_business` t3
   ON t2.`CITY` = t3.`CITY`
  LEFT JOIN
  fe_group.sf_group_wallet_log_business g
  ON t1.`ORDER_ID`= g.`business_id`
  AND g.`data_flag`= 1
  AND g.business_type = 4
  LEFT JOIN
  fe_group.sf_group_emp e
  ON g.`add_user_id`= e.`emp_user_id`
  AND e.`data_flag` = 1
  LEFT JOIN
  fe_group.sf_group_customer aa
  ON e.`group_customer_id`= aa.`group_customer_id`
  AND aa.`data_flag`= 1
 GROUP BY t3.`business_name`,t1.PAYMENT_TYPE_NAME,aa.`group_name`;
  
  CALL sh_process.`sp_sf_dw_task_log` (
    'sp_d_mp_finance_data_fetch_task',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
commit;
end