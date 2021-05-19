CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_mp_finance_month_income_result`(IN date_in DATE)
BEGIN
  SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
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
SET @stadate_top:= DATE_ADD(DATE_SUB(date_in,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(date_in,INTERVAL 1 DAY))+1 DAY);
SET @stadate_end:= date_in;
DELETE FROM fe_dm.dm_mp_finance_month_income_result WHERE sdate = DATE_FORMAT(@stadate_top,'%Y%m');
-- 微信支付-无人货架、自动贩卖机订单明细（不计补付款）
DROP TEMPORARY TABLE IF EXISTS fe_dm.sales_order_detail_wechat_temp;
CREATE TEMPORARY TABLE fe_dm.sales_order_detail_wechat_temp AS
SELECT
  t.order_id,
  t.pay_id,
  f.`SHELF_ID`,
  IF(f.shelf_type = 7,'自动贩卖机','无人货架') AS shelf_type,
  f.business_name AS BUSINESS_AREA,
  t.`PAY_AMOUNT`* COUNT(DISTINCT t.pay_id)-SUM(IFNULL(t.refund_amount,0)) AS pay_amount,
  SUM(IF(t.refund_amount>0,t.quantity_act,t.`QUANTITY`) * t.`SALE_PRICE`) AS GMV
FROM
 fe_dwd.`dwd_order_item_refund_day` t
JOIN 
 fe_dwd.dwd_shelf_base_day_all f
ON t.shelf_id = f.shelf_id
WHERE t.pay_date >= @stadate_top
AND t.pay_date < @stadate_end
AND t.pay_type IN (1,8)
GROUP BY t.`order_id`;
-- 微信支付(无人货架、自动贩卖机、货架补付款)收入汇总
INSERT INTO fe_dm.dm_mp_finance_month_income_result(
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
  t.shelf_type,
  t.business_area,
  SUM(t.GMV) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  0 AS third_amount
FROM
  fe_dm.sales_order_detail_wechat_temp t
GROUP BY t.business_area,t.shelf_type;
-- 顺手付，建行龙支付 云闪付 升腾支付、招行支付、E币支付-无人货架、自动贩卖机订单明细（不计补付款）
DROP TEMPORARY TABLE IF EXISTS fe_dm.sales_order_detail_ssf;
CREATE TEMPORARY TABLE fe_dm.sales_order_detail_ssf AS
SELECT
  t.order_id,
  t.pay_id,
  CASE WHEN t.pay_type = 2
       THEN '顺手付微信支付'
       WHEN t.pay_type = 3
       THEN '顺手付龙支付'
       WHEN t.pay_type = 4
       THEN 'E币支付'
       WHEN t.pay_type = 6
       THEN '顺手付云闪付'
       WHEN t.pay_type = 7
       THEN '招行一卡通'
       WHEN t.pay_type = 10
       THEN '顺手付一码付'
       WHEN t.pay_type = 13
       THEN '升腾支付'
  END AS pay_type,
  f.`SHELF_ID`,
  IF(f.shelf_type = 7,'自动贩卖机','无人货架') AS shelf_type,
  f.business_name AS BUSINESS_AREA,
  t.`PAY_AMOUNT`* COUNT(DISTINCT t.pay_id)-SUM(IFNULL(t.refund_amount,0)) AS pay_amount,
  SUM(IF(t.refund_amount>0,t.quantity_act,t.`QUANTITY`) * t.`SALE_PRICE`) AS GMV,
  IFNULL(t.third_discount_amount,0) AS third_discount_amount
FROM
 fe_dwd.`dwd_order_item_refund_day` t
JOIN 
 fe_dwd.dwd_shelf_base_day_all f
ON t.shelf_id = f.shelf_id
WHERE t.pay_date >= @stadate_top
AND t.pay_date < @stadate_end
AND t.pay_type IN (2,3,4,6,7,10,13)
GROUP BY t.`order_id`;
-- 顺手付、建行龙支付、云闪付、升腾支付、招行支付、E币支付的收入汇总（货架和自贩机）
INSERT INTO fe_dm.dm_mp_finance_month_income_result(
sdate               
,business_type       
,shelf_type          
,BUSINESS_AREA           
,GMV                
,pay_amount
,third_amount)
SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  t.pay_type AS business_type,
  t.shelf_type,
  t.`BUSINESS_AREA`,
  SUM(t.GMV) AS GMV,
  SUM(t.pay_amount) AS pay_amount,
  SUM(t.third_discount_amount) AS third_amount
FROM
  fe_dm.sales_order_detail_ssf t
GROUP BY t.pay_type ,t.BUSINESS_AREA ,t.shelf_type;   
-- 企业代购、餐卡支付、小蜜丰积分支付、中国移动和包支付-无人货架、自贩机订单明细（未经第三方对账）
DROP TEMPORARY TABLE IF EXISTS fe_dm.other_payment_detail;
CREATE TEMPORARY TABLE fe_dm.other_payment_detail(KEY idx_order(order_id),KEY idx_shelf(shelf_id)) AS
SELECT
  t.order_id,
  t.pay_id,
  CASE WHEN t.pay_type = 9
       THEN '餐卡支付'
       WHEN t.pay_type = 11
       THEN '企业代扣'
       WHEN t.pay_type = 12
       THEN '小蜜蜂积分支付'
       WHEN t.pay_type = 15
       THEN '中国移动和包支付'
  END AS pay_type,
  f.`SHELF_ID`,
  IF(f.shelf_type = 7,'自动贩卖机','无人货架') AS shelf_type,
  f.business_name AS BUSINESS_AREA,
  t.`PAY_AMOUNT`* COUNT(DISTINCT t.pay_id)-SUM(IFNULL(t.refund_amount,0)) AS pay_amount,
  SUM(IF(t.refund_amount>0,t.quantity_act,t.`QUANTITY`) * t.`SALE_PRICE`) AS GMV,
  IFNULL(t.third_discount_amount,0) AS third_discount_amount
FROM
 fe_dwd.`dwd_order_item_refund_day` t
JOIN 
 fe_dwd.dwd_shelf_base_day_all f
ON t.shelf_id = f.shelf_id
WHERE t.pay_date >= @stadate_top
AND t.pay_date < @stadate_end
AND t.pay_type IN (9,11,12,15)
GROUP BY t.`order_id`;
-- 企业代购、餐卡支付、小蜜丰积分支付、中国移动和包支付的收入汇总（货架和自贩机）
INSERT INTO fe_dm.dm_mp_finance_month_income_result(
 sdate                     
,business_type             
,shelf_type                
,BUSINESS_AREA                 
,GMV                       
,pay_amount                
,third_amount)
 SELECT
   DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
   CASE WHEN t1.pay_type = '企业代扣' AND e.`group_name`= '南昌黑鲨科技有限公司'
        THEN '企业代扣-黑鲨'
        WHEN t1.pay_type = '企业代扣' AND e.`group_name`= '湖北亿咖通科技有限公司'
        THEN '企业代扣-亿咖通'
        WHEN t1.pay_type = '企业代扣' AND e.`group_name`= '上海众链科技有限公司'
        THEN '企业代扣-众链'
        ELSE t1.pay_type
   END AS business_type,
   t1.shelf_type,
   t1.business_area,
   SUM(IFNULL(t1.GMV,0)) AS gmv,
   SUM(IFNULL(t1.pay_amount,0)) AS pay_amount,
   SUM(t1.third_discount_amount) AS third_amount
 FROM
  fe_dm.other_payment_detail t1
  LEFT JOIN
  fe_dwd.dwd_group_wallet_log_business g
  ON t1.`ORDER_ID`= g.`business_id`
  AND g.business_type = 4
  LEFT JOIN
  fe_dwd.dwd_group_emp_user_day e
  ON g.`add_user_id`= e.`emp_user_id`
  AND e.`data_flag` = 1
 GROUP BY t1.business_area,
 t1.shelf_type,
    CASE WHEN t1.pay_type = '企业代扣' AND e.`group_name`= '南昌黑鲨科技有限公司'
        THEN '企业代扣-黑鲨'
        WHEN t1.pay_type = '企业代扣' AND e.`group_name`= '湖北亿咖通科技有限公司'
        THEN '企业代扣-亿咖通'
        WHEN t1.pay_type = '企业代扣' AND e.`group_name`= '上海众链科技有限公司'
        THEN '企业代扣-众链'
        ELSE t1.pay_type
   END
 ;
 
-- 微信支付、E币支付-无人货架补付款收入汇总
INSERT INTO fe_dm.dm_mp_finance_month_income_result(
sdate               
,business_type       
,shelf_type          
,BUSINESS_AREA           
,GMV                
,pay_amount
,third_amount)
  SELECT
  DATE_FORMAT(@stadate_top,'%Y%m') AS sdate,
  CASE WHEN p.PAYMENT_TYPE_NAME IN ('wx','WeiXinPayJSAPI')
       THEN '微信支付-补付款'
       WHEN p.PAYMENT_TYPE_NAME IN ('E币支付','EPay')
       THEN 'e币支付-补付款'
       END AS business_type,
  '无人货架' AS shelf_type,
  f.`business_name` AS business_area,
  SUM(p.PAYMENT_MONEY) AS gmv,
  SUM(p.PAYMENT_MONEY) AS AMOUNT,
  0 AS third_amount
FROM
    fe_dwd.`dwd_sf_after_payment` p
  JOIN
    fe_dwd.dwd_shelf_base_day_all f
    ON p.`SHELF_ID` = f.`SHELF_ID`
WHERE p.PAYMENT_STATUS = 2
  AND p.`PAY_DATE` >= @stadate_top AND p.`PAY_DATE` < @stadate_end
  GROUP BY f.`business_name`,
    CASE WHEN p.PAYMENT_TYPE_NAME IN ('wx','WeiXinPayJSAPI')
       THEN '微信支付-补付款'
       WHEN p.PAYMENT_TYPE_NAME IN ('E币支付','EPay')
       THEN 'e币支付-补付款'
       END;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_mp_finance_month_income_result',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('蔡松林@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_mp_finance_month_income_result','dm_mp_finance_month_income_result','蔡松林');
END