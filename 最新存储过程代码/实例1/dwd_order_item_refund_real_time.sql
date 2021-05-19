CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_order_item_refund_real_time`()
BEGIN
-- =============================================
-- Author:	每半小时实时订单增量表
-- Create date: 2020/03/28
-- Modify date: 
-- Description:	
-- 	用于半小时增量
-- 
-- =============================================
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
-- 每天8点更新零点到八点的数据。8点到晚上11点半，每隔半小时跑增量的数据。 
if CURTIME()>'08:00:00'
then 
CASE 
WHEN CURTIME()<='08:10'
THEN
SET @start_date = DATE_FORMAT(CURDATE(),'%Y-%m-%d %H:%i:%s')
,@end_date = DATE_FORMAT(NOW(),'%Y-%m-%d %H:00:00');
WHEN  TIMESTAMPDIFF(MINUTE,DATE_FORMAT(NOW(),'%Y-%m-%d %H:00:00'),NOW())<30
THEN 
SET @start_date = DATE_ADD(DATE_FORMAT(NOW(),'%Y-%m-%d %H:00:00'),INTERVAL -30 MINUTE)
,@end_date = DATE_FORMAT(NOW(),'%Y-%m-%d %H:00:00');
ELSE 
SET @start_date = DATE_FORMAT(NOW(),'%Y-%m-%d %H:00:00')
,@end_date = DATE_ADD(DATE_FORMAT(NOW(),'%Y-%m-%d %H:00:00'),INTERVAL 30 MINUTE);
END CASE;
  
-- 增量更新半小时的数据
TRUNCATE TABLE fe_dwd.dwd_order_item_refund_real_time ;  
--  以支付成功时间为准
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_real_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_real_tmp_1 AS
SELECT DISTINCT
c.order_id,
c.order_type,
c.pay_id,
c.PAY_TYPE,
c.PAY_TIME,
c.PAY_AMOUNT,
c.PAY_STATE,
c.pay_method,
c.pay_merchant_id,
c.shelf_id,
c.third_discount_amount
FROM fe.sf_order_pay c 
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND c.PAY_TIME >= @start_date
AND c.PAY_TIME < @end_date
UNION 
SELECT DISTINCT
c.order_id,
c.order_type,
c.pay_id,
c.PAY_TYPE,
c.PAY_TIME,
c.PAY_AMOUNT,
c.PAY_STATE,
c.pay_method,
c.pay_merchant_id,
c.shelf_id,
c.third_discount_amount
FROM fe_pay.sf_order_pay_1 c 
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND c.PAY_TIME >= @start_date
AND c.PAY_TIME < @end_date
UNION 
SELECT DISTINCT
c.order_id,
c.order_type,
c.pay_id,
c.PAY_TYPE,
c.PAY_TIME,
c.PAY_AMOUNT,
c.PAY_STATE,
c.pay_method,
c.pay_merchant_id,
c.shelf_id,
c.third_discount_amount
FROM fe_pay.sf_order_pay_2 c 
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND c.PAY_TIME >= @start_date
AND c.PAY_TIME < @end_date
UNION 
SELECT DISTINCT
c.order_id,
c.order_type,
c.pay_id,
c.PAY_TYPE,
c.PAY_TIME,
c.PAY_AMOUNT,
c.PAY_STATE,
c.pay_method,
c.pay_merchant_id,
c.shelf_id,
c.third_discount_amount
FROM fe_pay.sf_order_pay_3 c 
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND c.PAY_TIME >= @start_date
AND c.PAY_TIME < @end_date;
CREATE INDEX idx_dwd_lsl_order_item_1_1
ON fe_dwd.dwd_lsl_order_item_real_tmp_1 (order_id);
-- 考虑一下是否需要添加限定时间
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_real_tmp_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_real_tmp_2 AS
SELECT DISTINCT
a.order_id,
c.pay_id,
a.order_status,
a.order_date,
a.PAY_DATE,
a.order_type,
a.user_id,
a.shelf_id,
a.COUPON_AMOUNT,
a.INTEGRAL_DISCOUNT,
a.PRODUCT_TOTAL_AMOUNT,
a.COMMIS_TOTAL_AMOUNT,
b.order_item_id,
b.product_id,
b.LIMIT_BUY_ID,
b.quantity,
b.sale_price,
    IFNULL(
      b.purchase_price,
      b.cost_price
    ) purchase_price,	
a.discount_amount AS o_discount_amount,    
b.discount_amount,
b.REAL_TOTAL_PRICE,
b.quantity_shipped,
b.cost_price ,
b.SUPPLIER_ID,
a.payment_type_gateway
FROM fe_dwd.dwd_lsl_order_item_real_tmp_1 c 
JOIN fe.sf_order_item b 
ON c.order_id = b.order_id
AND b.data_flag = 1
JOIN fe.sf_order a
ON a.order_id = c.order_id
AND a.data_flag = 1 ;
CREATE INDEX idx_dwd_lsl_order_item_2_1
ON fe_dwd.dwd_lsl_order_item_real_tmp_2 (order_id,pay_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_real_tmp_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_real_tmp_3 AS
SELECT 
b.order_id,
b.ORDER_ITEM_ID,
b.order_status,
b.order_type,
b.order_date,
a.pay_id,
IFNULL(a.PAY_TIME,b.PAY_DATE) PAY_DATE,
b.PAYMENT_TYPE_GATEWAY,
a.PAY_TYPE,
a.PAY_AMOUNT,
a.PAY_STATE,
a.pay_method,
a.pay_merchant_id,
b.user_id,
a.shelf_id,
b.product_id,
b.SUPPLIER_ID,
b.LIMIT_BUY_ID,
b.quantity,
b.quantity_shipped,
b.COST_PRICE,
IF(
  b.order_status = 6,
  b.quantity_shipped,
  b.quantity
) quantity_act,
b.sale_price,
b.purchase_price,
b.o_discount_amount,
b.discount_amount,
b.REAL_TOTAL_PRICE,
b.COUPON_AMOUNT,
b.INTEGRAL_DISCOUNT,
a.third_discount_amount,
b.PRODUCT_TOTAL_AMOUNT,
b.COMMIS_TOTAL_AMOUNT
FROM fe_dwd.dwd_lsl_order_item_real_tmp_1  a
JOIN fe_dwd.dwd_lsl_order_item_real_tmp_2  b
ON a.order_id = b.order_id
AND a.pay_id = b.pay_id;
-- 全量更新最新添加退款的表，这个表中是货架相关的退款信息，包含了fe_pay中的退款信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_real_tmp_4;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_real_tmp_4 AS
SELECT b.order_id ,
c.order_item_id,
b.refund_order_id,
b.apply_time,	
c.refund_amount ,  -- 取明细里的退款金额。
-- b.refund_status,
b.finish_time
FROM 
fe.sf_order_refund_item c 
JOIN  fe.sf_order_refund_order b 
ON  c.order_id = b.order_id 
AND c.refund_order_id = b.refund_order_id
AND b.data_flag = 1
AND c.data_flag = 1
AND b.refund_status = 5  -- 退款成功
;
CREATE INDEX dwd_lsl_order_item_tmp_4
ON fe_dwd.dwd_lsl_order_item_real_tmp_4 (order_id,order_item_id);
-- 更新一下明细里退款金额为0的  4月7号开发发版后解决。之后可以注释掉
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_real_tmp_4_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_real_tmp_4_1 AS
SELECT DISTINCT  order_id 
FROM fe_dwd.dwd_lsl_order_item_real_tmp_4 
WHERE refund_amount =0;
CREATE INDEX idx_dwd_lsl_order_item_tmp_4_1
ON fe_dwd.dwd_lsl_order_item_real_tmp_4_1 (order_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_real_tmp_4_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_real_tmp_4_2 AS
SELECT
a.order_item_id,
	a.QUANTITY AS quantity_item,
	b.QUANTITY,
	b.REAL_TOTAL_PRICE
FROM
fe.sf_order_refund_item a
JOIN fe_dwd.dwd_lsl_order_item_real_tmp_4_1 c
ON a.order_id = c.order_id
JOIN fe.sf_order_item b 
ON a.order_item_id = b.ORDER_ITEM_ID;
CREATE INDEX idx_dwd_lsl_order_item_tmp_4_2
ON fe_dwd.dwd_lsl_order_item_real_tmp_4_2 (order_item_id);
-- 更新一下临时表的退款金额
UPDATE fe_dwd.dwd_lsl_order_item_real_tmp_4 AS b
JOIN fe_dwd.dwd_lsl_order_item_real_tmp_4_2 AS a 
ON a.order_item_id = b.order_item_id
SET
b.refund_amount = a.quantity_item*a.REAL_TOTAL_PRICE/a.QUANTITY;
-- 添加家荣宽表的字段
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_replace_op_order_and_item_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_replace_op_order_and_item_1 AS
SELECT 
a.order_id,
SUM(a.`quantity_act`*a.`sale_price`) AS ogmv
FROM fe_dwd.dwd_lsl_order_item_real_tmp_3 a
GROUP BY 
a.order_id;
CREATE INDEX idx_dwd_replace_op_order_and_item_1
ON fe_dwd.dwd_replace_op_order_and_item_1  (order_id);
	
 INSERT INTO fe_dwd.dwd_order_item_refund_real_time
(
order_id,
ORDER_ITEM_ID,
order_status,
order_type,
order_date,
pay_id,
PAY_DATE,
PAYMENT_TYPE_GATEWAY,
PAY_TYPE,
PAY_AMOUNT,
PAY_STATE,
pay_method,
pay_merchant_id,
user_id,
shelf_id,
product_id,
PRODUCT_CODE2,
PRODUCT_NAME,
SUPPLIER_ID,
LIMIT_BUY_ID,
quantity,
quantity_shipped,
COST_PRICE,
quantity_act,
sale_price,
purchase_price,
discount_amount,
REAL_TOTAL_PRICE,
COUPON_AMOUNT,
INTEGRAL_DISCOUNT,
third_discount_amount,
PRODUCT_TOTAL_AMOUNT,
COMMIS_TOTAL_AMOUNT,
  refund_order_id,
  refund_amount,
  refund_finish_time,
  apply_time,
ogmv,
o_product_total_amount,
o_discount_amount,
o_coupon_amount,
o_third_discount_amount
)
SELECT 
t1.order_id,
t1.ORDER_ITEM_ID,
t1.order_status,
t1.order_type,
t1.order_date,
t1.pay_id,
t1.PAY_DATE,
t1.PAYMENT_TYPE_GATEWAY,
t1.PAY_TYPE,
t1.PAY_AMOUNT,
t1.PAY_STATE,
t1.pay_method,
t1.pay_merchant_id,
t1.user_id,
t1.shelf_id,
t1.product_id,
t2.PRODUCT_CODE2,
t2.PRODUCT_NAME,
t1.SUPPLIER_ID,
t1.LIMIT_BUY_ID,
t1.quantity,
t1.quantity_shipped,
t1.COST_PRICE,
t1.quantity_act,
t1.sale_price,
t1.purchase_price,
t1.discount_amount,
t1.REAL_TOTAL_PRICE,
t1.COUPON_AMOUNT,
t1.INTEGRAL_DISCOUNT,
t1.third_discount_amount,
t1.PRODUCT_TOTAL_AMOUNT,
t1.COMMIS_TOTAL_AMOUNT,
  t4.refund_order_id,
  t4.refund_amount,
  t4.finish_time AS refund_finish_time,
  t4.apply_time,
t3.ogmv,
t1.PRODUCT_TOTAL_AMOUNT AS o_product_total_amount,
t1.o_discount_amount,
t1.COUPON_AMOUNT AS o_coupon_amount,
t1.third_discount_amount AS o_third_discount_amount
  FROM
    fe_dwd.`dwd_lsl_order_item_real_tmp_3` t1
	LEFT JOIN 
	fe_dwd.dwd_product_base_day_all t2 
	ON t1.product_id = t2.PRODUCT_ID
	LEFT JOIN fe_dwd.dwd_replace_op_order_and_item_1 t3
	ON t1.order_id = t3.order_id 
	LEFT JOIN
	fe_dwd.dwd_lsl_order_item_real_tmp_4 t4
	ON t1.order_id = t4.order_id
AND t1.order_item_id = t4.order_item_id
	;
    
end if ;
-- 执行记录日志
 CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_order_item_refund_real_time',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
 
END