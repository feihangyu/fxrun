CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_order_item_refund_day_inc`()
BEGIN
   SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_3_date = SUBDATE(@end_date,INTERVAL 90 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @timestamp := CURRENT_TIMESTAMP();
    
-- 增量表
DELETE FROM fe_dwd.dwd_order_item_refund_day WHERE PAY_DATE >= @start_date;  
## 需要同步更新dwd_update_dwd_table_info 里面的脚本
--  以支付成功时间为准
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1_0;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1_0 AS
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

CREATE INDEX idx_dwd_lsl_order_item_1_0
ON fe_dwd.dwd_lsl_order_item_tmp_1_0 (order_id);


-- 组合支付的 先只取支付成功的
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_combin_order_item_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_combin_order_item_tmp_1 AS
SELECT a.requirement_id,a.order_id,a.pay_amount ,a.pay_state
FROM fe_pay.`sf_pay_requirement` a 
WHERE a.requirement_type = 'goodsOrder'
AND a.data_flag =1
AND a.pay_state =2 
;

CREATE INDEX dwd_lsl_combin_order_item_tmp_1
ON fe_dwd.dwd_lsl_combin_order_item_tmp_1 (requirement_id);

DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_combin_order_item_tmp_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_combin_order_item_tmp_2 AS
SELECT DISTINCT  -- 把支付成功的订单号转一下。支付失败的不考虑
b.order_id,  
c.order_type,
c.pay_id,
c.PAY_TYPE,
c.PAY_TIME,
b.PAY_AMOUNT,  -- 支付金额是全部的
c.PAY_STATE,
c.pay_method,
c.pay_merchant_id,
c.shelf_id,
c.third_discount_amount
FROM fe_dwd.dwd_lsl_order_item_tmp_1_0 c 
JOIN  fe_dwd.dwd_lsl_combin_order_item_tmp_1 b
ON b.requirement_id = c.order_id
AND c.PAY_TYPE =1 -- 取微信支付的那一条信息 e币或企业代付的那条不取
;


DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1 AS
SELECT * FROM fe_dwd.dwd_lsl_combin_order_item_tmp_2
union all
select * from fe_dwd.dwd_lsl_order_item_tmp_1_0
;

CREATE INDEX idx_dwd_lsl_order_item_tmp_1
ON fe_dwd.dwd_lsl_order_item_tmp_1 (order_id);

-- 考虑一下是否需要添加限定时间
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_2 AS
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
c.third_discount_amount,
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
FROM fe_dwd.dwd_lsl_order_item_tmp_1 c 
JOIN fe.sf_order_item b 
ON c.order_id = b.order_id
AND b.data_flag = 1
JOIN fe.sf_order a
ON a.order_id = c.order_id
AND a.data_flag = 1 ;
CREATE INDEX idx_dwd_lsl_order_item_2_1
ON fe_dwd.dwd_lsl_order_item_tmp_2 (order_id,pay_id);



DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1_1 AS
SELECT  a.ORDER_ID,a.PRODUCT_ID,
	a.quantity*a.SALE_PRICE-IFNULL(a.DISCOUNT_AMOUNT,0) amount
FROM fe_dwd.dwd_lsl_order_item_tmp_2 a;


  CREATE INDEX idx_oidwd_lsl_order_item_tmp_1_1
  ON fe_dwd.dwd_lsl_order_item_tmp_1_1 (ORDER_ID);

DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1_2 AS
SELECT  b.ORDER_ID,
       IFNULL(b.third_discount_amount,0)/COUNT(DISTINCT b.order_item_id) AS third_discount_amount
FROM fe_dwd.dwd_lsl_order_item_tmp_2 b
GROUP BY b.`order_id`;


  CREATE INDEX idx_oidwd_lsl_order_item_tmp_1_2
  ON fe_dwd.dwd_lsl_order_item_tmp_1_2 (ORDER_ID);

DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_3 AS
SELECT tt.ORDER_ID,tt.PRODUCT_ID,tt.amount+tt.third_discount_amount pay_amount_product
FROM
(
SELECT t1.ORDER_ID,t1.PRODUCT_ID,t1.amount,t2.third_discount_amount  
FROM
fe_dwd.dwd_lsl_order_item_tmp_1_1 t1
LEFT JOIN 
fe_dwd.dwd_lsl_order_item_tmp_1_2 t2
ON t1.ORDER_ID = t2.ORDER_ID
) tt
GROUP BY tt.ORDER_ID,tt.PRODUCT_ID;


CREATE INDEX idx_oidwd_lsl_order_item_tmp_3
ON fe_dwd.dwd_lsl_order_item_tmp_3 (ORDER_ID,PRODUCT_ID);



INSERT INTO fe_dwd.dwd_order_item_refund_day
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
pay_amount_product,
PAY_STATE,
pay_method,
pay_merchant_id,
user_id,
shelf_id,
product_id,
SUPPLIER_ID,
LIMIT_BUY_ID,
quantity,
quantity_shipped,
COST_PRICE,
quantity_act,
sale_price,
purchase_price,
o_discount_amount,
discount_amount,
REAL_TOTAL_PRICE,
COUPON_AMOUNT,
INTEGRAL_DISCOUNT,
third_discount_amount,
PRODUCT_TOTAL_AMOUNT,
COMMIS_TOTAL_AMOUNT
)
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
c.pay_amount_product,
a.PAY_STATE,
a.pay_method,
a.pay_merchant_id,
b.user_id,
b.shelf_id,
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
FROM fe_dwd.dwd_lsl_order_item_tmp_1  a
JOIN fe_dwd.dwd_lsl_order_item_tmp_2  b
ON a.order_id = b.order_id
AND a.pay_id = b.pay_id
JOIN fe_dwd.dwd_lsl_order_item_tmp_3 c
ON b.order_id = c.order_id
AND b.PRODUCT_ID = c.PRODUCT_ID
;

-- 全量更新最新添加退款的表，这个表中是货架相关的退款信息，包含了fe_pay中的退款信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_4;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_4 AS
SELECT b.order_id ,
c.order_item_id,
b.refund_order_id,
b.apply_time,	
SUM(c.refund_amount) refund_amount ,  -- 取明细里的退款金额。
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
GROUP BY b.order_id ,c.order_item_id
;

CREATE INDEX dwd_lsl_order_item_tmp_4
ON fe_dwd.dwd_lsl_order_item_tmp_4 (order_id,order_item_id);

-- 考虑重复支付的问题，退款只退一次，通过只取一次pay_id来限定
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_4_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_4_3 AS
SELECT b.pay_id,
a.order_id ,
a.order_item_id,
a.refund_order_id,
a.apply_time,	
a.refund_amount ,  
a.finish_time
FROM 
fe_dwd.dwd_order_item_refund_day AS b
JOIN fe_dwd.dwd_lsl_order_item_tmp_4 AS a 
ON a.order_id = b.order_id
AND a.order_item_id = b.order_item_id
GROUP BY a.order_id,a.order_item_id;

-- 
-- select * from fe_dwd.dwd_lsl_order_item_tmp_4_3 where order_id = 24649176600760000

CREATE INDEX dwd_lsl_order_item_tmp_4_3
ON fe_dwd.dwd_lsl_order_item_tmp_4_3 (order_id,order_item_id,pay_id);

-- 重复支付的订单，退款只退一次，取一条pay_id 来限定退一次的钱
UPDATE fe_dwd.dwd_order_item_refund_day AS b
JOIN fe_dwd.dwd_lsl_order_item_tmp_4_3 AS a 
ON a.order_id = b.order_id
AND a.order_item_id = b.order_item_id
AND a.pay_id = b.pay_id
SET b.refund_order_id = a.refund_order_id,
-- b.refund_status = a.refund_status,
b.refund_amount = a.refund_amount,
b.refund_finish_time = a.finish_time,
b.apply_time = a.apply_time,
b.load_time = CURRENT_TIMESTAMP;


-- 更新一下明细里退款金额为0的  4月7号开发发版后解决。之后可以注释掉
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_4_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_4_1 AS
SELECT DISTINCT  order_id 
FROM fe_dwd.dwd_lsl_order_item_tmp_4 
WHERE refund_amount =0;
CREATE INDEX idx_dwd_lsl_order_item_tmp_4_1
ON fe_dwd.dwd_lsl_order_item_tmp_4_1 (order_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_4_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_4_2 AS
SELECT
a.order_item_id,
	a.QUANTITY AS quantity_item,
	b.QUANTITY,
	b.REAL_TOTAL_PRICE
FROM
fe.sf_order_refund_item a
JOIN fe_dwd.dwd_lsl_order_item_tmp_4_1 c
ON a.order_id = c.order_id
JOIN fe.sf_order_item b 
ON a.order_item_id = b.ORDER_ITEM_ID;
CREATE INDEX idx_dwd_lsl_order_item_tmp_4_2
ON fe_dwd.dwd_lsl_order_item_tmp_4_2 (order_item_id);
UPDATE fe_dwd.dwd_order_item_refund_day AS b
JOIN fe_dwd.dwd_lsl_order_item_tmp_4_2 AS a 
ON a.order_item_id = b.order_item_id
SET
b.refund_amount = a.quantity_item*a.REAL_TOTAL_PRICE/a.QUANTITY,
b.load_time = CURRENT_TIMESTAMP;


-- 添加家荣宽表的字段
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_replace_op_order_and_item_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_replace_op_order_and_item_1 AS
SELECT 
a.order_id,
b.platform,
SUM(IF(a.refund_amount>0,a.quantity_act,a.`QUANTITY`) * a.`SALE_PRICE`) AS ogmv
FROM fe_dwd.dwd_order_item_refund_day a
LEFT JOIN fe.sf_order b
ON a.order_id = b.order_id 
AND b.DATA_FLAG =1
WHERE a.PAY_DATE >=SUBDATE(CURDATE(),2)
GROUP BY 
a.order_id;
CREATE INDEX idx_dwd_replace_op_order_and_item_1
ON fe_dwd.dwd_replace_op_order_and_item_1(order_id);
-- 替换婷姐feods.sf_order_item_temp
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE FROM fe_dwd.dwd_pub_order_item_recent_one_month WHERE pay_date < SUBDATE(CURDATE(),31);
  DELETE FROM  fe_dwd.dwd_pub_order_item_recent_one_month WHERE pay_date >= SUBDATE(CURDATE(),1) ;-- and pay_date < curdate();
 INSERT INTO fe_dwd.dwd_pub_order_item_recent_one_month
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
pay_amount_product,
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
platform,
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
t1.pay_amount_product,
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
t3.platform,
t1.sale_price,
t1.purchase_price,
t1.discount_amount,
t1.REAL_TOTAL_PRICE,
t1.COUPON_AMOUNT,
t1.INTEGRAL_DISCOUNT,
t1.third_discount_amount,
t1.PRODUCT_TOTAL_AMOUNT,
t1.COMMIS_TOTAL_AMOUNT,
  t1.refund_order_id,
  t1.refund_amount,
  t1.refund_finish_time,
  t1.apply_time,
t3.ogmv,
t1.PRODUCT_TOTAL_AMOUNT AS o_product_total_amount,
t1.o_discount_amount,
t1.COUPON_AMOUNT AS o_coupon_amount,
t1.third_discount_amount AS o_third_discount_amount
  FROM
    fe_dwd.`dwd_order_item_refund_day` t1
	LEFT JOIN 
	fe_dwd.dwd_product_base_day_all t2 
	ON t1.product_id = t2.PRODUCT_ID
	LEFT JOIN fe_dwd.dwd_replace_op_order_and_item_1 t3
	ON t1.order_id = t3.order_id 
  WHERE  t1.pay_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    AND t1.pay_date < CURDATE();
	
	
-- 替换婷姐feods.`wt_order_item_twomonth_temp`
  DELETE FROM fe_dwd.dwd_pub_order_item_recent_two_month WHERE pay_date < SUBDATE(CURDATE(),62);
  DELETE FROM fe_dwd.dwd_pub_order_item_recent_two_month WHERE pay_date >= SUBDATE(CURDATE(),1) ;-- AND pay_date < CURDATE() ;
  INSERT INTO fe_dwd.dwd_pub_order_item_recent_two_month
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
pay_amount_product,
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
platform,
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
t1.pay_amount_product,
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
t3.platform,
t1.sale_price,
t1.purchase_price,
t1.discount_amount,
t1.REAL_TOTAL_PRICE,
t1.COUPON_AMOUNT,
t1.INTEGRAL_DISCOUNT,
t1.third_discount_amount,
t1.PRODUCT_TOTAL_AMOUNT,
t1.COMMIS_TOTAL_AMOUNT,
  t1.refund_order_id,
  t1.refund_amount,
  t1.refund_finish_time,
  t1.apply_time,
t3.ogmv,
t1.PRODUCT_TOTAL_AMOUNT AS o_product_total_amount,
t1.o_discount_amount,
t1.COUPON_AMOUNT AS o_coupon_amount,
t1.third_discount_amount AS o_third_discount_amount
  FROM
    fe_dwd.`dwd_order_item_refund_day` t1
	LEFT JOIN 
	fe_dwd.dwd_product_base_day_all t2 
	ON t1.product_id = t2.PRODUCT_ID
	LEFT JOIN fe_dwd.dwd_replace_op_order_and_item_1 t3
	ON t1.order_id = t3.order_id 
  WHERE  t1.pay_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    AND t1.pay_date < CURDATE();
    
-- 添加一下替换家荣网易有数报表的表   
DELETE FROM fe_dwd.dwd_op_order_and_item_shelf7 WHERE pay_date >= SUBDATE(CURDATE(),1);
INSERT INTO  fe_dwd.`dwd_op_order_and_item_shelf7`  
(row_id
,order_id
,order_status
,order_type
,order_date
,pay_date
,user_id
,payment_type_gateway
,shelf_id
,product_id
,quantity
,quantity_act
,sale_price
,purchase_price
,discount_amount
,ogmv
,o_product_total_amount
,o_discount_amount
,o_coupon_amount
,o_third_discount_amount
)
SELECT 
t.row_id
,t.order_id
,t.order_status
,t.order_type
,t.order_date
,t.pay_date
,t.user_id
,t.payment_type_gateway
,t.shelf_id
,t.product_id
,t.quantity
,t.quantity_act
,t.sale_price
,t.purchase_price
,t.discount_amount
,t.ogmv
,t.o_product_total_amount
,t.o_discount_amount
,t.o_coupon_amount
,t.o_third_discount_amount
FROM
  fe_dwd.dwd_pub_order_item_recent_one_month t
  JOIN fe.sf_shelf s
    ON t.shelf_id = s.SHELF_ID
    AND s.DATA_FLAG = 1
    AND s.SHELF_TYPE = 7
    AND s.SHELF_NAME NOT REGEXP '测试'
where t.pay_date >=  DATE_SUB(CURDATE(), INTERVAL 1 DAY)
and t.pay_date < CURDATE()	;


	
    
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_order_item_refund_day_inc',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  
COMMIT;	
END