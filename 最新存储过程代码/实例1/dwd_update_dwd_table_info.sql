CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_update_dwd_table_info`()
BEGIN
   SET @start_date = SUBDATE(CURDATE(),INTERVAL 1 DAY);  -- 当天前一天
   SET @start_date2 = SUBDATE(CURDATE(),INTERVAL 2 DAY);  -- 当天前二天   
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
    
-- 用户表更新修改后的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.user_lsl_change_tmp;
CREATE TEMPORARY TABLE fe_dwd.user_lsl_change_tmp AS
SELECT 
t1.MEMBER_ID  AS user_id,
t1.REAL_NAME,
t1.NICK,
t1.CREATE_DATE,
t1.SEX  gender,
t1.BIRTHDAY,
t1.BELONG_INDUSTRY,
t1.REG_CHANNEL,					
t1.BIND_PHONE_DATE,
t1.EDU ,
t1.EMAIL,
CASE WHEN t1.IS_BIND_COMPANY > 0 THEN '已绑定企业' ELSE '未绑定' END AS IS_BIND_COMPANY,
t1.ADDRESS,
t1.WECHAT_ID
FROM fe.pub_member t1	
WHERE t1.LAST_UPDATE_DATE >= @start_date2 ;  -- 昨天有修改的记录
CREATE INDEX idx_dwd_user_lsl_change_tmp
ON fe_dwd.user_lsl_change_tmp (user_id);
-- 更新一下修改的字段
UPDATE fe_dwd.dwd_user_day_inc AS b
JOIN fe_dwd.user_lsl_change_tmp a 
ON a.user_id = b.user_id
SET b.REAL_NAME = a.REAL_NAME,
 b.NICK = a.NICK,
 b.CREATE_DATE = a.CREATE_DATE,
 b.gender = a.gender,
 b.BIRTHDAY = a.BIRTHDAY,
 b.BELONG_INDUSTRY = a.BELONG_INDUSTRY,
 b.REG_CHANNEL = a.REG_CHANNEL,
 b.BIND_PHONE_DATE = a.BIND_PHONE_DATE,
 b.EDU = a.EDU,
 b.EMAIL = a.EMAIL, 
 b.IS_BIND_COMPANY = a.IS_BIND_COMPANY,
 b.ADDRESS = a.ADDRESS,
 b.WECHAT_ID = a.WECHAT_ID,
 b.load_time  = ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY);  -- 便于datax第二天同步中间人为修改的数据到实例2
 
 
-- 补货表更新修改后的数据
-- 有修改的订单 order_id
DROP TEMPORARY TABLE IF EXISTS fe_dwd.replenish_lsl_change_change_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.replenish_lsl_change_change_tmp_1 AS
SELECT DISTINCT a.order_id
FROM fe.sf_product_fill_order a        -- 补货订单
WHERE a.`DATA_FLAG` = 1
AND DATEDIFF(a.last_update_time,add_time) >=1
AND a.last_update_time >=@start_date2 
UNION 
SELECT DISTINCT a.order_id
FROM fe.sf_product_fill_order_item a   -- 补货订单明细
WHERE a.`DATA_FLAG` = 1
AND DATEDIFF(a.last_update_time,add_time) >=1
AND a.last_update_time >= @start_date2 ;
CREATE INDEX idx_replenish_lsl_change_change_tmp_1
ON fe_dwd.replenish_lsl_change_change_tmp_1 (order_id);
-- 删除发生变化的数据
DELETE a.* FROM fe_dwd.dwd_fill_day_inc a 
INNER JOIN fe_dwd.replenish_lsl_change_change_tmp_1 b
ON a.order_id = b.order_id;  
  
-- 有修改的订单 的信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.replenish_lsl_change_change_tmp;
CREATE TEMPORARY TABLE fe_dwd.replenish_lsl_change_change_tmp AS
SELECT 
a.apply_time,
a.order_id,
b.ORDER_ITEM_ID,
b.PRODUCT_ID,
a.SEND_TIME,
a.FILL_TIME,
a.fill_type,
a.FILL_RESULT,
a.SHELF_ID,
b.SHELF_DETAIL_ID,
b.actual_apply_num,
b.actual_send_num,
b.actual_sign_num,
b.actual_fill_num,
a.order_status,
a.SUPPLIER_ID,
a.supplier_type,
b.SALE_PRICE,
b.PURCHASE_PRICE,
a.audit_status,
a.surplus_reason,
a.sale_faulty_type,
b.STOCK_NUM,
b.WEEK_SALE_NUM
,a.PRODUCT_TYPE_NUM
,a.PRODUCT_NUM
,a.TOTAL_PRICE
,a.FILL_USER_ID
,a.FILL_USER_NAME
,a.FILL_AUDIT_STATUS
,a.FILL_AUDIT_USER_ID
,a.FILL_AUDIT_USER_NAME
,a.FILL_AUDIT_TIME
,a.APPLY_USER_ID
,a.APPLY_USER_NAME
,a.RECEIVER_ID
,a.RECEIVER_NAME
,a.RECEIVER_PHONE
,a.BACK_STOCK_TIME
,a.BACK_STOCK_STATUS
,b.ERROR_NUM
,b.QUALITY_STOCK_NUM
,b.DEFECTIVE_STOCK_NUM
,b.ERROR_REASON
,b.FILL_ITEM_AUDIT_STATUS
,b.AUDIT_ERROR_NUM
,b.STOCK_STATUS 
,a.ADD_USER_ID
,a.CANCEL_REMARK
,a.last_update_time
FROM
fe.sf_product_fill_order a 
JOIN fe.sf_product_fill_order_item b
ON a.order_id = b.order_id
AND a.`DATA_FLAG` = 1
AND b.`DATA_FLAG` = 1
JOIN fe_dwd.replenish_lsl_change_change_tmp_1 c
ON a.order_id = c.order_id;
CREATE INDEX idx_replenish_change_tmp
ON fe_dwd.replenish_lsl_change_change_tmp (PRODUCT_ID,SHELF_ID);
INSERT INTO fe_dwd.dwd_fill_day_inc
(
apply_time,
order_id,
ORDER_ITEM_ID,
PRODUCT_ID,
SEND_TIME,
FILL_TIME,
FILL_TYPE,
FILL_RESULT,
SHELF_ID,
SHELF_DETAIL_ID,
actual_apply_num,
actual_send_num,
actual_sign_num,
ACTUAL_FILL_NUM,
order_status,
SUPPLIER_ID,
supplier_type,
SALE_PRICE,
PURCHASE_PRICE,
audit_status,
surplus_reason,
sale_faulty_type,
STOCK_NUM,
ALARM_QUANTITY,
WEEK_SALE_NUM,
NEW_FLAG,
SALES_FLAG
,PRODUCT_TYPE_NUM
,PRODUCT_NUM
,TOTAL_PRICE
,FILL_USER_ID
,FILL_USER_NAME
,FILL_AUDIT_STATUS
,FILL_AUDIT_USER_ID
,FILL_AUDIT_USER_NAME
,FILL_AUDIT_TIME
,APPLY_USER_ID
,APPLY_USER_NAME
,RECEIVER_ID
,RECEIVER_NAME
,RECEIVER_PHONE
,BACK_STOCK_TIME
,BACK_STOCK_STATUS
,ERROR_NUM
,QUALITY_STOCK_NUM
,DEFECTIVE_STOCK_NUM
,ERROR_REASON
,FILL_ITEM_AUDIT_STATUS
,AUDIT_ERROR_NUM
,STOCK_STATUS
,ADD_USER_ID
,CANCEL_REMARK
,last_update_time
,load_time
)
SELECT
a.apply_time,
a.order_id,
a.ORDER_ITEM_ID,
a.PRODUCT_ID,
a.SEND_TIME,
a.FILL_TIME,
a.fill_type,
a.FILL_RESULT,
a.SHELF_ID,
a.SHELF_DETAIL_ID,
a.actual_apply_num,
a.actual_send_num,
a.actual_sign_num,
a.actual_fill_num,
a.order_status,
a.SUPPLIER_ID,
a.supplier_type,
IFNULL(a.SALE_PRICE,f.SALE_PRICE) SALE_PRICE,
a.PURCHASE_PRICE,
a.audit_status,
a.surplus_reason,
a.sale_faulty_type,
a.STOCK_NUM,
i.ALARM_QUANTITY,
a.WEEK_SALE_NUM,
c.NEW_FLAG,
c.SALES_FLAG,
a.PRODUCT_TYPE_NUM
,a.PRODUCT_NUM
,a.TOTAL_PRICE
,a.FILL_USER_ID
,a.FILL_USER_NAME
,a.FILL_AUDIT_STATUS
,a.FILL_AUDIT_USER_ID
,a.FILL_AUDIT_USER_NAME
,a.FILL_AUDIT_TIME
,a.APPLY_USER_ID
,a.APPLY_USER_NAME
,a.RECEIVER_ID
,a.RECEIVER_NAME
,a.RECEIVER_PHONE
,a.BACK_STOCK_TIME
,a.BACK_STOCK_STATUS
,a.ERROR_NUM
,a.QUALITY_STOCK_NUM
,a.DEFECTIVE_STOCK_NUM
,a.ERROR_REASON
,a.FILL_ITEM_AUDIT_STATUS
,a.AUDIT_ERROR_NUM
,a.STOCK_STATUS
,a.ADD_USER_ID
,a.CANCEL_REMARK
,a.last_update_time
,ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY) AS load_time  -- 便于datax第二天同步中间人为修改的数据到实例2
FROM fe_dwd.replenish_lsl_change_change_tmp a 
LEFT JOIN fe.sf_shelf_product_detail_flag c 
    ON a.SHELF_ID=c.SHELF_ID 
    AND a.PRODUCT_ID=c.PRODUCT_ID 
    AND c.DATA_FLAG = 1
LEFT JOIN fe.`sf_shelf_product_detail` f
    ON a.`SHELF_ID` = f.SHELF_ID
    AND a.`PRODUCT_ID` = f.PRODUCT_ID
    AND f.DATA_FLAG = 1
LEFT JOIN fe.`sf_package_item` i
    ON i.ITEM_ID= f.ITEM_ID
    AND i.DATA_FLAG = 1;
    
       
-- 更新一下最近两个月的数据
-- 提取需要删除的行
DROP TEMPORARY TABLE IF EXISTS fe_dwd.replenish_lsl_change_change_tmp_1_1;
CREATE TEMPORARY TABLE fe_dwd.replenish_lsl_change_change_tmp_1_1 AS
SELECT DISTINCT a.order_id
FROM fe_dwd.dwd_fill_day_inc_recent_two_month a
JOIN fe_dwd.replenish_lsl_change_change_tmp_1 b 
ON a.order_id = b.order_id;
CREATE INDEX idx_dwd_replenish_lsl_change_change_tmp_1_1
ON fe_dwd.replenish_lsl_change_change_tmp_1_1 (order_id);
-- 删除发生变化的数据
DELETE a.* FROM fe_dwd.dwd_fill_day_inc_recent_two_month a 
INNER JOIN fe_dwd.replenish_lsl_change_change_tmp_1_1 b
ON a.order_id = b.order_id;  
  
-- 有修改的订单 的信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.replenish_lsl_change_change_tmp;
CREATE TEMPORARY TABLE fe_dwd.replenish_lsl_change_change_tmp AS 
SELECT 
a.apply_time,
a.order_id,
b.ORDER_ITEM_ID,
b.PRODUCT_ID,
a.SEND_TIME,
a.FILL_TIME,
a.fill_type,
a.FILL_RESULT,
a.SHELF_ID,
b.SHELF_DETAIL_ID,
b.actual_apply_num,
b.actual_send_num,
b.actual_sign_num,
b.actual_fill_num,
a.order_status,
a.SUPPLIER_ID,
a.supplier_type,
b.SALE_PRICE,
b.PURCHASE_PRICE,
a.audit_status,
a.surplus_reason,
a.sale_faulty_type,
b.STOCK_NUM,
b.WEEK_SALE_NUM
,a.PRODUCT_TYPE_NUM
,a.PRODUCT_NUM
,a.TOTAL_PRICE
,a.FILL_USER_ID
,a.FILL_USER_NAME
,a.FILL_AUDIT_STATUS
,a.FILL_AUDIT_USER_ID
,a.FILL_AUDIT_USER_NAME
,a.FILL_AUDIT_TIME
,a.APPLY_USER_ID
,a.APPLY_USER_NAME
,a.RECEIVER_ID
,a.RECEIVER_NAME
,a.RECEIVER_PHONE
,a.BACK_STOCK_TIME
,a.BACK_STOCK_STATUS
,b.ERROR_NUM
,b.QUALITY_STOCK_NUM
,b.DEFECTIVE_STOCK_NUM
,b.ERROR_REASON
,b.FILL_ITEM_AUDIT_STATUS
,b.AUDIT_ERROR_NUM
,b.STOCK_STATUS 
,a.ADD_USER_ID
,a.CANCEL_REMARK
,a.last_update_time
FROM
fe.sf_product_fill_order a 
JOIN fe.sf_product_fill_order_item b
ON a.order_id = b.order_id
AND a.`DATA_FLAG` = 1
AND b.`DATA_FLAG` = 1
JOIN fe_dwd.replenish_lsl_change_change_tmp_1_1 c
ON a.order_id = c.order_id;
CREATE INDEX idx_replenish_change_tmp
ON fe_dwd.replenish_lsl_change_change_tmp (PRODUCT_ID,SHELF_ID);
INSERT INTO fe_dwd.dwd_fill_day_inc_recent_two_month
(
apply_time,
order_id,
ORDER_ITEM_ID,
PRODUCT_ID,
SEND_TIME,
FILL_TIME,
FILL_TYPE,
FILL_RESULT,
SHELF_ID,
SHELF_DETAIL_ID,
actual_apply_num,
actual_send_num,
actual_sign_num,
ACTUAL_FILL_NUM,
order_status,
SUPPLIER_ID,
supplier_type,
SALE_PRICE,
PURCHASE_PRICE,
audit_status,
surplus_reason,
sale_faulty_type,
STOCK_NUM,
ALARM_QUANTITY,
WEEK_SALE_NUM,
NEW_FLAG,
SALES_FLAG
,PRODUCT_TYPE_NUM
,PRODUCT_NUM
,TOTAL_PRICE
,FILL_USER_ID
,FILL_USER_NAME
,FILL_AUDIT_STATUS
,FILL_AUDIT_USER_ID
,FILL_AUDIT_USER_NAME
,FILL_AUDIT_TIME
,APPLY_USER_ID
,APPLY_USER_NAME
,RECEIVER_ID
,RECEIVER_NAME
,RECEIVER_PHONE
,BACK_STOCK_TIME
,BACK_STOCK_STATUS
,ERROR_NUM
,QUALITY_STOCK_NUM
,DEFECTIVE_STOCK_NUM
,ERROR_REASON
,FILL_ITEM_AUDIT_STATUS
,AUDIT_ERROR_NUM
,STOCK_STATUS
,ADD_USER_ID
,CANCEL_REMARK
,last_update_time
,load_time
)
SELECT
a.apply_time,
a.order_id,
a.ORDER_ITEM_ID,
a.PRODUCT_ID,
a.SEND_TIME,
a.FILL_TIME,
a.fill_type,
a.FILL_RESULT,
a.SHELF_ID,
a.SHELF_DETAIL_ID,
a.actual_apply_num,
a.actual_send_num,
a.actual_sign_num,
a.actual_fill_num,
a.order_status,
a.SUPPLIER_ID,
a.supplier_type,
IFNULL(a.SALE_PRICE,f.SALE_PRICE) SALE_PRICE,
a.PURCHASE_PRICE,
a.audit_status,
a.surplus_reason,
a.sale_faulty_type,
a.STOCK_NUM,
i.ALARM_QUANTITY,
a.WEEK_SALE_NUM,
c.NEW_FLAG,
c.SALES_FLAG,
a.PRODUCT_TYPE_NUM
,a.PRODUCT_NUM
,a.TOTAL_PRICE
,a.FILL_USER_ID
,a.FILL_USER_NAME
,a.FILL_AUDIT_STATUS
,a.FILL_AUDIT_USER_ID
,a.FILL_AUDIT_USER_NAME
,a.FILL_AUDIT_TIME
,a.APPLY_USER_ID
,a.APPLY_USER_NAME
,a.RECEIVER_ID
,a.RECEIVER_NAME
,a.RECEIVER_PHONE
,a.BACK_STOCK_TIME
,a.BACK_STOCK_STATUS
,a.ERROR_NUM
,a.QUALITY_STOCK_NUM
,a.DEFECTIVE_STOCK_NUM
,a.ERROR_REASON
,a.FILL_ITEM_AUDIT_STATUS
,a.AUDIT_ERROR_NUM
,a.STOCK_STATUS
,a.ADD_USER_ID
,a.CANCEL_REMARK
,a.last_update_time
,ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY) AS load_time  
FROM fe_dwd.replenish_lsl_change_change_tmp a     -- 有修改的订单 的信息
LEFT JOIN fe.sf_shelf_product_detail_flag c 
    ON a.SHELF_ID=c.SHELF_ID 
    AND a.PRODUCT_ID=c.PRODUCT_ID 
    AND c.DATA_FLAG = 1
LEFT JOIN fe.`sf_shelf_product_detail` f
    ON a.`SHELF_ID` = f.SHELF_ID
    AND a.`PRODUCT_ID` = f.PRODUCT_ID
    AND f.DATA_FLAG = 1
LEFT JOIN fe.`sf_package_item` i
    ON i.ITEM_ID= f.ITEM_ID
    AND i.DATA_FLAG = 1;
	
-- 订单宽表
-- 先提取发生数据修改的表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_change_order_item_tmp_1_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_change_order_item_tmp_1_1 AS
SELECT 
DISTINCT c.order_id
FROM fe.sf_order_pay c     -- 订单支付表
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
-- and c.add_time < last_update_time
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date2
UNION
SELECT 
DISTINCT c.order_id
FROM fe_pay.sf_order_pay_1 c   
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date2
UNION
SELECT 
DISTINCT c.order_id
FROM fe_pay.sf_order_pay_2 c   -- 0
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time >@start_date2
UNION
SELECT 
DISTINCT c.order_id
FROM fe_pay.sf_order_pay_3 c   -- 0
WHERE c.PAY_STATE = 2  
AND c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date2
UNION
SELECT 
DISTINCT c.order_id
FROM fe.sf_order c   -- 系统订单主表
WHERE c.data_flag =1  
AND c.order_status IN ('2','6','7')
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date2
;
CREATE INDEX idx_dwd_change_lsl_order_item_1_1
ON fe_dwd.dwd_lsl_change_order_item_tmp_1_1 (order_id);
-- 再提取相关的信息  订单支付表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_change_lsl_order_item_tmp_1_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_change_lsl_order_item_tmp_1_2 AS
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
FROM fe.sf_order_pay c      -- 订单支付表
JOIN fe_dwd.dwd_lsl_change_order_item_tmp_1_1 b  -- 有修改的用户订单
ON c.order_id = b.order_id
and c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  ;
-- 再提取相关的信息  支付表一
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_change_lsl_order_item_tmp_1_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_change_lsl_order_item_tmp_1_3 AS
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
JOIN fe_dwd.dwd_lsl_change_order_item_tmp_1_1 b
ON c.order_id = b.order_id
AND c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  ;
-- 再提取相关的信息  支付表二
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_change_lsl_order_item_tmp_1_4;
CREATE TEMPORARY TABLE fe_dwd.dwd_change_lsl_order_item_tmp_1_4 AS
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
JOIN fe_dwd.dwd_lsl_change_order_item_tmp_1_1 b
ON c.order_id = b.order_id
AND c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  ;
-- 再提取相关的信息  支付表三
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_change_lsl_order_item_tmp_1_5;
CREATE TEMPORARY TABLE fe_dwd.dwd_change_lsl_order_item_tmp_1_5 AS
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
JOIN fe_dwd.dwd_lsl_change_order_item_tmp_1_1 b
ON c.order_id = b.order_id
AND c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  ;



-- 组合支付 先只取支付成功的
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_combin_order_item_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_combin_order_item_tmp_1 AS
SELECT a.requirement_id,a.order_id,a.pay_amount ,a.pay_state
FROM fe_pay.`sf_pay_requirement` a 
WHERE a.requirement_type = 'goodsOrder'
AND a.data_flag =1
AND a.pay_state =2 
AND DATEDIFF(last_update_time,add_time) >=1
AND a.last_update_time > @start_date2
;

CREATE INDEX dwd_lsl_combin_order_item_tmp_1_2
ON fe_dwd.dwd_lsl_combin_order_item_tmp_1 (requirement_id);

DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_combin_order_item_tmp_2_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_combin_order_item_tmp_2_1 AS
SELECT DISTINCT
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
FROM fe.sf_order_pay c 
JOIN  fe_dwd.dwd_lsl_combin_order_item_tmp_1 b
ON b.requirement_id = c.order_id
AND c.PAY_TYPE =1 -- 取微信支付的那一条信息 e币或企业代付的那条不取
;



-- 合并一下 支付表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_change_lsl_order_item_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_change_lsl_order_item_tmp_1 AS
SELECT * FROM fe_dwd.dwd_change_lsl_order_item_tmp_1_2
UNION 
SELECT * FROM fe_dwd.dwd_change_lsl_order_item_tmp_1_3
UNION 
SELECT * FROM fe_dwd.dwd_change_lsl_order_item_tmp_1_4
UNION 
SELECT * FROM fe_dwd.dwd_change_lsl_order_item_tmp_1_5
UNION 
SELECT * FROM fe_dwd.dwd_lsl_combin_order_item_tmp_2_1
;
CREATE INDEX idx_dwd_change_lsl_order_item_1
ON fe_dwd.dwd_change_lsl_order_item_tmp_1 (order_id,pay_id);
-- 取订单信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_change_order_item_tmp_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_change_order_item_tmp_2 AS
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
FROM fe_dwd.dwd_change_lsl_order_item_tmp_1 c  
JOIN fe.sf_order_item b 
ON c.order_id = b.order_id
AND b.data_flag = 1
JOIN fe.sf_order a
ON a.order_id = c.order_id
AND a.data_flag = 1 ;
CREATE INDEX idx_dwd_lsl_change_order_item_2_1
ON fe_dwd.dwd_lsl_change_order_item_tmp_2 (order_id,pay_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1_1 AS
SELECT  a.ORDER_ID,a.PRODUCT_ID,
	a.quantity*a.SALE_PRICE-IFNULL(a.DISCOUNT_AMOUNT,0) amount
FROM fe_dwd.dwd_lsl_change_order_item_tmp_2 a;
  CREATE INDEX idx_oidwd_lsl_order_item_tmp_1_1
  ON fe_dwd.dwd_lsl_order_item_tmp_1_1 (ORDER_ID);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1_2 AS
SELECT  b.ORDER_ID,
       IFNULL(b.third_discount_amount,0)/COUNT(DISTINCT b.order_item_id) AS third_discount_amount
FROM fe_dwd.dwd_lsl_change_order_item_tmp_2 b
GROUP BY b.`order_id`;
  CREATE INDEX idx_oidwd_lsl_order_item_tmp_1_2
  ON fe_dwd.dwd_lsl_order_item_tmp_1_2 (ORDER_ID);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_3_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_3_1 AS
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
CREATE INDEX idx_oidwd_lsl_order_item_tmp_3_1
ON fe_dwd.dwd_lsl_order_item_tmp_3_1 (ORDER_ID,PRODUCT_ID);
-- 提取需要删除的订单号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_change_order_item_tmp_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_change_order_item_tmp_3 AS
SELECT DISTINCT order_id,PAY_DATE
FROM fe_dwd.dwd_lsl_change_order_item_tmp_2;
CREATE INDEX idx_dwd_lsl_change_order_item_3_1_1
ON fe_dwd.dwd_lsl_change_order_item_tmp_3 (order_id);
-- 删除发生变化的数据
DELETE a.* FROM fe_dwd.dwd_order_item_refund_day a 
INNER JOIN fe_dwd.dwd_lsl_change_order_item_tmp_3 b
ON a.order_id = b.order_id;
-- 购买订单宽表_dwd
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
COMMIS_TOTAL_AMOUNT,
load_time
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
b.COMMIS_TOTAL_AMOUNT,
ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY)    -- 便于datax第二天同步中间人为修改的数据到实例2
FROM fe_dwd.dwd_change_lsl_order_item_tmp_1  a  -- 合并一下 支付表
JOIN fe_dwd.dwd_lsl_change_order_item_tmp_2  b  -- 取订单信息
ON a.order_id = b.order_id AND a.pay_id = b.pay_id
JOIN fe_dwd.dwd_lsl_order_item_tmp_3_1 c
ON b.order_id = c.order_id
AND b.PRODUCT_ID = c.PRODUCT_ID
;
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
AND b.order_id =1
WHERE a.PAY_DATE >=SUBDATE(CURDATE(),2)
GROUP BY 
a.order_id;
CREATE INDEX idx_dwd_replace_op_order_and_item_1
ON fe_dwd.dwd_replace_op_order_and_item_1  (order_id);
-- 提取需要删除的订单号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_change_order_item_tmp_3_one;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_change_order_item_tmp_3_one AS
SELECT DISTINCT a.order_id
FROM  fe_dwd.dwd_lsl_change_order_item_tmp_3 a 
WHERE a.PAY_DATE>= SUBDATE(CURDATE(),31);
CREATE INDEX idx_dwd_lsl_change_order_item_3_one
ON fe_dwd.dwd_lsl_change_order_item_tmp_3_one (order_id);
-- 删除发生变化的数据
DELETE a.* FROM fe_dwd.dwd_pub_order_item_recent_one_month a 
INNER JOIN fe_dwd.dwd_lsl_change_order_item_tmp_3_one b
ON a.order_id = b.order_id;
-- 购买订单宽表_dwd_one_month
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
o_third_discount_amount,
load_time
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
t1.third_discount_amount AS o_third_discount_amount,
ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY) AS load_time  
  FROM
    fe_dwd.`dwd_order_item_refund_day` t1
	JOIN fe_dwd.dwd_lsl_change_order_item_tmp_3_one c 
ON t1.order_id = c.order_id
	LEFT JOIN 
	fe_dwd.dwd_product_base_day_all t2 
	ON t1.product_id = t2.PRODUCT_ID
    LEFT JOIN fe_dwd.dwd_replace_op_order_and_item_1 t3
	ON t1.order_id = t3.order_id 	;
-- 提取需要删除的订单号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_change_order_item_tmp_3_two;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_change_order_item_tmp_3_two AS
SELECT DISTINCT a.order_id
FROM  fe_dwd.dwd_lsl_change_order_item_tmp_3 a 
WHERE a.PAY_DATE>= SUBDATE(CURDATE(),62);
CREATE INDEX idx_dwd_lsl_change_order_item_3_two
ON fe_dwd.dwd_lsl_change_order_item_tmp_3_two (order_id);
-- 删除发生变化的数据
DELETE a.* FROM fe_dwd.dwd_pub_order_item_recent_two_month a 
INNER JOIN fe_dwd.dwd_lsl_change_order_item_tmp_3_two b
ON a.order_id = b.order_id;
-- 购买订单宽表_dwd_one_month
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
o_third_discount_amount,
load_time
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
t1.third_discount_amount AS o_third_discount_amount,
ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY) AS load_time 
  FROM
    fe_dwd.`dwd_order_item_refund_day` t1
		JOIN fe_dwd.dwd_lsl_change_order_item_tmp_3_two c 
ON t1.order_id = c.order_id
	LEFT JOIN 
	fe_dwd.dwd_product_base_day_all t2 
	ON t1.product_id = t2.PRODUCT_ID
    LEFT JOIN fe_dwd.dwd_replace_op_order_and_item_1 t3
	ON t1.order_id = t3.order_id 		;
-- 更新一下字典的宽表
TRUNCATE TABLE fe_dwd.dwd_pub_dictionary;
INSERT INTO fe_dwd.dwd_pub_dictionary
(
DICTIONARY_ID,
DICTIONARY_CODE,
DICTIONARY_NAME,
ITEM_VALUE,
ITEM_NAME,
ITEM_SORT,
pub_code,
add_time,
last_update_time
)
SELECT 
a2.DICTIONARY_ID,
a1.DICTIONARY_CODE,
a1.DICTIONARY_NAME,
a2.ITEM_VALUE,
a2.ITEM_NAME,
a2.ITEM_SORT,
a2.pub_code,
a2.add_time,
a2.last_update_time
FROM fe.pub_dictionary_type a1 
JOIN fe.pub_dictionary_item a2 
ON a1.DICTIONARY_ID=a2.DICTIONARY_ID
ORDER BY a2.DICTIONARY_ID,a2.ITEM_SORT;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_check_tmp;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_check_tmp AS
SELECT  detail_id ,COUNT(*)  FROM fe_dwd.`dwd_check_base_day_inc`
GROUP BY detail_id
HAVING COUNT(*) >1;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_check_tmp_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_check_tmp_2 AS
SELECT b.detail_id,b.last_update_time FROM fe_dwd.dwd_shelf_product_check_tmp a
JOIN fe_dwd.`dwd_check_base_day_inc` b
ON a.detail_id = b.detail_id;
-- 删除数据较早的那一条
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_check_tmp_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_check_tmp_3 AS
SELECT detail_id,last_update_time,rank FROM (
SELECT  @rownum:=@rownum+1 AS rownum,# 行号
    IF(@x=uo.detail_id,@rank:=@rank+1,@rank:=1) rank,#处理排名，如果@x等于user_id，则表示@x被初始化，将@rank自增1
    @x:=uo.detail_id, # 初始化@x，@x为中间变量,在rank之后初始化,所以，rank初始化时，@x为null或者是上一个user_id的值
    detail_id,last_update_time 
FROM  
    fe_dwd.dwd_shelf_product_check_tmp_2  uo,
    (SELECT @rownum:=0,@rank:=0) init # 初始化信息表
ORDER BY detail_id ASC, last_update_time DESC
)result
WHERE rank=2;
DELETE a.* FROM fe_dwd.`dwd_check_base_day_inc` a,
 fe_dwd.dwd_shelf_product_check_tmp_3 b
WHERE a.detail_id = b.detail_id 
AND a.last_update_time = b.last_update_time;
-- 月初更新上月货架GMV、实收
IF DAY(CURRENT_DATE) = 1 THEN
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_1 AS
SELECT 
DATE(pay_date) pay_date,
order_id ,
shelf_id,
a.`PAY_AMOUNT`* COUNT(DISTINCT a.pay_id)-SUM(IFNULL(a.refund_amount,0)) AS pay_amount_shipped, -- 重复支付的数据也要算进来
SUM(IFNULL(a.quantity_act * a.`SALE_PRICE`,0)) AS GMV
FROM fe_dwd.`dwd_order_item_refund_day` a
WHERE a.PAY_DATE >= DATE_ADD(CURDATE()-DAY(CURDATE())+1,INTERVAL -1 MONTH )  
  AND a.PAY_DATE < DATE_ADD(CURDATE(),INTERVAL -DAY(CURDATE())+1 DAY)
GROUP BY DATE(pay_date),order_id,shelf_id;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_2 AS
SELECT 
pay_date,
shelf_id,
SUM(IFNULL(pay_amount_shipped,0)) pay_amount,
SUM(IFNULL(GMV,0)) AS GMV
FROM fe_dwd.dwd_lsl_shelf_2_1
GROUP BY pay_date,shelf_id;
CREATE INDEX idx_dwd_lsl_shelf_2_2
ON fe_dwd.dwd_lsl_shelf_2_2 (pay_date,shelf_id);
UPDATE fe_dwd.dwd_shelf_day_his AS b
JOIN fe_dwd.dwd_lsl_shelf_2_2 a 
ON a.pay_date = b.sdate
AND a.shelf_id = b.shelf_id
SET b.pay_amount = a.pay_amount,
 b.GMV = a.GMV,
 b.load_time = ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY);  -- 便于datax第二天同步中间人为修改的数据到实例2
 END IF;
 
 
-- 商城宽表
-- 先找出发生变化的订单号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_1_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_1_1 AS
SELECT 
DISTINCT c.order_id
FROM fe_goods.sf_group_order_pay  c     -- 订单支付表
WHERE c.PAY_STATE = 2  -- 支付成功
AND c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date2;
CREATE INDEX idx_dwd_group_order_refound_address_day_1_1
ON fe_dwd.dwd_group_order_refound_address_day_1_1 (order_id);
-- 删掉发生变化的订单
DELETE a.* FROM fe_dwd.dwd_group_order_refound_address_day a 
JOIN fe_dwd.dwd_group_order_refound_address_day_1_1 b 
ON a.order_id = b.order_id;
 -- 全量添加最新退款的表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_1_1_1 ;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_1_1_1  AS
SELECT ss.order_item_id,
  TRIM(sr.gateway_pay_id) AS gateway_pay_id_rufound,  -- 支付网关的退款单ID 
  IFNULL(sr.refund_amount, 0) AS refund_amount -- 退款金额              
FROM fe_goods.sf_group_order_refund_item ss 
JOIN fe_goods.sf_group_order_refund_pay sr
ON ss.refund_order_id = sr.refund_order_id 
AND sr.data_flag = 1
AND sr.state=2
AND ss.data_flag = 1
;
CREATE INDEX dwd_group_order_refound_address_day_1_1_1
ON fe_dwd.dwd_group_order_refound_address_day_1_1_1 (order_item_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_order_refound_address_day_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_1  AS 
SELECT
  CASE
	WHEN c.pay_type = 1
	THEN '微信支付'
	WHEN c.pay_type = 2
	THEN '手工线下支付'
	WHEN c.pay_type = 3
	THEN '月结付款'
	WHEN c.pay_type = 4
	THEN 'E币支付'
	WHEN c.pay_type = 5
	THEN '顺银支付'
	WHEN c.pay_type = 6
	THEN '顺手付云闪付'
	WHEN c.pay_type = 7
	THEN '招行一卡通'
	WHEN c.pay_type =8
	THEN '微信委托扣款'
	WHEN c.pay_type = 9
	THEN '餐卡支付'
	WHEN c.pay_type = 10
	THEN '顺手付一码付'
	WHEN c.pay_type = 11
	THEN '企业代扣'
	WHEN c.pay_type = 12
	THEN '小蜜蜂积分支付'
	WHEN c.pay_type = 13
	THEN '升腾支付'
	WHEN c.pay_type = 14
	THEN '兑换卡兑换'
	WHEN c.pay_type = 15
	THEN '中国移动和包支付'
	WHEN c.pay_type = 16
	THEN '组合支付'
	WHEN c.pay_type = 22
	THEN '微信H5支付'
	WHEN c.pay_type = 23
	THEN '微信刷脸支付'
	WHEN c.pay_type = 24
	THEN '顺丰丰侠支付'
	WHEN c.pay_type = 26
	THEN '云闪付直连免密支付'
	WHEN c.pay_type = 27
	THEN '招行免密支付'
	WHEN c.pay_type = 29
	THEN '劳保支付'
	WHEN c.pay_type = 30
	THEN '慰问支付'
	WHEN c.pay_type = 31
	THEN '万翼支付'	
  END AS pay_type_desc,  -- 支付类型
  c.pay_type,
  b.order_type order_type_number,  
 CASE
    WHEN b.order_type = 1
    THEN '实物订单'
    WHEN b.order_type = 2
    THEN '虚拟订单'
    WHEN b.order_type = 3
    THEN '第三方充值订单'
    WHEN b.order_type = 4
    THEN '欧非卡密商品订单'
    WHEN b.order_type = 5
    THEN '饿了么订单'
    WHEN b.order_type = 6
    THEN '网易严选订单'
    WHEN b.order_type = 7
    THEN '顺丰优选订单'
    WHEN b.order_type = 8
    THEN '美餐订单'
    WHEN b.order_type = 9
    THEN '生活缴费'
    WHEN b.order_type = 10
    THEN '拼团订单'
    WHEN b.order_type = 11
    THEN '滴滴订单'
    WHEN b.order_type = 12
    THEN '京东'
    WHEN b.order_type = 13
    THEN '口碑到店'
    WHEN b.order_type = 14
    THEN '票牛'
    WHEN b.order_type = 15
    THEN '本来生活'
    WHEN b.order_type = 16
    THEN '天虹现金券'
    WHEN b.order_type = 17
    THEN '库盒'
    WHEN b.order_type = 18
    THEN '饿了么团餐'
    WHEN b.order_type = 19
    THEN '苏宁订单'
  END AS order_type,  -- 订单类型
  b.sale_channel,
  CASE
    WHEN b.order_from = 1
    THEN '销售助手'
    WHEN b.order_from = 3
    THEN '企业采购'
    WHEN b.order_from = 2 AND b.sale_channel = '0' THEN '丰e能量站'
    WHEN b.order_from = 2 AND b.sale_channel = 'PAYQB' THEN '平安壹钱包'
    WHEN b.order_from = 2 AND b.sale_channel = 'QYFL' THEN '企业福利前台'
    WHEN b.order_from = 2 AND b.sale_channel = 'SFIM' THEN '丰声渠道'
    WHEN b.order_from = 2 AND b.sale_channel = 'ST_PAY' THEN '升腾'
    WHEN b.order_from = 2 AND b.sale_channel = 'XMF' THEN '小蜜丰'
    WHEN b.order_from = 2 AND b.sale_channel = 'ZCWL' THEN '中创物流'
    WHEN b.order_from = 2 AND b.sale_channel = 'fengxiang' THEN '丰享'
	WHEN b.order_from = 2 AND b.sale_channel = 'SF_COD' THEN '顺丰cod'
    WHEN b.order_from = 2 AND b.sale_channel = 'zxcy' THEN '正心诚意'
     WHEN b.order_from = 2 AND b.sale_channel = 'ZD' THEN '中电'
      WHEN b.order_from = 2 AND b.sale_channel = '1001' THEN '中小月结'
      WHEN b.order_from = 2 AND b.sale_channel = 'SF_FX' THEN '丰侠'
      WHEN b.order_from = 2 AND b.sale_channel = 'SYHNQ' THEN '速运湖南区兑换卡消费'
      WHEN b.order_from = 2 AND b.sale_channel = 'YKTQD' THEN '亿咖通渠道'
    WHEN b.order_from = 2 AND b.sale_channel = 'YKTKJQD' THEN '浙江亿咖通科技有限公司'
	WHEN b.order_from = 2 AND b.sale_channel = 'BJDC' THEN '福利商城'
	WHEN b.order_from = 2 AND b.sale_channel = 'ZXCYDX' THEN '正心诚意-电信'
	WHEN b.order_from = 2 AND b.sale_channel = 'ZXCY_ZXGH' THEN '正心诚意-正心关怀'
	WHEN b.order_from = 2 AND b.sale_channel = 'FSQD' THEN '飞书上的企业'  
	WHEN b.order_from = 2 AND b.sale_channel = 'WYYC' THEN '万翼云城' 
	WHEN b.order_from = 2 AND b.sale_channel = 'ZDKQD' THEN '中电科渠道' 
	WHEN b.order_from = 2 AND b.sale_channel = 'FAYD' THEN '福安移动'  
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME
  END AS sale_from,  -- 销售渠道
  CASE
    WHEN b.order_from = 1
    THEN 'bd下单'
    WHEN b.order_from = 2
    THEN '用户自主下单'
    WHEN b.order_from = 3
    THEN '企业用户下单'
  END AS order_from,  -- 订单来源
  b.freight_amount AS  freight_amount , -- '运费_订单',
  a.freight_amount AS  freight_amount_item, -- '运费_明细'
  b.supplyerid AS supplyerid,  -- 供应商ID
  REPLACE(
    REPLACE(
      REPLACE(s.group_name, CHAR(10), ''),
      CHAR(13),
      ''
    ),
    CHAR(9),
    ''
  ) AS group_name, -- 供应商名称
 b.order_date,  -- 订单日期
  b.order_user_id AS order_user_id,  -- 用户ID
  b.group_link_id,
  a.order_id AS order_id,  -- 订单号
  IF(c.parent_order_pay_id =0,c.order_id,c.parent_order_pay_id) parent_order_pay_id,
  a.order_item_id AS order_item_id,  -- 订单明细ID
  c.three_transaction_id AS three_transaction_id,  -- 第三方订单号
  c.gateway_pay_id AS gateway_pay_id ,  -- 支付网关,
  c.order_pay_id,
  h.item_name AS order_status_name,
  a.product_spec_id AS product_spec_id,  -- sku商品编码
  a.product_name,
  a.quantity AS quantity,  -- 销量
  a.purchase_unit_price AS purchase_unit_price,  -- 采购单价
  a.sale_unit_price AS sale_unit_price,  -- 销售单价
  a.origin_sale_unit_price AS origin_sale_unit_price,  -- 原销售单价
  b.order_discount_amount ,                          -- 优惠金额_订单                                                                                                                                                        
  b.coupon_total_amount  AS coupon_total_amount_order,  --  优惠券优惠金额_订单
  a.discount_total_amount AS discount_total_amount,  -- 折扣优惠总金额
  a.coupon_total_amount ,  --  优惠券优惠总金额
  a.real_total_amount AS real_total_amount,  -- 商品实收
  b.order_total_amount AS order_total_amount,  -- 最终订单结算金额
  b.sale_total_amount AS sale_total_amount,   -- 订单销售金额
  b.purchase_total_amount AS purchase_total_amount,  -- 订单采购总价
  c.pay_amount AS pay_amount,                    -- pay_订单实收  
  c.pay_discount_amount AS pay_discount_amount,  -- pay_优惠金额
  CASE
    WHEN c.pay_state = 1
    THEN '未支付'
    WHEN c.pay_state = 2
    THEN '已支付'
  END AS pay_state,  -- 支付状态
  sr.gateway_pay_id_rufound,  -- 支付网关的退款单ID   sr.gateway_pay_id
  IFNULL(sr.refund_amount, 0) AS refund_amount,  -- 退款金额                 sr.refund_amount
  a.cost_percent AS cost_percent,  -- 成本比例，单位
  c.pay_time , -- 支付日期
  b.finish_time,
  b.supply_channel
FROM
fe_dwd.dwd_group_order_refound_address_day_1_1 aaa
JOIN
  fe_goods.sf_group_order b
  ON aaa.order_id = b.order_id 
  JOIN fe_goods.sf_group_order_item a
    ON a.order_id = b.order_id
  JOIN fe_goods.sf_group_order_pay c  
    ON a.order_id = c.order_id
-- LEFT JOIN fe_goods.sf_group_order_refund_item ss ON (ss.order_item_id = a.order_item_id AND ss.data_flag = 1)  -- 订单退款表  获取明细订单的退款金额
-- LEFT JOIN fe_goods.sf_group_order_refund_pay sr ON (ss.refund_order_id = sr.refund_order_id AND sr.data_flag = 1 AND sr.state=2)
LEFT JOIN 
fe_dwd.dwd_group_order_refound_address_day_1_1_1 sr 
ON sr.order_item_id = a.order_item_id  
  LEFT JOIN
    (SELECT
      ITEM_VALUE,
      ITEM_NAME
    FROM
      fe.pub_dictionary_item
    WHERE dictionary_id = 192) d  -- 获取销售渠道
    ON b.sale_channel = d.ITEM_VALUE	
  LEFT JOIN fe_group.sf_group_supply s  -- 企业信息表  一个企业id对应一条记录
    ON s.group_id = a.supply_group_id   -- 企业ID=供应商企业ID
	AND s.data_flag =1
  LEFT JOIN
    (SELECT
      item_value,
      item_name
    FROM
      fe.pub_dictionary_item
    WHERE dictionary_id = 227) h   -- 订单状态
    ON b.order_status = h.ITEM_VALUE
WHERE a.data_flag = 1
  AND b.data_flag = 1
  AND c.pay_state = 2
  AND c.data_flag = 1 
;
CREATE INDEX idx_dwd_group_order_refound_address_day_1
ON fe_dwd.dwd_group_order_refound_address_day_1 (order_id,group_link_id);
/*
-- 测试的数据删除掉
DELETE FROM fe_dwd.dwd_group_order_refound_address_day_1  WHERE order_item_id = '21921467300900000';
-- 12月8号网易严选出故障，删除脏数据
DELETE FROM fe_dwd.dwd_group_order_refound_address_day_1  WHERE order_item_id = '23204761600670000' AND gateway_pay_id_rufound IS NULL ;
DELETE FROM fe_dwd.dwd_group_order_refound_address_day_1  WHERE order_item_id = '23204780201340007' AND gateway_pay_id_rufound IS NULL ;
*/
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_order_refound_address_day_2`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_2  AS 
SELECT 
a.order_item_id,
  a.group_link_id,
 e.link_name AS link_name,  -- 收货联系人
  e.mobile AS mobile,  -- 收货电话
  e.province AS province,  -- 收货省
  e.city AS city, -- 收货市
  e.district AS district,  -- 收货区
  REPLACE(
    REPLACE(
      REPLACE(e.address, CHAR(10), ''),
      CHAR(13),
      ''
    ),
    CHAR(9),
    ''
  ) AS address,  -- 详细地址
  f.add_time AS add_time,  -- 运单号添加时间
  REPLACE(
    REPLACE(
      REPLACE(
        e1.delivery_link_assign_id,
        CHAR(10),
        ''
      ),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS delivery_link_assign_id,  -- 发货单号
  REPLACE(
    REPLACE(
      REPLACE(f.express_number, CHAR(10), ''),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS express_number,  -- 运单号
  REPLACE(
    REPLACE(
      REPLACE(
        GROUP_CONCAT(f.express_group_name),
        CHAR(10),
        ''
      ),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS express_group_name  -- 物流公司
  FROM
fe_dwd.dwd_group_order_refound_address_day_1 a
    JOIN  fe_goods.sf_group_delivery_link e
    ON a.order_id = e.order_id    
	-- AND a.group_link_id = e.group_link_id
	AND e.data_flag=1 
    LEFT JOIN fe_goods.sf_group_delivery_link_assian e1   -- 收件人商品分派表  一个明细订单有多个运单号  剔除掉这些订单 不发散 获取发货单号
    ON e.delivery_link_id = e1.delivery_link_id
	AND a.order_item_id = e1.order_item_id
	AND e1.data_flag=1
  LEFT JOIN fe_goods.sf_group_delivery_express f  -- 订单人员快递信息表 获取运单号 物流公司 运单号添加时间 
    ON e.delivery_link_id = f.delivery_link_id   -- 收件人信息
	AND f.data_flag =1
	GROUP BY 
a.order_item_id,
  a.group_link_id;
CREATE INDEX idx_dwd_group_order_refound_address_day_2
ON fe_dwd.dwd_group_order_refound_address_day_2 (order_item_id,group_link_id);
INSERT INTO fe_dwd.dwd_group_order_refound_address_day(
pay_type,
pay_type_desc,
order_type_number,
order_type,
sale_channel,
sale_from,
order_from,
link_name,
mobile,
province,
city,
district,
address,
add_time,
delivery_link_assign_id,
express_number,
express_group_name,
freight_amount,
freight_amount_item,
supplyerid,
group_name,
order_date,
order_user_id,
parent_order_pay_id,
order_id,
order_item_id,
three_transaction_id,
gateway_pay_id,
order_pay_id,
order_status_name,
product_spec_id,
product_name,
quantity,
purchase_unit_price,
sale_unit_price,
origin_sale_unit_price,
order_discount_amount ,
coupon_total_amount_order,
discount_total_amount,
coupon_total_amount,
real_total_amount,
order_total_amount,
sale_total_amount,
purchase_total_amount,
pay_amount,
pay_discount_amount,
pay_state,
gateway_pay_id_rufound,
refund_amount,
cost_percent,
pay_time,
finish_time,
supply_channel,
load_time
)
SELECT
a.pay_type,
a.pay_type_desc,
a.order_type_number,
a.order_type,
a.sale_channel,
a.sale_from,
a.order_from,
b.link_name,
b.mobile,
b.province,
b.city,
b.district,
b.address,
b.add_time,
b.delivery_link_assign_id,
b.express_number,
b.express_group_name,
a.freight_amount,
a.freight_amount_item,
a.supplyerid,
a.group_name,
a.order_date,
a.order_user_id,
a.parent_order_pay_id,
a.order_id,
a.order_item_id,
a.three_transaction_id,
a.gateway_pay_id,
a.order_pay_id,
a.order_status_name,
a.product_spec_id,
a.product_name,
a.quantity,
a.purchase_unit_price,
a.sale_unit_price,
a.origin_sale_unit_price,
a.order_discount_amount ,
a.coupon_total_amount_order,
a.discount_total_amount,
a.coupon_total_amount,
a.real_total_amount,
a.order_total_amount,
a.sale_total_amount,
a.purchase_total_amount,
a.pay_amount,
a.pay_discount_amount,
a.pay_state,
a.gateway_pay_id_rufound,
a.refund_amount,
a.cost_percent,
a.pay_time,
a.finish_time,
a.supply_channel,
ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1 DAY) AS load_time 
FROM 
fe_dwd.dwd_group_order_refound_address_day_1 a 
LEFT JOIN
fe_dwd.dwd_group_order_refound_address_day_2 b 
ON a.order_item_id = b.order_item_id    ;
 
 
-- 盘点宽表
-- 先找出发生变化的订单号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_check_base_day_inc_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_check_base_day_inc_1 AS
SELECT 
DISTINCT c.check_id
FROM fe.sf_shelf_check_detail  c     
WHERE  c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date
UNION 
SELECT 
DISTINCT c.check_id
FROM fe.sf_shelf_check  c     
WHERE  c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date
;
CREATE INDEX idx_dwd_check_base_day_inc_1
ON fe_dwd.dwd_check_base_day_inc_1 (check_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_check_base_day_inc_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_check_base_day_inc_2 AS
    SELECT 
     a.DETAIL_ID              
    ,a.CHECK_ID               
    ,a.SHELF_ID               
    ,a.SHELF_CODE             
    ,a.PRODUCT_ID             
    ,a.STOCK_NUM              
    ,a.CHECK_NUM              
    ,a.total_error_num        
    ,a.ERROR_NUM              
    ,a.SALE_PRICE             
    ,a.ERROR_REASON           
    ,a.ERROR_PHOTO            
    ,a.production_date        
    ,a.production_date_photo  
    ,a.AUDIT_ERROR_NUM        
    ,a.AUDIT_STATUS           
    ,a.AUDIT_USER_ID          
    ,a.AUDIT_USER_NAME        
    ,a.AUDIT_TIME             
    ,a.AUDIT_TYPE             
    ,a.ATTRIBUTE1             
    ,a.ATTRIBUTE2             
    ,a.AUDIT_REMARK           
    ,a.REMARK                 
    ,a.ADD_TIME               
    ,a.ADD_USER_ID            
    ,a.LAST_UPDATE_USER_ID    
    ,a.LAST_UPDATE_TIME       
    ,a.DATA_FLAG              
    ,a.danger_flag            
    ,a.risk_source            
    ,a.auto_check_flag        
    ,a.date_empty_flag  
    ,b.OPERATE_TIME           
    ,b.OPERATOR_ID            
    ,b.OPERATOR_NAME          
    ,b.check_type             
    ,b.CHECK_STATUS           
    ,b.SHELF_PHOTO            
    ,b.PHOTO_AUDIT_STATUS     
    ,b.PHOTO_NOPASS_REASON    
    ,b.PHOTO_AUDIT_USER_ID    
    ,b.PHOTO_AUDIT_TIME  
    ,CURRENT_TIMESTAMP AS load_time
    FROM fe_dwd.dwd_check_base_day_inc_1 aa
	JOIN
	fe.sf_shelf_check_detail a 
	ON aa.check_id = a.check_id
    LEFT JOIN fe.sf_shelf_check b   
      ON  a.check_id = b.check_id 
	  AND a.data_flag=1 
	  AND b.data_flag=1
    WHERE b.OPERATE_TIME >= '2020-01-01' ;  --  只更新2020年的。因为宽表只有2020的数据
CREATE INDEX idx_dwd_check_base_day_inc_2
ON fe_dwd.dwd_check_base_day_inc_2 (check_id);
	
-- 删掉发生变化的订单
DELETE a.* FROM fe_dwd.dwd_check_base_day_inc a 
JOIN fe_dwd.dwd_check_base_day_inc_1 b 
ON a.check_id = b.check_id;	
INSERT INTO fe_dwd.dwd_check_base_day_inc 
SELECT * FROM fe_dwd.dwd_check_base_day_inc_2; 
 
-- 邀请活动表主表
-- 先找出发生变化的订单号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_activity_invitation_information_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_activity_invitation_information_1 AS
SELECT 
DISTINCT c.invite_id
FROM fe_activity.sf_activity_invitation  c     
WHERE  c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date
UNION 
SELECT 
DISTINCT c.invite_id
FROM fe_activity.sf_activity_invitation_detail  c     
WHERE  c.data_flag =1  
AND DATEDIFF(last_update_time,add_time) >=1
AND c.last_update_time > @start_date;
CREATE INDEX idx_dwd_activity_invitation_information_1
ON fe_dwd.dwd_activity_invitation_information_1 (invite_id);
-- 删掉发生变化的订单
DELETE a.* FROM fe_dwd.dwd_activity_invitation_information a 
JOIN fe_dwd.dwd_activity_invitation_information_1 b 
ON a.invite_id = b.invite_id;
INSERT INTO fe_dwd.`dwd_activity_invitation_information`
(
`invite_id`
,`activity_id`
,`inviter_user_id`
,`invite_type`
,`invite_status`
,`prize_record_id`
,`is_over`
,`invite_count`
,`prize_type`
,`reward`
,`remark`
,`invitee_user_id`
,`invitee_invite_status`
,`invitee_prize_record_id`
,`invitee_prize_type`
,`rinvitee_eward`
,`invitee_remark`
,add_time
,add_time_detail
)
SELECT 
a.`invite_id`
,a.`activity_id`
,a.`inviter_user_id`
,a.`invite_type`
,a.`invite_status`
,a.`prize_record_id`
,a.`is_over`
,a.`invite_count`
,a.`prize_type`
,a.`reward`
,a.`remark`
,b.`invitee_user_id`
,b.invite_status AS `invitee_invite_status`
,b.prize_record_id AS`invitee_prize_record_id`
,b.prize_type AS `invitee_prize_type`
,b.reward AS `rinvitee_eward`
,b.remark AS `invitee_remark`
,a.add_time
,b.add_time add_time_detail
FROM
fe_dwd.dwd_activity_invitation_information_1 aa
JOIN 
fe_activity.sf_activity_invitation a
ON aa.invite_id =a.invite_id 
LEFT JOIN fe_activity.sf_activity_invitation_detail b 
ON a.invite_id = b.invite_id 
AND a.data_flag = 1 
AND b.data_flag =1
; 
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_update_dwd_table_info',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  
COMMIT;
END