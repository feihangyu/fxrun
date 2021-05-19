CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_group_order_coupon_day`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
#商城订单临时表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_coupon_sc_order_temp;																	
CREATE TEMPORARY TABLE fe_dwd.dwd_coupon_sc_order_temp(KEY(order_id),KEY(order_date),KEY(pay_time))
SELECT d.order_date,d.pay_time,d.order_id,SUM(IFNULL(d.coupon_total_amount,0)) coupon_total_amount,
SUM(IFNULL(d.origin_sale_unit_price,d.sale_unit_price) * d.quantity + IFNULL(d.freight_amount, 0)) AS order_gmv,#GMV金额
SUM(IFNULL(d.refund_amount,0)) refund_amount #退款金额
#'shop' AS coupon_order_type#货架或者商城的订单类型,shop:商城 shelf：货架
FROM fe_dwd.dwd_group_order_refound_address_day d 
WHERE 1=1 
#按照d.pay_time增量抽取每天的数据
AND d.pay_time>= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) AND d.pay_time < CURRENT_DATE
GROUP BY order_id;
#货架订单临时表
DROP TEMPORARY TABLE IF EXISTS test.dwd_coupon_shelf_order_temp;																	
CREATE TEMPORARY TABLE test.dwd_coupon_shelf_order_temp(KEY(order_id),KEY(order_date),KEY(pay_date))
SELECT r.order_date,r.pay_date,r.order_id,
SUM(IFNULL(r.COUPON_AMOUNT,0)) COUPON_AMOUNT,
SUM(IFNULL(r.sale_price,r.purchase_price) * IFNULL(r.quantity,0)) order_gmv,#GMV金额
SUM(IFNULL(r.refund_amount,0)) refund_amount,#退款金额
CURRENT_TIMESTAMP AS add_time
#'shelf' AS coupon_order_type #货架或者商城的订单类型,shop:商城 shelf：货架
FROM fe_dwd.dwd_order_item_refund_day r
WHERE 1=1
AND r.pay_date>= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) AND r.pay_date < CURRENT_DATE
GROUP BY r.order_id;
DELETE FROM fe_dwd.dwd_group_order_coupon_day WHERE pay_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) AND pay_time < CURRENT_DATE;
INSERT INTO fe_dwd.dwd_group_order_coupon_day
(order_date
,pay_time
,received_time
,valid_start_time
,valid_end_time
,coupon_id
,coupon_name
,order_id
,coupon_total_amount
,coupon_status
,user_id,used_time
,discount_type
,coupon_usage
,discount_amount
,coupon_type
,cost_dept
,order_gmv
,refund_amount
,add_time
,coupon_order_type
)
#商城部分
SELECT 
d.order_date order_date,#订单日期
d.pay_time,#支付时间
c.received_time, #领取时间,
c.valid_start_time, #有效开始时间,
c.valid_end_time, #有效结束时间,
c.coupon_id, #优惠券ID,
m.coupon_name, #优惠券名称,
d.order_id, #订单ID
d.coupon_total_amount coupon_total_amount,#优惠券优惠金额
c.coupon_status, #优惠券状态,
c.user_id, #领券人,
c.used_time, #优惠券使用时间,
m.discount_type, #折扣类型, #（1: 满减 2: 立减 3: 折扣）
m.coupon_usage, #优惠券用途,
m.discount_amount, #折扣金额,#（折扣时，是折扣值，7表示7折，其他优惠类型表示优惠券的金额）
m.coupon_type, #1:通用券 2:商品券 3:品类券 4:尝鲜券
m.cost_dept, #费用归属部门,#（1市场组,2运营组,3采购组,4大客户组,5BD组,6经规组）
d.order_gmv,#GMV金额
d.refund_amount,#退款金额
CURRENT_TIMESTAMP AS add_time,
'shop' AS coupon_order_type#货架或者商城的订单类型,shop:商城 shelf：货架
FROM fe.sf_coupon_use c
LEFT JOIN fe_dwd.dwd_coupon_sc_order_temp d ON c.order_id=d.order_id
LEFT JOIN fe.sf_coupon_model m ON m.coupon_id=c.coupon_id
WHERE c.data_flag=1 AND m.data_flag=1 
#按c.add_time增量抽取每天的数据
AND c.add_time>= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) AND c.add_time < CURRENT_DATE
#货架部分
UNION ALL
SELECT 
r.order_date,#订单日期
r.pay_date pay_time,#支付时间
c.received_time, #领取时间,
c.valid_start_time, #有效开始时间,
c.valid_end_time, #有效结束时间,
c.coupon_id, #优惠券ID,
m.coupon_name, #优惠券名称,
r.order_id, #订单ID
r.COUPON_AMOUNT coupon_total_amount,#优惠券优惠金额
c.coupon_status, #优惠券状态,
c.user_id, #领券人,
c.used_time, #优惠券使用时间,
m.discount_type, #折扣类型, #（1: 满减 2: 立减 3: 折扣）
m.coupon_usage, #优惠券用途,
m.discount_amount, #折扣金额,#（折扣时，是折扣值，7表示7折，其他优惠类型表示优惠券的金额）
m.coupon_type, #1:通用券 2:商品券 3:品类券 4:尝鲜券
m.cost_dept, #费用归属部门,#（1市场组,2运营组,3采购组,4大客户组,5BD组,6经规组）
r.order_gmv,#GMV金额
r.refund_amount,#退款金额
CURRENT_TIMESTAMP AS add_time,
'shelf' AS coupon_order_type #货架或者商城的订单类型,shop:商城 shelf：货架
FROM fe.sf_coupon_use c
LEFT JOIN test.dwd_coupon_shelf_order_temp r ON r.order_id=c.order_id
LEFT JOIN fe.sf_coupon_model m ON m.coupon_id=c.coupon_id
WHERE c.data_flag=1 AND m.data_flag=1
AND c.add_time>= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) AND c.add_time < CURRENT_DATE;
-- order_date 应该是用 pay_time 支付时间增量更新
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_group_order_coupon_day',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('郑志省@', @user, @timestamp)
  );
  
COMMIT;	
END