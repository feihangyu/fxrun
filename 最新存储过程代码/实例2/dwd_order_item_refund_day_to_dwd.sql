CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_order_item_refund_day_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 订单宽表数据从fe_temp 到 fe_dwd 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_order_item_refund_day_tmp1;
CREATE TEMPORARY TABLE fe_dwd.dwd_order_item_refund_day_tmp1 AS
SELECT a.order_id,a.order_item_id 
FROM 
fe_temp.dwd_order_item_refund_day b -- 小表  104362  10万   小表要放在前面作为驱动表，加快速度
 JOIN fe_dwd.dwd_order_item_refund_day a   -- 大表 67434465 6700万
    ON a.order_id = b.order_id AND a.order_item_id=b.order_item_id
    AND b.load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;   
CREATE INDEX idx_order_item_id
ON fe_dwd.dwd_order_item_refund_day_tmp1(order_id);
DELETE a.* FROM fe_dwd.dwd_order_item_refund_day a   -- 先删除共同的部分  按照订单号删除即可
JOIN  fe_dwd.dwd_order_item_refund_day_tmp1  b
    ON a.order_id = b.order_id AND a.order_item_id=b.order_item_id;
INSERT INTO fe_dwd.dwd_order_item_refund_day (
row_id
,order_id
,ORDER_ITEM_ID
,order_status
,order_type
,order_date
,pay_id
,PAY_DATE
,PAYMENT_TYPE_GATEWAY
,PAY_TYPE
,PAY_AMOUNT
,pay_amount_product
,PAY_STATE
,pay_method
,pay_merchant_id
,user_id
,shelf_id
,product_id
,SUPPLIER_ID
,LIMIT_BUY_ID
,quantity
,quantity_shipped
,COST_PRICE
,quantity_act
,sale_price
,purchase_price
,o_discount_amount
,discount_amount
,REAL_TOTAL_PRICE
,COUPON_AMOUNT
,INTEGRAL_DISCOUNT
,third_discount_amount
,PRODUCT_TOTAL_AMOUNT
,COMMIS_TOTAL_AMOUNT
,refund_order_id
,refund_amount
,refund_finish_time
,apply_time
,load_time
)
SELECT 
a.row_id
,a.order_id
,a.ORDER_ITEM_ID
,a.order_status
,a.order_type
,a.order_date
,a.pay_id
,a.PAY_DATE
,a.PAYMENT_TYPE_GATEWAY
,a.PAY_TYPE
,a.PAY_AMOUNT
,a.pay_amount_product
,a.PAY_STATE
,a.pay_method
,a.pay_merchant_id
,a.user_id
,a.shelf_id
,a.product_id
,a.SUPPLIER_ID
,a.LIMIT_BUY_ID
,a.quantity
,a.quantity_shipped
,a.COST_PRICE
,a.quantity_act
,a.sale_price
,a.purchase_price
,o_discount_amount
,a.discount_amount
,a.REAL_TOTAL_PRICE
,a.COUPON_AMOUNT
,a.INTEGRAL_DISCOUNT
,a.third_discount_amount
,a.PRODUCT_TOTAL_AMOUNT
,a.COMMIS_TOTAL_AMOUNT
,a.refund_order_id
,a.refund_amount
,a.refund_finish_time
,a.apply_time
,a.load_time
FROM
  fe_temp.dwd_order_item_refund_day a
WHERE a.load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;   
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_order_item_refund_day_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_order_item_refund_day','dwd_order_item_refund_day_to_dwd','李世龙');
 
  COMMIT;	
END