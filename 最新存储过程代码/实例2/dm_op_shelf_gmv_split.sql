CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_gmv_split`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SELECT @month_id := DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 DAY),'%Y-%m'),
       @month_start := CONCAT(@month_id,'-01'),
       @month_end := ADDDATE(LAST_DAY(@month_start),1);     
#以下为补全数据做的日期设置   
#SELECT @sdate := sdate,
#       @month_id := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 1 DAY),'%Y-%m'),
#       @month_start := CONCAT(@month_id,'-01'),
#       @month_end := ADDDATE(LAST_DAY(@month_start),1);     
SET @run_date := CURRENT_DATE;
SET @add_user := CURRENT_USER;
SET @timestamp := CURRENT_TIMESTAMP;
-- 201901-201909通过下单时间计算
DROP TEMPORARY TABLE IF EXISTS fe_dm.sale_tmp;
CREATE TEMPORARY TABLE fe_dm.sale_tmp (PRIMARY KEY (order_id))
SELECT shelf_id,
       order_id,
       user_id,
       SUM(IFNULL(quantity_act * sale_price,0)) gmv,
       IFNULL(discount_amount,0)discount_amount,-- 折扣
       IFNULL(coupon_amount,0)coupon_amount,-- 优惠券
       IFNULL(third_discount_amount,0)third_discount_amount,-- 第三方
       payment_type_gateway
FROM fe_dwd.dwd_order_item_refund_day
WHERE order_date >= @month_start
AND order_date < @month_end
GROUP BY order_id;
-- 201910以后通过支付时间计算,10月、12月数据会和经规汇报给集团的gmv少2万多,原因核查不出来
DROP TEMPORARY TABLE IF EXISTS fe_dm.sale_tmp;
CREATE TEMPORARY TABLE fe_dm.sale_tmp (PRIMARY KEY (order_id))
SELECT shelf_id,
       order_id,
       user_id,
       SUM(IFNULL(quantity_act * sale_price,0)) AS gmv,
       o_discount_amount discount_amount,-- 折扣
       coupon_amount,-- 优惠券
       IFNULL(third_discount_amount,0) AS third_discount_amount,-- 第三方
       t.payment_type_gateway
FROM fe_dwd.dwd_order_item_refund_day t
WHERE pay_date >= @month_start
AND pay_date < @month_end
GROUP BY order_id;
-- 每日更新，每月1日结存上月数据
DELETE FROM fe_dm.dm_op_shelf_gmv_split WHERE month_id=@month_id;
INSERT INTO fe_dm.dm_op_shelf_gmv_split
(month_id,
shelf_id,
discount_order,
discount_gmv,
coupon_order,
coupon_gmv,
third_dis_order,
third_gmv,
over100_order,
order100_gmv,
normal_pay_type_gmv,
user_num,
load_time
)
SELECT @month_id month_id,
       shelf_id,
       COUNT(DISTINCT CASE WHEN discount_amount > 0 THEN order_id END)discount_order,-- 折扣订单数
       SUM(CASE WHEN discount_amount > 0 THEN gmv END)discount_gmv,-- 折扣订单gmv
       COUNT(DISTINCT CASE WHEN coupon_amount > 0 THEN order_id END)coupon_order,-- 优惠券订单数
       SUM(CASE WHEN coupon_amount > 0 THEN gmv END)coupon_gmv,-- 优惠券订单gmv
       COUNT(DISTINCT CASE WHEN third_discount_amount > 0 THEN order_id END)third_dis_order,-- 第三方优惠订单数
       SUM(CASE WHEN third_discount_amount > 0 THEN gmv END)third_gmv,-- 第三方优惠gmv
       COUNT(CASE WHEN gmv >= 100 THEN order_id END)over100_order,-- 单笔订单>=100的订单数
       SUM(CASE WHEN gmv >= 100 THEN gmv END)order100_gmv,-- 单笔订单>=100的gmv
       SUM(CASE WHEN payment_type_gateway LIKE '%WeiXin%' OR payment_type_gateway LIKE '%微信%' OR payment_type_gateway LIKE '%WX%' OR payment_type_gateway = 'EPay' OR payment_type_gateway = 'foodCard' THEN gmv END)normal_pay_type_gmv, -- 202010后主流支付途径gmv(微信、餐卡、E币)
       COUNT(DISTINCT user_id)user_num, -- 月用户数去重
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.sale_tmp
GROUP BY shelf_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_gmv_split',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_gmv_split','dm_op_shelf_gmv_split','朱星华');
  COMMIT;	
END