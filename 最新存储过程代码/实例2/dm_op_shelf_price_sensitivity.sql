CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_price_sensitivity`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate, 1);
SET @month_start := SUBDATE(@sub_1, DAY(@sub_1) - 1);
SET @month_id := DATE_FORMAT(@sub_1, '%Y-%m');	   
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT business_name,
       shelf_id,
       shelf_type_desc shelf_type
FROM fe_dwd.dwd_shelf_base_day_all
WHERE shelf_type IN (1,2,3,5,8)
AND shelf_status = 2
AND revoke_status = 1
AND activate_time <= @month_start;
-- 货架月销售(剔除订单金额>100)
DROP TEMPORARY TABLE IF EXISTS fe_dm.sale_tmp;
CREATE TEMPORARY TABLE fe_dm.sale_tmp(PRIMARY KEY(shelf_id))
SELECT s.business_name,
       t.shelf_id, 
       SUM(IF(t.refund_amount > 0,t.quantity_act,t.quantity) * t.sale_price) gmv,
       COUNT(DISTINCT t.order_id) orders,-- 总订单数
       IFNULL(COUNT(DISTINCT CASE WHEN t.o_discount_amount > 0 OR t.o_coupon_amount > 0 THEN t.order_id END),0)discount_order,-- 有优惠的订单数
       COUNT(DISTINCT user_id)users,-- 总用户数
       IFNULL(COUNT(DISTINCT CASE WHEN b.sex IN (1,2) THEN t.user_id END),0) sex_users,-- 有性别信息的总用户数
       IFNULL(COUNT(DISTINCT CASE WHEN b.sex = 1 THEN user_id END),0)male_user,-- 男性用户数
       IFNULL(COUNT(DISTINCT CASE WHEN b.sex = 2 THEN user_id END),0)female_user,-- 女性用户数
       IFNULL(SUM(t.o_discount_amount  * t.sale_price * t.quantity_act / t.ogmv),0) discount,  -- 折算后折扣金额
       IFNULL(SUM(t.o_coupon_amount * t.sale_price * t.quantity_act / t.ogmv),0) coupon,   -- 折算后优惠券金额
       IFNULL(SUM(t.o_third_discount_amount * t.sale_price * t.quantity_act / t.ogmv),0) third_discount    -- 折算后第三方优惠金额
FROM fe_dwd.dwd_pub_order_item_recent_two_month t
JOIN fe_dm.shelf_tmp s ON t.shelf_id = s.shelf_id
LEFT JOIN
(SELECT m.user_id AS member_id,
        m.gender AS sex
 FROM fe_dwd.dwd_user_day_inc m
)b ON t.user_id = b.member_id
WHERE t.pay_date >= @month_start
AND t.pay_date < @sdate
AND ogmv <= 100
GROUP BY t.shelf_id;
-- 货架订单优惠占比、平均折扣占比、平均优惠占比、支付优惠占比、女性用户占比、平均优惠占比排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.rate_tmp;
CREATE TEMPORARY TABLE fe_dm.rate_tmp(PRIMARY KEY(shelf_id))
SELECT business_name,
       shelf_id,
       gmv,
       orders,
       discount_order,
       users,
       sex_users,
       female_user,
       discount,
       coupon,
       third_discount,
       dis_order_rate,
       dis_rate,
       coupon_rate,
       third_rate,
       female_rate,
       dis_rate * 0.5 + coupon_rate * 5 + third_rate * 0.5 + dis_order_rate + female_rate * 0.5 price_sensitivity,
       IF(@pre := coupon_rate,@rank := @rank + 1,@rank) coupon_rank
FROM
(
SELECT business_name,
       shelf_id,
       gmv,
       orders,
       discount_order,
       users,
       sex_users,
       female_user,
       discount,
       coupon,
       third_discount,
       discount_order / orders dis_order_rate,
       discount / gmv dis_rate,
       coupon / gmv coupon_rate,
       third_discount / gmv third_rate,
       IF(sex_users < 10,0.5,female_user / sex_users) female_rate
FROM fe_dm.sale_tmp
ORDER BY coupon_rate DESC
)a,(SELECT @pre := NULL,@rank := 0)r;
-- 货架价格敏感度降序排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.price_sensitivity_tmp;
CREATE TEMPORARY TABLE fe_dm.price_sensitivity_tmp(PRIMARY KEY(shelf_id))
SELECT business_name,
       shelf_id,
       price_sensitivity,
       IF(@pre := price_sensitivity,@rank := @rank + 1,@rank) rank
FROM
(
SELECT business_name,
       shelf_id,
       price_sensitivity
FROM fe_dm.rate_tmp
ORDER BY price_sensitivity ASC
)a,(SELECT @pre := NULL,@rank := 1)r;
-- 货架平均优惠占比最大排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.max_coupon_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.max_coupon_rank_tmp AS
SELECT MAX(coupon_rank)max_coupon_rank
FROM fe_dm.rate_tmp;
-- 货架价格敏感度最大排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.max_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.max_rank_tmp AS
SELECT MAX(rank)max_rank
FROM fe_dm.price_sensitivity_tmp;
-- 货架排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.rank_tmp;
CREATE TEMPORARY TABLE fe_dm.rank_tmp AS
SELECT r.business_name,
       r.shelf_id,
       r.gmv,
       r.orders,
       r.discount_order,
       r.users,
       r.sex_users,
       r.female_user,
       r.discount,
       r.coupon,
       r.third_discount,
       r.dis_order_rate,
       r.dis_rate,
       r.coupon_rate,
       r.third_rate,
       r.female_rate,
       t.price_sensitivity,
       r.coupon_rank, -- 平均优惠占比排名
       t.rank,        -- 价格敏感系数排名
       (SELECT max_coupon_rank FROM fe_dm.max_coupon_rank_tmp) max_coupon_rank,
       (SELECT max_rank FROM fe_dm.max_rank_tmp)max_rank
FROM fe_dm.rate_tmp r
LEFT JOIN fe_dm.price_sensitivity_tmp t ON r.shelf_id = t.shelf_id;
-- 无人货架价格敏感度 每日更新，每月1日截存上月数据
DELETE FROM fe_dm.dm_op_shelf_price_sensitivity WHERE month_id = @month_id;
INSERT INTO fe_dm.dm_op_shelf_price_sensitivity
(month_id
,business_name
,shelf_id
,gmv
,orders
,discount_order
,users
,sex_users
,female_user
,discount
,coupon
,third_discount
,dis_order_rate
,dis_rate
,coupon_rate
,third_rate
,female_rate
,price_sensitivity
,coupon_rank
,rank
,sensitive_level
,load_time
)
SELECT @month_id month_id,
       business_name,
       shelf_id,
       gmv,
       orders,
       discount_order,
       users,
       sex_users,
       female_user,
       discount,
       coupon,
       third_discount,
       dis_order_rate,
       dis_rate,
       coupon_rate,
       third_rate,
       female_rate,
       price_sensitivity,
       coupon_rank, -- 平均优惠占比排名
       rank,        -- 价格敏感系数排名
       CASE WHEN coupon_rank <= max_coupon_rank * 0.03 THEN '用券敏感'
            WHEN rank >= max_rank * 0.8 THEN '敏感'
            WHEN rank >= max_rank * 0.3 AND rank <= max_rank * 0.8 THEN '一般敏感'
       ELSE '不敏感' END AS sensitive_level,
       @timestamp AS load_time
FROM fe_dm.rank_tmp
HAVING gmv >= 200;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_price_sensitivity',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华（唐进）@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_price_sensitivity','dm_op_shelf_price_sensitivity','朱星华（唐进）');
 
END