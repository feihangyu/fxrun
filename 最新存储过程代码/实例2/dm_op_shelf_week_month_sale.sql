CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_week_month_sale`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE,
    @sub_1 := SUBDATE(@sdate,1),
    @sunday := SUBDATE(@sdate,WEEKDAY(@sdate) + 1),
    @week_start := SUBDATE(@sunday,6),
    @week_end := ADDDATE(@sunday,1), 
    @month_id := DATE_FORMAT(@sub_1, '%Y-%m'),
    @month_start := SUBDATE(@sub_1, DAY(@sub_1) - 1),
    @month_end := ADDDATE(LAST_DAY(@month_start),1);
-- 已激活货架
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.shelf_type,
       IF(a.activate_time < @month_start,1,0)is_history_shelf -- 激活在本月1日前的记为存量
FROM fe_dwd.`dwd_shelf_base_day_all` a
WHERE a.shelf_type != 9
AND a.shelf_status = 2
AND (a.shelf_name NOT LIKE '%测试%' OR shelf_id IN('86662','86664'));
-- 周补付款
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_wafter_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_wafter_tmp (PRIMARY KEY (shelf_id))
SELECT shelf_id,
       SUM(IFNULL(AFTER_PAYMENT_MONEY,0)) week_afterpay
FROM fe_dwd.dwd_shelf_day_his  -- fjr_shelf_dgmv
WHERE sdate >= @week_start
AND sdate < @week_end
GROUP BY shelf_id;
-- 月补付款
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_mafter_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_mafter_tmp (PRIMARY KEY (shelf_id))
SELECT shelf_id,
       SUM(IFNULL(AFTER_PAYMENT_MONEY,0)) afterpay
FROM fe_dwd.dwd_shelf_day_his  -- fjr_shelf_dgmv
WHERE sdate >= @month_start
AND sdate < @month_end
GROUP BY shelf_id;
-- 对接系统货架周销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_wsale_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_wsale_tmp (PRIMARY KEY (shelf_id))
SELECT shelf_id,
       SUM(IFNULL(week_amount,0)) week_amount,
       SUM(IFNULL(week_gmv,0)) week_gmv,
       SUM(IFNULL(week_pay_total,0)) week_pay_total,
       SUM(IFNULL(week_discount,0)) week_discount,
       SUM(IFNULL(week_coupon,0)) week_coupon,
       SUM(IFNULL(week_orders,0)) week_orders,
       SUM(IFNULL(week_users,0)) week_users
       
FROM
(
SELECT w.shelf_id,
       w.qty_sal AS week_amount,
       w.gmv AS week_gmv,
       w.o_product_total_amount AS week_pay_total,
       w.o_discount_amount AS week_discount,
       w.o_coupon_amount AS week_coupon,
       w.orders AS week_orders,
       a.users AS week_users
FROM fe_dm.dm_shelf_wgmv w  -- fjr_shelf_wgmv
LEFT JOIN
(SELECT shelf_id,
        COUNT(DISTINCT user_id)users
FROM fe_dwd.dwd_pub_order_item_recent_two_month
WHERE pay_date >= @week_start
AND pay_date < @week_end
GROUP BY shelf_id
)a ON w.shelf_id = a.shelf_id
WHERE w.sdate = @sunday
UNION ALL
SELECT shelf_id,
       SUM(IFNULL(amount,0)) week_amount,
       SUM(IFNULL(total,0)) week_gmv,
       SUM(IFNULL(pay_total,0)) week_pay_total,
       SUM(IFNULL(discount,0)) week_discount,
       0 AS coupon,
       COUNT(DISTINCT order_id)week_orders,
       COUNT(DISTINCT user_id)week_users
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @week_start
AND pay_date < @week_end
AND refund_status = '无'
GROUP BY shelf_id
)a
GROUP BY shelf_id;
-- 周架均数据 每周一更新并截存上周数据
if weekday(@sdate)=0 then
DELETE FROM fe_dm.dm_op_shelf_week_sale WHERE week_end = @sunday ;
INSERT INTO fe_dm.dm_op_shelf_week_sale
(week_end
,shelf_id
,is_history_shelf
,week_amount
,week_gmv
,week_afterpay
,week_pay_total
,week_discount
,week_coupon
,week_orders
,week_users
)
SELECT @sunday week_end,
       s.shelf_id,
       s.is_history_shelf,
       t.week_amount,
       t.week_gmv,
       a.week_afterpay,
       t.week_pay_total,
       t.week_discount, 
       t.week_coupon,
       t.week_orders,
       t.week_users
FROM fe_dm.shelf_tmp s
LEFT JOIN fe_dm.shelf_wsale_tmp t ON s.shelf_id = t.shelf_id
LEFT JOIN fe_dm.shelf_wafter_tmp a ON s.shelf_id = a.shelf_id;
end if;
-- 对接系统货架月销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_msale_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_msale_tmp (PRIMARY KEY (shelf_id))
SELECT a.shelf_id,
       SUM(IFNULL(amount,0)) amount,
       SUM(IFNULL(gmv,0)) gmv,
       SUM(IFNULL(pay_total,0)) pay_total,
       SUM(IFNULL(discount,0)) discount, 
       SUM(IFNULL(coupon,0)) coupon,
       SUM(IFNULL(orders,0)) orders,
       SUM(IFNULL(users,0)) users
FROM
(SELECT m.shelf_id,
        m.qty_sal AS amount,
        m.gmv,
        m.o_product_total_amount AS pay_total,
        m.o_discount_amount AS discount,
        m.o_coupon_amount AS coupon,
        m.orders,
        a.users
FROM fe_dm.dm_shelf_mgmv m  -- fjr_shelf_mgmv
LEFT JOIN
(SELECT shelf_id,
        COUNT(DISTINCT user_id)users
FROM fe_dwd.dwd_pub_order_item_recent_two_month
WHERE pay_date >= @month_start
AND pay_date < @month_end
GROUP BY shelf_id
)a ON m.shelf_id = a.shelf_id
WHERE m.month_id = @month_id
UNION ALL
SELECT shelf_id,
       SUM(IFNULL(amount,0)) amount,
       SUM(IFNULL(total,0)) gmv,
       SUM(IFNULL(pay_total,0)) pay_total,
       SUM(IFNULL(discount,0)) discount,
       0 AS coupon,
       COUNT(DISTINCT order_id)orders,
       COUNT(DISTINCT user_id)users
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @month_start
AND pay_date < @month_end
AND refund_status = '无'
GROUP BY shelf_id
)a
GROUP BY shelf_id;
-- 月架均数据 已激活货架架均月销售，需每日更新，每月1日截存上月数据
DELETE FROM fe_dm.dm_op_shelf_month_sale WHERE month_id=@month_id;
INSERT INTO fe_dm.dm_op_shelf_month_sale
(month_id
,shelf_id
,is_history_shelf
,amount
,gmv
,afterpay
,pay_total
,discount
,coupon
,orders
,users
)
SELECT @month_id month_id,
       s.shelf_id,
       s.is_history_shelf,
       t.amount,
       t.gmv,
       a.afterpay,
       t.pay_total,
       t.discount,
       t.coupon,
       t.orders,
       t.users
FROM fe_dm.shelf_tmp s
LEFT JOIN fe_dm.shelf_msale_tmp t ON s.shelf_id = t.shelf_id
LEFT JOIN fe_dm.shelf_mafter_tmp a ON s.shelf_id = a.shelf_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_week_month_sale',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_week_sale','dm_op_shelf_week_month_sale','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_month_sale','dm_op_shelf_week_month_sale','朱星华');
  COMMIT;	
END