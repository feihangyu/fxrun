CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_paytype_dashboard_daily`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @time0='00:28',@time1='00:35';
DELETE FROM feods.d_ma_paytype_dashboard_daily WHERE sdate>=IF(CURTIME() BETWEEN @time0 AND @time1,SUBDATE(p_sdate,3),p_sdate) AND sdate<ADDDATE(p_sdate,1) OR sdate<SUBDATE(p_sdate,31) ;
INSERT INTO feods.d_ma_paytype_dashboard_daily
    (sdate, city_name, payment_type, gmv, amount, after_pay_amount, discount_amount, coupon_amount, order_num)
SELECT sdate, city_name, payment_type
     ,SUM(gmv) gmv,SUM(amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(discount_amount) discount_amount,SUM(coupon_amount) coupon_amount,SUM(order_num) order_num
FROM feods.d_ma_shelf_paytype_sale_daily
WHERE sdate>=IF(CURTIME() BETWEEN @time0 AND @time1 ,SUBDATE(p_sdate,3),p_sdate) AND sdate<ADDDATE(p_sdate,1)
GROUP BY sdate, city_name, payment_type
;
# 用户数
UPDATE feods.d_ma_paytype_dashboard_daily t1
JOIN
    (SELECT sdate,city_name,payment_type,COUNT(DISTINCT user_id) user_num
    FROM
        (SELECT DATE(a1.ORDER_DATE) sdate,a1.SHELF_ID,SUBSTRING_INDEX(SUBSTRING_INDEX(AREA_ADDRESS, ',', 2), ',', - 1)  city_name
            ,CASE WHEN a1.PAYMENT_TYPE_NAME IN ('E币支付','餐卡支付','顺手付云闪付','小蜜蜂积分支付','招行一卡通') THEN a1.PAYMENT_TYPE_NAME  WHEN a1.PAYMENT_TYPE_NAME LIKE '微信%' THEN '微信' ELSE 'other' END payment_type
            , a1.USER_ID
        FROM fe.sf_order a1
        JOIN fe.sf_order_item a2 ON a1.ORDER_ID = a2.ORDER_ID AND a2.DATA_FLAG = 1
        LEFT JOIN fe.sf_order_refund_order a3 ON a1.ORDER_ID = a3.order_id AND a3.refund_status = 5 AND a3.data_flag = 1
        LEFT JOIN fe.sf_shelf a4 ON a1.SHELF_ID=a4.SHELF_ID
        WHERE a1.ORDER_DATE >= IF(CURTIME() BETWEEN @time0 AND @time1,SUBDATE(p_sdate,3),p_sdate) AND a1.ORDER_DATE < DATE_ADD(p_sdate, INTERVAL 1 DAY)
            AND a1.DATA_FLAG = 1 AND a1.ORDER_STATUS IN (2, 6, 7)
            AND IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY) > 0
         ) aa
    GROUP BY sdate,city_name,payment_type
    ) t2 ON t1.sdate=t2.sdate AND t1.city_name=t2.city_name AND t1.payment_type=t2.payment_type
SET t1.user_num=t2.user_num
WHERE t1.sdate>=IF(CURTIME() BETWEEN @time0 AND @time1 ,SUBDATE(p_sdate,3),p_sdate) AND t1.sdate<ADDDATE(p_sdate,1)
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_paytype_dashboard_daily',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));

END