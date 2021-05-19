CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_paytype_sale_monthly`(IN p_sdate DATE)
BEGIN
SET @smonth= DATE_FORMAT(p_sdate,'%Y-%m-01') ;
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 删除数据
DELETE FROM feods.d_ma_shelf_paytype_sale_monthly WHERE sdate=date_add(@smonth ,interval -1 month )  OR sdate<DATE_ADD(@smonth,INTERVAL -3 MONTH );
# 插入月数据
INSERT INTO feods.d_ma_shelf_paytype_sale_monthly
    ( sdate, shelf_id, payment_type,city_name,branch_code,set_position
    , gmv, amount, after_pay_amount, discount_amount, coupon_amount, order_num)
SELECT date_add(@smonth ,interval -1 month )  sdate,SHELF_ID,payment_type,city_name,branch_code,set_position
     ,SUM(GMV) GMV ,SUM(amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,SUM(COUPON_AMOUNT) COUPON_AMOUNT
    ,SUM(order_num) order_num
FROM feods.d_ma_shelf_paytype_sale_daily
WHERE sdate>=DATE_ADD(@smonth,INTERVAL -1 MONTH ) AND sdate<@smonth
GROUP BY SHELF_ID,payment_type
;
# 用户数
UPDATE feods.d_ma_shelf_paytype_sale_monthly t1
JOIN
    (SELECT sdate,shelf_id,payment_type,COUNT(DISTINCT user_id) user_num
    FROM
        (SELECT DATE(a1.ORDER_DATE) sdate,a1.SHELF_ID
            ,CASE WHEN a1.PAYMENT_TYPE_NAME IN ('E币支付','餐卡支付','顺手付云闪付','小蜜蜂积分支付','招行一卡通') THEN a1.PAYMENT_TYPE_NAME  WHEN a1.PAYMENT_TYPE_NAME LIKE '微信%' THEN '微信' ELSE 'other' END payment_type
            , a1.USER_ID
        FROM fe.sf_order a1
        JOIN fe.sf_order_item a2 ON a1.ORDER_ID = a2.ORDER_ID AND a2.DATA_FLAG = 1
        LEFT JOIN fe.sf_order_refund_order a3 ON a1.ORDER_ID = a3.order_id AND a3.refund_status = 5 AND a3.data_flag = 1
        WHERE a1.ORDER_DATE >= DATE_ADD(@smonth,INTERVAL -1 MONTH ) AND a1.ORDER_DATE <@smonth
            AND a1.DATA_FLAG = 1 AND a1.ORDER_STATUS IN (2, 6, 7)
            AND IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY) > 0
         ) aa
    GROUP BY sdate,SHELF_ID,payment_type
    ) t2 ON  t1.shelf_id=t2.shelf_id AND t1.payment_type=t2.payment_type
SET t1.user_num=t2.user_num
WHERE t1.sdate=date_add(@smonth ,interval -1 month ) 
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_shelf_paytype_sale_monthly',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
 
END