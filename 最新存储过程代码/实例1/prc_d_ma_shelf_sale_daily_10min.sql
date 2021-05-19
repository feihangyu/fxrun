CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_sale_daily_10min`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @smonth= DATE_FORMAT(p_sdate,'%Y-%m-01');
DELETE FROM feods.d_ma_shelf_sale_daily WHERE sdate=p_sdate ;
# 插入日数据
INSERT INTO feods.d_ma_shelf_sale_daily
    ( sdate, SHELF_ID
    , sale_num, GMV, amount, after_pay_amount, DISCOUNT_AMOUNT, COUPON_AMOUNT, order_num,user_num ,refund_amount)
SELECT t1.sdate ,t1.SHELF_ID #业务主键
    ,t1.sale_num  sale_num
    ,t1.GMV GMV
    ,t1.amount amount
    ,after_pay_amount after_pay_amount,t1.DISCOUNT_AMOUNT,t1.COUPON_AMOUNT
    ,t1.order_num,t1.user_num
    ,t1.refund_amount refund_amount
FROM
    (SELECT sdate,SHELF_ID
        ,SUM(sale_num) sale_num,SUM(GMV) GMV,SUM(amount) amount ,SUM(after_pay_amount) after_pay_amount
        ,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,SUM(COUPON_AMOUNT) COUPON_AMOUNT,COUNT(1) order_num,COUNT(DISTINCT USER_ID) user_num
        ,SUM(refund_amount) refund_amount
    FROM
        (SELECT  DATE(a1.ORDER_DATE) sdate,a1.SHELF_ID, a1.ORDER_ID ,a1.USER_ID
            ,SUM((IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY)) *a2.SALE_PRICE) GMV
            ,a1.PRODUCT_TOTAL_AMOUNT-IFNULL(a3.refund_amount,0) amount
            ,0 after_pay_amount,a1.DISCOUNT_AMOUNT,a1.COUPON_AMOUNT,IFNULL(a3.refund_amount,0) refund_amount
            ,SUM((IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY) )) sale_num
        FROM fe.sf_order a1
        JOIN fe.sf_order_item a2 ON a1.ORDER_ID = a2.ORDER_ID AND a2.DATA_FLAG=1
        LEFT JOIN fe.sf_order_refund_order a3 ON a1.ORDER_ID = a3.order_id AND a3.refund_status = 5 AND a3.data_flag=1
        WHERE a1.ORDER_DATE >= p_sdate  AND a1.ORDER_DATE < DATE_ADD(p_sdate,INTERVAL 1 DAY)
          AND a1.DATA_FLAG = 1  AND a1.ORDER_STATUS IN (2, 6, 7)
          AND IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY) > 0
        GROUP BY a1.ORDER_ID
        UNION ALL #批量订单
        SELECT DATE(a1.APPLY_TIME) sdate,a1.SUPPLIER_ID shelf_id ,a1.ORDER_ID,NULL user_id
             ,a1.TOTAL_PRICE gmv,SUM(a2.bank_actual_price) amount,0 after_pay_amount
             ,a1.TOTAL_PRICE-SUM(a2.bank_actual_price) discount_amount,0 coupon_amount,0 refund_amount
             ,a1.PRODUCT_NUM sale_num
        FROM fe.sf_product_fill_order a1
        JOIN fe.sf_product_fill_order_extend a2 ON a2.order_id = a1.ORDER_ID
        WHERE a1.APPLY_TIME>=p_sdate  AND a1.APPLY_TIME<DATE_ADD(p_sdate,INTERVAL 1 DAY)
            AND a1.order_status = 11 AND  a1.sales_bussniess_channel = 1
            AND a1.sales_order_status = 3 AND a1.sales_audit_status = 2 AND a1.fill_type =13
            AND a2.bank_actual_price>0 # 实收金额大于0
        GROUP BY a1.ORDER_ID
        UNION ALL #补付款
        SELECT DATE(PAYMENT_DATE) sdate,IFNULL(real_shelf_id,SHELF_ID) shelf_id1,NULL order_id,NULL user_od
             ,PAYMENT_MONEY gmv, PAYMENT_MONEY amount, PAYMENT_MONEY after_pay_amount
             ,0 discount_amount,0 coupon_amount,0 refund_amount,0 sale_num
        FROM fe.sf_after_payment
        WHERE PAYMENT_DATE>=p_sdate AND PAYMENT_DATE<DATE_ADD(p_sdate,INTERVAL 1 DAY )
            AND PAYMENT_STATUS=2
        GROUP BY sdate,shelf_id1
        ) aa
    GROUP BY sdate,SHELF_ID) t1
;
# 插入本月有销售但今日无销售的货架
/*INSERT INTO feods.d_ma_shelf_sale_daily
    ( sdate,SHELF_ID)
SELECT p_sdate,t1.SHELF_ID
FROM ( SELECT DISTINCT SHELF_ID FROM feods.d_ma_shelf_sale_daily  WHERE sdate>=@smonth AND sdate<p_sdate ) t1
LEFT JOIN feods.d_ma_shelf_sale_daily t2 ON t2.sdate=p_sdate AND t2.SHELF_ID=t1.SHELF_ID
JOIN feods.fjr_work_days t3 ON  t3.sdate=p_sdate
WHERE t2.SHELF_ID IS NULL
;*/
# 工作日天数
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN( SELECT MONTH(a1.sdate) smonth ,SUM(IF(a2.sdate>=a1.sdate,a2.workday_num,0)) workday_num_m,SUM(IF(a2.sdate<a1.sdate,a2.workday_num,0)) workday_num_lm
    FROM fe_dwd.dwd_pub_work_day a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate>=DATE_ADD(a1.sdate,INTERVAL -1 MONTH) AND a2.sdate<DATE_ADD(a1.sdate,INTERVAL 1 MONTH )
    WHERE a1.sdate>=DATE_ADD(@smonth,INTERVAL -1 MONTH)  AND a1.sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH ) AND DAY(a1.sdate)=1
    GROUP BY a1.sdate
    ) t2 ON t2.smonth=MONTH(t1.sdate)
SET t1.workday_num=t2.workday_num_m ,t1.workday_num_lm=t2.workday_num_lm
WHERE t1.sdate=p_sdate
;
# 城市
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN fe.sf_shelf t2 ON t1.SHELF_ID=t2.SHELF_ID
SET t1.city_name=SUBSTRING_INDEX(SUBSTRING_INDEX(t2.AREA_ADDRESS, ',', 2), ',', - 1)
WHERE t1.sdate=p_sdate
;

# 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_shelf_sale_daily_10min',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
END