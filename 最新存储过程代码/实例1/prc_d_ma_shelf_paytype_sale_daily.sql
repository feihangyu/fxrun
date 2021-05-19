CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_paytype_sale_daily`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
set @date_st=p_sdate;
SET @time0='00:28',@time1='00:35';
set @date0=IF(CURTIME() BETWEEN @time0 AND @time1,SUBDATE(@date_st,3),@date_st),@date1=ADDDATE(@date_st,1);
SET @smonth= DATE_FORMAT(@date_st,'%Y-%m-01');


# 删除数据
DELETE FROM feods.d_ma_shelf_paytype_sale_daily WHERE (sdate>=@date0 and  sdate<@date1 ) OR sdate<SUBDATE(@date_st,40);
# 插入日数据
INSERT INTO feods.d_ma_shelf_paytype_sale_daily
    ( sdate, shelf_id, payment_type
    , gmv, amount, after_pay_amount, discount_amount, coupon_amount, order_num, user_num )
SELECT sdate,SHELF_ID,PAYMENT_TYPE_NAME
     ,SUM(GMV) GMV ,SUM(amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,SUM(COUPON_AMOUNT) COUPON_AMOUNT
        ,COUNT(1) order_num,COUNT(DISTINCT USER_ID) user_num
FROM
    (SELECT  a1.ORDER_ID, a1.SHELF_ID,a1.USER_ID,DATE(a1.ORDER_DATE) sdate
        ,CASE WHEN a1.PAYMENT_TYPE_NAME IN ('E币支付','餐卡支付','顺手付云闪付','小蜜蜂积分支付','招行一卡通','中国移动和包支付') THEN a1.PAYMENT_TYPE_NAME  WHEN a1.PAYMENT_TYPE_NAME LIKE '微信%' THEN '微信' ELSE 'other' END PAYMENT_TYPE_NAME
        ,SUM((IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY)) *a2.SALE_PRICE) GMV
        ,a1.PRODUCT_TOTAL_AMOUNT-IFNULL(a3.refund_amount,0) amount ,a1.DISCOUNT_AMOUNT,a1.COUPON_AMOUNT
        ,0 after_pay_amount
    FROM fe.sf_order a1
    JOIN fe.sf_order_item a2 ON a1.ORDER_ID = a2.ORDER_ID AND a2.DATA_FLAG=1
    LEFT JOIN fe.sf_order_refund_order a3 ON a1.ORDER_ID = a3.order_id AND a3.refund_status = 5 AND a3.data_flag=1
    WHERE a1.ORDER_DATE >=@date0 AND a1.ORDER_DATE<@date1
      AND a1.DATA_FLAG = 1  AND a1.ORDER_STATUS IN (2, 6, 7)
      AND IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY) > 0
    GROUP BY a1.ORDER_ID
    UNION ALL #电商批量订单
    SELECT a1.ORDER_ID,a1.SUPPLIER_ID shelf_id,NULL user_id,DATE(a1.APPLY_TIME) sdate,'other' PAYMENT_TYPE_NAME
         ,a1.TOTAL_PRICE gmv,SUM(a2.bank_actual_price) amount,a1.TOTAL_PRICE-SUM(a2.bank_actual_price) discount_amount,0 coupon_amount
         ,0 after_pay_amount
    FROM fe.sf_product_fill_order a1
    JOIN fe.sf_product_fill_order_extend a2 ON a2.order_id = a1.ORDER_ID
    WHERE a1.APPLY_TIME>=@date0 AND a1.APPLY_TIME<@date1
        AND a1.order_status = 11 AND  a1.sales_bussniess_channel = 1
        AND a1.sales_order_status = 3 AND a1.sales_audit_status = 2 AND a1.fill_type =13
        AND a2.bank_actual_price>0 # 实收金额大于0
    GROUP BY a1.ORDER_ID
    UNION ALL #补付款数据
    SELECT NULL order_id,IFNULL(real_shelf_id,SHELF_ID) SHELF_ID,a1.USER_ID,DATE(PAYMENT_DATE) sdate
         ,CASE WHEN  a1.PAYMENT_TYPE_NAME='WeiXinPayJSAPI' THEN '微信' WHEN a1.PAYMENT_TYPE_NAME='EPay' THEN 'E币支付' ELSE 'other'END
         ,a1.PAYMENT_MONEY gmv,a1.PAYMENT_MONEY amount,0 discount_amount,0 coupon_amount,PAYMENT_MONEY after_pay_amount
    FROM fe.sf_after_payment a1
    WHERE PAYMENT_DATE>=@date0 AND PAYMENT_DATE<@date1
        AND a1.PAYMENT_STATUS=2
    ) aa
GROUP BY sdate,SHELF_ID,PAYMENT_TYPE_NAME
;
# 城市 分支号码 投放位置
UPDATE feods.d_ma_shelf_paytype_sale_daily t1
JOIN fe.sf_shelf t2 ON t1.SHELF_ID=t2.SHELF_ID
LEFT JOIN fe.pub_shelf_manager t3 ON t2.MANAGER_ID=t3.MANAGER_ID
LEFT JOIN feods.zs_city_business t5 ON t5.city=t2.CITY
SET t1.city_name=IFNULL(t5.CITY_NAME,'other')
WHERE t1.sdate>=@date0 AND t1.sdate<@date1
;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'prc_d_ma_shelf_paytype_sale_daily',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('纪伟铨@',@user,@timestamp)
);
END