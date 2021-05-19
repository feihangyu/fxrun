CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_sale_daily`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @smonth= DATE_FORMAT(@sdate,'%Y-%m-01');
SET @date0=IF(DAY(@sdate) <5,DATE_SUB(@smonth,INTERVAL 1 MONTH),@smonth);
DELETE FROM feods.d_ma_shelf_sale_daily WHERE (sdate>=@date0 AND sdate<ADDDATE(@sdate,1)) OR sdate<DATE_SUB(@smonth,INTERVAL 14 MONTH );
DELETE FROM feods.d_ma_shelf_sale_daily WHERE sdate<DATE_SUB(@smonth,INTERVAL 1 MONTH )  AND GMV=0 ; #删除历史上一个月前GMV<0的货架
# 插入日数据
INSERT INTO feods.d_ma_shelf_sale_daily
    ( sdate, SHELF_ID
    , sale_num, GMV, amount, after_pay_amount, DISCOUNT_AMOUNT, COUPON_AMOUNT, order_num ,refund_amount)
SELECT t1.sdate ,t1.SHELF_ID #业务主键
    ,t1.sale_num  sale_num
    ,t1.GMV GMV
    ,t1.amount amount
    ,after_pay_amount after_pay_amount,t1.DISCOUNT_AMOUNT,t1.COUPON_AMOUNT
    ,t1.order_num
    ,t1.refund_amount refund_amount
FROM
    (SELECT sdate,SHELF_ID
        ,SUM(sale_num) sale_num,SUM(GMV) GMV,SUM(amount) amount ,SUM(after_pay_amount) after_pay_amount
        ,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,SUM(COUPON_AMOUNT) COUPON_AMOUNT,COUNT(1) order_num
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
        WHERE a1.ORDER_DATE >= @date0  AND a1.ORDER_DATE < ADDDATE(@sdate,1)
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
        WHERE a1.APPLY_TIME>=@date0  AND a1.APPLY_TIME<ADDDATE(@sdate,1)
            AND a1.order_status = 11 AND  a1.sales_bussniess_channel = 1
            AND a1.sales_order_status = 3 AND a1.sales_audit_status = 2 AND a1.fill_type =13
            AND a2.bank_actual_price>0 # 实收金额大于0
        GROUP BY a1.ORDER_ID
        UNION ALL #补付款
        SELECT sdate,shelf_id,NULL order_id,NULL user_id
             ,payment_money gmv,payment_money amount,payment_money after_pay_amount
             ,0  discount_amount,0 coupon_amount,0 refund_amount,0 sale_num
        FROM feods.fjr_shelf_dgmv a1
        WHERE a1.sdate>=@date0  AND a1.sdate<ADDDATE(@sdate,1) AND payment_money>0
        ) aa
    GROUP BY sdate,SHELF_ID) t1
;
    # 在月最后一天插入所有有销售终端
INSERT INTO  feods.d_ma_shelf_sale_daily
    (sdate,SHELF_ID)
SELECT t1.sdate,t1.SHELF_ID
FROM
    (SELECT a2.sdate,a1.shelf_id
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate>=DATE(a1.ACTIVATE_TIME) AND a2.sdate<=DATE(IFNULL(a1.REVOKE_TIME,CURDATE()))
        AND a2.sdate BETWEEN @date0 AND @sdate
    WHERE  SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
    ) t1
LEFT JOIN feods.d_ma_shelf_sale_daily t2 ON t1.sdate=t2.sdate AND t1.SHELF_ID=t2.SHELF_ID
WHERE t2.pid IS NULL
;    # 工作日天数
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN( SELECT MONTH(a1.sdate) smonth ,SUM(IF(a2.sdate>=a1.sdate,a2.workday_num,0)) workday_num_m,SUM(IF(a2.sdate<a1.sdate,a2.workday_num,0)) workday_num_lm
    FROM fe_dwd.dwd_pub_work_day a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate>=DATE_ADD(a1.sdate,INTERVAL -1 MONTH) AND a2.sdate<DATE_ADD(a1.sdate,INTERVAL 1 MONTH )
    WHERE a1.sdate>=DATE_ADD(@smonth,INTERVAL -1 MONTH)  AND a1.sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH ) AND DAY(a1.sdate)=1
    GROUP BY a1.sdate
    ) t2 ON t2.smonth=MONTH(t1.sdate)
SET t1.workday_num=t2.workday_num_m ,t1.workday_num_lm=t2.workday_num_lm
WHERE t1.sdate>=IF(DAY(@sdate)<5,DATE_SUB(@smonth,INTERVAL 1 MONTH),@smonth) AND t1.sdate<DATE_ADD(@sdate,INTERVAL 1 DAY)
;
    # 城市,货架类型,店主ID
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.shelf_id=t1.SHELF_ID
SET t1.city_name=t2.CITY_NAME,t1.shelf_type=t2.shelf_type_desc ,t1.manager_id=t2.MANAGER_ID
WHERE t1.sdate>=@date0 AND t1.sdate<ADDDATE(@sdate,1)
;
    # 更新日用户数
UPDATE
    (SELECT sdate,SHELF_ID,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT DATE(order_date) sdate,shelf_id,user_id,COUNT(DISTINCT order_id) orders
        FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
        WHERE a1.ORDER_DATE >= @date0  AND a1.ORDER_DATE < ADDDATE(@sdate,1)
          AND quantity_act > 0
        GROUP BY sdate,shelf_id,user_id
        ) aa
    GROUP BY sdate,SHELF_ID
    ) t2
JOIN feods.d_ma_shelf_sale_daily t1 ON t1.sdate=t2.sdate AND t1.SHELF_ID=t2.SHELF_ID
SET t1.user_num=t2.user_num,t1.user_num_reorder=t2.user_num_reorder
WHERE t1.sdate>=@date0 AND t1.sdate<ADDDATE(@sdate,1)
;
    #昨日数据
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN feods.d_ma_shelf_sale_daily t2 ON t2.sdate=SUBDATE(t1.sdate,1) AND t2.SHELF_ID=t1.SHELF_ID
SET t1.gmv_ld=t2.GMV,t1.order_num_ld=t2.order_num,t1.user_num_ld=t2.user_num
WHERE t1.sdate>=@date0 AND t1.sdate<ADDDATE(@sdate,1);
#更本月度信息
    #更新业绩
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN (SELECT MONTH(a1.sdate) smonth,a1.SHELF_ID
           ,SUM(a1.GMV) gmv_m,SUM(a1.amount) amount_m,SUM(a1.sale_num) sale_num_m,SUM(a1.order_num) order_num_m
           ,SUM(a1.COUPON_AMOUNT) COUPON_AMOUNT_M, SUM(a1.DISCOUNT_AMOUNT) DISCOUNT_AMOUNT_M,SUM(a1.refund_amount) refund_amount_m
           ,SUM(a1.after_pay_amount) after_pay_amount_m
      FROM feods.d_ma_shelf_sale_daily a1
      WHERE a1.sdate>=@date0 AND a1.sdate<ADDDATE(@sdate,1)
      GROUP BY smonth,a1.SHELF_ID
      ) t2 ON t1.SHELF_ID=t2.SHELF_ID AND MONTH(t1.sdate)=t2.smonth
SET t1.GMV_m=t2.gmv_m,t1.amount_m=t2.amount_m,t1.sale_num_m=t2.sale_num_m,t1.order_num_m=t2.order_num_m,t1.COUPON_AMOUNT_m=t2.COUPON_AMOUNT_M
    ,t1.DISCOUNT_AMOUNT_m=t2.DISCOUNT_AMOUNT_M ,t1.refund_amount_m=t2.refund_amount_m,t1.after_pay_amount_m=t2.after_pay_amount_m
WHERE t1.sdate=@sdate OR (DAY(@sdate)<5 AND t1.sdate=SUBDATE(@smonth,1))
;
    # 更新月用户数
UPDATE
    (SELECT smonth,SHELF_ID,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT MONTH(order_date) smonth,shelf_id,user_id,COUNT(DISTINCT order_id) orders
        FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
        WHERE a1.ORDER_DATE >= @date0  AND a1.ORDER_DATE < ADDDATE(@sdate,1)
          AND quantity_act > 0
        GROUP BY smonth,shelf_id,user_id
        ) aa
    GROUP BY smonth,SHELF_ID
     ) t2
JOIN feods.d_ma_shelf_sale_daily t1 ON  t2.shelf_id=t1.SHELF_ID AND t2.smonth=MONTH(t1.sdate)
SET t1.user_num_m=t2.user_num,t1.user_num_reorder_m=t2.user_num_reorder
WHERE  t1.sdate=@sdate OR (DAY(@sdate)<5 AND t1.sdate=SUBDATE(@smonth,1) )
;
    # 更新上月信息
UPDATE feods.d_ma_shelf_sale_daily t1
JOIN feods.d_ma_shelf_sale_daily t2 ON t2.sdate=LAST_DAY(DATE_SUB(t1.sdate,INTERVAL 1 MONTH)) AND t1.SHELF_ID=t2.SHELF_ID
SET t1.GMV_lm=t2.GMV_m,t1.amount_lm=t2.amount_m
WHERE t1.sdate>=@date0 AND t1.sdate<DATE_ADD(@sdate,INTERVAL 1 DAY)
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('prc_d_ma_shelf_sale_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
END