CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_paytype_sale_daily`(IN p_sdate DATE,IN p_if_history SMALLINT)
BEGIN
/*
    Author:	市场
    Create date: 2020-3-28
    Modify date:
    Description:	货架销售日报(订单支付日期)
    每月1号 5点到五点半 更新上月数据, 其他日期每日5点到五点半更新 前一日数据, 其他时间段更新今天数据
*/
#初始参数
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每天传参今日日期
SET @if_history=IF(p_if_history=1 OR CURTIME() BETWEEN '02:05' AND '02:35' ,1,0 ); #是否更新历史数据
SET @smonth= DATE_FORMAT(@sdate,'%Y-%m-01');
SET @date0=CASE WHEN @if_history=1 THEN SUBDATE(@sdate,3) ELSE @sdate END;
#删除历史数据
DELETE FROM fe_dm.dm_ma_shelf_paytype_sale_daily WHERE (sdate>=@date0 AND sdate<ADDDATE(@sdate,1)) OR sdate<DATE_SUB(@smonth,INTERVAL 4 MONTH );
#插入数据
INSERT INTO fe_dm.dm_ma_shelf_paytype_sale_daily
    (  sdate, shelf_id, payment_type
    , gmv, amount ,after_pay_amount, discount_amount,third_discount_amount, coupon_amount, order_num, user_num )
SELECT sdate, shelf_id, payment_type
    ,SUM(GMV) gmv,SUM(pay_amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(o_discount_amount) discount_amount
    ,IFNULL(SUM(third_discount_amount),0) third_discount_amount,SUM(o_coupon_amount) coupon_amount
    ,COUNT(order_id) order_num,COUNT(DISTINCT user_id) user_num
 FROM
    (SELECT  DATE(PAY_DATE) sdate,a1.shelf_id
        ,CASE WHEN a1.PAYMENT_TYPE_GATEWAY IN ('WeiXinPayJSAPI','SFPayJSAPI','WeiXinContractPay') THEN '微信' WHEN a2.pid IS NOT NULL THEN a2.ITEM_NAME ELSE'其他' END payment_type
        ,SUM(a1.`quantity_act`*a1.`sale_price`) AS GMV
        ,a1.PAY_AMOUNT-SUM(IFNULL(a1.refund_amount,0)) AS pay_amount  -- 有重复支付的问题，实收要多算一次
        ,0 after_pay_amount,o_discount_amount
        ,third_discount_amount,o_coupon_amount
        ,a1.user_id,a1.order_id
    FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
    LEFT JOIN fe_dwd.dwd_pub_dictionary a2 ON a2.DICTIONARY_ID=471 AND a2.ITEM_VALUE=a1.PAYMENT_TYPE_GATEWAY
    WHERE PAY_DATE>=@date0 AND PAY_DATE<ADDDATE(@sdate,1)
    GROUP BY order_id
    UNION ALL #补付款
    SELECT DATE(PAYMENT_DATE) sdate,IFNULL(real_shelf_id,SHELF_ID) shelf_id1,IF(PAYMENT_TYPE_NAME LIKE'w%','微信','E币支付') payment_type
        ,PAYMENT_MONEY gmv,PAYMENT_MONEY pay_amount
        ,PAYMENT_MONEY PAYMENT_MONEY,0,0,0,USER_ID,NULL
    FROM fe_dwd.dwd_sf_after_payment a1
    WHERE PAYMENT_DATE>=@date0 AND PAYMENT_DATE<ADDDATE(@sdate,1)
        AND PAYMENT_STATUS=2
    ) t1
GROUP BY sdate, SHELF_ID,payment_type
;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_paytype_sale_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_paytype_sale_daily','dm_ma_shelf_paytype_sale_daily','纪伟铨');
END