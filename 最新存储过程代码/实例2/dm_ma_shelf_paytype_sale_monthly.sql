CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_paytype_sale_monthly`(IN p_sdate DATE)
BEGIN
SET @sdate=p_sdate;
SET @smonth= DATE_FORMAT(@sdate,'%Y-%m-01') ;
SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
# 删除数据
DELETE FROM fe_dm.dm_ma_shelf_paytype_sale_monthly WHERE sdate=@smonth  OR sdate<DATE_ADD(@smonth,INTERVAL -12 MONTH );
# 插入月数据
INSERT INTO fe_dm.dm_ma_shelf_paytype_sale_monthly
    ( sdate, shelf_id, payment_type
    , gmv, amount, after_pay_amount, discount_amount, coupon_amount
    ,third_discount_amount, order_num)
SELECT @smonth  sdate,SHELF_ID,payment_type
     ,SUM(GMV) GMV ,SUM(amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,SUM(COUPON_AMOUNT) COUPON_AMOUNT
    ,SUM(third_discount_amount) third_discount_amount,SUM(order_num) order_num
FROM fe_dm.dm_ma_shelf_paytype_sale_daily
WHERE sdate>=@smonth AND sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH )
GROUP BY SHELF_ID,payment_type
;
# 用户数
UPDATE fe_dm.dm_ma_shelf_paytype_sale_monthly t1
JOIN
    (SELECT a1.SHELF_ID
            ,CASE WHEN a1.PAYMENT_TYPE_GATEWAY IN ('WeiXinPayJSAPI','SFPayJSAPI','WeiXinContractPay') THEN '微信' WHEN a2.pid IS NOT NULL THEN a2.ITEM_NAME ELSE'其他' END payment_type
            ,COUNT(DISTINCT user_id) user_num
        FROM fe_dwd.dwd_order_item_refund_day a1
        LEFT JOIN fe_dwd.dwd_pub_dictionary a2 ON a2.DICTIONARY_ID=471 AND a2.ITEM_VALUE=a1.PAYMENT_TYPE_GATEWAY
        WHERE PAY_DATE>=@smonth AND PAY_DATE<DATE_ADD(@smonth,INTERVAL 1 MONTH )
        GROUP BY SHELF_ID,payment_type
    ) t2 ON  t1.shelf_id=t2.shelf_id AND t1.payment_type=t2.payment_type
SET t1.user_num=t2.user_num
WHERE t1.sdate=@smonth
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_paytype_sale_monthly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_paytype_sale_monthly','dm_ma_shelf_paytype_sale_monthly','纪伟铨');
END