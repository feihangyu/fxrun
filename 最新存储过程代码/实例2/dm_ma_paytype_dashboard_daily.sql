CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_paytype_dashboard_daily`(IN p_sdate DATE,IN p_if_history SMALLINT)
BEGIN
/*
    Author:	市场
    Create date: 2020-4-15
    Modify date:
    Description:	支付渠道维度日报看板
    每半小时更新一次
*/
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每天传参今日日期
SET @if_history=IF(p_if_history=1 OR CURTIME() BETWEEN '02:05' AND '02:35' ,1,0 );
SET @date0=CASE WHEN @if_history=1 THEN SUBDATE(@sdate,3) ELSE @sdate END ;
#删除历史数据
DELETE FROM fe_dm.dm_ma_paytype_dashboard_daily WHERE sdate>=@date0  OR sdate<SUBDATE(@sdate,31) ;#保持近31天数据
#插入数据
INSERT INTO fe_dm.dm_ma_paytype_dashboard_daily
    (sdate, business_name, payment_type
    , gmv, amount, after_pay_amount, discount_amount, coupon_amount, order_num)
SELECT sdate,business_name,a1.payment_type
    ,SUM(gmv) ,SUM(amount) ,SUM(after_pay_amount) ,SUM(discount_amount) ,SUM(coupon_amount) ,SUM(order_num)
FROM fe_dm.dm_ma_shelf_paytype_sale_daily a1
JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
WHERE sdate>=@date0 AND sdate<ADDDATE(@sdate,1)
GROUP BY sdate,business_name,a1.payment_type
;
    #用户数
UPDATE
    (SELECT DATE(PAY_DATE) sdate,business_name
          ,CASE WHEN a1.PAYMENT_TYPE_GATEWAY IN ('WeiXinPayJSAPI','SFPayJSAPI','WeiXinContractPay') THEN '微信' WHEN a3.pid IS NOT NULL THEN a3.ITEM_NAME ELSE'其他' END payment_type
         ,COUNT(a1.user_id) user_num
    FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
    LEFT JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.DICTIONARY_ID=471 AND a3.ITEM_VALUE=a1.PAYMENT_TYPE_GATEWAY
    WHERE a1.PAY_DATE>=@date0 AND a1.PAY_DATE<ADDDATE(@sdate,1)
    GROUP BY sdate,business_name,payment_type
    ) a1
JOIN fe_dm.dm_ma_paytype_dashboard_daily a2 ON a2.sdate=a1.sdate AND a2.business_name=a1.business_name AND a2.payment_type=a1.payment_type
SET a2.user_num=a1.user_num
;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_paytype_dashboard_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_paytype_dashboard_daily','dm_ma_paytype_dashboard_daily','纪伟铨');
 
END