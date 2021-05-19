CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_sale_daily`(IN p_sdate DATE,IN p_if_history SMALLINT)
BEGIN
/*
    Author:	市场
    Create date: 2020-3-28
    Modify date:
    Description:	货架销售日报(订单支付日期)
    每月1号 02:05到02:35 更新上月至今数据, 其他日期每日02:05到02:35更新 前一日数据, 其他时间段更新今天数据
*/
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每天传参今日日期
SET @if_history=IF(p_if_history=1 OR CURTIME() BETWEEN '02:05' AND '02:35' ,1,0 );
SET @smonth= DATE_FORMAT(@sdate,'%Y-%m-01');
SET @date0=CASE WHEN DAY(@sdate)=1 AND @if_history=1 THEN DATE_SUB(@smonth,INTERVAL 1 MONTH )
                WHEN @if_history=1 THEN SUBDATE(@sdate,1) ELSE @sdate END;
#删除历史数据
DELETE FROM fe_dm.dm_ma_shelf_sale_daily WHERE (sdate>=@date0 AND sdate<ADDDATE(@sdate,1)) OR (sdate<DATE_SUB(@smonth,INTERVAL 1 MONTH ) AND GMV=0)  ;#保持历史所有数据
#插入数据
INSERT INTO fe_dm.dm_ma_shelf_sale_daily
    ( sdate, SHELF_ID
    , sale_num, GMV, amount, order_num,user_num,DISCOUNT_AMOUNT,COUPON_AMOUNT,refund_amount )
SELECT sdate, SHELF_ID
    , SUM(sale_num),SUM(GMV) GMV,SUM(pay_amount) pay_amount,COUNT(1) order_num,COUNT(DISTINCT user_id) user_num
    ,SUM(o_discount_amount) o_discount_amount,SUM(coupon_amount) o_coupon_amount,SUM(refund_amount) refund_amount
FROM
    (SELECT a.order_id
        ,shelf_id,user_id,coupon_amount,o_discount_amount,DATE(PAY_DATE) sdate
        ,a.`PAY_AMOUNT`* COUNT(DISTINCT a.pay_id)-SUM(IFNULL(a.refund_amount,0)) AS pay_amount  -- 有重复支付的问题，实收要多算一次
        ,SUM(a.`quantity_act`*a.`sale_price`) AS GMV
        ,SUM(quantity_act) sale_num,IFNULL(SUM(refund_amount),0)  refund_amount
    FROM fe_dwd.dwd_pub_order_item_recent_two_month a #实时数据需要 必须用两个月的宽表
    WHERE PAY_DATE>=@date0 AND PAY_DATE<ADDDATE(@sdate,1)
    GROUP BY order_id
    ) t
GROUP BY sdate, SHELF_ID
;   # 补齐运营终端
INSERT INTO  fe_dm.dm_ma_shelf_sale_daily
    (sdate,SHELF_ID)
SELECT t1.sdate,t1.SHELF_ID
FROM
    (SELECT a2.sdate,a1.shelf_id
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate>=DATE(a1.ACTIVATE_TIME) AND a2.sdate<=DATE(IFNULL(a1.REVOKE_TIME,CURDATE()))
        AND a2.sdate BETWEEN @date0 AND @sdate
    WHERE  SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
    ) t1
LEFT JOIN fe_dm.dm_ma_shelf_sale_daily  t2 ON t1.sdate=t2.sdate AND t1.SHELF_ID=t2.SHELF_ID
WHERE t2.pid IS NULL
;   #补付款
UPDATE
    (SELECT DATE(PAYMENT_DATE) sdate,IFNULL(real_shelf_id,SHELF_ID) shelf_id1
        ,SUM(PAYMENT_MONEY) PAYMENT_MONEY
    FROM fe_dwd.dwd_sf_after_payment
    WHERE PAYMENT_DATE>=@date0 AND PAYMENT_DATE<ADDDATE(@sdate,1)
        AND PAYMENT_STATUS=2
    GROUP BY sdate,shelf_id1) a1
JOIN fe_dm.dm_ma_shelf_sale_daily a2 ON a2.sdate=a1.sdate AND a2.SHELF_ID=a1.shelf_id1
SET a2.GMV=a2.GMV+a1.PAYMENT_MONEY,a2.amount=a2.amount+a1.PAYMENT_MONEY,a2.after_pay_amount=a1.PAYMENT_MONEY
;   # 插入历史数据
IF @if_history=1 THEN
       # 更新历史复购用户数
    UPDATE
        (SELECT sdate,SHELF_ID,SUM(IF(orders>1,1,0)) user_num_reorder
        FROM
            (SELECT DATE(PAY_DATE) sdate,shelf_id,user_id,COUNT(DISTINCT order_id) orders
            FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
            WHERE a1.PAY_DATE >= @date0  AND a1.PAY_DATE < ADDDATE(@sdate,1)
              AND quantity_act > 0
            GROUP BY sdate,shelf_id,user_id
            ) aa
        GROUP BY sdate,SHELF_ID
        ) t2
    JOIN fe_dm.dm_ma_shelf_sale_daily t1 ON t1.sdate=t2.sdate AND t1.SHELF_ID=t2.SHELF_ID
    SET t1.user_num_reorder=t2.user_num_reorder
    WHERE t1.sdate>=@date0 AND t1.sdate<@sdate
    ;
    #昨日数据
    UPDATE fe_dm.dm_ma_shelf_sale_daily t1
    JOIN fe_dm.dm_ma_shelf_sale_daily t2 ON t2.sdate=SUBDATE(t1.sdate,1) AND t2.SHELF_ID=t1.SHELF_ID
    SET t1.gmv_ld=t2.GMV,t1.order_num_ld=t2.order_num,t1.user_num_ld=t2.user_num
    WHERE t1.sdate>=@date0 AND t1.sdate<ADDDATE(@sdate,1);
    # 工作日天数
    UPDATE fe_dm.dm_ma_shelf_sale_daily t1
    JOIN( SELECT MONTH(a1.sdate) smonth
               ,SUM(IF(a2.sdate>=a1.sdate,a2.workday_num,0)) workday_num_m
        FROM fe_dwd.dwd_pub_work_day a1
        JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate>=DATE_ADD(a1.sdate,INTERVAL -1 MONTH) AND a2.sdate<DATE_ADD(a1.sdate,INTERVAL 1 MONTH )
        WHERE a1.sdate>=DATE_SUB(@smonth,INTERVAL 1 MONTH ) AND a1.sdate<ADDDATE(@sdate,1) AND DAY(a1.sdate)=1
        GROUP BY a1.sdate
        ) t2 ON t2.smonth=MONTH(t1.sdate)
    SET t1.workday_num=t2.workday_num_m
    WHERE t1.sdate>=@date0 AND t1.sdate<ADDDATE(@sdate,1)
    ;
END IF;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_sale_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_sale_daily','dm_ma_shelf_sale_daily','纪伟铨');
END