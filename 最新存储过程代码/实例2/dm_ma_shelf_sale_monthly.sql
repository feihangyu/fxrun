CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_sale_monthly`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime:= CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #默认输入前一天
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
/*
    Author:	市场
    Create date: 2020-3-30
    Modify date:
    Description:	货架销售月报(订单支付日期)
    每天更新前一天的当月数据
*/
#删除数据
DELETE FROM fe_dm.dm_ma_shelf_sale_monthly WHERE (sdate=@smonth) OR ( sdate<DATE_SUB(@smonth,INTERVAL 1 MONTH ) AND GMV=0) ; #暂不限制存储月数
# 插入当月数据
INSERT INTO fe_dm.dm_ma_shelf_sale_monthly
    (sdate, SHELF_ID
    , sale_num, GMV, amount, after_pay_amount, DISCOUNT_AMOUNT, COUPON_AMOUNT, order_num, refund_amount,GMV_wd,after_pay_amount_wd)
SELECT @smonth , a1.SHELF_ID
    ,SUM(sale_num) sale_num,SUM(GMV), SUM(amount) amount ,SUM(after_pay_amount) after_pay_amount,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT
    ,SUM(COUPON_AMOUNT) COUPON_AMOUNT,SUM(order_num) order_num,SUM(refund_amount) refund_amount
    ,SUM(IF(a2.if_work_day=1 ,a1.GMV,0)) GMV_wd,SUM(IF(a2.if_work_day=1,a1.after_pay_amount,0)) after_pay_amount_wd
FROM fe_dm.dm_ma_shelf_sale_daily a1
JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate=a1.sdate
WHERE a1.sdate>=@smonth AND a1.sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH )
    AND a1.sdate<CURDATE()
GROUP BY SHELF_ID
;# 更新本月 天数
UPDATE
    (SELECT shelf_id,SUM(1) days ,SUM(IF(a2.if_work_day=1,1,0)) days_wd
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON  a2.sdate>=@smonth AND a2.sdate<=@sdate
        AND a2.sdate>=DATE(a1.ACTIVATE_TIME) AND a2.sdate<=IFNULL(DATE(a1.REVOKE_TIME),CURDATE())
    WHERE  SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
    GROUP BY shelf_id
    )t1
JOIN fe_dm.dm_ma_shelf_sale_monthly  t2 ON t2.sdate=@smonth AND t2.SHELF_ID=t1.shelf_id
SET t2.days=t1.days ,t2.days_wd=t1.days_wd
;# 更新本月用户数,复购用户数
UPDATE
    (SELECT SHELF_ID,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT shelf_id,user_id,COUNT(DISTINCT order_id) orders
	    FROM fe_dwd.dwd_pub_order_item_recent_two_month
	    WHERE PAY_DATE>=@smonth AND PAY_DATE<DATE_ADD(@smonth,INTERVAL 1 MONTH )
        GROUP BY shelf_id, user_id
        )t
    GROUP BY SHELF_ID
    ) t2
JOIN fe_dm.dm_ma_shelf_sale_monthly t1   ON t1.sdate=@smonth AND  t1.SHELF_ID=t2.SHELF_ID
SET t1.user_num=t2.user_num,t1.user_num_reorder=t2.user_num_reorder
;
#上月数据
UPDATE fe_dm.dm_ma_shelf_sale_monthly a1
JOIN fe_dm.dm_ma_shelf_sale_monthly a2 ON  a2.sdate=DATE_SUB(a1.sdate,INTERVAL 1 MONTH ) AND a2.SHELF_ID=a1.SHELF_ID
SET a1.order_num_lm=a2.order_num,a1.user_num_lm=a2.user_num,a1.gmv_lm=a2.GMV
WHERE a1.sdate=@smonth
;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_sale_monthly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_sale_monthly','dm_ma_shelf_sale_monthly','纪伟铨');
END