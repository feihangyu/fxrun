CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_area_sale_hourly`(IN p_sdate DATE,IN p_if_history TINYINT)
BEGIN
/*
    Author:	市场
    Create date: 2020-3-28
    Modify date:
    Description:	市场同比环比对比数据小时维度(订单支付日期)
    每天5:00至5:30 更新前七天数据,其他时间段更新当天数据
*/
#初始参数
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每天传参今日日期
SET @if_history=IF(p_if_history=1 OR CURTIME() BETWEEN '02:05' AND '02:35' ,1,0 ); #是否更新历史数据
SET @smonth= DATE_FORMAT(@sdate,'%Y-%m-01');
SET @date0=CASE WHEN @if_history=1 THEN SUBDATE(@sdate,7) ELSE @sdate END;
#删除历史数据
IF @if_history=1 THEN
    TRUNCATE fe_dm.dm_ma_area_sale_hourly;
ELSE
    DELETE FROM fe_dm.dm_ma_area_sale_hourly WHERE sdate=@date0 ;
END IF;
#插入数据
INSERT INTO fe_dm.dm_ma_area_sale_hourly
    (  sdate, HOUR, business_area
    , gmv, amount , discount_amount, coupon_amount, order_num, user_num,sale_num )
SELECT sdate, HOUR, business_name
    ,SUM(GMV) gmv,SUM(pay_amount) amount ,SUM(o_discount_amount) discount_amount,SUM(o_coupon_amount) coupon_amount
    ,COUNT(order_id) order_num,COUNT(DISTINCT user_id) user_num,SUM(sale_num)
 FROM
    (SELECT  DATE(PAY_DATE) sdate,HOUR(PAY_DATE) HOUR,a2.business_name
        ,SUM(a1.`quantity_act`*a1.`sale_price`) AS GMV
        ,a1.PAY_AMOUNT-SUM(IFNULL(a1.refund_amount,0))+IFNULL(third_discount_amount,0) AS pay_amount  -- 有重复支付的问题，实收要多算一次
        ,SUM(quantity_act) sale_num,o_discount_amount,o_coupon_amount,a1.user_id,a1.order_id
    FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
    WHERE PAY_DATE>=@date0 AND PAY_DATE<ADDDATE(@sdate,1)
    GROUP BY order_id
    UNION ALL #补付款
    SELECT DATE(PAYMENT_DATE) sdate,HOUR(PAYMENT_DATE) HOUR,a2.business_name
        ,PAYMENT_MONEY gmv,PAYMENT_MONEY pay_amount
        ,0,0,0,NULL,NULL
    FROM fe_dwd.dwd_sf_after_payment a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=IFNULL(real_shelf_id,a1.SHELF_ID)
    WHERE PAYMENT_DATE>=@date0 AND PAYMENT_DATE<ADDDATE(@sdate,1)
        AND PAYMENT_STATUS=2
    ) t1
GROUP BY sdate, HOUR, business_name
;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_area_sale_hourly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_area_sale_hourly','dm_ma_area_sale_hourly','纪伟铨');
 
END