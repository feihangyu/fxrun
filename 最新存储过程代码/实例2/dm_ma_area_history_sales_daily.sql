CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_area_history_sales_daily`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime:= CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #默认输入前一天
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
/*
    Author:	市场
    Create date: 2020-3-30
    Modify date:
    Description:	货架订单状态销售日报
    BI平台区域销售板块的模型宽表
*/
#删除数据
DELETE FROM fe_dm.dm_ma_area_history_sales_daily WHERE order_date=@sdate OR order_date<SUBDATE(@sdate,150) ; #暂不限制存储月数
# 插入数据
INSERT INTO fe_dm.dm_ma_area_history_sales_daily
    (order_date, shelf_id, order_status, amount, gmv, cogs, order_num, user_num, COUPON_AMOUNT, DISCOUNT_AMOUNT)
SELECT @sdate,shelf_id,order_status
     ,SUM(amount) amount,SUM(gmv) gmv,SUM(cogs) cogs,SUM(1) order_num,COUNT(DISTINCT user_id) user_num,SUM(o_coupon_amount) COUPON_AMOUNT,SUM(o_discount_amount) DISCOUNT_AMOUNT
FROM
    (SELECT DATE(PAY_DATE) order_date,shelf_id,user_id,order_status
        ,PAY_AMOUNT-IFNULL(SUM(a1.refund_amount),0) amount,SUM(quantity_act*sale_price) gmv
        ,SUM(quantity_act*purchase_price) cogs,o_coupon_amount,o_discount_amount
    FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
    WHERE PAY_DATE>=@sdate AND PAY_DATE<ADDDATE(@sdate,1)
    GROUP BY order_id) a1
GROUP BY shelf_id,order_status;
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_area_history_sales_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_area_history_sales_daily','dm_ma_area_history_sales_daily','纪伟铨');
END