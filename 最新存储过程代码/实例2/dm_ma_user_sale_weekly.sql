CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_user_sale_weekly`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场
-- Create date: 2020-3-19
-- Modify date:
-- Description: 每周运行一次插入当周用户维度数据
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
#删除数据
SET @spartition=CONCAT('pweek',MOD(YEARWEEK(@sweek),100));
SET @sql1=CONCAT('alter table  fe_dm.dm_ma_user_sale_weekly truncate partition ',@spartition);
PREPARE sqlstr FROM @sql1;
EXECUTE sqlstr;
#插入数据
INSERT INTO fe_dm.dm_ma_user_sale_weekly
    ( sdate, user_id, orders, discount_orders,sale_qty, gmv, pay_amount)
SELECT @sweek,user_id,COUNT(DISTINCT order_id) orders,COUNT(DISTINCT IF(discount_amount+COUPON_AMOUNT+third_discount_amount>0,order_id,NULL)) discount_orders
    ,SUM(quantity_act) sale_qty,SUM(gmv),SUM(pay_amount_product-refund_amount) pay_amount
FROM
    (SELECT a1.order_id
        #,a1.PAY_DATE,a1.shelf_id,a1.product_id
        ,a1.user_id
        ,quantity_act*sale_price gmv,pay_amount_product,quantity_act
        ,discount_amount,COUPON_AMOUNT,IFNULL(third_discount_amount,0) third_discount_amount
        ,IFNULL(refund_amount,0) refund_amount
    FROM fe_dwd.dwd_order_item_refund_day a1
    WHERE PAY_DATE>=@sweek AND PAY_DATE<ADDDATE(@sweek,7)
    ) a1
GROUP BY user_id
;
UPDATE fe_dm.dm_ma_user_sale_weekly a1
JOIN (
    SELECT USER_ID,SUM(PAYMENT_MONEY) after_payment
    FROM fe_dwd.dwd_sf_after_payment
    WHERE PAYMENT_DATE>=@sweek AND PAYMENT_DATE<ADDDATE(@sweek,7)
        AND PAYMENT_STATUS=2
    GROUP BY USER_ID
    ) a2 ON a2.USER_ID=a1.user_id
SET a1.after_payment=a2.after_payment
WHERE a1.sdate=@sweek
;
#记录日志
 CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_user_sale_weekly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
END