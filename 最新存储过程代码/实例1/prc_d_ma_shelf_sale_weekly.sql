CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_sale_weekly`(IN p_sweek DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sweek;
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));#当周一
DELETE FROM feods.d_ma_shelf_sale_weekly WHERE (sweek=@sweek OR sweek<SUBDATE(@sweek,7*100 )) ;
# 插入当周数据
INSERT INTO feods.d_ma_shelf_sale_weekly
    ( sweek, SHELF_ID ,shelf_type,manager_id, city_name
    , sale_num, GMV, amount, after_pay_amount, DISCOUNT_AMOUNT, COUPON_AMOUNT, order_num,refund_amount)
SELECT @sweek sweek, a1.SHELF_ID,a3.ITEM_NAME shelf_type,a2.MANAGER_ID,a1.city_name
    ,SUM(sale_num) sale_num,SUM(GMV), SUM(amount) amount ,SUM(after_pay_amount) after_pay_amount,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT
    ,SUM(COUPON_AMOUNT) COUPON_AMOUNT,SUM(order_num) order_num,SUM(refund_amount) refund_amount
FROM feods.d_ma_shelf_sale_daily a1
JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.SHELF_ID
LEFT JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.ITEM_VALUE=a2.shelf_type AND a3.DICTIONARY_ID=8
WHERE a1.sdate>=@sweek AND  a1.sdate<ADDDATE(@sweek,7)
GROUP BY SHELF_ID
;
# 更新本周用户数,复购用户数
UPDATE feods.d_ma_shelf_sale_weekly t1
JOIN
    (SELECT SHELF_ID,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT shelf_id,user_id,COUNT(DISTINCT order_id) orders
	    FROM fe_dwd.dwd_pub_order_item_recent_two_month
	    WHERE PAY_DATE>=@sweek AND PAY_DATE<ADDDATE(@sweek,7)
        GROUP BY shelf_id, user_id
        )t
    GROUP BY SHELF_ID
    ) t2 ON   t1.SHELF_ID=t2.SHELF_ID
SET t1.user_num=t2.user_num,t1.user_num_reorder=t2.user_num_reorder
WHERE  t1.sweek=@sweek
;

    #更新上周数据
UPDATE feods.d_ma_shelf_sale_weekly a1
JOIN feods.d_ma_shelf_sale_weekly a2 ON a2.sweek=SUBDATE(@sweek,7) AND a2.SHELF_ID=a1.SHELF_ID
SET a1.gmv_last=a2.GMV,a1.order_num_last=a2.order_num,a1.user_num_last=a2.user_num
WHERE a1.sweek=@sweek;

-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('prc_d_ma_shelf_sale_weekly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
END