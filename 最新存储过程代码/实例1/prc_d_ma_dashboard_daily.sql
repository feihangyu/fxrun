CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_dashboard_daily`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @smonth=DATE_FORMAT(CURDATE(),'%Y-%m-01')
    ,@sweek=DATE_ADD(CURDATE(),INTERVAL -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 DAY)
;
### 每10分钟更新今日数据,如果时间在7:00 至 7:05之间 则更新近15天数据
DELETE  FROM feods.d_ma_city_dashboard_daily WHERE sdate>=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE()) OR (CURTIME() BETWEEN '07:00' AND '07:05' AND sdate<DATE_SUB(@sweek,INTERVAL 12 WEEK))  ;
    # 插入订单数据
INSERT INTO feods.d_ma_city_dashboard_daily
    (sdate, city_name, workday_num
    ,sale_num, GMV, amount, after_pay_amount, discount_amount, coupon_amount, order_num,shelfnum_sale
    )
SELECT sdate,IFNULL(city_name,'other') city_name1,workday_num
     ,SUM(sale_num) sale_num, SUM(GMV),SUM(amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(discount_amount) discount_amount,SUM(coupon_amount) coupon_amount,SUM(order_num) order_num
     ,SUM( IF(sale_num>0 AND a2.SHELF_TYPE NOT IN (2,4,5) ,1,0) ) shelfnum_sale
FROM feods.d_ma_shelf_sale_daily a1
LEFT JOIN fe.sf_shelf a2 ON a1.SHELF_ID=a2.SHELF_ID
WHERE sdate>=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE())
GROUP BY sdate,city_name1
;
    # 更热日用户数
UPDATE feods.d_ma_city_dashboard_daily t1
JOIN
    (SELECT sdate,cc.CITY_NAME,COUNT(DISTINCT USER_ID) user_num
    FROM
        (SELECT a1.SHELF_ID, a1.USER_ID,DATE(ORDER_DATE) sdate
        FROM fe.sf_order a1
        JOIN fe.sf_order_item a2 ON a1.ORDER_ID = a2.ORDER_ID AND a2.DATA_FLAG = 1
        LEFT JOIN fe.sf_order_refund_order a3
            ON a1.ORDER_ID = a3.order_id AND a3.refund_status = 5 AND a3.data_flag = 1
        WHERE a1.ORDER_DATE >= IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE())
            AND a1.DATA_FLAG = 1
            AND a1.ORDER_STATUS IN (2, 6, 7)
            AND IF(a1.ORDER_STATUS = 6, a2.quantity_shipped, a2.QUANTITY) > 0
        UNION ALL
        SELECT IFNULL(a1.real_shelf_id, a1.SHELF_ID) SHELF_ID, user_id,DATE(PAYMENT_DATE) sdate
        FROM fe.sf_after_payment a1
        WHERE a1.PAYMENT_DATE >=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE())
            AND a1.PAYMENT_STATUS = 5
        ) aa
    LEFT JOIN fe.sf_shelf bb ON aa.SHELF_ID=bb.SHELF_ID
    LEFT JOIN feods.fjr_city_business cc ON bb.CITY=cc.CITY
    GROUP BY sdate,cc.CITY_NAME
    )t2 ON t1.sdate=t2.sdate AND t1.city_name=t2.CITY_NAME
SET t1.user_num=t2.user_num
WHERE t1.sdate>=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE())
;
    # 插入未有销售城市数据
INSERT INTO feods.d_ma_city_dashboard_daily
    (sdate,city_name)
SELECT a2.sdate,a1.city_name
FROM fe.zs_city_business a1
JOIN feods.fjr_work_days a2 ON a2.sdate>=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE()) AND a2.sdate<=CURDATE()
LEFT JOIN feods.d_ma_city_dashboard_daily a3 ON  a3.sdate=a2.sdate AND a3.city_name=a1.CITY_NAME
WHERE a3.pid IS NULL
;
    # 更新运营终端数量 新增终端数 撤架终端数
UPDATE feods.d_ma_city_dashboard_daily t1
JOIN
    (SELECT a2.sdate,IFNULL(city_name,'other') city_name1
	    ,COUNT(1) shelfnum_operating
	    ,COUNT(CASE WHEN DATE(a1.ACTIVATE_TIME)=a2.sdate THEN 1  END )  shelfnum_new
	    ,COUNT(CASE WHEN DATE(a1.REVOKE_TIME)=a2.sdate THEN 1 END  ) shelfnum_revoke
    FROM fe.sf_shelf a1
    LEFT JOIN feods.fjr_city_business a3 ON a1.CITY=a3.CITY
    JOIN feods.fjr_work_days a2 ON a2.sdate>=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE()) AND a2.sdate<=CURDATE()
                                       AND IFNULL(DATE(a1.ACTIVATE_TIME),'2017-10-01')<=a2.sdate AND IFNULL(REVOKE_TIME,'2099-12-31')>=a2.sdate
    WHERE a1.DATA_FLAG=1 AND a1.SHELF_STATUS NOT IN (10,1) AND a1.SHELF_TYPE NOT IN (9,2,4,5)
    GROUP BY sdate,city_name1
    ) t2 ON t1.sdate=t2.sdate AND t1.city_name=t2.city_name1
SET t1.shelfnum_operating=t2.shelfnum_operating,t1.shelfnum_new=t2.shelfnum_new,t1.shelfnum_revoke=t2.shelfnum_revoke
WHERE t1.sdate>=IF(CURTIME() BETWEEN '07:00' AND '07:05',DATE_SUB(CURDATE(),INTERVAL 14 DAY),CURDATE())
;
# 更新当日终端生命周期维度货架商品数据
UPDATE feods.d_ma_city_dashboard_daily t1
JOIN
    (SELECT city_name
        ,SUM(IF(if_valid =1,1,0)) productnum_valid
        ,SUM(IF(if_valid =1 AND if_sale=1 AND if_out=0 AND SALES_FLAG IN(1,2,3),1,0)) productnum_valid_qualified
        ,SUM(IF(if_valid =1 AND if_sale=1 AND if_out=0 AND SALES_FLAG IN(1,2,3) AND STOCK_QUANTITY>0,1,0)) productnum_valid_qualified_stored
    FROM feods.d_ma_shelf_product_temp
    GROUP BY city_name
    ) t2 ON t1.city_name=t2.city_name
SET t1.productnum_valid=t2.productnum_valid,t1.productnum_valid_qualified=t2.productnum_valid_qualified,t1.productnum_valid_qualified_stored=t2.productnum_valid_qualified_stored
WHERE t1.sdate=CURDATE()
;
# 当日终端生命周期维度货架库存等数据
UPDATE
    (SELECT a1.CITY_NAME
            ,SUM(IF(a2.pid IS NOT NULL,1,0)) shelfnum_valid
            ,SUM(IF(a2.pid IS NOT NULL AND a4.stock_type='其他',1,0)) shelfnum_valid_quantity_satisfied
            ,SUM(IF(a1.REVOKE_STATUS IN (1,4) AND a4.stock_quantity=0,1,0 )) shelfnum_0stored
            ,SUM(IF(a1.REVOKE_STATUS IN (1,4) AND a1.CLOSE_TYPE=6 AND a1.WHETHER_CLOSE=2 AND a4.stock_quantity>0,1,0 ))shelfnum_closed
    FROM fe_dwd.dwd_shelf_base_day_all a1
    LEFT JOIN feods.zs_shelf_flag a2 ON a1.SHELF_ID=a2.shelf_id AND a2.ext4 >0 #货架生命周期维度
    LEFT JOIN feods.zs_buhuo_shelf_action_history  a4 ON a4.sdate=curdate() AND a1.SHELF_ID=a4.shelf_id
    GROUP BY city_name
    ) t2
JOIN feods.d_ma_city_dashboard_daily t1 ON t1.city_name=t2.city_name
SET t1.shelfnum_valid=t2.shelfnum_valid,t1.shelfnum_valid_quantity_satisfied=t2.shelfnum_valid_quantity_satisfied
  ,t1.shelfnum_0stored=t2.shelfnum_0stored,t1.shelfnum_closed=t2.shelfnum_closed
WHERE t1.sdate=CURDATE()
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_dashboard_daily',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
END