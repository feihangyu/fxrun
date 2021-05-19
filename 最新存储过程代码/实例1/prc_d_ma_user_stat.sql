CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_user_stat`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @sweek_monday :=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));
SET @smonth :=DATE_FORMAT(@sdate,'%Y-%m-01');
#插入当日数据
DELETE FROM feods.d_ma_user_daily WHERE sdate=@sdate OR sdate<SUBDATE(@sdate,380);
INSERT INTO feods.d_ma_user_daily
    (sdate, city_name, user_num, user_num_multiorder, user_num_shelf, user_num_shelf_multiorder, user_num_shelf6, user_num_shelf6_multiorder, user_num_shelf7, user_num_shelf7_multiorder)
SELECT @sdate sdate,t1.CITY_NAME
     ,COUNT(DISTINCT USER_ID) user_num,COUNT(DISTINCT IF(ordernum>1,USER_ID,NULL)) user_num_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (7),USER_ID,NULL))user_num_shelf7,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (7),USER_ID,NULL)) user_num_shelf7_multiorder
FROM feods.zs_city_business t1
LEFT JOIN
    (SELECT  USER_ID,bb.SHELF_TYPE,bb.CITY_NAME,COUNT(1) ordernum
    FROM
        (SELECT a1.SHELF_ID, a1.USER_ID
        FROM fe_dwd.dwd_order_item_refund_day a1
        WHERE a1.PAY_DATE >= @sdate  AND a1.PAY_DATE < ADDDATE(@sdate, 1) AND a1.order_date>=@sdate
            AND a1.quantity_act>0
        UNION ALL
        SELECT IFNULL(a1.real_shelf_id, a1.SHELF_ID) SHELF_ID, user_id
        FROM fe.sf_after_payment a1
        WHERE a1.PAYMENT_DATE >=@sdate AND a1.PAYMENT_DATE < ADDDATE(@sdate, 1)
            AND a1.PAYMENT_STATUS = 5
        ) aa
    JOIN fe_dwd.dwd_shelf_base_day_all bb ON aa.shelf_id=bb.shelf_id
    GROUP BY USER_ID,bb.SHELF_TYPE
    ) t2 ON t1.CITY_NAME=t2.CITY_NAME
GROUP BY t1.CITY_NAME;
#插入当周数据
#IF DAYOFWEEK(@sdate)=1 THEN #周日才更新
DELETE FROM feods.d_ma_user_weekly WHERE sdate=@sweek_monday OR sdate<SUBDATE(@sdate,720);
INSERT INTO feods.d_ma_user_weekly
    (sdate, city_name , user_num, user_num_multiorder, user_num_shelf, user_num_shelf_multiorder
    , user_num_shelf6, user_num_shelf6_multiorder, user_num_shelf7, user_num_shelf7_multiorder)
SELECT @sweek_monday sdate,t1.CITY_NAME
     ,COUNT(DISTINCT USER_ID) user_num,COUNT(DISTINCT IF(ordernum>1,USER_ID,NULL)) user_num_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (7),USER_ID,NULL))user_num_shelf7,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (7),USER_ID,NULL)) user_num_shelf7_multiorder
FROM
    (SELECT  USER_ID,bb.SHELF_TYPE,bb.CITY_NAME,COUNT(1) ordernum
    FROM
        (SELECT  a1.SHELF_ID, a1.USER_ID
        FROM fe_dwd.dwd_order_item_refund_day a1
        WHERE a1.PAY_DATE >= @sweek_monday  AND a1.PAY_DATE < ADDDATE(@sdate, 1) AND a1.order_date>=@sweek_monday
            AND a1.quantity_act > 0
        UNION ALL
        SELECT IFNULL(a1.real_shelf_id, a1.SHELF_ID) SHELF_ID, user_id
        FROM fe.sf_after_payment a1
        WHERE a1.PAYMENT_DATE >=@sweek_monday AND a1.PAYMENT_DATE < ADDDATE(@sdate, 1)
            AND a1.PAYMENT_STATUS = 5
        ) aa
    JOIN fe_dwd.dwd_shelf_base_day_all bb ON bb.shelf_id=aa.shelf_id
    GROUP BY USER_ID,bb.SHELF_TYPE
    ) t1
GROUP BY t1.CITY_NAME;
#END IF;
#插入当月数据
#IF @sdate=LAST_DAY(@sdate) THEN #月末再更新当月数据
DELETE FROM feods.d_ma_user_monthly WHERE sdate=@smonth OR sdate<SUBDATE(@sdate,5000);
INSERT INTO feods.d_ma_user_monthly
    (sdate, city_name, user_num, user_num_multiorder, user_num_shelf, user_num_shelf_multiorder
    , user_num_shelf6, user_num_shelf6_multiorder, user_num_shelf7, user_num_shelf7_multiorder)
SELECT @smonth sdate,t1.CITY_NAME
     ,COUNT(DISTINCT USER_ID) user_num,COUNT(DISTINCT IF(ordernum>1,USER_ID,NULL)) user_num_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (7),USER_ID,NULL))user_num_shelf7,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (7),USER_ID,NULL)) user_num_shelf7_multiorder
FROM
    (SELECT  USER_ID,bb.SHELF_TYPE,bb.CITY_NAME,COUNT(1) ordernum
    FROM
        (SELECT a1.SHELF_ID, a1.USER_ID
        FROM fe_dwd.dwd_order_item_refund_day a1
        WHERE a1.PAY_DATE >= @smonth  AND a1.PAY_DATE < ADDDATE(@sdate, 1) AND a1.order_date>=@smonth
            AND a1.quantity_act > 0
        UNION ALL
        SELECT IFNULL(a1.real_shelf_id, a1.SHELF_ID) SHELF_ID, user_id
        FROM fe.sf_after_payment a1
        WHERE a1.PAYMENT_DATE >=@smonth AND a1.PAYMENT_DATE < ADDDATE(@sdate, 1)
            AND a1.PAYMENT_STATUS = 5
        ) aa
    JOIN fe_dwd.dwd_shelf_base_day_all bb ON bb.shelf_id=aa.SHELF_ID
    GROUP BY USER_ID,bb.SHELF_TYPE
    ) t1
GROUP BY t1.CITY_NAME
;
#END IF;
CALL sh_process.`sp_sf_dw_task_log`('prc_d_ma_user_stat',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END