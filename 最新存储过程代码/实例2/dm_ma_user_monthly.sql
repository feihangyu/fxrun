CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_user_monthly`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每天默认传入前一天
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)) #当周一
    ,@smonth=DATE_FORMAT(@sdate,'%Y-%m-01') #月1号
    ;
#删除历史数据
DELETE FROM fe_dm.dm_ma_user_monthly WHERE sdate=@smonth OR sdate<SUBDATE(@sdate,5000);
#临时数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_stat;# 用户下单统计
CREATE TEMPORARY TABLE fe_dm.tmp_user_stat(INDEX(business_name)) AS
SELECT  USER_ID,bb.SHELF_TYPE,bb.business_name,COUNT(1) ordernum
    FROM
        (SELECT a1.SHELF_ID, a1.USER_ID
        FROM fe_dwd.dwd_order_item_refund_day a1
        WHERE a1.PAY_DATE >= @smonth AND a1.PAY_DATE < DATE_ADD(@smonth,INTERVAL 1 MONTH )
            AND a1.quantity_act>0
        GROUP BY a1.order_id
        UNION ALL
        SELECT IFNULL(a1.real_shelf_id, a1.SHELF_ID) SHELF_ID, user_id
        FROM fe_dwd.dwd_sf_after_payment a1
        WHERE a1.PAYMENT_DATE >= @smonth AND a1.PAYMENT_DATE < DATE_ADD(@smonth,INTERVAL 1 MONTH )
            AND a1.PAYMENT_STATUS = 5
        ) aa
    JOIN fe_dwd.dwd_shelf_base_day_all bb ON aa.shelf_id=bb.shelf_id
    GROUP BY USER_ID,bb.SHELF_TYPE;
INSERT INTO fe_dm.dm_ma_user_monthly
    (sdate, business_name, user_num, user_num_multiorder, user_num_shelf, user_num_shelf_multiorder, user_num_shelf6, user_num_shelf6_multiorder, user_num_shelf7, user_num_shelf7_multiorder)
SELECT @smonth sdate,t1.business_name
     ,COUNT(DISTINCT USER_ID) user_num,COUNT(DISTINCT IF(ordernum>1,USER_ID,NULL)) user_num_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE NOT IN (6,7),USER_ID,NULL)) user_num_shelf_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (6),USER_ID,NULL)) user_num_shelf6_multiorder
     ,COUNT(DISTINCT IF( SHELF_TYPE IN (7),USER_ID,NULL))user_num_shelf7,COUNT(DISTINCT IF(ordernum>1 AND SHELF_TYPE IN (7),USER_ID,NULL)) user_num_shelf7_multiorder
FROM (SELECT DISTINCT business_name FROM  fe_dwd.dwd_city_business ) t1
LEFT JOIN fe_dm.tmp_user_stat t2 ON t2.business_name=t1.business_name
GROUP BY t1.business_name;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_user_monthly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user),@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_user_monthly','dm_ma_user_monthly','纪伟铨');
END