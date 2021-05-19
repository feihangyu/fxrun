CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_ma_user_stat_info`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SET @sdate=CURRENT_DATE; #每天默认传入前一天
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)) #当周一
    ,@sweek_end= SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)-6) #当周日
    ,@smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
# 插入新用户
INSERT INTO fe_dm.dm_ma_user_stat_info
    (user_id,birthday)
SELECT a1.user_id,IFNULL(a1.BIRTHDAY,'0000-00-00') birthday
FROM fe_dwd.dwd_user_day_inc a1
LEFT JOIN fe_dm.dm_ma_user_stat_info a2 ON a2.user_id=a1.user_id
WHERE a2.user_id IS NULL
;
#更新数据
    #最近前5喜好商品
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_product5;
CREATE TEMPORARY TABLE fe_dm.tmp_product5(INDEX(user_id)) AS
    SELECT user_id,SUBSTRING_INDEX(GROUP_CONCAT(product_id ORDER BY quantity_act DESC),',',5) product_id_top5
    FROM
        (SELECT user_id,product_id,SUM(quantity_act) quantity_act
        FROM fe_dwd.dwd_pub_order_item_recent_one_month a1
        WHERE PAY_DATE>=SUBDATE(@sdate,28) AND PAY_DATE<@sdate
        GROUP BY user_id,product_id) a1
    GROUP BY user_id
;
UPDATE fe_dm.dm_ma_user_stat_info a1
JOIN fe_dm.tmp_product5 a2 ON a2.user_id =a1.user_id
SET a1.product_id_top5=a2.product_id_top5
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_ma_user_stat_info',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('纪伟铨@', @user, @timestamp)
  );
COMMIT;
    END