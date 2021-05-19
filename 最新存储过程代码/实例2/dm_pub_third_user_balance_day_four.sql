CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_pub_third_user_balance_day_four`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    fe_dm.dm_en_order_user
  WHERE sdate = CURDATE();
INSERT INTO fe_dm.dm_en_order_user(user_id, sdate)
SELECT DISTINCT t.order_user_id , CURDATE() sdate
FROM (
SELECT DISTINCT so.order_user_id
FROM fe_dwd.dwd_group_order_refound_address_day  so 
WHERE so.pay_state = 2 
AND so.order_date >= (SELECT MAX(a.sdate) FROM fe_dm.dm_en_order_user AS a ) 
AND so.order_user_id NOT IN (SELECT b.user_id FROM fe_dm.dm_en_order_user AS b )
UNION ALL 
SELECT DISTINCT sp1.order_user_id
FROM fe_dwd.dwd_group_emp_user_day sc    
JOIN fe_dwd.dwd_group_order_refound_address_day sp1 ON sc.order_id = sp1.order_id  
WHERE sc.`data_flag` =1
AND sp1.`pay_state` = 2  
AND sp1.`order_date` >= (SELECT MAX(a1.sdate) FROM fe_dm.dm_en_order_user AS a1 ) 
AND sp1.order_user_id NOT IN (SELECT b1.user_id FROM fe_dm.dm_en_order_user AS b1 )
)t 
ON DUPLICATE KEY UPDATE sdate = CURDATE()
;
 COMMIT;
 -- 清表后将 用户各个渠道首单日期插入表中
TRUNCATE TABLE fe_dm.`dm_en_user_channle_first`;
INSERT INTO fe_dm.dm_en_user_channle_first(sdate,user_id,sale_channel,order_time,order_date)
SELECT CURDATE()sdate, order_user_id,sale_channel,MIN(order_date) AS min_date_time, DATE(MIN(order_date)) min_date
FROM fe_dwd.dwd_group_order_refound_address_day
WHERE pay_state = 2
GROUP BY order_user_id,sale_channel
ON DUPLICATE KEY UPDATE sdate = CURDATE()   -- 唯一键冲突时 更新 sdate 
;
COMMIT;
  -- 将新用户首单日对应的余额插入表中
DELETE FROM fe_dm.`dm_en_new_user_balance` 
WHERE order_date = SUBDATE(CURDATE(),INTERVAL 1 DAY) ;
INSERT INTO fe_dm.`dm_en_new_user_balance`(user_id,sale_channel,item_name,order_time,order_date,open_id,balance)
SELECT sf.user_id ,sf.sale_channel,d.ITEM_VALUE,sf.order_time,sf.order_date,po.open_id,sb.balance
FROM fe_dm.dm_en_user_channle_first sf
JOIN fe_dwd.dwd_user_day_inc po ON po.USER_ID = sf.user_id AND po.open_type = sf.sale_channel    
LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe_dwd.dwd_pub_dictionary WHERE dictionary_id=192) d ON sf.sale_channel=d.ITEM_VALUE    
JOIN fe_dwd.dwd_sf_third_user_balance sb ON sb.`open_id` = po.open_id AND  sb.channel = po.open_type     
WHERE sb.data_flag = 1
GROUP BY sf.user_id ,sf.sale_channel,po.open_id
HAVING sf.order_date = SUBDATE(CURDATE(),INTERVAL 1 DAY)
ON DUPLICATE KEY UPDATE balance = balance
;
COMMIT;
  -- 结存每日第三余额表更新的用户
DELETE FROM fe_dm.dm_pub_third_user_balance_day 
WHERE sdate = SUBDATE(CURDATE(),INTERVAL 1 DAY);
INSERT INTO fe_dm.dm_pub_third_user_balance_day(open_id,balance,channel,sdate)
SELECT sb.open_id,
sb.balance,
sb.channel,
DATE(sb.last_update_time)
FROM fe_dwd.dwd_sf_third_user_balance sb
WHERE sb.last_update_time >= SUBDATE(CURDATE(),INTERVAL 1 DAY)
AND sb.last_update_time < CURDATE();
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_pub_third_user_balance_day_four',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('黎尼和@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_user','dm_pub_third_user_balance_day_four','黎尼和');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_user_channle_first','dm_pub_third_user_balance_day_four','黎尼和');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_new_user_balance','dm_pub_third_user_balance_day_four','黎尼和');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_pub_third_user_balance_day','dm_pub_third_user_balance_day_four','黎尼和');
 
END