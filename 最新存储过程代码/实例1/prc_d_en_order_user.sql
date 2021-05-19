CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_order_user`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.d_en_order_user
  WHERE sdate = CURDATE();
INSERT INTO feods.d_en_order_user(user_id, sdate)
SELECT DISTINCT t.order_user_id , CURDATE() sdate
FROM (
SELECT DISTINCT so.order_user_id
FROM fe_goods.`sf_group_order` so
JOIN fe_goods.sf_group_order_pay sp ON so.order_id = sp.order_id 
WHERE so.`data_flag` =1
AND sp.`pay_state` = 2 
AND so.`order_date` >= (SELECT MAX(a.sdate) FROM feods.d_en_order_user AS a ) 
AND so.order_user_id NOT IN (SELECT b.user_id FROM feods.d_en_order_user AS b )
UNION ALL 
SELECT DISTINCT sc.order_user_id
FROM fe_goods.`sf_scan_order`  sc
JOIN fe_goods.sf_group_order_pay sp1 ON sc.order_id = sp1.order_id
WHERE sc.`data_flag` =1
AND sp1.`pay_state` = 2  
AND sc.`order_date` >= (SELECT MAX(a1.sdate) FROM feods.d_en_order_user AS a1 ) 
AND sc.order_user_id NOT IN (SELECT b1.user_id FROM feods.d_en_order_user AS b1 )
)t 
ON DUPLICATE KEY UPDATE sdate = CURDATE()
;
 COMMIT;
 -- 清表后将 用户各个渠道首单日期插入表中
TRUNCATE TABLE feods.`d_en_user_channle_first`;
INSERT INTO feods.d_en_user_channle_first(sdate,user_id,sale_channel,order_time,order_date)
SELECT CURDATE()sdate, b.order_user_id,b.sale_channel,MIN(b.order_date) AS min_date_time, DATE(MIN(b.order_date)) min_date
FROM fe_goods.sf_group_order b 
JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
WHERE b.data_flag=1 
AND pay_state = 2
AND c.data_flag = 1
AND b.data_flag = 1
GROUP BY order_user_id,b.sale_channel
ON DUPLICATE KEY UPDATE sdate = CURDATE()   -- 唯一键冲突时 更新 sdate 
;
COMMIT;
  -- 将新用户首单日对应的余额插入表中
DELETE FROM feods.`d_en_new_user_balance` 
WHERE order_date = SUBDATE(CURDATE(),INTERVAL 1 DAY) ;
INSERT INTO feods.`d_en_new_user_balance`(user_id,sale_channel,item_name,order_time,order_date,open_id,balance)
SELECT sf.user_id ,sf.sale_channel,d.ITEM_VALUE,sf.order_time,sf.order_date,po.open_id,sb.balance
FROM feods.d_en_user_channle_first sf
JOIN fe.pub_user_open po ON po.USER_ID = sf.user_id AND po.open_type = sf.sale_channel
LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON sf.sale_channel=d.ITEM_VALUE
JOIN `fe_goods`.`sf_third_user_balance` sb ON sb.`open_id` = po.open_id AND  sb.channel = po.open_type 
WHERE sb.data_flag = 1
AND po.data_flag = 1
GROUP BY sf.user_id ,sf.sale_channel,po.open_id
HAVING sf.order_date = SUBDATE(CURDATE(),INTERVAL 1 DAY)
ON DUPLICATE KEY UPDATE balance = balance
;
COMMIT;
  -- 结存每日第三余额表更新的用户
DELETE FROM feods.sf_third_user_balance_day 
WHERE sdate = SUBDATE(CURDATE(),INTERVAL 1 DAY);
INSERT INTO feods.sf_third_user_balance_day(open_id,balance,channel,sdate)
SELECT sb.open_id,
sb.balance,
sb.channel,
DATE(sb.last_update_time)
FROM fe_goods.sf_third_user_balance sb
WHERE sb.last_update_time >= SUBDATE(CURDATE(),INTERVAL 1 DAY)
AND sb.last_update_time < CURDATE();
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_en_order_user',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
COMMIT;
END