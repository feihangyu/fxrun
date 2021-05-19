CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_next_week_birthday`()
BEGIN
	SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
	SET @date = CURDATE();
	DROP TABLE IF EXISTS feods.d_ma_next_week_birthday_temp;
	CREATE TEMPORARY TABLE feods.d_ma_next_week_birthday_temp
	(member_id BIGINT PRIMARY KEY) AS 
	SELECT 	member_id
		,sm.`CREATE_DATE` 
		,CONCAT(SUBSTRING(sm.MOBILE_PHONE,1,3),
			     SUBSTRING(sm.MOBILE_PHONE,8,1),
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='0','9',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='1','5',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='2','4',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='3','0',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='4','3',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='5','8',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='6','1',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='7','7',
				 IF(SUBSTRING(sm.MOBILE_PHONE,5,1)='8','2','6'))))))))),
			    SUBSTRING(sm.MOBILE_PHONE,10,1),
		  SUBSTRING(sm.MOBILE_PHONE,7,1),
			     SUBSTRING(sm.MOBILE_PHONE,4,1),
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='0','9',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='1','5',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='2','4',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='3','0',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='4','3',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='5','8',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='6','1',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='7','7',
				 IF(SUBSTRING(sm.MOBILE_PHONE,9,1)='8','2','6'))))))))),
			    SUBSTRING(sm.MOBILE_PHONE,6,1),
		  SUBSTRING(sm.MOBILE_PHONE,11,1)) AS 'phone'
		,REPLACE(REPLACE(REPLACE(REAL_NAME,CHAR(10),''),CHAR(9),''),CHAR(13),'' ) AS 'real_name'
		,REPLACE(REPLACE(REPLACE(NICK,CHAR(10),''),CHAR(9),''),CHAR(13),'' ) AS 'nick'
		,SEX 
		,BIRTHDAY
		,TIMESTAMPDIFF(YEAR, BIRTHDAY, CURDATE()) AS 'age'
	FROM fe.pub_member sm   # 下周生日用户 
	WHERE DATE_FORMAT(BIRTHDAY, '%m-%d') >= DATE_FORMAT(DATE_ADD(@date,INTERVAL -WEEKDAY(@date)+7 DAY),'%m-%d') 
	AND DATE_FORMAT(BIRTHDAY, '%m-%d') < DATE_FORMAT(DATE_ADD(@date,INTERVAL -WEEKDAY(@date)+14 DAY),'%m-%d') 
	;
	TRUNCATE TABLE feods.d_ma_next_week_birthday;
	INSERT INTO feods.d_ma_next_week_birthday
	(member_id,create_time,phone, `name`, nick, sex ,birthday ,age ,last_buy_time ,last_buy_shelf)
	SELECT a.member_id,
	  a.CREATE_DATE,
	  a.phone,
	  a.real_name,
	  a.nick,
	  a.sex,
	  a.birthday,
	  a.age,
	  zf.last_buy_time ,
	  zf.shelf_id 
	FROM feods.d_ma_next_week_birthday_temp a 
	LEFT JOIN feods.`zs_shelf_member_flag` zf ON zf.user_id = a.member_id
	WHERE a.phone IS NOT NULL
	;
	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_next_week_birthday',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
	COMMIT;
END