CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_tag_num`()
BEGIN
   DECLARE t_error INTEGER; 
   DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
   START TRANSACTION;
	SET @end_date = CURDATE();
	SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
	SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
	INSERT INTO feods.d_ma_tag_num(sdate, `type`, num, date_num )
	-- UPDATE feods.d_ma_tag_num tt1
	-- 
	-- JOIN 
	-- (
	  -- 性别用户
	  
	 SELECT tt2.sdate, tt2.`type`, tt1.num+tt2.date_num AS num , tt2.date_num
	 FROM feods.d_ma_tag_num tt1
	 JOIN
	 (
	SELECT 
	@start_date AS sdate,  -- 统计日期
	'总的有性别信息用户数' AS `type`, -- 用户类型
	COUNT(DISTINCT t.member_id) date_num  -- 日增量
	FROM
	(
	-- 总用户信息表
	SELECT              
	a.member_id                   
	FROM       
	fe.`pub_member` a
	WHERE a.sex IS NOT NULL
	AND CREATE_DATE >=@start_date 
	AND CREATE_DATE < @end_date
	UNION ALL
	-- 用户职场形象信息收集回答表
	SELECT 
	b.user_id 
	FROM  
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10000
	AND b.content_id IN (100000,100001)
	UNION ALL
	-- 科技对接信息 （可以考虑忽略）
	-- SELECT 
	-- c.user_id
	-- FROM
	-- feods.`user_age_gender` c  
	-- WHERE c.gender <> 3
	-- 
	-- UNION ALL
	-- 线下调查问卷用户信息表
	-- SELECT 
	-- d.pid
	-- FROM
	-- feods.`questionnaire_survey_member` d
	-- WHERE  d.sex IN (1,2)
	-- UNION ALL
	-- 寄件标签 新增有性别标签用户数
	SELECT t1.MEMBER_ID
	FROM
	(SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a WHERE a.sex IS  NULL 
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date        
	 )t1
	 INNER JOIN feods.zs_user_send_tech_new zus ON zus.mobile_phone = t1.MOBILE_PHONE AND zus.sex IS NOT NULL
	 
	 UNION ALL
	 -- 收件标签 新增有性别标签用户数
	SELECT t2.MEMBER_ID
	FROM
	(SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a WHERE a.sex IS  NULL 
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date        
	 )t2
	 INNER JOIN feods.`zs_user_receive_tech_new` zur ON zur.mobile_phone = t2.MOBILE_PHONE AND zur.sex IS NOT NULL
	) t
	UNION ALL
	-- 年龄
	SELECT 
	@start_date AS 截止日期,
	'总的有年龄信息用户数' AS 用户类型,
	COUNT(DISTINCT t.member_id) 总的有年龄信息用户数 
	FROM
	(
	-- 总用户信息表
	SELECT 
	a.member_id
	FROM
	fe.`pub_member` a
	WHERE a.BIRTHDAY IS NOT NULL
	AND CREATE_DATE >= @start_date
	AND CREATE_DATE < @end_date
	UNION ALL
	-- 用户职场形象信息收集回答表h5
	SELECT 
	b.user_id
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND
	b.survey_id =1000
	AND 
	b.question_id = 10002
	AND b.content_id IN (100015,
	100016,
	100017,
	100018,
	100019
	)
	UNION ALL
	-- 科技对接信息 （可以考虑忽略）
	-- SELECT 
	-- c.user_id
	-- FROM
	-- feods.`user_age_gender` c 
	-- WHERE c.age_level <> 5
	-- UNION ALL
	-- 线下调查问卷用户信息表
	-- SELECT 
	-- d.pid
	-- FROM
	-- feods.`questionnaire_survey_member` d
	-- WHERE  d.AGE_LEVEL  <> 5
	-- UNION ALL
	-- 寄件标签 
	SELECT t1.MEMBER_ID
	FROM
	(
	 SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a
	 WHERE a.BIRTHDAY IS  NULL
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date
	 )t1 INNER JOIN feods.zs_user_send_tech_new zus ON zus.mobile_phone = t1.MOBILE_PHONE AND zus.age IS NOT NULL
	 
	UNION ALL
	-- 收件标签  
	SELECT t2.MEMBER_ID
	FROM
	(
	 SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a
	 WHERE a.BIRTHDAY IS  NULL
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date
	 )t2 INNER JOIN feods.zs_user_send_tech_new zur ON zur.mobile_phone = t2.MOBILE_PHONE AND zur.age IS NOT NULL 
	) t
	UNION ALL
	-- 职业
	SELECT 
	@start_date AS 截止日期,
	'总的有职业信息用户数' AS 用户类型,
	COUNT(DISTINCT t.member_id) 总的有职业信息用户数 
	FROM
	(
	-- 总用户信息表
	SELECT 
	a.member_id
	FROM
	fe.`pub_member` a
	WHERE a.BELONG_INDUSTRY IS NOT NULL
	AND CREATE_DATE >= @start_date
	AND CREATE_DATE < @end_date
	UNION ALL
	-- 用户职场形象信息收集回答表h5
	SELECT 
	b.user_id
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10001
	AND b.content_id IN (100002,
	100003,
	100004,
	100005,
	100006,
	100007,
	100008,
	100009,
	100010,
	100011,
	100012,
	100013,
	100014
	)
	-- UNION ALL
	-- 线下调查问卷用户信息表 
	-- SELECT 
	-- d.pid
	-- FROM
	-- feods.`questionnaire_survey_member` d
	-- WHERE  d.PROFESSION IS NOT NULL  
	) t
	UNION ALL
	-- 口味
	-- 用户职场形象信息收集回答表h5
	SELECT 
	@start_date AS 截止日期,
	'有口味信息用户数' AS 用户类型,
	COUNT(DISTINCT b.user_id) AS 有口味信息用户数
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10003
	AND b.content_id IN (100015,
	100020,
	100021,
	100022,
	100023,
	100024,
	100025,
	100026,
	100027
	)
	UNION ALL
	  -- 所有 性别 年龄 职业 口味
	SELECT 
	@start_date AS 截止日期,
	'有标签的用户总数' AS 用户类型,
	COUNT(DISTINCT t.member_id) AS 有标签的用户总数
	FROM
	(
	-- 总用户信息表 之 性别
	SELECT 
	a.member_id
	FROM
	fe.`pub_member` a
	WHERE a.sex IS NOT NULL
	AND CREATE_DATE >= @start_date
	AND CREATE_DATE < @end_date 
	UNION ALL 
	-- 用户职场形象信息收集回答表h5
	SELECT 
	b.user_id
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10000
	AND b.content_id IN (100000,100001)
	-- UNION ALL
	-- 科技对接
	-- SELECT 
	-- c.user_id
	-- FROM
	-- feods.`user_age_gender` c 
	-- WHERE c.gender <> 3
	-- 
	-- UNION ALL
	-- 线下调查问卷用户信息表
	-- SELECT 
	-- d.pid
	-- FROM
	-- feods.`questionnaire_survey_member` d
	-- WHERE  d.sex IN (1,2)
	UNION ALL
	-- 用户信息表之 年龄
	SELECT 
	a.member_id
	FROM
	fe.`pub_member` a
	WHERE a.BIRTHDAY IS NOT NULL
	AND CREATE_DATE >= @start_date
	AND CREATE_DATE < @end_date
	UNION ALL
	-- 用户职场形象信息收集回答表 之 年龄
	SELECT 
	b.user_id
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @strat_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10002
	AND b.content_id IN (100015,
	100016,
	100017,
	100018,
	100019
	)
	UNION ALL
	-- 科技对接 之 年龄
	-- SELECT 
	-- c.user_id
	-- FROM
	-- feods.`user_age_gender` c 
	-- WHERE c.age_level <> 5
	-- 
	-- UNION ALL
	-- 线下问卷调查 之 年龄
	-- SELECT 
	-- d.pid
	-- FROM
	-- feods.`questionnaire_survey_member` d
	-- WHERE  d.AGE_LEVEL  <> 5
	-- UNION ALL
	-- 总用户信息表之 行业
	SELECT 
	a.member_id
	FROM
	fe.`pub_member` a
	WHERE a.BELONG_INDUSTRY IS NOT NULL
	AND CREATE_DATE >= @start_date
	AND CREATE_DATE < @end_date
	UNION ALL
	-- 用户职场形象信息收集回答表 之 职业
	SELECT 
	b.user_id
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10001
	AND b.content_id IN (100002,
	100003,
	100004,
	100005,
	100006,
	100007,
	100008,
	100009,
	100010,
	100011,
	100012,
	100013,
	100014
	)
	-- UNION ALL
	-- 
	-- -- 线下问卷调查之 职业
	-- SELECT 
	-- d.pid
	-- FROM
	-- feods.`questionnaire_survey_member` d
	-- WHERE  d.PROFESSION IS NOT NULL   --  职业
	UNION ALL
	-- 用户职场形象信息收集回答表 之 口味
	SELECT 
	b.user_id
	FROM 
	fe_activity.`sf_user_profile_survey_answer` b
	WHERE b.add_time >= @start_date
	AND b.add_time < @end_date
	AND b.survey_id =1000
	AND b.question_id = 10003
	AND b.content_id IN (100015,
	100020,
	100021,
	100022,
	100023,
	100024,
	100025,
	100026,
	100027
	)
	UNION ALL
	SELECT t1.MEMBER_ID
	FROM
	(
	 -- 寄件标签 之 年龄
	 SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a
	 WHERE a.BIRTHDAY IS  NULL
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date
	 )t1 INNER JOIN feods.zs_user_send_tech_new zus ON zus.mobile_phone = t1.MOBILE_PHONE AND zus.age IS NOT NULL
	 
	 UNION ALL
	 
	 -- 寄件标签 之 性别
	 SELECT t2.MEMBER_ID
	FROM
	(SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a WHERE a.sex IS  NULL 
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date
	 )t2
	 INNER JOIN feods.zs_user_send_tech_new zus ON zus.mobile_phone = t2.MOBILE_PHONE AND zus.sex IS NOT NULL
	 
	 UNION ALL
	 
	SELECT t3.MEMBER_ID
	FROM
	(
	 -- 收件标签 之 年龄
	 SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a
	 WHERE a.BIRTHDAY IS NULL
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date
	 )t3 INNER JOIN feods.`zs_user_receive_tech_new` zur ON zur.mobile_phone = t3.MOBILE_PHONE AND zur.age IS NOT NULL
	 
	 UNION ALL
	 
	SELECT t4.MEMBER_ID
	FROM
	(
	 -- 收件标签 之 性别
	 SELECT a.MOBILE_PHONE,a.MEMBER_ID
	 FROM fe.`pub_member` a
	 WHERE a.sex IS NULL
	 AND CREATE_DATE >= @start_date
	 AND CREATE_DATE < @end_date
	 )t4 INNER JOIN feods.`zs_user_receive_tech_new` zur ON zur.mobile_phone = t4.MOBILE_PHONE AND zur.sex IS NOT NULL
	 
	)t
	UNION ALL
	-- 总用户标签表
	SELECT 
	@start_date AS 截止日期,
	'总用户数' AS 用户类型,
	COUNT(DISTINCT t.member_id) AS 总用户数
	FROM fe.`pub_member` t
	WHERE create_date >= @start_date
	AND create_date < @end_date
	) tt2 ON tt1.sdate = SUBDATE(tt2.sdate,INTERVAL 1 DAY) AND tt1.type = tt2.type
	ON DUPLICATE KEY UPDATE add_time =  CURRENT_TIMESTAMP()
	-- SET tt1.date_num = tt2.date_num
	;
	IF t_error = 1 THEN  
             ROLLBACK;  
         ELSE  
             COMMIT;  
         END IF;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_tag_num',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
    END