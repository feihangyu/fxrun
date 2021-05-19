CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_user_day_inc`()
BEGIN
   SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dwd.dwd_user_day_inc WHERE CREATE_DATE >= @start_date;
## 需要同步更新dwd_update_dwd_table_info 里面的脚本
-- 抽取每一天新增的用户    
DROP TEMPORARY TABLE IF EXISTS fe_dwd.user_lsl_tmp;
CREATE TEMPORARY TABLE fe_dwd.user_lsl_tmp AS
SELECT 
t1.MEMBER_ID  AS user_id,
t1.REAL_NAME,
t1.NICK,
t1.CREATE_DATE,
t1.SEX  gender,
t1.BIRTHDAY,
t1.BELONG_INDUSTRY,
b.ITEM_NAME AS BELONG_INDUSTRY_desc,
t1.REG_CHANNEL,
ff.item_name AS REG_CHANNEL_desc,					
t1.BIND_PHONE_DATE,
CASE WHEN LENGTH(IF(LENGTH(t1.mobile_phone)>0,t1.mobile_phone,d.mobile_phone))>0 THEN '已填电话号码' ELSE '未填电话号码' END if_register,     -- CASE WHEN IF(t1.mobile_phone IS NULL,d.mobile_phone,t1.mobile_phone) IS NULL THEN '未填电话号码' ELSE '已填电话号码' END if_register,
IF(LENGTH(t1.mobile_phone)>0,t1.mobile_phone,d.mobile_phone) AS mobile_phone,        -- IF(t1.mobile_phone IS NULL,d.mobile_phone,t1.mobile_phone) AS mobile_phone,
t1.EDU ,
e.item_name AS EDU_desc,
t1.EMAIL,
CASE WHEN t1.IS_BIND_COMPANY > 0 THEN '已绑定企业' ELSE '未绑定' END AS IS_BIND_COMPANY,
t1.ADDRESS,
t1.WECHAT_ID,
CASE
      WHEN c.second_user_type = 1
      THEN '全职店主'
      WHEN c.second_user_type = 2
      THEN '兼职店主' ELSE '非店主'
    END AS manager_type,
f.member_level,
d.OPEN_TYPE
FROM fe.pub_member t1	
LEFT JOIN fe.pub_dictionary_item b ON (t1.BELONG_INDUSTRY=b.ITEM_VALUE AND b.DICTIONARY_ID=6)
LEFT JOIN fe.pub_user_integral_growth f ON t1.MEMBER_ID=f.user_id
LEFT JOIN fe.pub_shelf_manager c ON t1.MOBILE_PHONE =c.MOBILE_PHONE AND c.data_flag = 1
LEFT JOIN
  (SELECT user_id,mobile_phone,GROUP_CONCAT(open_type SEPARATOR '/') AS OPEN_TYPE FROM fe.pub_user_open 
   WHERE data_flag = 1
   GROUP BY user_id
   ) d
 ON t1.MEMBER_ID = d.USER_ID 
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 263
) e ON t1.EDU = e.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 35
) ff ON t1.REG_CHANNEL = ff.item_value
WHERE t1.CREATE_DATE  >= @start_date
    AND t1.CREATE_DATE < @end_date;
	
CREATE INDEX idx_user_id
ON fe_dwd.user_lsl_tmp (user_id);
CREATE INDEX idx_wechat_id
ON fe_dwd.user_lsl_tmp (wechat_id);
CREATE INDEX idx_mobile_phone
ON fe_dwd.user_lsl_tmp (mobile_phone);
	
-- 提取顺丰员工的基表  获取手机号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.tmp_lsl_phone;   
CREATE TEMPORARY TABLE  fe_dwd.tmp_lsl_phone
( 
mobile	VARCHAR(50)		COMMENT 'mobile',                                              
PRIMARY KEY (`mobile`)
) ENGINE=INNODB DEFAULT CHARSET=utf8 COMMENT='user_tmp';
	
INSERT INTO fe_dwd.tmp_lsl_phone
(
mobile  
)
SELECT 
DISTINCT b.mobile 
FROM fe_group.sf_group_emp b
WHERE LENGTH(b.mobile) > 0 AND b.data_flag=1
UNION 
SELECT 
DISTINCT c.MOBILE_PHONE AS mobile 
FROM fe.pub_user_open c
WHERE data_flag = 1 
AND c.open_type IN ('SFIM','XMF')  -- 餐卡渠道 小蜜蜂
AND LENGTH(c.MOBILE_PHONE) > 0
;	
CREATE INDEX idx_tmp_lsl_phone
ON fe_dwd.tmp_lsl_phone (mobile);
	
-- 提取顺丰员工的基表  获取手机号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.order_lsl_sf_tmp;
CREATE TEMPORARY TABLE fe_dwd.order_lsl_sf_tmp AS	
SELECT DISTINCT
b.mobile_phone
FROM 	
fe_dwd.user_lsl_tmp b 
JOIN 
fe_dwd.tmp_lsl_phone s
ON s.mobile=b.mobile_phone ;	
	
-- 每周末更新一下用户是否顺丰员工 
UPDATE fe_dwd.dwd_user_day_inc AS b
JOIN fe_dwd.order_lsl_sf_tmp a 
ON a.MOBILE_PHONE = b.MOBILE_PHONE
SET b.if_sfer = IF(a.mobile_phone IS NULL,0,1),
b.load_time  = CURRENT_TIMESTAMP
;
-- WHERE @week_flag = 1;
-- 更新一下用户手机号 绑定时间 第三方平台标识
DROP TEMPORARY TABLE IF EXISTS fe_dwd.tmp_lsl_phone_update;   
CREATE TEMPORARY TABLE  fe_dwd.tmp_lsl_phone_update AS
SELECT DISTINCT
t1.MEMBER_ID  AS user_id,
t1.BIND_PHONE_DATE,
IF(t1.mobile_phone IS NULL,d.mobile_phone,t1.mobile_phone) AS mobile_phone,
d.OPEN_TYPE
FROM fe.pub_member t1
LEFT JOIN
  (SELECT user_id,mobile_phone,GROUP_CONCAT(open_type SEPARATOR '/') AS OPEN_TYPE FROM fe.pub_user_open 
   WHERE data_flag = 1
   GROUP BY user_id
   ) d
 ON t1.MEMBER_ID = d.USER_ID 
WHERE t1.LAST_UPDATE_DATE  >= @start_date;
 
CREATE INDEX idx_order_lsl_level_1
ON fe_dwd.tmp_lsl_phone_update (user_id);
 
UPDATE fe_dwd.dwd_user_day_inc AS b
JOIN fe_dwd.tmp_lsl_phone_update a 
ON a.user_id = b.user_id
SET b.mobile_phone = a.mobile_phone,
b.if_register = CASE WHEN LENGTH(a.mobile_phone)>0 THEN '已填电话号码' ELSE '未填电话号码' END,
 b.BIND_PHONE_DATE = a.BIND_PHONE_DATE,
 b.OPEN_TYPE = a.OPEN_TYPE,
 b.load_time  = CURRENT_TIMESTAMP
;
-- 更新一下用户会员等级  存量用户的等级也会变化，需要更新一下
DROP TEMPORARY TABLE IF EXISTS fe_dwd.level_lsl_tmp;
CREATE TEMPORARY TABLE fe_dwd.level_lsl_tmp AS
SELECT 
user_integral_id,
user_id,
member_level
FROM
fe.pub_user_integral_growth a 
WHERE a.last_update_time >= @start_date;
CREATE INDEX idx_order_lsl_level_1
ON fe_dwd.level_lsl_tmp (user_id);
UPDATE fe_dwd.dwd_user_day_inc AS b
JOIN fe_dwd.level_lsl_tmp a 
ON a.user_id=b.user_id
SET b.member_level = a.member_level,
 b.load_time  = CURRENT_TIMESTAMP;	
-- 提取用户首单的时间  提取当天订单表中的订单，取首单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.order_lsl_tmp;
CREATE TEMPORARY TABLE fe_dwd.order_lsl_tmp AS
SELECT e.user_id,e.order_date ,e.shelf_id  FROM 
    (SELECT 
    IF(@user_id = user_id, @rank := @rank + 1,@rank := 0) AS rank,
    @user_id := user_id AS user_id ,
     @order_date := order_date AS order_date, 
     @shelf_id := shelf_id AS shelf_id
    FROM
    (
    SELECT a.user_id,a.order_date,a.shelf_id
    FROM fe.sf_order a
    WHERE  a.data_flag = 1
	AND a.order_date >= @start_date 
    ORDER BY a.user_id,a.order_date 
    ) m1) e
WHERE e.rank = 0	;
CREATE INDEX idx_order_lsl_tmp_2
ON fe_dwd.order_lsl_tmp (shelf_id);
-- 提取首单货架地区信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_order_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_order_tmp AS
  SELECT DISTINCT
	b.CITY_NAME,
	b.CITY,
	b.BUSINESS_CODE,
	b.business_name,
	b.REGION_CODE,
	b.REGION_NAME,
    a.user_id,
	a.order_date AS min_order_date,
	s.shelf_id
  FROM fe_dwd.order_lsl_tmp a   -- 提取用户首单的时间
    JOIN fe.sf_shelf s
      ON a.shelf_id = s.shelf_id
      AND s.data_flag = 1
    JOIN feods.fjr_city_business b
      ON b.city = s.city;
  
  
CREATE INDEX idx_user_lsl_tmp_3
ON fe_dwd.area_order_tmp (user_id);
-- 更新一下注册了但是没有购买的用户，跑数时段购买了的首单信息
UPDATE fe_dwd.dwd_user_day_inc AS b
JOIN fe_dwd.area_order_tmp a 
ON a.user_id = b.user_id
SET b.min_order_date = a.min_order_date,
 b.CITY_NAME = a.CITY_NAME,
 b.business_area = a.business_name,
 b.REGION_NAME = a.REGION_NAME,
 b.first_shelf_id = a.shelf_id,
 b.load_time  = CURRENT_TIMESTAMP
WHERE
	b.min_order_date IS NULL;
INSERT INTO fe_dwd.dwd_user_day_inc
(
region_name,
business_area,
CITY_NAME,
user_id,
REAL_NAME,
NICK,
CREATE_DATE ,
gender,
BIRTHDAY,
BELONG_INDUSTRY,
BELONG_INDUSTRY_desc ,
REG_CHANNEL,
REG_CHANNEL_desc,
if_sfer,
manager_type,
OPEN_TYPE,
member_level,
BIND_PHONE_DATE,
mobile_phone,
if_register,
IS_BIND_COMPANY,
EDU,
EMAIL,
ADDRESS,
min_order_date,
first_shelf_id,
WECHAT_ID,
WECHAT_TYPE ,
NICKNAME    
)
SELECT 
b.REGION_NAME,
b.business_name AS business_area ,
b.CITY_NAME,
a.user_id,
a.REAL_NAME,
a.NICK,
a.CREATE_DATE,
a.gender,
a.BIRTHDAY,
a.BELONG_INDUSTRY,
a.BELONG_INDUSTRY_desc,
a.REG_CHANNEL,
a.REG_CHANNEL_desc,
IF(d.mobile_phone IS NULL,0,1) AS if_sfer,
a.manager_type,
a.OPEN_TYPE,
a.member_level,
a.BIND_PHONE_DATE,
a.mobile_phone,
a.if_register,
a.IS_BIND_COMPANY,
a.EDU,
a.EMAIL,
a.ADDRESS,
b.min_order_date,
b.shelf_id first_shelf_id,
a.WECHAT_ID,
c.WECHAT_TYPE,
c.NICKNAME
FROM 
fe_dwd.user_lsl_tmp a 
LEFT JOIN 
fe_dwd.area_order_tmp b
ON a.user_id = b.user_id
LEFT JOIN
fe.pub_user_wechat c 
ON a.WECHAT_ID = c.WECHAT_ID
LEFT JOIN 
fe_dwd.order_lsl_sf_tmp d 
ON a.mobile_phone=d.mobile_phone
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_user_day_inc',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END