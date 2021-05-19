CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_member_research_crr_4`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
set @time_1 := CURRENT_TIMESTAMP();
#22、专一商品
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		product_id,
		abc
	FROM
		(
			SELECT
				t1.user_id,
				t2.order_qty,
				t1.product_id,
				t1.qty,
				qty / order_qty AS abc
			FROM
				(
					SELECT
						b.user_id,
						a.product_id,
						count(DISTINCT a.order_id) AS qty
					FROM
						fe.sf_order_item a
					LEFT JOIN fe.sf_order b ON a.order_id = b.order_id
					WHERE
						b.order_status = 2
					AND b.ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
					GROUP BY
						b.user_id,
						a.product_id
				) t1
			LEFT JOIN (
				SELECT
					user_id,
					count(DISTINCT order_id) AS order_qty
				FROM
					fe.sf_order
				WHERE
					order_status = 2
				AND ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
				GROUP BY
					user_id
			) t2 ON t1.user_id = t2.user_id
			WHERE
				t2.order_qty > 5
			AND qty / order_qty > 0.3
			ORDER BY
				qty / order_qty DESC
		) t1
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.product_id = a.product_id;
set @time_3 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_1--@time_3",@time_1,@time_3);
set @time_5 := CURRENT_TIMESTAMP();
#23、用户基本信息
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		member_id AS user_id,
		CASE
	WHEN SEX = 1 THEN
		'男'
	WHEN SEX = 2 THEN
		'女'
	ELSE
		'未知'
	END AS SEX,
	BIRTHDAY,
	TIMESTAMPDIFF(YEAR, BIRTHDAY, CURDATE()) AS age,
	CASE
WHEN IS_BIND_COMPANY > 0 THEN
	'已绑定企业'
ELSE
	'未绑定'
END AS IS_BIND_COMPANY,
 b.item_name AS EDU,
 c.item_name AS REG_CHANNEL,
 d.item_name AS BELONG_INDUSTRY_user
FROM
	fe.pub_member a
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 263
) b ON a.EDU = b.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 35
) c ON a.REG_CHANNEL = c.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 6
) d ON a.BELONG_INDUSTRY = d.item_value #where SEX is not null
#limit 10000
) a ON a.user_id = b.user_id
SET b.SEX = a.SEX,
 b.BIRTHDAY = a.BIRTHDAY,
 b.age = a.age,
 b.IS_BIND_COMPANY = a.IS_BIND_COMPANY,
 b.EDU = a.EDU,
 b.REG_CHANNEL = a.REG_CHANNEL,
 b.BELONG_INDUSTRY_user = a.BELONG_INDUSTRY_user;
set @time_7 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_5--@time_7",@time_5,@time_7);
set @time_9 := CURRENT_TIMESTAMP();
#1、客单价、购买时段
#2、分享型用户（有效、无效），参与用户
###24、购买时段
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		stype,
		order_qty
	FROM
		(
			SELECT
				user_id,
				stype,
				order_qty
			FROM
				(
					SELECT
						b.user_id,
						CASE
					WHEN date_format(b.order_date, '%H:%i') BETWEEN '06:00'
					AND '10:00' THEN
						'早餐'
					WHEN date_format(b.order_date, '%H:%i') BETWEEN '11:30'
					AND '14:00' THEN
						'午餐'
					WHEN date_format(b.order_date, '%H:%i') BETWEEN '15:00'
					AND '17:00' THEN
						'下午茶'
					WHEN date_format(b.order_date, '%H:%i') BETWEEN '17:00'
					AND '19:00' THEN
						'晚间餐'
					WHEN date_format(b.order_date, '%H:%i') >= '19:00'
					OR date_format(b.order_date, '%H:%i') <= '05:00' THEN
						'加班'
					ELSE
						'其它'
					END AS stype,
					count(DISTINCT b.order_id) AS order_qty
				FROM
					fe.sf_order b
				WHERE
					b.order_status IN (2, 6, 7) 
				AND ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 56 DAY)
				GROUP BY
					b.user_id,
					CASE
				WHEN date_format(b.order_date, '%H:%i') BETWEEN '06:00'
				AND '10:00' THEN
					'早餐'
				WHEN date_format(b.order_date, '%H:%i') BETWEEN '11:30'
				AND '14:00' THEN
					'午餐'
				WHEN date_format(b.order_date, '%H:%i') BETWEEN '15:00'
				AND '17:00' THEN
					'下午茶'
				WHEN date_format(b.order_date, '%H:%i') BETWEEN '17:00'
				AND '19:00' THEN
					'晚间餐'
				WHEN date_format(b.order_date, '%H:%i') >= '19:00'
				OR date_format(b.order_date, '%H:%i') <= '05:00' THEN
					'加班'
				ELSE
					'其它'
				END
				) t1
			ORDER BY
				user_id,
				order_qty DESC
		) tx
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.buy_time = a.stype;
set @time_11 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_9--@time_11",@time_9,@time_11);
set @time_13 := CURRENT_TIMESTAMP();
#25、客单价
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		GMV / order_qty AS kdj
	FROM
		(
			SELECT
				b.user_id,
				count(DISTINCT b.order_id) AS order_qty,
				sum(
					DISCOUNT_AMOUNT + COUPON_AMOUNT + PRODUCT_TOTAL_AMOUNT
				) AS GMV
			FROM
				fe.sf_order b
			WHERE
				b.order_status IN (2, 6, 7) #and user_id=17
			AND ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 56 DAY)
			GROUP BY
				b.user_id
		) t1
) a ON a.user_id = b.user_id
SET b.kdj = a.kdj;
set @time_15 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_13--@time_15",@time_13,@time_15);
set @time_17 := CURRENT_TIMESTAMP();
#26、分享型
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		sum(invite_qty) AS invite_qty,
		sum(invite_user_qty) AS invite_user_qty,
		sum(invite_secc_user_qty) AS invite_secc_user_qty,
		sum(invite_fail_user_qty) AS invite_fail_user_qty
	FROM
		(
			SELECT
				a.inviter_user_id AS user_id,
				count(DISTINCT a.invite_id) AS invite_qty,
				count(DISTINCT b.invitee_user_id) AS invite_user_qty,
				count(
					DISTINCT CASE
					WHEN b.invite_status = 1 THEN
						b.invitee_user_id
					END
				) AS invite_secc_user_qty,
				count(
					DISTINCT CASE
					WHEN b.invite_status = 3 THEN
						b.invitee_user_id
					END
				) AS invite_fail_user_qty
			FROM
				fe_activity.sf_activity_invitation a
			JOIN fe_activity.sf_activity_invitation_detail b ON a.invite_id = b.invite_id
			WHERE
				a.add_time >= '2019-04-01'
			GROUP BY
				a.inviter_user_id
			UNION ALL
				SELECT
					a.add_user_id,
					count(DISTINCT a.activity_id),
					count(
						DISTINCT CASE
						WHEN a.add_user_id <> b.user_id THEN
							b.user_id
						END
					),
					count(
						DISTINCT CASE
						WHEN a.add_user_id <> b.user_id
						AND b.prize_state = 1 THEN
							b.user_id
						END
					),
					count(
						DISTINCT CASE
						WHEN a.add_user_id <> b.user_id
						AND b.prize_state = 2 THEN
							b.user_id
						END
					)
				FROM
					fe_activity.sf_friend_coupon a
				JOIN fe_activity.sf_prize_record b ON a.activity_id = b.activity_id
				WHERE
					a.add_time >= '2019-03-01'
				GROUP BY
					a.add_user_id
		) t1
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.invite_qty = a.invite_qty,
 b.invite_user_qty = a.invite_user_qty,
 b.invite_secc_user_qty = a.invite_secc_user_qty,
 b.invite_fail_user_qty = a.invite_fail_user_qty;
set @time_19 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_17--@time_19",@time_17,@time_19);
set @time_21 := CURRENT_TIMESTAMP();
#27、参与
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		sum(qty) AS be_invite_qty
	FROM
		(
			SELECT
				invitee_user_id AS user_id,
				count(1) AS qty
			FROM
				fe_activity.sf_activity_invitation_detail
			WHERE
				data_flag = 1
			AND add_time >= '2019-04-01'
			GROUP BY
				invitee_user_id
			UNION
				SELECT
					b.user_id,
					count(1)
				FROM
					fe_activity.sf_friend_coupon a
				JOIN fe_activity.sf_prize_record b ON a.activity_id = b.activity_id
				WHERE
					a.add_user_id <> b.user_id
				AND a.add_time >= '2019-03-01'
				GROUP BY
					b.user_id
		) t1
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.be_invite_qty = a.be_invite_qty;
set @time_23 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_21--@time_23",@time_21,@time_23);
set @time_25 := CURRENT_TIMESTAMP();
#28
UPDATE feods.user_research as b
left join (select t1.user_id
,case when if(e>=2,2,e)=if(b>=2,2,b) and if(b>=2,2,b)=if(c>=2,2,c) and if(c>=2,2,c)=if(d>=2,2,d) and if(b>=2,2,b)=2 then '持续高频'
when if(e>=2,2,e)=if(b>=2,2,b) and if(b>=2,2,b)=if(c>=2,2,c) and if(c>=2,2,c)=if(d>=2,2,d) and if(b>=2,2,b)=1 then '持续低频'
when if(d>=2,2,d)>=if(e>=2,2,e) and if(b>=2,2,b)>=if(c>=2,2,c) and if(c>=2,2,c)>=if(d>=2,2,d) and if(b>=2,2,b)>=1 then '单调上涨'
when if(d>=2,2,d)<=if(e>=2,2,e) and if(b>=2,2,b)<=if(c>=2,2,c) and if(c>=2,2,c)<=if(d>=2,2,d) and if(e>=2,2,e)>=1 then '单调下降'
when e+b+c+d>=3 then '高频波动'
when e+b+c+d<3 and e+b+c+d>=1 then '低频波动' else '其他' end as user_type_1
,case when if(e>=2,2,e)=if(f>=2,2,f) and if(f>=2,2,f)=if(c>=2,2,c) and if(c>=2,2,c)=if(d>=2,2,d) and if(c>=2,2,c)=2 then '持续高频'
when if(e>=2,2,e)=if(f>=2,2,f) and if(f>=2,2,f)=if(c>=2,2,c) and if(c>=2,2,c)=if(d>=2,2,d) and if(c>=2,2,c)=1 then '持续低频'
when if(d>=2,2,d)>=if(e>=2,2,e) and if(e>=2,2,e)>=if(f>=2,2,f) and if(c>=2,2,c)>=if(d>=2,2,d) and if(c>=2,2,c)>=1 then '单调上涨'
when if(d>=2,2,d)<=if(e>=2,2,e) and if(e>=2,2,e)<=if(f>=2,2,f) and if(c>=2,2,c)<=if(d>=2,2,d) and if(f>=2,2,f)>=1 then '单调下降'
when e+f+c+d>=3 then '高频波动'
when e+f+c+d<3 and e+f+c+d>=1 then '低频波动' else '其他' end as user_type_2
,case when if(e>=2,2,e)=if(f>=2,2,f) and if(f>=2,2,f)=if(g>=2,2,g) and if(g>=2,2,g)=if(d>=2,2,d) and if(d>=2,2,d)=2 then '持续高频'
when if(e>=2,2,e)=if(f>=2,2,f) and if(f>=2,2,f)=if(g>=2,2,g) and if(g>=2,2,g)=if(d>=2,2,d) and if(d>=2,2,d)=1 then '持续低频'
when if(d>=2,2,d)>=if(e>=2,2,e) and if(e>=2,2,e)>=if(f>=2,2,f) and if(f>=2,2,f)>=if(g>=2,2,g) and if(d>=2,2,d)>=1 then '单调上涨'
when if(d>=2,2,d)<=if(e>=2,2,e) and if(e>=2,2,e)<=if(f>=2,2,f) and if(f>=2,2,f)<=if(g>=2,2,g) and if(g>=2,2,g)>=1 then '单调下降'
when e+f+g+d>=3 then '高频波动'
when e+f+g+d<3 and e+f+g+d>=1 then '低频波动' else '其他' end as user_type_3
from
(select user_id,week_order_qty
,substring_index(substring_index(week_order_qty, ',',-8),',',1)as h
,substring_index(substring_index(week_order_qty, ',',-7),',',1)as g
,substring_index(substring_index(week_order_qty, ',',-6),',',1)as f
,substring_index(substring_index(week_order_qty, ',',-5),',',1)as e
,substring_index(substring_index(week_order_qty, ',',-4),',',1)as d
,substring_index(substring_index(week_order_qty, ',',-3),',',1)as c
,substring_index(substring_index(week_order_qty, ',',-2),',',1)as b
,substring_index(week_order_qty, ',',-1) as a
from feods.user_research
) t1
) a on a.user_id=b.user_id
set b.user_type_1=a.user_type_1
,b.user_type_2=a.user_type_2
,b.user_type_3=a.user_type_3
;
set @time_27 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_25--@time_27",@time_25,@time_27);
##29、是否有未使用优惠券
	#先将该字段更新为都没有优惠券
update feods.user_research set if_coupon=0;
set @time_30 := CURRENT_TIMESTAMP();
	#将符合条件的都更新为有优惠券
UPDATE feods.user_research t1
JOIN
(SELECT u.user_id,1 AS if_coupon
FROM fe.sf_coupon_record r
JOIN feods.user_research u ON r.USER_ID=u.user_id
WHERE COUPON_STATUS=2
AND ORDER_ID IS NULL
AND VALID_TIME>=CURDATE()
UNION
SELECT u.user_id,1 AS if_coupon
FROM fe.sf_coupon_use r
JOIN feods.user_research u ON r.USER_ID=u.user_id
WHERE COUPON_STATUS=2
AND data_flag=1
AND order_id IS NULL
AND valid_end_time>=CURDATE()
)t2 ON t1.user_id=t2.user_id
SET t1.if_coupon=t2.if_coupon
;
set @time_32 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_30--@time_32",@time_30,@time_32);
set @time_34 := CURRENT_TIMESTAMP();
#30、是否关注微信
UPDATE feods.user_research t1
JOIN
(SELECT c.user_id,SUBSCRIBE AS if_subscribe
FROM fe.pub_user_wechat a
JOIN fe.pub_member b ON a.WECHAT_ID=b.WECHAT_ID
JOIN feods.user_research c ON c.user_id=b.MEMBER_ID
WHERE a.SUBSCRIBE=1
AND a.WECHAT_TYPE=1
)t2 ON t1.user_id=t2.user_id
SET t1.if_subscribe=t2.if_subscribe
;
set @time_36 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_4","@time_34--@time_36",@time_34,@time_36);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_member_research_crr_4',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
COMMIT;
END