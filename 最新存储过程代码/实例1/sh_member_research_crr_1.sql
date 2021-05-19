CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_member_research_crr_1`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
set @time_1 := CURRENT_TIMESTAMP();
#####1、新增用户（每天）
INSERT INTO feods.user_research (
	CREATE_DATE,
	user_id,
	BIND_PHONE_DATE,
	mobile_phone,
	if_register
) SELECT
	a.CREATE_DATE,
	a.member_id AS user_id,
	a.BIND_PHONE_DATE,
	a.mobile_phone,
	CASE
WHEN a.mobile_phone IS NULL THEN
	'未填电话号码'
ELSE
	'已填电话号码'
END if_register
FROM
	fe.pub_member a
LEFT JOIN feods.user_research b ON a.member_id = b.user_id
WHERE
	b.user_id IS NULL;
set @time_3 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_1--@time_3",@time_1,@time_3);
set @time_5 := CURRENT_TIMESTAMP();
#2、更新手机号绑定状态（日）
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		a.member_id AS user_id,
		a.BIND_PHONE_DATE,
		ifnull(
			a.mobile_phone,
			b.mobile_phone
		) AS mobile_phone,
		CASE
	WHEN ifnull(
		a.mobile_phone,
		b.mobile_phone
	) IS NULL THEN
		'未填电话号码'
	ELSE
		'已填电话号码'
	END if_register,
	b.OPEN_TYPE
FROM
	fe.pub_member a
LEFT JOIN fe.pub_user_open b ON a.MEMBER_ID = b.USER_ID
) a ON a.user_id = b.user_id
SET b.BIND_PHONE_DATE = a.BIND_PHONE_DATE,
 b.mobile_phone = a.mobile_phone,
 b.if_register = a.if_register,
 b.OPEN_TYPE = a.OPEN_TYPE;
set @time_7 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_5--@time_7",@time_5,@time_7);
set @time_9 := CURRENT_TIMESTAMP();
###3、订单数（日）
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		min(order_date) AS min_order_date,
		max(order_date) AS max_order_date,
		count(DISTINCT order_id) AS order_qty
	FROM
		fe.sf_order
	WHERE
		order_status IN (2, 6, 7)
	AND order_date > (
		SELECT
			max(max_order_date)
		FROM
			feods.user_research
	)
	AND order_date < CURDATE()
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.order_qty = ifnull(b.order_qty, 0) + a.order_qty;
set @time_11 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_9--@time_11",@time_9,@time_11);
set @time_13 := CURRENT_TIMESTAMP();
##4、更新最小下单时间（日）
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		min(order_date) AS min_order_date,
		max(order_date) AS max_order_date,
		count(DISTINCT order_id) AS order_qty
	FROM
		fe.sf_order
	WHERE
		order_status IN (2, 6, 7)
	AND order_date > (
		SELECT
			max(max_order_date)
		FROM
			feods.user_research
	)
	AND order_date < CURDATE()
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.min_order_date = a.min_order_date
WHERE
	b.min_order_date IS NULL;
set @time_15 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_13--@time_15",@time_13,@time_15);
set @time_17 := CURRENT_TIMESTAMP();
##5、更新最大下单时间（日）
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		min(order_date) AS min_order_date,
		max(order_date) AS max_order_date,
		count(DISTINCT order_id) AS order_qty
	FROM
		fe.sf_order
	WHERE
		order_status IN (2, 6, 7)
	AND order_date > (
		SELECT
			max(max_order_date)
		FROM
			feods.user_research
	)
	AND order_date < CURDATE()
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.max_order_date = a.max_order_date
WHERE
	ifnull(
		b.max_order_date,
		DATE_SUB(CURDATE(), INTERVAL 8 DAY)
	) < a.max_order_date;
set @time_19 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_17--@time_19",@time_17,@time_19);
set @time_21 := CURRENT_TIMESTAMP();
#6、新用户——前几周的周销售订单数（周）
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		CONCAT(
			i,
			',',
			h,
			',',
			g,
			',',
			f,
			',',
			e,
			',',
			d,
			',',
			c,
			',',
			b,
			',',
			a
		) AS week_order_qty
	FROM
		(
			SELECT
				a.user_id,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 1 WEEK)
					) THEN
						order_id
					END
				) AS a,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 2 WEEK)
					) THEN
						order_id
					END
				) AS b,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 3 WEEK)
					) THEN
						order_id
					END
				) AS c,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 4 WEEK)
					) THEN
						order_id
					END
				) AS d,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 5 WEEK)
					) THEN
						order_id
					END
				) AS e,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 6 WEEK)
					) THEN
						order_id
					END
				) AS f,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 7 WEEK)
					) THEN
						order_id
					END
				) AS g,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 8 WEEK)
					) THEN
						order_id
					END
				) AS h,
				count(
					DISTINCT CASE
					WHEN WEEKOFYEAR(order_date) = WEEKOFYEAR(
						DATE_SUB(CURDATE(), INTERVAL 9 WEEK)
					) THEN
						order_id
					END
				) AS i
			FROM
				fe.sf_order a
			LEFT JOIN feods.user_research b ON a.user_id = b.user_id
			WHERE
				order_status IN (2, 6, 7)
			AND order_date >= date_sub(CURDATE(), INTERVAL 70 DAY) #and user_id=1015748
			AND b.last_week_date IS NULL
			GROUP BY
				a.user_id #limit 1000
		) t1
) a ON a.user_id = b.user_id
SET b.week_order_qty = a.week_order_qty,
 b.last_week_date = YEARWEEK(CURDATE(), 1)
WHERE
	a.user_id IS NOT NULL;
set @time_23 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_21--@time_23",@time_21,@time_23);
set @time_25 := CURRENT_TIMESTAMP();
##7、更新上周销量周（周）
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		count(DISTINCT order_id) AS last_week_order_qty
	FROM
		fe.sf_order
	WHERE
		ORDER_STATUS IN (2, 6, 7)
	AND order_date >= DATE_SUB(
		DATE_SUB(
			CURDATE(),
			INTERVAL DATE_FORMAT(CURDATE(), '%w') - 1 DAY
		),
		INTERVAL 1 WEEK
	)
	AND order_date < DATE_SUB(
		CURDATE(),
		INTERVAL DATE_FORMAT(CURDATE(), '%w') - 1 DAY
	)
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.last_week_order_qty = a.last_week_order_qty;
set @time_27 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sh_member_research_crr_1","@time_25--@time_27",@time_25,@time_27);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_member_research_crr_1',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
COMMIT;
END