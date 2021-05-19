CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_member_research_crr_3`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @time_1 := CURRENT_TIMESTAMP();
#####17、库存情况
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		a.shelf_id,
		SUM(a.STOCK_QUANTITY) AS STOCK_QUANTITY,
		SUM(
			CASE
			WHEN a.SALES_FLAG = 5
			AND a.NEW_FLAG = 2 THEN
				a.STOCK_QUANTITY * a.SALE_PRICE
			END
		) AS stock_value_5,
		SUM(
			a.STOCK_QUANTITY * a.SALE_PRICE
		) AS stock_value
	FROM fe_dwd.dwd_shelf_product_day_all a
	WHERE
		a.STOCK_QUANTITY >= 0
	GROUP BY
		a.shelf_id
) a ON a.shelf_id = b.shelf_id
SET b.STOCK_QUANTITY = a.STOCK_QUANTITY,
 b.stock_value_5 = a.stock_value_5,
 b.stock_value = a.stock_value;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_member_research_crr_3","@time_1--@time_3",@time_1,@time_3);
SET @time_5 := CURRENT_TIMESTAMP();
#####18、最后一次盘点时间
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		shelf_id,
		MAX(OPERATE_TIME) AS max_OPERATE_TIME
	FROM
		fe.sf_shelf_check
	WHERE
		OPERATE_TIME >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
	GROUP BY
		shelf_id
) a ON a.shelf_id = b.shelf_id
SET b.max_OPERATE_TIME = a.max_OPERATE_TIME;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_member_research_crr_3","@time_5--@time_7",@time_5,@time_7);
SET @time_9 := CURRENT_TIMESTAMP();
####19、货架注册信息
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		b.shelf_id,
		c.item_name AS BELONG_INDUSTRY,
		d.item_name AS BUSINESS_CHARACTERISTICS,
		b.FLOOR_STAFF_NUM,
		e.item_name AS COMPETING_STATUS,
		CONCAT(
			SUBSTRING_INDEX(STAFF_AGE_STAGES, ',', 1),
			'-',
			SUBSTRING_INDEX(STAFF_AGE_STAGES, ',' ,- 1)
		) AS STAFF_AGE_STAGES,
		f.item_name AS FEMALE_RATIO,
		g.item_name AS WORK_OVERTIME_FREQ,
		CASE
	WHEN a.OVERTIME_MEAL = 1 THEN
		'有加班餐'
	ELSE
		'无'
	END AS OVERTIME_MEAL,
	CASE
WHEN a.WORK_NIGHT = 1 THEN
	'有夜班'
ELSE
	'无夜班'
END AS WORK_NIGHT,
 CASE
WHEN a.HAVE_FRIDGE = 1 THEN
	'有冰箱'
ELSE
	'无冰箱'
END AS HAVE_FRIDGE,
 h.item_name AS EMPLOYEE_WELFARE,
 i.item_name AS WORK_ATMOSPHERE
FROM
	fe.sf_shelf_apply_record a
LEFT JOIN fe.sf_shelf_apply b ON a.RECORD_ID = b.RECORD_ID
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 6
) c ON a.BELONG_INDUSTRY = c.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 120
) d ON a.BUSINESS_CHARACTERISTICS = d.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 76
) e ON a.COMPETING_STATUS = e.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 124
) f ON a.FEMALE_RATIO = f.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 125
) g ON a.WORK_OVERTIME_FREQ = g.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 129
) h ON a.WORK_OVERTIME_FREQ = h.item_value
LEFT JOIN (
	SELECT
		item_value,
		item_name
	FROM
		fe.pub_dictionary_item
	WHERE
		dictionary_id = 145
) i ON a.WORK_OVERTIME_FREQ = i.item_value
WHERE
	a.data_flag = 1
AND b.AUDIT_STATUS = 2
AND b.shelf_id IS NOT NULL
AND b.shelf_id > 0
) a ON a.shelf_id = b.shelf_id
SET b.BELONG_INDUSTRY = a.BELONG_INDUSTRY,
 b.BUSINESS_CHARACTERISTICS = a.BUSINESS_CHARACTERISTICS,
 b.FLOOR_STAFF_NUM = a.FLOOR_STAFF_NUM,
 b.COMPETING_STATUS = a.COMPETING_STATUS,
 b.STAFF_AGE_STAGES = a.STAFF_AGE_STAGES,
 b.FEMALE_RATIO = a.FEMALE_RATIO,
 b.WORK_OVERTIME_FREQ = a.WORK_OVERTIME_FREQ,
 b.OVERTIME_MEAL = a.OVERTIME_MEAL,
 b.WORK_NIGHT = a.WORK_NIGHT,
 b.HAVE_FRIDGE = a.HAVE_FRIDGE,
 b.EMPLOYEE_WELFARE = a.EMPLOYEE_WELFARE,
 b.WORK_ATMOSPHERE = a.WORK_ATMOSPHERE;
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_member_research_crr_3","@time_9--@time_11",@time_9,@time_11);
SET @time_13 := CURRENT_TIMESTAMP();
#用户
#20、补付款
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		SUM(PAYMENT_MONEY) AS PAYMENT_MONEY,
		MAX(PAY_DATE) AS PAY_DATE
	FROM
		fe.sf_after_payment
	WHERE
		PAYMENT_STATUS = 2
	GROUP BY
		user_id
) a ON a.user_id = b.user_id
SET b.PAYMENT_MONEY = a.PAYMENT_MONEY,
 b.PAY_DATE = a.PAY_DATE;
SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_member_research_crr_3","@time_13--@time_15",@time_13,@time_15);
SET @time_17 := CURRENT_TIMESTAMP();
#21、活动敏感
UPDATE feods.user_research AS b
LEFT JOIN (
	SELECT
		user_id,
		e AS huodongmingan,
		CONCAT(
			IFNULL(a, ''),
			',',
			IFNULL(b, ''),
			',',
			IFNULL(c, ''),
			',',
			IFNULL(d, '')
		) AS user_yxgj
	FROM
		(
			SELECT
				user_id,
				CASE
			WHEN COUNT(
				DISTINCT CASE
				WHEN COUPON_AMOUNT > 0 THEN
					order_id
				END
			) / COUNT(DISTINCT order_id) > 0.3 THEN
				1
			END AS a,
			CASE
		WHEN COUNT(
			DISTINCT CASE
			WHEN DISCOUNT_AMOUNT > 0 THEN
				order_id
			END
		) / COUNT(DISTINCT order_id) > 0.3 THEN
			2
		END AS b,
		CASE
	WHEN COUNT(
		DISTINCT CASE
		WHEN DISCOUNT_AMOUNT > 0
		OR COUPON_AMOUNT > 0 THEN
			order_id
		END
	) / COUNT(DISTINCT order_id) > 0.3 THEN
		3
	END AS c,
	CASE
WHEN COUNT(
	DISTINCT CASE
	WHEN DISCOUNT_AMOUNT = 0
	AND COUPON_AMOUNT = 0 THEN
		order_id
	END
) / COUNT(DISTINCT order_id) > 0.3 THEN
	4
END AS d,
 CASE
WHEN COUNT(
	DISTINCT CASE
	WHEN DISCOUNT_AMOUNT + COUPON_AMOUNT > 0 THEN
		order_id
	END
) / COUNT(DISTINCT order_id) > 0.7 THEN
	1
WHEN COUNT(
	DISTINCT CASE
	WHEN DISCOUNT_AMOUNT + COUPON_AMOUNT > 0 THEN
		order_id
	END
) / COUNT(DISTINCT order_id) BETWEEN 0.5
AND 0.7 THEN
	2
WHEN COUNT(
	DISTINCT CASE
	WHEN DISCOUNT_AMOUNT + COUPON_AMOUNT > 0 THEN
		order_id
	END
) / COUNT(DISTINCT order_id) < 0.5 THEN
	3
END AS e
FROM
	fe.sf_order
WHERE
	order_date >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
GROUP BY
	user_id
		) t1
) a ON a.user_id = b.user_id
SET b.huodongmingan = a.huodongmingan,
 b.user_yxgj = a.user_yxgj;
SET @time_19 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_member_research_crr_3","@time_17--@time_19",@time_17,@time_19);

-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_member_research_crr_3',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
COMMIT;
END