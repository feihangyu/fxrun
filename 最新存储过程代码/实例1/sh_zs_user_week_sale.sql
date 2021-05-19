CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_zs_user_week_sale`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @week_end := SUBDATE(
	CURRENT_DATE,
	WEEKDAY(CURRENT_DATE) + 1
)
 ;
DELETE
FROM
	feods.zs_user_week_sale
WHERE
	sdate =@week_end or sdate<subdate(@week_end,7*60);
INSERT INTO feods.zs_user_week_sale (
	sdate,
	user_id,
	buy_rate,
	coupon_order,
	discount_order,
	share_order,
	amount,
	gmv
)
SELECT
	@week_end  AS sdate,
	t1.user_id,
	COUNT(DISTINCT order_id) AS buy_rate,
	COUNT(
		DISTINCT CASE
		WHEN t1.COUPON_AMOUNT > 0 THEN
			t1.ORDER_ID
		END
	) AS coupon_order,
	COUNT(
		DISTINCT CASE
		WHEN t1.discount_amount > 0 THEN
			t1.ORDER_ID
		END
	) AS discount_order,
	t2.share_order,
	SUM(PRODUCT_TOTAL_AMOUNT) - SUM(refund_amount) AS amount,
	SUM(gmv) AS gmv
FROM
	(
		SELECT
			o.order_id,
			o.ORDER_STATUS,
			o.SHELF_ID,
			DATE_FORMAT(o.ORDER_DATE, '%Y-%m-%d') ORDER_DATE,
			o.USER_ID,
			o.PRODUCT_TOTAL_AMOUNT,
			o.DISCOUNT_AMOUNT,
			o.COUPON_AMOUNT,
			SUM(
				CASE
				WHEN o.ORDER_STATUS = 2 THEN
					a.QUANTITY * a.SALE_PRICE
				ELSE
					a.quantity_shipped * a.SALE_PRICE
				END
			) AS GMV,
			SUM(IFNULL(e.refund_amount, 0)) AS refund_amount
		FROM
			fe.sf_order_item a
		JOIN fe.sf_order o ON a.order_id = o.ORDER_ID
		LEFT JOIN fe.sf_order_refund_order e ON (
			o.ORDER_ID = e.order_id
			AND e.refund_status = 5
			AND e.data_flag = 1
			AND o.ORDER_STATUS = 6
		)
		WHERE
			o.ORDER_STATUS IN (2, 6, 7)
		AND o.ORDER_DATE >= SUBDATE(@week_end, 7 - 1)
		AND o.ORDER_DATE < ADDDATE(@week_end, 1)
		GROUP BY
			o.order_id
	) t1
LEFT JOIN (
	SELECT
		sdate,
		user_id,
		COUNT(DISTINCT ORDER_ID) AS share_order
	FROM
		(
			/*SELECT
				@week_end AS sdate,
				user_id,
				ORDER_ID
			FROM
				fe.sf_coupon_record_his
			WHERE
				USERD_TIME >= SUBDATE(@week_end, 7 - 1)
			AND USERD_TIME < ADDDATE(@week_end, 1)
			AND COUPON_CHANNEL = 3
			AND COUPON_STATUS = 3

			UNION  */
				SELECT
					@week_end  AS sdate,
					user_id,
					ORDER_ID
				FROM
					fe.sf_coupon_record
				WHERE
					USERD_TIME >= SUBDATE(@week_end, 7 - 1)
				AND USERD_TIME < ADDDATE(@week_end, 1)
				AND COUPON_CHANNEL = 3
				AND COUPON_STATUS = 3
		) d1
	GROUP BY
		sdate,
		user_id
) t2 ON t1.user_id = t2.user_id
GROUP BY
	@week_end ,
	t1.user_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('sh_zs_user_week_sale',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('未知@', @user, @timestamp));
END