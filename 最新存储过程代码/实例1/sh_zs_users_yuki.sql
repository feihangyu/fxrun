CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_zs_users_yuki`()
BEGIN
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
#1、更新昨天购买用户
UPDATE feods.zs_users b
JOIN
(SELECT
	o.user_id,
	MAX(ORDER_DATE) AS lastOrderDay,
	COUNT(DISTINCT o.ORDER_ID)+u.frequency AS frequency,
	sum(o.PRODUCT_TOTAL_AMOUNT)+u.monetary AS monetary,
	SUM(
		CASE
		WHEN COUPON_AMOUNT > 0 THEN
			COUPON_AMOUNT
		END
	) +u.couponCount AS couponCount
FROM
	fe.sf_order o
JOIN feods.zs_users u ON o.USER_ID=u.USER_ID
WHERE
	order_status IN (2, 6, 7)
AND ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
AND ORDER_DATE < CURDATE()
GROUP BY
	o.user_id)a ON a.user_id=b.user_id
SET b.lastOrderDay=a.lastOrderDay,b.frequency=a.frequency,b.monetary=a.monetary,b.couponCount=a.couponCount
;
#2、插入昨日首单用户数据
INSERT INTO feods.zs_users 
(user_id,firstOrderDay,lastOrderDay,frequency,monetary,couponCount)
SELECT
	a.user_id,
	MIN(ORDER_DATE) AS firstOrderDay,
	MAX(ORDER_DATE) AS lastOrderDay,
	COUNT(DISTINCT ORDER_ID) AS frequency,
	sum(PRODUCT_TOTAL_AMOUNT) AS monetary,
	SUM(
		CASE
		WHEN COUPON_AMOUNT > 0 THEN
			COUPON_AMOUNT
		END
	) AS couponCount
FROM
	fe.sf_order a
LEFT JOIN feods.zs_users b ON a.USER_ID=b.USER_ID
WHERE
	order_status IN (2, 6, 7)
AND ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
AND ORDER_DATE < CURDATE()
AND b.USER_ID IS NULL
GROUP BY
	a.user_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_zs_users_yuki',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('雅琪@', @user, @timestamp));
COMMIT;
END