CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_zs_shelf_grade`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
  TRUNCATE TABLE feods.zs_shelf_grade;
  INSERT INTO feods.zs_shelf_grade (SHELF_ID, GRADE, add_time)
  SELECT
    SHELF_ID, shelf_type, add_time
  FROM
    (SELECT
      w.SHELF_ID, s.huosun, q.GMV, ABS(s.huosun), IFNULL(xx.bk_money, 0),
      CASE
        WHEN (s.huosun - IFNULL(xx.bk_money, 0)) / (
          q.GMV + ABS(s.huosun) + IFNULL(xx.bk_money, 0)
        ) >= 0.08
        THEN 1
        WHEN (s.huosun - IFNULL(xx.bk_money, 0)) / (
          q.GMV + ABS(s.huosun) + IFNULL(xx.bk_money, 0)
        ) >= 0.05
        AND s.huosun / (q.GMV + ABS(s.huosun)) < 0.08
        THEN 2
        WHEN (s.huosun - IFNULL(xx.bk_money, 0)) / (
          q.GMV + ABS(s.huosun) + IFNULL(xx.bk_money, 0)
        ) >= 0.03
        AND s.huosun / (q.GMV + ABS(s.huosun)) < 0.05
        THEN 3
        WHEN (s.huosun - IFNULL(xx.bk_money, 0)) / (
          q.GMV + ABS(s.huosun) + IFNULL(xx.bk_money, 0)
        ) < 0.03
        THEN 5
      END AS shelf_type, CURDATE() AS add_time
    FROM
      (SELECT
        SHELF_ID
      FROM
        fe.sf_shelf
      WHERE DATA_FLAG = 1
        AND SHELF_STATUS = 2
        AND SHELF_CODE <> ''
        AND MANAGER_NAME NOT LIKE '%作废%'
        AND LEFT(SHELF_CODE, 1) != 'Z') w
      LEFT JOIN
        (SELECT
          a.SHELF_ID, - (
            SUM(a.ERROR_NUM * a.SALE_PRICE) + SUM(
              IF(
                a.AUDIT_STATUS = 2, a.AUDIT_ERROR_NUM * a.SALE_PRICE, 0
              )
            )
          ) huosun
        FROM
          fe.sf_shelf_check_detail AS a
          LEFT JOIN fe.sf_shelf_check AS b
            ON a.CHECK_ID = b.CHECK_ID
        WHERE (
            a.ERROR_REASON = 3
            OR a.ERROR_REASON IS NULL
          )
          AND DATE_FORMAT(b.OPERATE_TIME, '%Y-%m-%d') BETWEEN DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 0 DAY), '%Y-%m-01'
          )
          AND DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 0 DAY), '%Y-%m-%d'
          )
        GROUP BY a.SHELF_ID) s
        ON w.SHELF_ID = s.SHELF_ID
      LEFT JOIN
        (SELECT
          f.shelf_id, SUM(e.QUANTITY * e.SALE_PRICE) GMV
        FROM
          fe.sf_order_item AS e
          LEFT JOIN fe.sf_order AS f
            ON e.order_id = f.ORDER_ID
        WHERE f.ORDER_STATUS = 2
          AND DATE_FORMAT(f.ORDER_DATE, '%Y-%m-%d') BETWEEN DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 0 DAY), '%Y-%m-01'
          )
          AND DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 0 DAY), '%Y-%m-%d'
          )
        GROUP BY f.SHELF_ID) q
        ON w.SHELF_ID = q.SHELF_ID
      LEFT JOIN
        (SELECT
          SHELF_id, SUM(PAYMENT_MONEY) AS bk_money
        FROM
          fe.sf_after_payment
        WHERE PAYMENT_STATUS = 2
          AND DATE_FORMAT(PAY_DATE, '%Y-%m-%d') BETWEEN DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 0 DAY), '%Y-%m-01'
          )
          AND DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 0 DAY), '%Y-%m-%d'
          )
        GROUP BY SHELF_id) xx
        ON w.SHELF_ID = xx.SHELF_ID
    WHERE w.SHELF_ID NOT IN (
        51018, 51019, 41158, 41159, 73060, 73061, 73062, 73063, 73064, 73065, 73066, 75071, 75072, 75073
      )) t1
  WHERE shelf_type IS NOT NULL;
  DELETE
  FROM
    feods.zs_shelf_grade
  WHERE SHELF_ID = 89437
    AND CURRENT_DATE BETWEEN 20191001
    AND 20191031;
	
  --   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sh_zs_shelf_grade',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('未知@', @user, @timestamp)
  );
  
  COMMIT;
END