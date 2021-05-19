CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_shelf_check_month`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.pj_shelf_check_month
  WHERE smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  INSERT INTO feods.pj_shelf_check_month (
    smonth,
    city_name,
    shelf_id,
    shelf_code,
    shelf_name,
    manager_id,
    real_name,
    BRANCH_CODE,
    BRANCH_NAME,
    min_OPERATE_TIME,
    max_OPERATE_TIME,
    CHECK_times,
    CHECK_product_qty,
    CHECK_not0_product_qty,
    check_b0_qty,
    check_s0_qty,
    pandian_total,
    pandian_tic,
    sf_code,
    manager_type
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    ) AS smonth,
    t1.city_name,
    t1.shelf_id,
    t1.shelf_code,
    t1.shelf_name,
    t1.manager_id,
    t1.real_name,
    t1.BRANCH_CODE,
    t1.BRANCH_NAME,
    t2.min_OPERATE_TIME,
    t2.max_OPERATE_TIME,
    t2.CHECK_times,
    t2.CHECK_product_qty,
    t2.CHECK_not0_product_qty,
    t2.check_b0_qty,
    t2.check_s0_qty,
    CASE
      WHEN t2.shelf_id
      THEN 'pandian_total'
      ELSE 'no_pandian_total'
    END AS pandian_total,
    CASE
      WHEN (DAY(t2.max_OPERATE_TIME) BETWEEN 25 AND 31) AND t1.second_user_type=2 AND t2.shelf_id IS NOT NULL
      THEN 'pandian_tic'
      WHEN (DAY(t2.max_OPERATE_TIME) BETWEEN 20 AND 31) AND t1.second_user_type=1 AND t2.shelf_id IS NOT NULL
      THEN 'pandian_tic'
      ELSE 'no_pandian_tic'
    END AS pandian_tic,
    t1.SF_CODE,
    t1.second_user_type
  FROM
    (SELECT
      SUBSTRING_INDEX(
        SUBSTRING_INDEX(a.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      ) AS city_name,
      a.shelf_id,
      a.shelf_code,
      a.shelf_name,
      a.manager_id,
      b.real_name,
      b.BRANCH_CODE,
      b.BRANCH_NAME,
      b.SF_CODE,
      b.second_user_type
    FROM
      fe.sf_shelf a,
      fe.pub_shelf_manager b
    WHERE a.manager_id = b.manager_id
      AND a.data_flag = 1 AND b.data_flag = 1
      AND a.shelf_status = 2
      AND a.revoke_status NOT IN (6,7,9)
      AND a.SHELF_CODE <> ''
      AND a.SHELF_TYPE NOT IN (4,8,9)
      AND a.shelf_id NOT IN (67236,73560,73561,81538,81539,81540,85516,87318,87319,87726,87728)) t1
    LEFT JOIN
      (SELECT
        b.shelf_id,
        MIN(b.OPERATE_TIME) AS min_OPERATE_TIME,
        MAX(b.OPERATE_TIME) AS max_OPERATE_TIME,
        COUNT(DISTINCT a.CHECK_ID) AS CHECK_times,
        COUNT(DISTINCT a.product_id) AS CHECK_product_qty,
        COUNT(
          DISTINCT
          CASE
            WHEN a.ERROR_NUM <> 0
            THEN a.product_id
          END
        ) AS CHECK_not0_product_qty,
        SUM(
          CASE
            WHEN a.ERROR_NUM > 0
            THEN a.ERROR_NUM
          END
        ) AS check_b0_qty,
        SUM(
          CASE
            WHEN a.ERROR_NUM < 0
            THEN a.ERROR_NUM
          END
        ) AS check_s0_qty
      FROM
        fe.sf_shelf_check b
        left join
          fe.sf_shelf_check_detail a
        on a.CHECK_ID = b.CHECK_ID
      WHERE b.data_flag = 1 AND a.data_flag = 1
        AND b.operate_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
        AND b.operate_time < DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),INTERVAL 1 DAY)
      GROUP BY b.shelf_id) t2
      ON t1.shelf_id = t2.shelf_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_shelf_check_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));      
  COMMIT;
END