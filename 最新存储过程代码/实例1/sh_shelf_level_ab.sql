CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_shelf_level_ab`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.pj_shelf_level_ab
  WHERE smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  INSERT INTO feods.pj_shelf_level_ab (
    smonth,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SF_CODE,
    REAL_NAME,
    shelf_type,
    shelf_id,
    ACTIVATE_date,
    ACTIVATE_date_old,
    REVOKE_date,
    REVOKE_date_old,
    SHELF_STATUS,
    day_long,
    if_band,
    SHELF_HANDLE_STATUS,
    gmv,
    order_num,
    shelf_level_t,
    shelf_level
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    ) AS smonth,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(t3.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city_name,
    t5.BRANCH_CODE,
    t5.BRANCH_NAME,
    t5.SF_CODE,
    t5.REAL_NAME,
    t3.shelf_type,
    t1.shelf_id,
    t1.ACTIVATE_date,
    t1.ACTIVATE_date_old,
    t1.REVOKE_date,
    t1.REVOKE_date_old,
    t1.SHELF_STATUS,
    t1.day_long,
    CASE
      WHEN t5.shelf_id IS NOT NULL
      THEN 1
      ELSE 0
    END if_band,
    t5.SHELF_HANDLE_STATUS,
    IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0) AS gmv,
    t2.order_num,
    CASE
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (1, 2, 3, 10, 11, 12)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (4, 5, 6, 7, 8, 9)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 60
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 80
      THEN '甲级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 50
      THEN '乙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 80
      THEN '甲级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 50
      THEN '乙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级2'
    END AS shelf_level_t,
    CASE
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (1, 2, 3, 10, 11, 12)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (4, 5, 6, 7, 8, 9)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 60
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 80
      THEN '甲级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 50
      THEN '乙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 80
      THEN '甲级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 50
      THEN '乙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级2'
    END AS shelf_level
  FROM
    (SELECT
      shelf_id,
      SHELF_STATUS,
      REVOKE_date_old,
      ACTIVATE_date_old,
      ACTIVATE_date,
      REVOKE_date,
      COUNT(
        DISTINCT
        CASE
          WHEN if_work_day =1
          THEN sdate
        END
      ) AS day_long
    FROM
      (SELECT
        shelf_id,
        SHELF_STATUS,
        ACTIVATE_date AS ACTIVATE_date_old,
        REVOKE_date AS REVOKE_date_old,
        CASE
          WHEN ACTIVATE_date < date01
          THEN date01
          ELSE ACTIVATE_date
        END AS ACTIVATE_date,
        CASE
          WHEN REVOKE_date < date30
          THEN REVOKE_date
          ELSE date30
        END AS REVOKE_date
      FROM
        (SELECT
          shelf_id,
          SHELF_STATUS,
          STR_TO_DATE(
            DATE_FORMAT(ACTIVATE_TIME, '%Y%m%d'),
            '%Y%m%d'
          ) AS ACTIVATE_date,
          STR_TO_DATE(
            DATE_FORMAT(REVOKE_TIME, '%Y%m%d'),
            '%Y%m%d'
          ) AS REVOKE_date,
          STR_TO_DATE(
            DATE_FORMAT(
              DATE_SUB(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                INTERVAL DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%d%'
                ) - 1 DAY
              ),
              '%Y%m%d'
            ),
            '%Y%m%d'
          ) AS date01,
          STR_TO_DATE(
            DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            ),
            '%Y%m%d'
          ) AS date30
        FROM
          fe.sf_shelf
        WHERE SHELF_STATUS IN (2, 3, 5)
          AND shelf_type IN (1, 2, 3, 5, 8)) t1
      WHERE DATE_FORMAT(ACTIVATE_date, '%Y%m%d') <= DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m%d'
        )
        AND (
          REVOKE_date IS NULL
          OR REVOKE_date > DATE_FORMAT(
            DATE_SUB(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              INTERVAL DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%d%'
              ) - 1 DAY
            ),
            '%Y%m%d'
          )
        )) t1
      LEFT JOIN fe_dwd.dwd_pub_work_day t2
        ON t2.sdate >= t1.ACTIVATE_date
        AND t2.sdate <= t1.REVOKE_date
    GROUP BY shelf_id) t1
    LEFT JOIN
      (SELECT
        b.shelf_id,
        SUM(a.quantity * a.sale_price) AS gmv,
        COUNT(DISTINCT a.order_id) AS order_num
      FROM
        fe.sf_order_item a
        LEFT JOIN fe.sf_order b
          ON a.order_id = b.order_id
      WHERE order_date >= DATE_SUB(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%d%'
          ) - 1 DAY
        )
        AND order_date < CURDATE()
        AND b.order_status = 2
      GROUP BY b.shelf_id) t2
      ON t1.shelf_id = t2.shelf_id
    LEFT JOIN fe.sf_shelf t3
      ON t1.shelf_id = t3.shelf_id
    LEFT JOIN fe.pub_shelf_manager t5
      ON t3.MANAGER_ID = t5.manager_id
    LEFT JOIN
      (SELECT
        SHELF_id,
        SUM(PAYMENT_MONEY) AS bk_money
      FROM
        fe.sf_after_payment
      WHERE PAYMENT_STATUS = 2
        AND PAY_DATE >= DATE_SUB(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%d%'
          ) - 1 DAY
        )
        AND PAY_DATE < CURDATE()
      GROUP BY SHELF_id) t4
      ON t1.shelf_id = t4.shelf_id
    LEFT JOIN
      (SELECT
        shelf_id,
        MIN(SHELF_HANDLE_STATUS) AS SHELF_HANDLE_STATUS
      FROM
        (SELECT
          a.MAIN_SHELF_ID AS shelf_id,
          MIN(SHELF_HANDLE_STATUS) AS SHELF_HANDLE_STATUS
        FROM
          fe.sf_shelf_relation_record a
          LEFT JOIN
            (SELECT
              MAIN_SHELF_ID,
              SECONDARY_SHELF_ID,
              MAX(ADD_TIME) AS ADD_TIME
            FROM
              fe.sf_shelf_relation_record
            WHERE DATA_FLAG = 1
            GROUP BY MAIN_SHELF_ID,
              SECONDARY_SHELF_ID) b
            ON a.MAIN_SHELF_ID = b.MAIN_SHELF_ID
            AND a.SECONDARY_SHELF_ID = b.SECONDARY_SHELF_ID
            AND a.ADD_TIME = b.ADD_TIME
        WHERE a.SHELF_HANDLE_STATUS IN (9, 10)
          AND b.SECONDARY_SHELF_ID IS NOT NULL
          AND a.ADD_TIME < CURDATE()
          AND IFNULL(UNBIND_TIME, CURDATE()) > DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%d%'
            ) - 1 DAY
          )
        GROUP BY a.MAIN_SHELF_ID
        UNION
        ALL
        SELECT DISTINCT
          a.SECONDARY_SHELF_ID AS shelf_id,
          a.SHELF_HANDLE_STATUS
        FROM
          fe.sf_shelf_relation_record a
          LEFT JOIN
            (SELECT
              MAIN_SHELF_ID,
              SECONDARY_SHELF_ID,
              MAX(ADD_TIME) AS ADD_TIME
            FROM
              fe.sf_shelf_relation_record
            WHERE DATA_FLAG = 1
            GROUP BY MAIN_SHELF_ID,
              SECONDARY_SHELF_ID) b
            ON a.MAIN_SHELF_ID = b.MAIN_SHELF_ID
            AND a.SECONDARY_SHELF_ID = b.SECONDARY_SHELF_ID
            AND a.ADD_TIME = b.ADD_TIME
        WHERE a.SHELF_HANDLE_STATUS IN (9, 10)
          AND b.SECONDARY_SHELF_ID IS NOT NULL
          AND a.ADD_TIME < CURDATE()
          AND IFNULL(UNBIND_TIME, CURDATE()) > DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%d%'
            ) - 1 DAY
          )) t1
      GROUP BY shelf_id) t5
      ON t1.shelf_id = t5.shelf_id
    LEFT JOIN
      (SELECT
        CASE
          WHEN DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%Y%m'
          ) = '201901'
          THEN 12
          WHEN DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%Y%m'
          ) = '201902'
          THEN 6
          ELSE COUNT(1)
        END days
      FROM
        fe_dwd.dwd_pub_work_day
      WHERE DATE_FORMAT(sdate, '%Y%m') = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m'
        )
        AND if_work_day = 1 ) t6
      ON 1 = 1;
  #CREATE TABLE feods.pj_shelf_level_ab_df2 like feods.pj_shelf_level_ab;
   DELETE
  FROM
    feods.pj_shelf_level_ab_df2
  WHERE smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  INSERT INTO feods.pj_shelf_level_ab_df2
  SELECT
    t.*
  FROM
    feods.pj_shelf_level_ab t
    JOIN fe.sf_shelf s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 2
  WHERE t.smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  DELETE
    t
  FROM
    feods.pj_shelf_level_ab t
    JOIN fe.sf_shelf s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 2
  WHERE t.smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  DELETE
  FROM
    feods.pj_shelf_level_ab_week
  WHERE sweek = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y'
    ) * 100+ WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY));
  INSERT INTO feods.pj_shelf_level_ab_week (
    sweek,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SF_CODE,
    REAL_NAME,
    shelf_type,
    shelf_id,
    ACTIVATE_date,
    ACTIVATE_date_old,
    REVOKE_date,
    REVOKE_date_old,
    SHELF_STATUS,
    day_long,
    if_band,
    SHELF_HANDLE_STATUS,
    gmv,
    order_num,
    shelf_level_t,
    shelf_level
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y'
    ) * 100+ WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) AS sweek,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(t3.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city_name,
    t5.BRANCH_CODE,
    t5.BRANCH_NAME,
    t5.SF_CODE,
    t5.REAL_NAME,
    t3.shelf_type,
    t1.shelf_id,
    t1.ACTIVATE_date,
    t1.ACTIVATE_date_old,
    t1.REVOKE_date,
    t1.REVOKE_date_old,
    t1.SHELF_STATUS,
    t1.day_long,
    CASE
      WHEN t5.shelf_id IS NOT NULL
      THEN 1
      ELSE 0
    END if_band,
    t5.SHELF_HANDLE_STATUS,
    IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0) AS gmv,
    t2.order_num,
    CASE
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (1, 2, 3, 10, 11, 12)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (4, 5, 6, 7, 8, 9)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 60
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 80
      THEN '甲级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 50
      THEN '乙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 80
      THEN '甲级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 50
      THEN '乙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long >= 10
      THEN '丙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t1.day_long < 10
      THEN '丁级2'
    END AS shelf_level_t,
    CASE
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (1, 3, 8)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (1, 2, 3, 10, 11, 12)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 40
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND MONTH(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) IN (4, 5, 6, 7, 8, 9)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 60
      THEN '甲级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 25
      THEN '乙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NULL
      AND t3.shelf_type IN (2, 5)
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 80
      THEN '甲级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 50
      THEN '乙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 10
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 80
      THEN '甲级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 50
      THEN '乙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days >= 10
      THEN '丙级2'
      WHEN t5.shelf_id IS NOT NULL
      AND SHELF_HANDLE_STATUS = 9
      AND (
        IFNULL(t2.gmv, 0) + IFNULL(t4.bk_money, 0)
      ) / t6.days < 10
      THEN '丁级2'
    END AS shelf_level
  FROM
    (SELECT
      shelf_id,
      SHELF_STATUS,
      REVOKE_date_old,
      ACTIVATE_date_old,
      ACTIVATE_date,
      REVOKE_date,
      COUNT(
        DISTINCT
        CASE
          WHEN if_work_day = 1
          THEN sdate
        END
      ) AS day_long
    FROM
      (SELECT
        shelf_id,
        SHELF_STATUS,
        ACTIVATE_date AS ACTIVATE_date_old,
        REVOKE_date AS REVOKE_date_old,
        CASE
          WHEN ACTIVATE_date < date01
          THEN date01
          ELSE ACTIVATE_date
        END AS ACTIVATE_date,
        CASE
          WHEN REVOKE_date < date30
          THEN REVOKE_date
          ELSE date30
        END AS REVOKE_date
      FROM
        (SELECT
          shelf_id,
          SHELF_STATUS,
          STR_TO_DATE(
            DATE_FORMAT(ACTIVATE_TIME, '%Y%m%d'),
            '%Y%m%d'
          ) AS ACTIVATE_date,
          STR_TO_DATE(
            DATE_FORMAT(REVOKE_TIME, '%Y%m%d'),
            '%Y%m%d'
          ) AS REVOKE_date,
          DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL (
              CASE
                WHEN DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                ) = 0
                THEN 7
                ELSE DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                )
              END
            ) - 1 DAY
          ) AS date01,
          STR_TO_DATE(
            DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            ),
            '%Y%m%d'
          ) AS date30
        FROM
          fe.sf_shelf
        WHERE SHELF_STATUS IN (2, 3, 5)
          AND shelf_type IN (1, 2, 3, 5, 8)) t1
      WHERE DATE_FORMAT(ACTIVATE_date, '%Y%m%d') <= DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m%d'
        )
        AND (
          REVOKE_date IS NULL
          OR REVOKE_date > DATE_FORMAT(
            DATE_SUB(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              INTERVAL (
                CASE
                  WHEN DATE_FORMAT(
                    DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                    '%w'
                  ) = 0
                  THEN 7
                  ELSE DATE_FORMAT(
                    DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                    '%w'
                  )
                END
              ) - 1 DAY
            ),
            '%Y%m%d'
          )
        )) t1
      LEFT JOIN fe_dwd.dwd_pub_work_day t2
        ON t2.sdate >= t1.ACTIVATE_date
        AND t2.sdate <= t1.REVOKE_date
    GROUP BY shelf_id) t1
    LEFT JOIN
      (SELECT
        b.shelf_id,
        SUM(a.quantity * a.sale_price) AS gmv,
        COUNT(DISTINCT a.order_id) AS order_num
      FROM
        fe.sf_order_item a
        LEFT JOIN fe.sf_order b
          ON a.order_id = b.order_id
      WHERE order_date >= DATE_SUB(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL (
            CASE
              WHEN DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              ) = 0
              THEN 7
              ELSE DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              )
            END
          ) - 1 DAY
        )
        AND order_date < CURDATE()
        AND b.order_status = 2
      GROUP BY b.shelf_id) t2
      ON t1.shelf_id = t2.shelf_id
    LEFT JOIN fe.sf_shelf t3
      ON t1.shelf_id = t3.shelf_id
    LEFT JOIN fe.pub_shelf_manager t5
      ON t3.MANAGER_ID = t5.manager_id
    LEFT JOIN
      (SELECT
        SHELF_id,
        SUM(PAYMENT_MONEY) AS bk_money
      FROM
        fe.sf_after_payment
      WHERE PAYMENT_STATUS = 2
        AND PAY_DATE >= DATE_SUB(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL (
            CASE
              WHEN DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              ) = 0
              THEN 7
              ELSE DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              )
            END
          ) - 1 DAY
        )
        AND PAY_DATE < CURDATE()
      GROUP BY SHELF_id) t4
      ON t1.shelf_id = t4.shelf_id
    LEFT JOIN
      (SELECT
        shelf_id,
        MIN(SHELF_HANDLE_STATUS) AS SHELF_HANDLE_STATUS
      FROM
        (SELECT
          a.MAIN_SHELF_ID AS shelf_id,
          MIN(SHELF_HANDLE_STATUS) AS SHELF_HANDLE_STATUS
        FROM
          fe.sf_shelf_relation_record a
          LEFT JOIN
            (SELECT
              MAIN_SHELF_ID,
              SECONDARY_SHELF_ID,
              MAX(ADD_TIME) AS ADD_TIME
            FROM
              fe.sf_shelf_relation_record
            WHERE DATA_FLAG = 1
            GROUP BY MAIN_SHELF_ID,
              SECONDARY_SHELF_ID) b
            ON a.MAIN_SHELF_ID = b.MAIN_SHELF_ID
            AND a.SECONDARY_SHELF_ID = b.SECONDARY_SHELF_ID
            AND a.ADD_TIME = b.ADD_TIME
        WHERE a.SHELF_HANDLE_STATUS IN (9, 10)
          AND b.SECONDARY_SHELF_ID IS NOT NULL
          AND a.ADD_TIME < CURDATE()
          AND IFNULL(UNBIND_TIME, CURDATE()) > DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL (
              CASE
                WHEN DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                ) = 0
                THEN 7
                ELSE DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                )
              END
            ) - 1 DAY
          )
        GROUP BY a.MAIN_SHELF_ID
        UNION
        ALL
        SELECT DISTINCT
          a.SECONDARY_SHELF_ID AS shelf_id,
          a.SHELF_HANDLE_STATUS
        FROM
          fe.sf_shelf_relation_record a
          LEFT JOIN
            (SELECT
              MAIN_SHELF_ID,
              SECONDARY_SHELF_ID,
              MAX(ADD_TIME) AS ADD_TIME
            FROM
              fe.sf_shelf_relation_record
            WHERE DATA_FLAG = 1
            GROUP BY MAIN_SHELF_ID,
              SECONDARY_SHELF_ID) b
            ON a.MAIN_SHELF_ID = b.MAIN_SHELF_ID
            AND a.SECONDARY_SHELF_ID = b.SECONDARY_SHELF_ID
            AND a.ADD_TIME = b.ADD_TIME
        WHERE a.SHELF_HANDLE_STATUS IN (9, 10)
          AND b.SECONDARY_SHELF_ID IS NOT NULL
          AND a.ADD_TIME < CURDATE()
          AND IFNULL(UNBIND_TIME, CURDATE()) > DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL (
              CASE
                WHEN DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                ) = 0
                THEN 7
                ELSE DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                )
              END
            ) - 1 DAY
          )) t1
      GROUP BY shelf_id) t5
      ON t1.shelf_id = t5.shelf_id
    LEFT JOIN
      (SELECT
        COUNT(1) AS days
      FROM
        fe_dwd.dwd_pub_work_day
      WHERE WEEKOFYEAR(sdate) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
        AND DATE_FORMAT(sdate, '%Y') = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y'
        )
        AND if_work_day = 1 ) t6
      ON 1 = 1;
  DELETE
  FROM
    feods.pj_shelf_level_ab_day
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
  INSERT INTO feods.pj_shelf_level_ab_day (
    business_area,
    smonth,
    sdate,
    slevel,
    shelf_qty
  )
  SELECT
    b.business_area,
    smonth,
    DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS sdate,
    CASE
      WHEN shelf_level = "甲级2"
      THEN "甲级"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 1
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 2
      THEN "乙级（冰箱）"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 3
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 5
      THEN "乙级（冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 1
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 2
      THEN "乙级（冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 3
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 5
      THEN "乙级（冰箱）"
      WHEN shelf_level = "丙级2"
      THEN "丙级"
      WHEN shelf_level = "丁级2"
      THEN "丁级"
      ELSE shelf_level
    END AS slevel,
    SUM(
      CASE
        WHEN shelf_level = "甲级2"
        THEN 2
        WHEN shelf_level = "乙级2"
        THEN 2
        WHEN shelf_level = "丙级2"
        THEN 2
        WHEN shelf_level = "丁级2"
        THEN 2
        WHEN ISNULL(shelf_level)
        THEN 0
        ELSE 1
      END
    ) AS shelf_qty
  FROM
    feods.pj_shelf_level_ab a
    LEFT JOIN feods.zs_city_business b
      ON a.city_name = b.CITY_NAME
  WHERE shelf_status = 2
    AND DATE_FORMAT(activate_date_old, '%Y%m') < smonth
    AND a.smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    )
  GROUP BY b.business_area,
    smonth,
    CASE
      WHEN shelf_level = "甲级2"
      THEN "甲级"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 1
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 2
      THEN "乙级（冰箱）"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 3
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级2"
      AND shelf_type = 5
      THEN "乙级（冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 1
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 2
      THEN "乙级（冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 3
      THEN "乙级（不含冰箱）"
      WHEN shelf_level = "乙级"
      AND shelf_type = 5
      THEN "乙级（冰箱）"
      WHEN shelf_level = "丙级2"
      THEN "丙级"
      WHEN shelf_level = "丁级2"
      THEN "丁级"
      ELSE shelf_level
    END;

#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_shelf_level_ab',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));

  COMMIT;
END