CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_c_lei_buhuo_data`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  SELECT
    @sdate := SUBDATE(CURRENT_DATE, 1),
    @month_start := DATE_FORMAT(@sdate, '%Y%m%01'),
    @month_end := LAST_DAY(@sdate),
    @ym := DATE_FORMAT(@sdate, '%Y%m'),
    @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  DELETE
  FROM
    feods.pj_c_lei_buhuo_data
  WHERE smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  INSERT INTO feods.pj_c_lei_buhuo_data (
    smonth,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SF_CODE,
    REAL_NAME,
    shelf_type,
    shelf_id,
    SHELF_STATUS,
    CLOSE_TYPE,
    REVOKE_STATUS,
    WHETHER_CLOSE,
    ACTIVATE_TIME,
    REVOKE_TIME,
    stock_quantity,
    gmv_type,
    yuny_days,
    stock_dabiao_days,
    have_stock_days,
    sku_dabiao_day
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    ) AS smonth,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city_name,
    b.BRANCH_CODE,
    b.BRANCH_NAME,
    b.SF_CODE,
    b.REAL_NAME,
    b.SHELF_TYPE,
    a.shelf_id,
    b.SHELF_STATUS,
    b.CLOSE_TYPE,
    b.REVOKE_STATUS,
    b.WHETHER_CLOSE,
    b.ACTIVATE_TIME,
    b.REVOKE_TIME,
    c.stock_quantity,
    CASE
      WHEN h.gmv >= 10
      THEN 'GMV>=10'
      ELSE 'GMV<10'
    END AS gmv_type,
    - (
      DATEDIFF(
        (
          CASE
            WHEN b.ACTIVATE_TIME > @month_start
            THEN b.ACTIVATE_TIME
            ELSE @month_start
          END
        ),
        CASE
          WHEN DATE_SUB(f.ADD_TIME, INTERVAL 1 DAY) < (
            CASE
              WHEN IFNULL(b.REVOKE_TIME, CURRENT_DATE()) < @month_end
              THEN IFNULL(b.REVOKE_TIME, CURRENT_DATE())
              ELSE @month_end
            END
          )
          THEN DATE_SUB(f.ADD_TIME, INTERVAL 1 DAY)
          ELSE (
            CASE
              WHEN IFNULL(b.REVOKE_TIME, CURRENT_DATE()) < @month_end
              THEN IFNULL(b.REVOKE_TIME, CURRENT_DATE())
              ELSE @month_end
            END
          )
        END
      ) - 1
    ) AS '运营天数',
    SUM(
      (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 1)
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 1)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 1)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 1
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 1
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY1_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 1
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 1
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY1_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY1_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 2)
          AND a.DAY1_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 2)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY2_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 2)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY2_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 2
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY2_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 2
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY2_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 2
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY2_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 2
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY2_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY2_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY2_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 3)
          AND a.DAY3_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 3)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY3_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 3)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY3_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 3
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY3_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 3
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY3_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 3
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY3_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 3
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY3_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY3_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY3_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 4)
          AND a.DAY4_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 4)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY4_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 4)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY4_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 4
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY4_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 4
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY4_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 4
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY4_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 4
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY4_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY4_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY4_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 5)
          AND a.DAY5_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 5)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY5_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 5)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY5_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 5
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY5_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 5
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY5_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 5
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY5_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 5
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY5_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY5_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY5_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 6)
          AND a.DAY6_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 6)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY6_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 6)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY6_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 6
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY6_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 6
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY6_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 6
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY6_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 6
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY6_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY6_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY6_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 7)
          AND a.DAY7_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 7)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY7_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 7)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY7_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 7
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY7_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 7
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY7_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 7
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY7_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 7
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY7_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY7_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY7_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 8)
          AND a.DAY8_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 8)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY8_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 8)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY8_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 8
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY8_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 8
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY8_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 8
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY8_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 8
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY8_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY8_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY8_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 9)
          AND a.DAY9_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 9)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY9_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 9)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY9_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 9
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY9_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 9
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY9_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 9
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY9_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 9
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY9_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY9_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY9_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 10)
          AND a.DAY10_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 10)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY10_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 10)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY10_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 10
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY10_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 10
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY10_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 10
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY10_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 10
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY10_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY10_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY10_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 11)
          AND a.DAY11_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 11)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY11_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 11)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY11_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 11
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY11_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 11
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY11_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 11
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY11_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 11
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY11_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY11_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY11_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 12)
          AND a.DAY12_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 12)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY12_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 12)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY12_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 12
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY12_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 12
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY12_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 12
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY12_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 12
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY12_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY12_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY12_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 13)
          AND a.DAY13_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 13)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY13_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 13)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY13_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 13
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY13_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 13
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY13_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 13
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY13_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 13
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY13_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY13_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY13_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 14)
          AND a.DAY14_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 14)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY14_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 14)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY14_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 14
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY14_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 14
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY14_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 14
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY14_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 14
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY14_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY14_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY14_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 15)
          AND a.DAY15_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 15)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY15_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 15)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY15_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 15
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY15_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 15
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY15_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 15
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY15_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 15
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY15_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY15_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY15_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 16)
          AND a.DAY16_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 16)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY16_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 16)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY16_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 16
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY16_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 16
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY16_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 16
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY16_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 16
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY16_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY16_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY16_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 17)
          AND a.DAY17_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 17)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY17_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 17)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY17_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 17
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY17_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 17
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY17_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 17
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY17_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 17
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY17_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY17_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY17_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 18)
          AND a.DAY18_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 18)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY18_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 18)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY18_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 18
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY18_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 18
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY18_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 18
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY18_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 18
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY18_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY18_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY18_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 19)
          AND a.DAY19_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 19)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY19_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 19)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY19_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 19
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY19_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 19
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY19_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 19
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY19_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 19
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY19_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY19_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY19_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 20)
          AND a.DAY20_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 20)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY20_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 20)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY20_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 20
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY20_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 20
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY20_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 20
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY20_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 20
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY20_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY20_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY20_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 21)
          AND a.DAY21_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 21)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY21_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 21)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY21_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 21
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY21_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 21
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY21_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 21
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY21_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 21
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY21_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY21_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY21_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 22)
          AND a.DAY22_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 22)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY22_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 22)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY22_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 22
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY22_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 22
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY22_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 22
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY22_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 22
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY22_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY22_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY22_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 23)
          AND a.DAY23_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 23)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY23_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 23)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY23_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 23
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY23_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 23
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY23_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 23
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY23_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 23
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY23_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY23_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY23_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 24)
          AND a.DAY24_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 24)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY24_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 24)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY24_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 24
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY24_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 24
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY24_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 24
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY24_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 24
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY24_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY24_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY24_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 25)
          AND a.DAY25_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 25)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY25_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 25)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY25_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 25
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY25_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 25
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY25_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 25
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY25_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 25
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY25_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY25_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY25_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 26)
          AND a.DAY26_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 26)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY26_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 26)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY26_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 26
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY26_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 26
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY26_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 26
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY26_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 26
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY26_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY26_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY26_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 27)
          AND a.DAY27_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 27)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY27_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 27)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY27_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 27
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY27_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 27
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY27_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 27
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY27_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 27
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY27_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY27_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY27_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 28)
          AND a.DAY28_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 28)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY28_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 28)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY28_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 28
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY28_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 28
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY28_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 28
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY28_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 28
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY28_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY28_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY28_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 29)
          AND a.DAY29_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 29)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY29_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 29)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY29_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 29
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY29_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 29
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY29_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 29
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY29_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 29
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY29_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY29_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY29_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 30)
          AND a.DAY30_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 30)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY30_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 30)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY30_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 30
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY30_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 30
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY30_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 30
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY30_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 30
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY30_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY30_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY30_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN (e.smonth < @ym
            OR e.sday <= 31)
          AND a.DAY31_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 31)
          AND d.PACKAGE_MODEL = 3
          AND a.DAY31_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN (d.smonth < @ym
            OR d.sday <= 31)
          AND d.PACKAGE_MODEL = 4
          AND a.DAY31_QUANTITY BETWEEN 110
          AND 500
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 31
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY31_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN f.smonth = @ym
          AND f.sday > 31
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY31_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 31
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY31_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN g.smonth = @ym
          AND g.sday > 31
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY31_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (1, 3)
          AND a.DAY31_QUANTITY BETWEEN 110
          AND 300
          THEN 1
          WHEN IFNULL(d.PACKAGE_MODEL, 0) + IFNULL(e.PACKAGE_MODEL, 0) + IFNULL(f.PACKAGE_MODEL, 0) = 0
          AND b.SHELF_TYPE IN (2, 5)
          AND a.DAY31_QUANTITY BETWEEN 90
          AND 220
          THEN 1
          ELSE 0
        END
      )
    ) AS '达标库存天数',
    SUM(
      (
        CASE
          WHEN a.DAY1_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY2_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY3_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY4_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY5_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY6_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY7_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY8_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY9_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY10_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY11_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY12_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY13_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY14_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY15_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY16_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY17_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY18_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY19_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY20_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY21_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY22_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY23_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY24_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY25_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY26_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY27_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY28_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY29_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY30_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      ) + (
        CASE
          WHEN a.DAY31_QUANTITY > 0
          THEN 1
          ELSE 0
        END
      )
    ) AS '有库存天数',
    (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty1 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty1 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty2 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty2 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty3 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty3 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty4 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty4 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty5 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty5 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty6 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty6 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty7 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty7 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty8 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty8 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty9 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty9 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty10 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty10 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty11 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty11 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty12 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty12 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty13 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty13 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty14 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty14 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty15 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty15 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty16 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty16 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty17 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty17 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty18 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty18 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty19 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty19 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty20 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty20 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty21 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty21 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty22 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty22 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty23 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty23 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty24 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty24 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty25 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty25 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty26 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty26 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty27 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty27 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty28 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty28 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty29 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty29 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty30 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty30 >= 30
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN b.SHELF_TYPE IN (2, 5)
        AND a.sku_qty30 >= 10
        THEN 1
        WHEN b.SHELF_TYPE IN (1, 3)
        AND a.sku_qty30 >= 30 #Willow change 30 from 25 at @ym16
         THEN 1
        ELSE 0
      END
    ) AS '库存达标天数'
  FROM
    (SELECT
      a.shelf_id,
      SUM(DAY1_QUANTITY) AS DAY1_QUANTITY,
      SUM(DAY2_QUANTITY) AS DAY2_QUANTITY,
      SUM(DAY3_QUANTITY) AS DAY3_QUANTITY,
      SUM(DAY4_QUANTITY) AS DAY4_QUANTITY,
      SUM(DAY5_QUANTITY) AS DAY5_QUANTITY,
      SUM(DAY6_QUANTITY) AS DAY6_QUANTITY,
      SUM(DAY7_QUANTITY) AS DAY7_QUANTITY,
      SUM(DAY8_QUANTITY) AS DAY8_QUANTITY,
      SUM(DAY9_QUANTITY) AS DAY9_QUANTITY,
      SUM(DAY10_QUANTITY) AS DAY10_QUANTITY,
      SUM(DAY11_QUANTITY) AS DAY11_QUANTITY,
      SUM(DAY12_QUANTITY) AS DAY12_QUANTITY,
      SUM(DAY13_QUANTITY) AS DAY13_QUANTITY,
      SUM(DAY14_QUANTITY) AS DAY14_QUANTITY,
      SUM(DAY15_QUANTITY) AS DAY15_QUANTITY,
      SUM(DAY16_QUANTITY) AS DAY16_QUANTITY,
      SUM(DAY17_QUANTITY) AS DAY17_QUANTITY,
      SUM(DAY18_QUANTITY) AS DAY18_QUANTITY,
      SUM(DAY19_QUANTITY) AS DAY19_QUANTITY,
      SUM(DAY20_QUANTITY) AS DAY20_QUANTITY,
      SUM(DAY21_QUANTITY) AS DAY21_QUANTITY,
      SUM(DAY22_QUANTITY) AS DAY22_QUANTITY,
      SUM(DAY23_QUANTITY) AS DAY23_QUANTITY,
      SUM(DAY24_QUANTITY) AS DAY24_QUANTITY,
      SUM(DAY25_QUANTITY) AS DAY25_QUANTITY,
      SUM(DAY26_QUANTITY) AS DAY26_QUANTITY,
      SUM(DAY27_QUANTITY) AS DAY27_QUANTITY,
      SUM(DAY28_QUANTITY) AS DAY28_QUANTITY,
      SUM(DAY29_QUANTITY) AS DAY29_QUANTITY,
      SUM(DAY30_QUANTITY) AS DAY30_QUANTITY,
      SUM(DAY31_QUANTITY) AS DAY31_QUANTITY,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY1_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty1,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY2_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty2,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY3_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty3,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY4_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty4,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY5_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty5,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY6_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty6,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY7_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty7,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY8_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty8,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY9_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty9,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY10_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty10,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY11_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty11,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY12_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty12,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY13_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty13,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY14_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty14,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY15_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty15,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY16_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty16,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY17_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty17,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY18_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty18,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY19_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty19,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY20_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty20,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY21_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty21,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY22_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty22,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY23_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty23,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY24_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty24,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY25_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty25,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY26_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty26,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY27_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty27,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY28_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty28,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY29_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty29,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY30_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty30,
      COUNT(
        DISTINCT
        CASE
          WHEN DAY31_QUANTITY > 0
          THEN product_id
        END
      ) AS sku_qty31
    FROM
      fe.sf_shelf_product_stock_detail AS a
      LEFT JOIN fe.sf_shelf b
        ON a.shelf_id = b.shelf_id
    WHERE STAT_DATE = @y_m
    GROUP BY a.shelf_id) AS a
    LEFT JOIN
      (SELECT
        a.shelf_id,
        a.AREA_ADDRESS,
        a.SHELF_TYPE,
        a.REVOKE_STATUS,
        a.WHETHER_CLOSE,
        a.CLOSE_TYPE,
        a.ACTIVATE_TIME,
        b.BRANCH_CODE,
        b.BRANCH_NAME,
        b.SF_CODE,
        b.REAL_NAME,
        CASE
          WHEN a.REVOKE_STATUS = 9
          THEN 3
          ELSE a.SHELF_STATUS
        END AS SHELF_STATUS,
        CASE
          WHEN a.REVOKE_STATUS = 9
          THEN a.LAST_UPDATE_TIME
          ELSE a.REVOKE_TIME
        END AS REVOKE_TIME
      FROM
        fe.sf_shelf a
        LEFT JOIN fe.pub_shelf_manager b
          ON a.MANAGER_ID = b.MANAGER_ID) b
      ON a.shelf_id = b.shelf_id
    LEFT JOIN
      (SELECT
        shelf_id,
        SUM(stock_quantity) AS stock_quantity
      FROM
        fe.sf_shelf_product_detail
      WHERE DATA_FLAG = 1
      GROUP BY shelf_id) c
      ON a.shelf_id = c.shelf_id
    LEFT JOIN
      (SELECT
        MAIN_SHELF_ID AS shelf_id,
        PACKAGE_MODEL,
        ADD_TIME,
        DATE_FORMAT(ADD_TIME, '%Y%m') AS smonth,
        DAY(ADD_TIME) AS sday
      FROM
        fe.sf_shelf_relation_record
      WHERE SHELF_HANDLE_STATUS = 9
        AND PACKAGE_MODEL IN (3, 4)) d
      ON a.shelf_id = d.shelf_id
    LEFT JOIN
      (SELECT
        MAIN_SHELF_ID AS shelf_id,
        PACKAGE_MODEL,
        MAX(ADD_TIME) AS ADD_TIME,
        DATE_FORMAT(MAX(ADD_TIME), '%Y%m') AS smonth,
        DAY(MAX(ADD_TIME)) AS sday
      FROM
        fe.sf_shelf_relation_record
      WHERE SHELF_HANDLE_STATUS = 9
        AND PACKAGE_MODEL IN (5)
      GROUP BY MAIN_SHELF_ID,
        PACKAGE_MODEL) e
      ON a.shelf_id = e.shelf_id
    LEFT JOIN
      (SELECT
        SECONDARY_SHELF_ID AS shelf_id,
        PACKAGE_MODEL,
        ADD_TIME,
        DATE_FORMAT(ADD_TIME, '%Y%m') AS smonth,
        DAY(ADD_TIME) AS sday
      FROM
        fe.sf_shelf_relation_record
      WHERE SHELF_HANDLE_STATUS = 9
      GROUP BY SECONDARY_SHELF_ID) f
      ON a.shelf_id = f.shelf_id
    LEFT JOIN
      (SELECT
        MAIN_SHELF_ID AS shelf_id,
        DATE_FORMAT(MIN(ADD_TIME), '%Y%m') AS smonth,
        DAY(MIN(ADD_TIME)) AS sday
      FROM
        fe.sf_shelf_relation_record
      WHERE SHELF_HANDLE_STATUS = 9
      GROUP BY MAIN_SHELF_ID) g
      ON a.shelf_id = g.shelf_id
    LEFT JOIN
      (SELECT
        shelf_id,
        SUM(GMV) AS gmv
      FROM
        (SELECT
          shelf_id,
          SUM(a.quantity * a.sale_price) gmv
        FROM
          fe.sf_order_item a
          LEFT JOIN fe.sf_order b
            ON a.order_id = b.order_id
        WHERE order_status = 2
          AND order_date BETWEEN SUBDATE(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) - 1 DAY
          )
          AND CURDATE()
        GROUP BY shelf_id
        UNION
        ALL
        SELECT
          SHELF_id,
          SUM(PAYMENT_MONEY) AS gmv
        FROM
          fe.sf_after_payment
        WHERE PAYMENT_STATUS = 2
          AND PAY_DATE BETWEEN SUBDATE(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) - 1 DAY
          )
          AND CURDATE()
        GROUP BY SHELF_id) t1
      GROUP BY shelf_id) h
      ON a.shelf_id = h.shelf_id
  WHERE (
      b.SHELF_STATUS IN (2, 5)
      OR DATE_FORMAT(b.REVOKE_TIME, '%Y%m') = @ym
    )
    AND a.shelf_id NOT IN (
      18037,
      22566,
      36784,
      7852,
      35743,
      57467,
      57468,
      57469,
      57470,
      57471,
      57472,
      57457,
      57458,
      57459,
      57460,
      2,
      9,
      24,
      175,
      4929,
      44454,
      46575,
      52825,
      54880,
      56137,
      59557,
      59558,
      59559,
      59560,
      59561,
      64180,
      64381,
      65390,
      41249,
      47339,
      15043
    )
    AND b.shelf_type != 5 #Willow add at @ym16
   GROUP BY a.shelf_id;
  DELETE
  FROM
    feods.pj_c_lei_sale5_lv
  WHERE smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
  INSERT INTO feods.pj_c_lei_sale5_lv (
    smonth,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SF_CODE,
    REAL_NAME,
    shelf_id,
    sale5_stock_qty,
    sale5_stock_value,
    stock_qty,
    stock_value
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    ) AS smonth,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(c.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city_name,
    e.BRANCH_CODE,
    e.BRANCH_NAME,
    e.SF_CODE,
    e.REAL_NAME,
    a.shelf_id,
    SUM(
      CASE
        WHEN a.stock_quantity > 3
        AND b.sales_flag = 5
        AND b.new_flag = 2
        THEN a.stock_quantity
        ELSE 0
      END
    ) AS sale5_stock_qty,
    SUM(
      CASE
        WHEN a.stock_quantity > 3
        AND b.sales_flag = 5
        AND b.new_flag = 2
        THEN a.stock_quantity * a.sale_price
        ELSE 0
      END
    ) AS sale5_stock_value,
    SUM(a.stock_quantity) AS stock_qty,
    SUM(a.stock_quantity * a.sale_price) AS stock_value
  FROM
    fe.sf_shelf_product_detail a
    LEFT JOIN fe.sf_shelf_product_detail_flag b
      ON a.shelf_id = b.shelf_id
      AND a.product_id = b.product_id
    LEFT JOIN fe.sf_shelf c
      ON a.shelf_id = c.shelf_id
    LEFT JOIN fe.zs_city_business d
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(c.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      ) = d.CITY_NAME
    LEFT JOIN fe.pub_shelf_manager e
      ON c.manager_id = e.manager_id
  WHERE a.stock_quantity > 0
    AND c.SHELF_TYPE IN (1, 2, 3, 4, 5)
    AND c.shelf_status = 2
  GROUP BY DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    ),
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(c.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ),
    e.BRANCH_CODE,
    e.BRANCH_NAME,
    e.SF_CODE,
    e.REAL_NAME,
    a.shelf_id;
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_c_lei_buhuo_data',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
  COMMIT;
END