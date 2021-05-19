CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_shelf_nps`(IN in_sdate DATE)
BEGIN
  SET @sdate := in_sdate,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    feods.fjr_kpi_shelf_nps
  WHERE sdate >= CONCAT(
      DATE_FORMAT(
        SUBDATE(@sdate, INTERVAL 2 MONTH),
        '%Y-%m'
      ),
      '-01'
    )
    AND sdate < LAST_DAY(SUBDATE(@sdate, INTERVAL 2 MONTH));
  DELETE
  FROM
    feods.fjr_kpi_shelf_nps
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi_shelf_nps (
    sdate,
    region,
    business_area,
    shelf_id,
    new_products,
    add_user
  )
  SELECT
    t.sdate,
    t.REGION_NAME,
    t.BUSINESS_NAME,
    t.SHELF_ID,
    ifnull(SUM(t.new_products), 0) new_products,
    @add_user
  FROM
    (SELECT
      @sdate sdate,
      b.REGION_NAME,
      b.BUSINESS_NAME,
      s.SHELF_ID,
      COUNT(1) new_products
    FROM
      fe.sf_shelf_product_detail_flag f,
      fe.sf_shelf s,
      feods.fjr_city_business b
    WHERE s.SHELF_ID = f.SHELF_ID
      AND s.city = b.city
      AND f.DATA_FLAG = 1
      AND s.DATA_FLAG = 1
      AND s.SHELF_type IN (1, 2, 3, 5)
      AND s.ACTIVATE_TIME < SUBDATE(LAST_DAY(@sdate), 13)
      AND (
        s.REVOKE_TIME IS NULL
        OR s.REVOKE_TIME >= CONCAT(
          DATE_FORMAT(@sdate, '%Y-%m'),
          '-01'
        )
      )
      AND f.FIRST_FILL_TIME >= ADDDATE(s.ACTIVATE_TIME, 14)
      AND f.FIRST_FILL_TIME >= SUBDATE(
        CONCAT(
          DATE_FORMAT(@sdate, '%Y-%m'),
          '-01'
        ),
        14-1
      )
      AND f.FIRST_FILL_TIME < ADDDATE(@sdate, 1)
    GROUP BY s.SHELF_ID
    UNION
    ALL
    SELECT
      @sdate sdate,
      b.REGION_NAME,
      b.BUSINESS_NAME,
      s.SHELF_ID,
      0 new_products
    FROM
      fe.sf_shelf s,
      feods.fjr_city_business b
    WHERE s.city = b.city
      AND s.DATA_FLAG = 1
      AND s.SHELF_type IN (1, 2, 3, 5)
      AND s.ACTIVATE_TIME < SUBDATE(LAST_DAY(@sdate), 13)
      AND (
        s.REVOKE_TIME IS NULL
        OR s.REVOKE_TIME >= CONCAT(
          DATE_FORMAT(@sdate, '%Y-%m'),
          '-01'
        )
      )) t
  GROUP BY t.SHELF_ID;
  CALL feods.sp_task_log (
    'sp_kpi_shelf_nps',
    @sdate,
    CONCAT(
      'fjr_d_97b4946679f673cb850f48c0f5c3680f',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END