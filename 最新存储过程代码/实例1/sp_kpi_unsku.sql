CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_unsku`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := current_user, @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    feods.fjr_kpi_unsku
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi_unsku (
    sdate, region, business_area, shelf_id, shelf_type, skus, add_user
  )
  SELECT
    @sdate, b.region_name, b.business_name, d.SHELF_ID, s.shelf_type, COUNT(*) ct, @add_user
  FROM
    fe.sf_shelf_product_detail d
    join fe.sf_shelf s
      on d.SHELF_ID = s.SHELF_ID
      and s.DATA_FLAG = 1
      AND s.SHELF_STATUS = 2
      AND s.REVOKE_STATUS = 1
      AND s.WHETHER_CLOSE = 2
      AND s.shelf_type IN (1, 2, 3, 5)
    join feods.fjr_city_business b
      on s.city = b.city
  WHERE d.DATA_FLAG = 1
    AND d.STOCK_QUANTITY > 0
  GROUP BY d.SHELF_ID
  HAVING ct < (
      CASE
        WHEN s.shelf_type IN (1, 3)
        THEN 25
        ELSE 10
      END
    );
  CALL feods.sp_task_log (
    'sp_kpi_unsku', @sdate, CONCAT(
      'yingnansong_d_93055956fbd2454dd3bed5f5cc8a0177', @timestamp, @add_user
    )
  );
  COMMIT;
END