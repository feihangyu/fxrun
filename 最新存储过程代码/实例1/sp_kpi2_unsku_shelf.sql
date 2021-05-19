CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_unsku_shelf`()
BEGIN
  #run after sh_process.sh_shelf_level_ab
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @last_month_id := DATE_FORMAT(
    SUBDATE(@sdate, INTERVAL 1 MONTH), '%Y%m'
  ), @last2_month_id := DATE_FORMAT(
    SUBDATE(@sdate, INTERVAL 2 MONTH), '%Y%m'
  );
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1), @last_month_end := SUBDATE(@sdate, DAY(@sdate));
  SET @last_month_start := SUBDATE(
    @last_month_end, DAY(@last_month_end) - 1
  );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT DISTINCT
    s.shelf_id, s.shelf_type
  FROM
    feods.pj_shelf_level_ab t
    JOIN fe.sf_shelf s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 1
      AND s.shelf_status = 2
      AND s.shelf_type IN (1, 2, 3, 5)
      AND s.revoke_status = 1
      AND ! ISNULL(s.shelf_id)
  WHERE t.smonth IN (@last_month_id, @last2_month_id)
    AND t.shelf_level IN (
      '丙级', '丙级2', '乙级', '乙级2', '甲级', '甲级2'
    );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_business_tmp;
  CREATE TEMPORARY TABLE feods.shelf_business_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, b.business_name
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id);
  DELETE
  FROM
    feods.fjr_kpi2_unsku_shelf
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi2_unsku_shelf (
    sdate, shelf_id, shelf_type, skus_sto, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, s.shelf_type, COUNT(*) skus_sto, @add_user
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.stock_quantity > 0
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE indicate_id = 105
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, s.business_name, 'w' indicate_type, 105 indicate_id, 'fjr_kpi2_unsku_shelf' indicate_name, COUNT(*) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_unsku_shelf t
    JOIN feods.shelf_business_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.sdate = @last_week_end
    AND t.skus_sto < IF(t.shelf_type IN (1, 3), 25, 10)
  GROUP BY s.business_name;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, s.business_name, 'm' indicate_type, 105 indicate_id, 'fjr_kpi2_unsku_shelf' indicate_name, COUNT(*) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_unsku_shelf t
    JOIN feods.shelf_business_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.sdate = @last_month_end
    AND t.skus_sto < IF(t.shelf_type IN (1, 3), 25, 10)
  GROUP BY s.business_name;
  SELECT
    @shelfs_w := SUM(indicate_value)
  FROM
    feods.fjr_kpi2_monitor_area t
  WHERE t.indicate_id = 105
    AND t.indicate_type = 'w'
    AND t.sdate = @last_week_end;
  SELECT
    @shelfs_m := SUM(indicate_value)
  FROM
    feods.fjr_kpi2_monitor_area t
  WHERE t.indicate_id = 105
    AND t.indicate_type = 'm'
    AND t.sdate = @last_month_start;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE indicate_id = 105
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @last_week_end, 'w', 105, 'fjr_kpi2_unsku_shelf', @shelfs_w, @add_user
    ), (
      @last_month_start, 'm', 105, 'fjr_kpi2_unsku_shelf', @shelfs_m, @add_user
    );
  CALL feods.sp_task_log (
    'sp_kpi2_unsku_shelf', @sdate, CONCAT(
      'fjr_d_ba57c8b23f5fcf4239c0d1106710dbbc', @timestamp, @add_user
    )
  );
  COMMIT;
END