CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_shelf_gmv_uprate_week`(in_week_end DATE)
BEGIN
  #run after sh_process.sp_shelf_profile_week	
   SET @week_end := SUBDATE(
    in_week_end,
    DAYOFWEEK(in_week_end) - 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@week_end, 1),
  @last_week_end := SUBDATE(@week_end, 7),
  @last_week_start := SUBDATE(@week_end, 13);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp AS
  SELECT
    s.shelf_id
  FROM
    fe.sf_shelf s
  WHERE s.data_flag = 1
    AND s.shelf_type IN (1, 2, 3, 5)
    AND s.activate_time < @last_week_start
    AND (
      s.revoke_time IS NULL
      OR s.revoke_time >= @add_day
    );
  CREATE INDEX idx_shelf_id
  ON feods.shelf_tmp (shelf_id);
  DELETE
  FROM
    feods.fjr_kpi2_shelf_gmv_uprate_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi2_shelf_gmv_uprate_week (
    week_end,
    shelf_id,
    gmv,
    gmv_lw,
    add_user
  )
  SELECT
    @week_end week_end,
    t.shelf_id,
    IFNULL(b1.gmv, 0) gmv,
    IFNULL(b2.gmv, 0) gmv_lw,
    @add_user add_user
  FROM
    feods.shelf_tmp t
    LEFT JOIN feods.fjr_shelf_sal_base b1
      ON t.shelf_id = b1.shelf_id
      AND b1.week_end = @week_end
    LEFT JOIN feods.fjr_shelf_sal_base b2
      ON t.shelf_id = b2.shelf_id
      AND b2.week_end = @last_week_end
  WHERE b1.gmv > 0
    OR b2.gmv > 0;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 101;
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @week_end sdate,
    'w' indicate_type,
    101 indicate_id,
    'fjr_kpi2_shelf_gmv_uprate_week' indicate_name,
    ROUND(SUM(gmv) / SUM(gmv_lw) - 1, 6) indicate_value,
    @add_user add_user
  FROM
    feods.fjr_kpi2_shelf_gmv_uprate_week t
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 101;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate,
    business_name,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @week_end sdate,
    s.business_name,
    'w' indicate_type,
    101 indicate_id,
    'fjr_kpi2_shelf_gmv_uprate_week' indicate_name,
    ROUND(SUM(gmv) / SUM(gmv_lw) - 1, 6) indicate_value,
    @add_user add_user
  FROM
    feods.fjr_kpi2_shelf_gmv_uprate_week t
    JOIN
      (SELECT
        t.shelf_id,
        b.business_name
      FROM
        fe.sf_shelf t
        JOIN feods.fjr_city_business b
          ON t.city = b.city
      WHERE t.data_flag = 1) s
      ON t.shelf_id = s.shelf_id
  WHERE t.week_end = @week_end
  GROUP BY s.business_name;
  CALL feods.sp_task_log (
    'sp_kpi2_shelf_gmv_uprate_week',
    @week_end,
    CONCAT(
      'fjr_w_7a68547750a5ad0a24a33b9034f64c2f',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END