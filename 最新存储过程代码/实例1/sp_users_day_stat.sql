CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_users_day_stat`(in_date DATE)
BEGIN
  SET @sdate := in_date,
  @add_user := CURRENT_USER,
  @timestamp := current_timestamp;
  SET @add_date := ADDDATE(@sdate, 1),
  @week_first_date := SUBDATE(@sdate, WEEKDAY(@sdate)),
  @month_first_date := SUBDATE(@sdate, DAY(@sdate) - 1),
  @year_first_date := SUBDATE(@sdate, DAYOFYEAR(@sdate) - 1);
  DROP TEMPORARY TABLE IF EXISTS feods.user_tmp;
  CREATE TEMPORARY TABLE feods.user_tmp AS
  SELECT DISTINCT
    s.business_name,
    o.user_id
  FROM
    fe_dwd.dwd_order_item_refund_day o
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON o.shelf_id = s.shelf_id
  WHERE o.order_status in (2,6,7)
    AND o.pay_date >= @sdate
    AND o.pay_date < @add_date;
	
  CREATE INDEX idx_user_tmp_business_name_shelf_id
  ON feods.user_tmp (business_name, user_id);
  DELETE
  FROM
    feods.fjr_user_firstday_week_tran
  WHERE first_pur_date_week < @week_first_date
    OR first_pur_date_week >= @sdate;
  INSERT INTO feods.fjr_user_firstday_week_tran (
    business_name,
    first_pur_date_week,
    user_id
  )
  SELECT
    t.business_name,
    @sdate,
    t.user_id
  FROM
    feods.user_tmp t
    LEFT JOIN feods.fjr_user_firstday_week_tran w
      ON t.business_name = w.business_name
      AND t.user_id = w.user_id
  WHERE w.user_id IS NULL;
  DELETE
  FROM
    feods.fjr_user_firstday_month_tran
  WHERE first_pur_date_month < @month_first_date
    OR first_pur_date_month >= @sdate;
  INSERT INTO feods.fjr_user_firstday_month_tran (
    business_name,
    first_pur_date_month,
    user_id
  )
  SELECT
    t.business_name,
    @sdate,
    t.user_id
  FROM
    feods.user_tmp t
    LEFT JOIN feods.fjr_user_firstday_month_tran m
      ON t.business_name = m.business_name
      AND t.user_id = m.user_id
  WHERE m.user_id IS NULL;
  DELETE
  FROM
    feods.fjr_user_firstday_year_tran
  WHERE first_pur_date_year < @year_first_date
    OR first_pur_date_year >= @sdate;
  INSERT INTO feods.fjr_user_firstday_year_tran (
    business_name,
    first_pur_date_year,
    user_id
  )
  SELECT
    t.business_name,
    @sdate,
    t.user_id
  FROM
    feods.user_tmp t
    LEFT JOIN feods.fjr_user_firstday_year_tran yr
      ON t.business_name = yr.business_name
      AND t.user_id = yr.user_id
  WHERE yr.user_id IS NULL;
  DELETE
  FROM
    feods.fjr_user_firstday_tran
  WHERE first_pur_date >= @sdate;
  INSERT INTO feods.fjr_user_firstday_tran (
    business_name,
    first_pur_date,
    user_id
  )
  SELECT
    t.business_name,
    @sdate,
    t.user_id
  FROM
    feods.user_tmp t
    LEFT JOIN feods.fjr_user_firstday_tran a
      ON t.business_name = a.business_name
      AND t.user_id = a.user_id
  WHERE a.user_id IS NULL;
  DELETE
  FROM
    feods.fjr_users_dayct_week_tran
  WHERE sdate < @week_first_date
    OR sdate >= @sdate;
  INSERT INTO feods.fjr_users_dayct_week_tran (sdate, business_name, users_week)
  SELECT
    @sdate,
    t.business_name,
    COUNT(*) users_week
  FROM
    feods.fjr_user_firstday_week_tran t
  WHERE t.first_pur_date_week = @sdate
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.fjr_users_dayct_month_tran
  WHERE sdate < @month_first_date
    OR sdate >= @sdate;
  INSERT INTO feods.fjr_users_dayct_month_tran (sdate, business_name, users_month)
  SELECT
    @sdate,
    t.business_name,
    COUNT(*) users_month
  FROM
    feods.fjr_user_firstday_month_tran t
  WHERE t.first_pur_date_month = @sdate
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.fjr_users_dayct_year_tran
  WHERE sdate < @year_first_date
    OR sdate >= @sdate;
  INSERT INTO feods.fjr_users_dayct_year_tran (sdate, business_name, users_year)
  SELECT
    @sdate,
    t.business_name,
    COUNT(*) users_year
  FROM
    feods.fjr_user_firstday_year_tran t
  WHERE t.first_pur_date_year = @sdate
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.fjr_users_dayct_tran
  WHERE sdate >= @sdate;
  INSERT INTO feods.fjr_users_dayct_tran (sdate, business_name, users)
  SELECT
    @sdate,
    t.business_name,
    COUNT(*) users
  FROM
    feods.fjr_user_firstday_tran t
  WHERE t.first_pur_date = @sdate
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.fjr_users_day_stat
  WHERE sdate >= @sdate;
  INSERT INTO feods.fjr_users_day_stat (
    sdate,
    business_name,
    users_new,
    users_day,
    users_week,
    users_month,
    users_year,
    users,
    add_user
  )
  SELECT
    @sdate,
    t.business_name,
    IFNULL(ct1.users, 0),
    IFNULL(u1.ct, 0),
    IFNULL(ctw.users_week, 0),
    IFNULL(ctm.users_month, 0),
    IFNULL(cty.users_year, 0),
    IFNULL(cta.users, 0),
    @add_user
  FROM
    (SELECT DISTINCT
      b.business_name
    FROM
      feods.fjr_city_business b) t
    LEFT JOIN feods.fjr_users_dayct_tran ct1
      ON t.business_name = ct1.business_name
      AND ct1.sdate = @sdate
    LEFT JOIN
      (SELECT
        t.business_name,
        COUNT(*) ct
      FROM
        feods.user_tmp t
      GROUP BY t.business_name) u1
      ON t.business_name = u1.business_name
    LEFT JOIN
      (SELECT
        t.business_name,
        SUM(t.users_week) users_week
      FROM
        feods.fjr_users_dayct_week_tran t
      GROUP BY t.business_name) ctw
      ON t.business_name = ctw.business_name
    LEFT JOIN
      (SELECT
        t.business_name,
        SUM(t.users_month) users_month
      FROM
        feods.fjr_users_dayct_month_tran t
      GROUP BY t.business_name) ctm
      ON t.business_name = ctm.business_name
    LEFT JOIN
      (SELECT
        t.business_name,
        SUM(t.users_year) users_year
      FROM
        feods.fjr_users_dayct_year_tran t
      GROUP BY t.business_name) cty
      ON t.business_name = cty.business_name
    LEFT JOIN
      (SELECT
        t.business_name,
        SUM(t.users) users
      FROM
        feods.fjr_users_dayct_tran t
      GROUP BY t.business_name) cta
      ON t.business_name = cta.business_name;
  CALL feods.sp_task_log (
    'sp_users_day_stat',
    @sdate,
    concat(
      'fjr_d_a6622ca9931477473d29838d0f8f99aa',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END