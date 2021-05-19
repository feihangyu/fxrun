CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_avggmv_week`(in_week_end DATE)
BEGIN
  SET @week_end := SUBDATE(
    in_week_end,
    DAYOFWEEK(in_week_end) - 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_week_tmp,
  feods.shelf_sal_tmp,
  feods.days_tmp;
  CREATE TEMPORARY TABLE feods.oi_week_tmp AS
  SELECT
    date(o.order_date) sdate,
    o.shelf_id,
    SUM(oi.quantity * oi.sale_price) gmv
  FROM
    fe.sf_order_item oi
    JOIN fe.sf_order o
      ON oi.order_id = o.order_id
      AND o.order_status = 2
      AND o.order_date >= SUBDATE(@week_end, 7-1)
      AND o.order_date < ADDDATE(@week_end, 1)
  GROUP BY date(o.order_date),
    o.shelf_id;
  CREATE INDEX idx_oi_week_tmp_shelf_id
  ON feods.oi_week_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.shelf_sal_tmp AS
  SELECT
    t.city,
    t.shelf_id,
    SUM(t.gmv) gmv,
    SUM(t.gmv_wd) gmv_wd,
    SUM(t.payment_money) payment_money,
    SUM(t.payment_money_wd) payment_money_wd
  FROM
    (SELECT
      s.city,
      t.shelf_id,
      SUM(t.gmv) gmv,
      SUM(
        CASE
          w.if_work_day
          WHEN 1
          THEN t.gmv
        END
      ) gmv_wd,
      0 payment_money,
      0 payment_money_wd
    FROM
      feods.oi_week_tmp t
      JOIN feods.fjr_work_days w
        ON t.sdate = w.sdate
      JOIN fe.sf_shelf s
        ON t.shelf_id = s.shelf_id
        AND t.sdate > s.activate_time
        AND s.data_flag = 1
        AND (
          s.revoke_time IS NULL
          OR s.revoke_time > ADDDATE(@week_end, 1)
        )
        AND s.shelf_type IN (1, 2, 3, 5)
    GROUP BY t.shelf_id
    UNION
    ALL
    SELECT
      s.city,
      s.shelf_id,
      0 gmv,
      0 gmv_wd,
      SUM(t.payment_money) payment_money,
      SUM(
        CASE
          w.if_work_day
          WHEN 1
          THEN t.payment_money
        END
      ) payment_money_wd
    FROM
      fe.sf_after_payment t
      JOIN feods.fjr_work_days w
        ON t.payment_date >= w.sdate
        AND t.payment_date < ADDDATE(w.sdate, 1)
      JOIN fe.sf_shelf s
        ON t.shelf_id = s.shelf_id
        AND t.PAYMENT_DATE >= ADDDATE(s.activate_time, 1)
        AND s.data_flag = 1
        AND (
          s.revoke_time IS NULL
          OR s.revoke_time > ADDDATE(@week_end, 1)
        )
        AND s.shelf_type IN (1, 2, 3, 5)
    WHERE t.payment_status = 2
    GROUP BY t.shelf_id) t
  GROUP BY t.shelf_id;
  CREATE INDEX idx_shelf_sal_tmp_shelf_id
  ON feods.shelf_sal_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.days_tmp AS
  SELECT
    s.shelf_id,
    COUNT(*) days,
    SUM(w.if_work_day) days_wd
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_work_days w
      ON s.activate_time < w.sdate
      AND w.sdate >= SUBDATE(@week_end, 7-1)
      AND w.sdate < ADDDATE(@week_end, 1)
  WHERE s.data_flag = 1
    AND s.shelf_type IN (1, 2, 3, 5)
    AND (
      s.revoke_time IS NULL
      OR s.revoke_time > ADDDATE(@week_end, 1)
    )
  GROUP BY s.shelf_id;
  CREATE INDEX idx_days_tmp_shelf_id
  ON feods.days_tmp (shelf_id);
  DELETE
  FROM
    feods.fjr_kpi_avggmv_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi_avggmv_week (
    business_last_month,
    week_end,
    region,
    business_area,
    shelf_id,
    days,
    days_wd,
    gmv,
    gmv_wd,
    payment_money,
    payment_money_wd,
    add_user
  )
  SELECT
    DATE_FORMAT(
      SUBDATE(
        @week_end,
        INTERVAL 2-
        (SELECT
          w.bussiness_month
        FROM
          feods.fjr_work_days w
        WHERE w.sdate = @week_end) MONTH
      ),
      '%Y-%m'
    ) month_id,
    @week_end week_end,
    b.region_name,
    b.business_name,
    t.shelf_id,
    ifnull(d.days, 0),
    IFNULL(d.days_wd, 0),
    IFNULL(t.gmv, 0),
    IFNULL(t.gmv_wd, 0),
    IFNULL(t.payment_money, 0),
    IFNULL(t.payment_money_wd, 0),
    @add_user
  FROM
    feods.shelf_sal_tmp t
    JOIN feods.days_tmp d
      ON t.shelf_id = d.shelf_id
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE (
      IFNULL(t.gmv, 0) + IFNULL(t.payment_money, 0)
    ) / d.days_wd BETWEEN 10
    AND 1000;
  DELETE
  FROM
    feods.fjr_kpi_ns_avggmv_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi_ns_avggmv_week (
    business_last_month,
    week_end,
    region,
    business_area,
    shelf_id,
    days,
    days_wd,
    gmv,
    gmv_wd,
    payment_money,
    payment_money_wd,
    add_user
  )
  SELECT
    t.business_last_month,
    t.week_end,
    t.region,
    t.business_area,
    t.shelf_id,
    IFNULL(t.days, 0),
    IFNULL(t.days_wd, 0),
    IFNULL(t.gmv, 0),
    IFNULL(t.gmv_wd, 0),
    IFNULL(t.payment_money, 0),
    IFNULL(t.payment_money_wd, 0),
    @add_user
  FROM
    feods.fjr_kpi_avggmv_week t
    JOIN fe.sf_shelf s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 1
      AND t.week_end < ADDDATE(s.ACTIVATE_TIME, 30-1+7)
      AND t.week_end >= ADDDATE(s.ACTIVATE_TIME, 3-1)
  WHERE t.week_end = @week_end;
  CALL feods.sp_task_log (
    'sp_kpi_avggmv_week',
    @week_end,
    CONCAT(
      'fjr_w_3144ed3453d61190e525ef9288bcaf91',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END