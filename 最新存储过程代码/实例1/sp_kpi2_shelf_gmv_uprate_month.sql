CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_shelf_gmv_uprate_month`(in_month_id CHAR(7))
BEGIN
  #run after sh_process.dwd_order_item_refund_day_inc	
   SET @month_id := in_month_id, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @str := '';
  SET @month_start := CONCAT(@month_id, '-01');
  SET @month_end := LAST_DAY(@month_start);
  SET @add_day := ADDDATE(@month_end, 1);
  SET @last_month_start := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @ym := DATE_FORMAT(@month_start, '%Y%m');
  SET @last_ym := DATE_FORMAT(@last_month_start, '%Y%m');
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.shelf_id
  FROM
    fe.sf_shelf s
  WHERE s.data_flag = 1
    AND s.shelf_type IN (1, 2, 3, 5)
    AND s.activate_time < @last_month_start
    AND (
      s.revoke_time IS NULL
      OR s.revoke_time >= @add_day
    )
    AND ! ISNULL(s.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_tmp;
  CREATE TEMPORARY TABLE feods.gmv_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(t.sale_price * t.quantity_act) gmv
  FROM
    fe_dwd.dwd_order_item_refund_day t
  WHERE t.pay_date >= @month_start
    AND t.pay_date < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_last_tmp;
  CREATE TEMPORARY TABLE feods.gmv_last_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(t.sale_price * t.quantity_act) gmv
  FROM
    fe_dwd.dwd_order_item_refund_day t
  WHERE t.pay_date >= @last_month_start
    AND t.pay_date < @month_start
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.fjr_kpi2_shelf_gmv_uprate_month
  WHERE month_id = @month_id;
  INSERT INTO feods.fjr_kpi2_shelf_gmv_uprate_month (
    month_id, shelf_id, gmv, gmv_lm, add_user
  )
  SELECT
    @month_id month_id, t.shelf_id, IFNULL(b1.gmv, 0) gmv, IFNULL(b2.gmv, 0) gmv_lm, @add_user add_user
  FROM
    feods.shelf_tmp t
    LEFT JOIN feods.gmv_tmp b1
      ON t.shelf_id = b1.shelf_id
    LEFT JOIN feods.gmv_last_tmp b2
      ON t.shelf_id = b2.shelf_id
  WHERE b1.gmv > 0
    OR b2.gmv > 0;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 101;
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, 'm' indicate_type, 101 indicate_id, 'fjr_kpi2_shelf_gmv_uprate_month' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lm) - 1, 6) indicate_value, @add_user add_user
  FROM
    feods.fjr_kpi2_shelf_gmv_uprate_month t
  WHERE t.month_id = @month_id;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 101;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, s.business_name, 'm' indicate_type, 101 indicate_id, 'fjr_kpi2_shelf_gmv_uprate_month' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lm) - 1, 6) indicate_value, @add_user add_user
  FROM
    feods.fjr_kpi2_shelf_gmv_uprate_month t
    JOIN
      (SELECT
        t.shelf_id, b.business_name
      FROM
        fe.sf_shelf t
        JOIN feods.fjr_city_business b
          ON t.city = b.city
      WHERE t.data_flag = 1) s
      ON t.shelf_id = s.shelf_id
  WHERE t.month_id = @month_id
  GROUP BY s.business_name;
  CALL feods.sp_task_log (
    'sp_kpi2_shelf_gmv_uprate_month', @month_start, CONCAT(
      'fjr_m_cfaf6a3dbd7c92de4927b11a6be40ffd', @timestamp, @add_user
    )
  );
  COMMIT;
END