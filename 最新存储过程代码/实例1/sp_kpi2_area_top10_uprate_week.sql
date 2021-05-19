CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_area_top10_uprate_week`(in_week_end DATE)
BEGIN
  #run after sh_process.sp_kpi2_new_out_storate
#run after sh_process.sp_kpi2_unsku_shelf_his
#run after sh_process.sp_kpi2_shelf_level_stat
#run after sh_process.sp_kpi2_area_product_satrate
#run after sh_process.sp_kpi2_area_product_satrate_his
#run after sh_process.sp_kpi2_outlet_rate
#run after sh_process.sp_kpi2_outlet_rate_his
#run after sh_process.sp_kpi2_area_top10_uprate_month
#run after sh_process.sp_kpi2_np_success_rate_month
#run after sh_process.sp_kpi2_unsku_shelf
#run after sh_process.sp_kpi2_shelf_gmv_uprate_month
#run after sh_process.sp_kpi2_sale_vs_stock_month
#run after sh_process.sp_kpi2_sale_vs_stock_week
#run after sh_process.sp_kpi2_new_out_storate_his
#run after sh_process.sp_kpi2_shelf_gmv_uprate_week
   SET @week_end := SUBDATE(
    in_week_end, DAYOFWEEK(in_week_end) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@week_end, 1), @last_week_end := SUBDATE(@week_end, 7), @week_start := SUBDATE(@week_end, 6), @last_week_start := SUBDATE(@week_end, 13);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp AS
  SELECT
    s.shelf_id, b.business_name
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1
    AND s.shelf_type IN (1, 2, 3, 5)
    AND s.activate_time < @last_week_start
    AND (
      s.revoke_time IS NULL
      OR s.revoke_time >= @add_day
    );
  CREATE INDEX idx_shelf_id
  ON feods.shelf_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp AS
  SELECT
    t.shelf_id, oi.product_id, SUM(oi.sale_price * oi.quantity) gmv
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
  WHERE t.order_status = 2
    AND t.order_date >= @week_start
    AND t.order_date < @add_day
  GROUP BY t.shelf_id, oi.product_id;
  CREATE INDEX idx_shelf_id
  ON feods.oi_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_tmp;
  CREATE TEMPORARY TABLE feods.gmv_tmp AS
  SELECT
    s.business_name, t.product_id, SUM(t.gmv) gmv
  FROM
    feods.oi_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY s.business_name, t.product_id;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.order_gmv_tmp;
  CREATE TEMPORARY TABLE feods.order_gmv_tmp AS
  SELECT
    t.business_name, t.order_num, t.product_id, t.gmv
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @order_area
          WHEN t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @order_area := t.business_name business_name, t.product_id, t.gmv
    FROM
      feods.gmv_tmp t
    ORDER BY t.business_name, t.gmv DESC) t
  WHERE t.order_num <= 10;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_lw_tmp;
  CREATE TEMPORARY TABLE feods.oi_lw_tmp AS
  SELECT
    t.shelf_id, oi.product_id, SUM(oi.sale_price * oi.quantity) gmv
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
  WHERE t.order_status = 2
    AND t.order_date >= @last_week_start
    AND t.order_date < @week_start
  GROUP BY t.shelf_id, oi.product_id;
  CREATE INDEX idx_shelf_id
  ON feods.oi_lw_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_lw_tmp;
  CREATE TEMPORARY TABLE feods.gmv_lw_tmp AS
  SELECT
    s.business_name, t.product_id, SUM(t.gmv) gmv
  FROM
    feods.oi_lw_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY s.business_name, t.product_id;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.order_gmv_lw_tmp;
  CREATE TEMPORARY TABLE feods.order_gmv_lw_tmp AS
  SELECT
    t.business_name, t.order_num, t.product_id, t.gmv
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @order_area
          WHEN t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @order_area := t.business_name business_name, t.product_id, t.gmv
    FROM
      feods.gmv_lw_tmp t
    ORDER BY t.business_name, t.gmv DESC) t
  WHERE t.order_num <= 10;
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_lwp_tmp;
  CREATE TEMPORARY TABLE feods.gmv_lwp_tmp AS
  SELECT
    t.business_name, l.order_num, t.gmv
  FROM
    feods.gmv_tmp t
    JOIN feods.order_gmv_lw_tmp l
      ON t.business_name = l.business_name
      AND t.product_id = l.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.area10_tmp;
  CREATE TEMPORARY TABLE feods.area10_tmp AS
  SELECT
    t.business_name, n.order_num
  FROM
    (SELECT DISTINCT
      t.business_name
    FROM
      feods.fjr_city_business t) t
    JOIN
      (SELECT
        t.number + 1 order_num
      FROM
        feods.fjr_number t
      WHERE t.number < 10) n
      ON 1;
  DELETE
  FROM
    feods.fjr_kpi2_area_top10_uprate_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi2_area_top10_uprate_week (
    week_end, business_name, order_num, product_id, gmv, product_id_lw, gmv_lw, gmv_lwp, add_user
  )
  SELECT
    @week_end week_end, t.business_name, t.order_num, IFNULL(b1.product_id, 0) product_id, IFNULL(b1.gmv, 0) gmv, IFNULL(b2.product_id, 0) product_id_lw, IFNULL(b2.gmv, 0) gmv_lw, IFNULL(b12.gmv, 0) gmv_lwp, @add_user add_user
  FROM
    feods.area10_tmp t
    LEFT JOIN feods.order_gmv_tmp b1
      ON t.business_name = b1.business_name
      AND t.order_num = b1.order_num
    LEFT JOIN feods.order_gmv_lw_tmp b2
      ON t.business_name = b2.business_name
      AND t.order_num = b2.order_num
    LEFT JOIN feods.gmv_lwp_tmp b12
      ON t.business_name = b12.business_name
      AND t.order_num = b12.order_num;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 102;
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, 'w' indicate_type, 102 indicate_id, 'fjr_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    feods.fjr_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 102;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, t.business_name, 'w' indicate_type, 102 indicate_id, 'fjr_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    feods.fjr_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 202;
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, 'w' indicate_type, 202 indicate_id, 'fjr_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv_lwp) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    feods.fjr_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 202;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, t.business_name, 'w' indicate_type, 202 indicate_id, 'fjr_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv_lwp) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    feods.fjr_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name;
  CALL feods.sp_task_log (
    'sp_kpi2_area_top10_uprate_week', @week_end, CONCAT(
      'fjr_w_046e658479d0ea730af633fa0c4f07d8', @timestamp, @add_user
    )
  );
  COMMIT;
END