CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_new_out_storate`()
BEGIN
  #run after sh_process.sp_kpi2_outlet_rate_his
#run after sh_process.sp_kpi2_sale_vs_stock_week
#run after sh_process.sp_kpi2_area_product_satrate
#run after sh_process.sp_kpi2_area_product_satrate_his
#run after sh_process.sp_kpi2_np_success_rate_month
#run after sh_process.sp_kpi2_unsku_shelf_his
#run after sh_process._his
#run after sh_process.sp_kpi2_shelf_level_stat
#run after sh_process.sp_kpi2_area_top10_uprate_week
#run after sh_process.sp_kpi2_sale_vs_stock_month
#run after sh_process.sp_kpi2_area_top10_uprate_month
#run after sh_process.sp_kpi2_shelf_gmv_uprate_month
#run after sh_process.sp_kpi2_outlet_rate
#run after sh_process.sp_kpi2_unsku_shelf
#run after sh_process.sp_kpi2_shelf_gmv_uprate_week
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1), @last_month_end := SUBDATE(@sdate, DAY(@sdate));
  SET @last_month_start := SUBDATE(
    @last_month_end, DAY(@last_month_end) - 1
  );
  SELECT
    @version_id := MAX(t.version)
  FROM
    feods.zs_product_dim_sserp t;
  DROP TEMPORARY TABLE IF EXISTS feods.dim_tmp;
  CREATE TEMPORARY TABLE feods.dim_tmp AS
  SELECT
    t.business_area business_name, t.product_id, t.product_type in (
      '新增（试运行）', '新增（免费货）'
    ) if_new
  FROM
    feods.zs_product_dim_sserp t
  WHERE t.product_type IN (
      '停补', '停补（替补）', '新增（试运行）', '新增（免费货）', '淘汰', '淘汰（替补）', '退出'
    );
  CREATE INDEX idx_business_name_product_id
  ON feods.dim_tmp (business_name, product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp AS
  SELECT
    s.shelf_id, b.business_name
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1;
  CREATE INDEX idx_shelf_id
  ON feods.shelf_tmp (shelf_id);
  DELETE
  FROM
    feods.fjr_kpi2_new_out_storate
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi2_new_out_storate (
    sdate, version_id, business_name, product_id, if_new, if_out, qty_sto, val_sto, add_user
  )
  SELECT
    @sdate sdate, @version_id version_id, s.business_name, t.product_id, IFNULL(d.if_new, 0) if_new, IFNULL(d.if_new = 0, 0) if_out, SUM(t.stock_quantity) qty_sto, SUM(t.stock_quantity * t.sale_price) val_sto, @add_user
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.dim_tmp d
      ON t.product_id = d.product_id
      AND s.business_name = d.business_name
  WHERE t.data_flag = 1
  GROUP BY s.business_name, t.product_id;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE indicate_id IN (107, 108)
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 107 indicate_id, 'fjr_kpi2_new_out_storate' indicate_name, ROUND(
      SUM(IF(t.if_new, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_new_out_storate t
  WHERE t.sdate = @last_week_end
  GROUP BY t.business_name;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 108 indicate_id, 'fjr_kpi2_new_out_storate' indicate_name, ROUND(
      SUM(IF(t.if_out, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_new_out_storate t
  WHERE t.sdate = @last_week_end
  GROUP BY t.business_name;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, t.business_name, 'm' indicate_type, 107 indicate_id, 'fjr_kpi2_new_out_storate' indicate_name, ROUND(
      SUM(IF(t.if_new, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_new_out_storate t
  WHERE t.sdate = @last_month_end
  GROUP BY t.business_name;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, t.business_name, 'm' indicate_type, 108 indicate_id, 'fjr_kpi2_new_out_storate' indicate_name, ROUND(
      SUM(IF(t.if_out, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_new_out_storate t
  WHERE t.sdate = @last_month_end
  GROUP BY t.business_name;
  SELECT
    @np_w := SUM(IF(t.if_new, t.val_sto, 0)), @out_w := SUM(IF(t.if_out, t.val_sto, 0)), @tot_w := SUM(t.val_sto)
  FROM
    feods.fjr_kpi2_new_out_storate t
  WHERE t.sdate = @last_week_end;
  SELECT
    @np_m := SUM(IF(t.if_new, t.val_sto, 0)), @out_m := SUM(IF(t.if_out, t.val_sto, 0)), @tot_m := SUM(t.val_sto)
  FROM
    feods.fjr_kpi2_new_out_storate t
  WHERE t.sdate = @last_month_end;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE indicate_id IN (107, 108)
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @last_week_end, 'w', 107, 'fjr_kpi2_new_out_storate', @np_w / @tot_w, @add_user
    ), (
      @last_month_start, 'm', 107, 'fjr_kpi2_new_out_storate', @np_m / @tot_m, @add_user
    ), (
      @last_week_end, 'w', 108, 'fjr_kpi2_new_out_storate', @out_w / @tot_w, @add_user
    ), (
      @last_month_start, 'm', 108, 'fjr_kpi2_new_out_storate', @out_m / @tot_m, @add_user
    );
  CALL feods.sp_task_log (
    'sp_kpi2_new_out_storate', @sdate, CONCAT(
      'fjr_d_4aa302f10f7ddaeff10d91e9348059ee', @timestamp, @add_user
    )
  );
  COMMIT;
END