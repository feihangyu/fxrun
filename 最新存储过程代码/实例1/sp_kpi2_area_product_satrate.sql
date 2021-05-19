CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_area_product_satrate`()
BEGIN
  #run after sh_process.sp_kpi2_sale_vs_stock_month
#run after sh_process.sp_kpi2_shelf_gmv_uprate_week
#run after sh_process.sp_kpi2_area_top10_uprate_week
#run after sh_process.sp_kpi2_outlet_rate
#run after sh_process.sp_kpi2_sale_vs_stock_week
#run after sh_process.sp_kpi2_area_top10_uprate_month
#run after sh_process.sp_kpi2_np_success_rate_month
#run after sh_process.sp_kpi2_new_out_storate
#run after sh_process.sp_kpi2_unsku_shelf
#run after sh_process.sp_kpi2_new_out_storate_his
#run after sh_process.sp_kpi2_shelf_gmv_uprate_month
#run after sh_process.sp_kpi2_area_product_satrate_his
#run after sh_process.sp_kpi2_shelf_level_stat
#run after sh_process.sp_kpi2_unsku_shelf_his
#run after sh_process.sp_kpi2_outlet_rate_his
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @areas := 0, @areas_w := 0, @areas_m := 0;
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1), @last_month_end := SUBDATE(@sdate, DAY(@sdate));
  set @last_month_start := subdate(
    @last_month_end, day(@last_month_end) - 1
  );
  DROP TEMPORARY TABLE IF EXISTS feods.dim_tmp;
  CREATE TEMPORARY TABLE feods.dim_tmp AS
  SELECT
    t.version version_id, t.business_area business_name, t.product_id
  FROM
    feods.zs_product_dim_sserp t
  WHERE t.product_type IN (
      '原有', '新增（正式运行）'
    );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp AS
  SELECT
    t.shelf_id, b.business_name, t.shelf_status
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1;
  CREATE INDEX idx_shelf_id
  ON feods.shelf_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  CREATE TEMPORARY TABLE feods.sto_tmp AS
  SELECT
    d.business_name, d.product_id, COUNT(*) shelfs_sto
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN feods.dim_tmp d
      ON t.product_id = d.product_id
      AND s.business_name = d.business_name
  WHERE t.data_flag = 1
    AND t.stock_quantity > 0
  GROUP BY d.business_name, d.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.act_tmp;
  CREATE TEMPORARY TABLE feods.act_tmp AS
  SELECT
    s.business_name, COUNT(*) shelfs_act
  FROM
    feods.shelf_tmp s
  WHERE s.shelf_status = 2
  GROUP BY s.business_name;
  DELETE
  FROM
    feods.fjr_kpi2_area_product_satrate
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi2_area_product_satrate (
    sdate, version_id, business_name, product_id, shelfs_sto, shelfs_act, add_user
  )
  SELECT
    @sdate sdate, t.version_id, t.business_name, t.product_id, IFNULL(sto.shelfs_sto, 0) shelfs_sto, IFNULL(act.shelfs_act, 0) shelfs_act, @add_user add_user
  FROM
    feods.dim_tmp t
    LEFT JOIN feods.sto_tmp sto
      ON t.business_name = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN feods.act_tmp act
      ON t.business_name = act.business_name;
  SELECT
    @areas := COUNT(*)
  FROM
    feods.act_tmp t;
  SELECT
    @areas_w := round(COUNT(*) / @areas, 6)
  FROM
    (SELECT
      t.business_name, COUNT(*) ct
    FROM
      feods.fjr_kpi2_area_product_satrate t
    WHERE t.sdate = @last_week_end
      AND t.shelfs_sto >= .3 * t.shelfs_act
    GROUP BY t.business_name
    HAVING ct > 35) t;
  SELECT
    @areas_m := ROUND(COUNT(*) / @areas, 6)
  FROM
    (SELECT
      t.business_name, COUNT(*) ct
    FROM
      feods.fjr_kpi2_area_product_satrate t
    WHERE t.sdate = @last_month_end
      AND t.shelfs_sto >= .3 * t.shelfs_act
    GROUP BY t.business_name
    HAVING ct > 35) t;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE indicate_id = 104
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @last_week_end, 'w', 104, 'fjr_kpi2_area_product_satrate', @areas_w, @add_user
    ), (
      @last_month_start, 'm', 104, 'fjr_kpi2_area_product_satrate', @areas_m, @add_user
    );
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE indicate_id = 104
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 104 indicate_id, 'fjr_kpi2_area_product_satrate' indicate_name, COUNT(*) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_area_product_satrate t
  WHERE t.sdate = @last_week_end
    AND t.shelfs_sto >= .3 * t.shelfs_act
  GROUP BY t.business_name;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, t.business_name, 'm' indicate_type, 104 indicate_id, 'fjr_kpi2_area_product_satrate' indicate_name, COUNT(*) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_area_product_satrate t
  WHERE t.sdate = @last_month_end
    AND t.shelfs_sto >= .3 * t.shelfs_act
  GROUP BY t.business_name;
  CALL feods.sp_task_log (
    'sp_kpi2_area_product_satrate', @sdate, CONCAT(
      'fjr_d_48417191a20b7e9e8f0e9ed87259c0af', @timestamp, @add_user
    )
  );
  COMMIT;
END