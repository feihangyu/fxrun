CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_area_product_satis_rate`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @areas := 0, @areas_w := 0, @areas_m := 0;
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1), @last_month_end := SUBDATE(@sdate, DAY(@sdate));
  set @last_month_start := subdate(
    @last_month_end, day(@last_month_end) - 1
  );
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.dim_tmp AS
  SELECT
    t.version version_id, t.business_area business_name, t.product_id
  FROM
    fe_dwd.dwd_pub_product_dim_sserp t
  WHERE t.product_type IN (
      '原有', '新增（正式运行）'
    );
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp AS
  SELECT
    t.shelf_id, t.business_name, t.shelf_status
  FROM
    fe_dwd.dwd_shelf_base_day_all  t;
  
  
  CREATE INDEX idx_shelf_id
  ON fe_dm.shelf_tmp (shelf_id);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_tmp;
  CREATE TEMPORARY TABLE fe_dm.sto_tmp AS
  SELECT
    d.business_name, d.product_id, COUNT(*) shelfs_sto
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN fe_dm.dim_tmp d
      ON t.product_id = d.product_id
      AND s.business_name = d.business_name
  WHERE t.stock_quantity > 0
  GROUP BY d.business_name, d.product_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.act_tmp;
  CREATE TEMPORARY TABLE fe_dm.act_tmp AS
  SELECT
    s.business_name, COUNT(*) shelfs_act
  FROM
    fe_dm.shelf_tmp s
  WHERE s.shelf_status = 2
  GROUP BY s.business_name;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_area_product_satis_rate
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_kpi2_area_product_satis_rate (
    sdate, version_id, business_name, product_id, shelfs_sto, shelfs_act, add_user
  )
  SELECT
    @sdate sdate, t.version_id, t.business_name, t.product_id, IFNULL(sto.shelfs_sto, 0) shelfs_sto, IFNULL(act.shelfs_act, 0) shelfs_act, @add_user add_user
  FROM
    fe_dm.dim_tmp t
    LEFT JOIN fe_dm.sto_tmp sto
      ON t.business_name = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN fe_dm.act_tmp act
      ON t.business_name = act.business_name;
  SELECT
    @areas := COUNT(*)
  FROM
    fe_dm.act_tmp t;
  SELECT
    @areas_w := round(COUNT(*) / @areas, 6)
  FROM
    (SELECT
      t.business_name, COUNT(*) ct
    FROM
      fe_dm.dm_op_kpi2_area_product_satis_rate t
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
      fe_dm.dm_op_kpi2_area_product_satis_rate t
    WHERE t.sdate = @last_month_end
      AND t.shelfs_sto >= .3 * t.shelfs_act
    GROUP BY t.business_name
    HAVING ct > 35) t;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE indicate_id = 104
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @last_week_end, 'w', 104, 'dm_op_kpi2_area_product_satis_rate', @areas_w, @add_user
    ), (
      @last_month_start, 'm', 104, 'dm_op_kpi2_area_product_satis_rate', @areas_m, @add_user
    );
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE indicate_id = 104
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 104 indicate_id, 'dm_op_kpi2_area_product_satis_rate' indicate_name, COUNT(*) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_area_product_satis_rate t
  WHERE t.sdate = @last_week_end
    AND t.shelfs_sto >= .3 * t.shelfs_act
  GROUP BY t.business_name;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, t.business_name, 'm' indicate_type, 104 indicate_id, 'dm_op_kpi2_area_product_satis_rate' indicate_name, COUNT(*) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_area_product_satis_rate t
  WHERE t.sdate = @last_month_end
    AND t.shelfs_sto >= .3 * t.shelfs_act
  GROUP BY t.business_name;
  
  
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_area_product_satis_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_area_product_satis_rate','dm_op_kpi2_area_product_satis_rate','李世龙');
COMMIT;
    END