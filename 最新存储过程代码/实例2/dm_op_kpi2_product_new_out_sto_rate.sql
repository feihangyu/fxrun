CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_product_new_out_sto_rate`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1), @last_month_end := SUBDATE(@sdate, DAY(@sdate));
  SET @last_month_start := SUBDATE(
    @last_month_end, DAY(@last_month_end) - 1
  );
  SELECT
    @version_id := MAX(t.version)
  FROM
    fe_dwd.dwd_pub_product_dim_sserp t;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.dim_tmp AS
  SELECT
    t.business_area business_name, t.product_id, t.product_type IN (
      '新增（试运行）', '新增（免费货）'
    ) if_new
  FROM
    fe_dwd.dwd_pub_product_dim_sserp t
  WHERE t.product_type IN (
      '停补', '停补（替补）', '新增（试运行）', '新增（免费货）', '淘汰', '淘汰（替补）', '退出'
    );
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.dim_tmp (business_name, product_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp AS
SELECT
    t.shelf_id, t.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all  t
  ;
  
  CREATE INDEX idx_shelf_id
  ON fe_dm.shelf_tmp (shelf_id);
  DELETE
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate
  WHERE sdate = @sdate;
  
  
  INSERT INTO fe_dm.dm_op_kpi2_product_new_out_sto_rate (
    sdate, version_id, business_name, product_id, if_new, if_out, qty_sto, val_sto, add_user
  )
  SELECT
    @sdate sdate, @version_id version_id, s.business_name, t.product_id,
	IFNULL(d.if_new, 0) if_new, IFNULL(d.if_new = 0, 0) if_out, SUM(t.stock_quantity) qty_sto,
	SUM(t.stock_quantity * t.sale_price) val_sto, @add_user
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dm.dim_tmp d
      ON t.product_id = d.product_id
      AND s.business_name = d.business_name
  GROUP BY s.business_name, t.product_id;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE indicate_id IN (107, 108)
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 
	107 indicate_id, 'dm_op_kpi2_product_new_out_sto_rate' indicate_name, ROUND(
      SUM(IF(t.if_new, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate t
  WHERE t.sdate = @last_week_end
  GROUP BY t.business_name;
  
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 
	108 indicate_id, 'dm_op_kpi2_product_new_out_sto_rate' indicate_name, ROUND(
      SUM(IF(t.if_out, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate t
  WHERE t.sdate = @last_week_end
  GROUP BY t.business_name;
  
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, t.business_name, 'm' indicate_type, 
	107 indicate_id, 'dm_op_kpi2_product_new_out_sto_rate' indicate_name, ROUND(
      SUM(IF(t.if_new, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate t
  WHERE t.sdate = @last_month_end
  GROUP BY t.business_name;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_month_start sdate, t.business_name, 'm' indicate_type, 108 indicate_id, 'dm_op_kpi2_product_new_out_sto_rate' indicate_name, ROUND(
      SUM(IF(t.if_out, t.val_sto, 0)) / SUM(t.val_sto), 6
    ) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate t
  WHERE t.sdate = @last_month_end
  GROUP BY t.business_name;
  
  
  SELECT
    @np_w := SUM(IF(t.if_new, t.val_sto, 0)), @out_w := SUM(IF(t.if_out, t.val_sto, 0)), @tot_w := SUM(t.val_sto)
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate t
  WHERE t.sdate = @last_week_end;
  
  
  SELECT
    @np_m := SUM(IF(t.if_new, t.val_sto, 0)), @out_m := SUM(IF(t.if_out, t.val_sto, 0)), @tot_m := SUM(t.val_sto)
  FROM
    fe_dm.dm_op_kpi2_product_new_out_sto_rate t
  WHERE t.sdate = @last_month_end;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE indicate_id IN (107, 108)
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @last_week_end, 'w', 107, 'dm_op_kpi2_product_new_out_sto_rate', @np_w / @tot_w, @add_user
    ), (
      @last_month_start, 'm', 107, 'dm_op_kpi2_product_new_out_sto_rate', @np_m / @tot_m, @add_user
    ), (
      @last_week_end, 'w', 108, 'dm_op_kpi2_product_new_out_sto_rate', @out_w / @tot_w, @add_user
    ), (
      @last_month_start, 'm', 108, 'dm_op_kpi2_product_new_out_sto_rate', @out_m / @tot_m, @add_user
    );
  
  
  
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_product_new_out_sto_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_product_new_out_sto_rate','dm_op_kpi2_product_new_out_sto_rate','李世龙');
COMMIT;
    END