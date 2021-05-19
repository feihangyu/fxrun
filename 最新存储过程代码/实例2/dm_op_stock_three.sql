CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_stock_three`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @sdate_lm := SUBDATE(@sdate, INTERVAL 1 MONTH);
  SET @y_m_lm := DATE_FORMAT(@sdate_lm, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@add_day, @d);
  SET @month_start_lm := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @last_day := LAST_DAY(@sdate);
  SET @d_m := DAY(@last_day);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, t.business_name
  FROM
    fe_dwd.`dwd_shelf_base_day_all` t
  WHERE t.shelf_status = 2
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.sp_tmp;
  CREATE TEMPORARY TABLE fe_dwd.sp_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.`dm_op_product_shelf_sto_month`
  WHERE month_id = @y_m
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.`dm_op_product_shelf_sto_month`
  WHERE month_id = @y_m_lm
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.`dm_op_product_shelf_sal_month`
  WHERE month_id = @y_m
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.`dm_op_product_shelf_sal_month`
  WHERE month_id = @y_m_lm
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.sp_sto_sal_tmp;
  CREATE TEMPORARY TABLE fe_dwd.sp_sto_sal_tmp (
    PRIMARY KEY (shelf_id, product_id), KEY (product_id)
  )
  SELECT
    t.shelf_id, 
    t.product_id, 
    sto.qty_end AS sto_qty,
    sto.qty_end * d.sale_price AS sto_val, 
    sto_lm.qty_end AS sto_qty_lm,
    sto_lm.qty_end * d.sale_price AS sto_val_lm, 
    sal.gmv AS gmv, 
    sal_lm.gmv AS gmv_lm
  FROM
    fe_dwd.sp_tmp t
    JOIN fe_dwd.`dwd_shelf_product_day_all` d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
    LEFT JOIN fe_dm.`dm_op_product_shelf_sto_month` sto
      ON t.shelf_id = sto.shelf_id
      AND t.product_id = sto.product_id
      AND sto.month_id = @y_m
    LEFT JOIN fe_dm.`dm_op_product_shelf_sto_month` sto_lm
      ON t.shelf_id = sto_lm.shelf_id
      AND t.product_id = sto_lm.product_id
      AND sto_lm.month_id = @y_m_lm
    LEFT JOIN fe_dm.`dm_op_product_shelf_sal_month` sal
      ON t.shelf_id = sal.shelf_id
      AND t.product_id = sal.product_id
      AND sal.month_id = @y_m
    LEFT JOIN fe_dm.`dm_op_product_shelf_sal_month` sal_lm
      ON t.shelf_id = sal_lm.shelf_id
      AND t.product_id = sal_lm.product_id
      AND sal_lm.month_id = @y_m_lm
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.s_sto_sal_tmp;
  CREATE TEMPORARY TABLE fe_dwd.s_sto_sal_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, s.business_name, SUM(t.sto_val) sto_val, SUM(t.sto_val_lm) sto_val_lm, SUM(t.gmv) gmv, SUM(t.gmv_lm) gmv_lm
  FROM
    fe_dwd.sp_sto_sal_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.p_sto_sal_tmp;
  CREATE TEMPORARY TABLE fe_dwd.p_sto_sal_tmp (
    PRIMARY KEY (product_id, business_name)
  )
  SELECT
    t.product_id, s.business_name, SUM(t.sto_qty) AS sto_qty,SUM(t.sto_val) sto_val, SUM(t.sto_qty_lm) AS sto_qty_lm,SUM(t.sto_val_lm) sto_val_lm, SUM(t.gmv) gmv, SUM(t.gmv_lm) gmv_lm
  FROM
    fe_dwd.sp_sto_sal_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
  GROUP BY t.product_id, s.business_name;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.sto_sal_tmp;
  CREATE TEMPORARY TABLE fe_dwd.sto_sal_tmp (PRIMARY KEY (business_name))
  SELECT
    t.business_name, SUM(t.sto_val) sto_val, SUM(t.sto_val_lm) sto_val_lm, SUM(t.gmv) gmv, SUM(t.gmv_lm) gmv_lm
  FROM
    fe_dwd.p_sto_sal_tmp t
  WHERE ! ISNULL(t.business_name)
  GROUP BY t.business_name;
  DELETE
  FROM
    fe_dm.dm_op_stock_area
  WHERE sdate >= @month_start
    AND sdate < @add_day;
	
  INSERT INTO  fe_dm.dm_op_stock_area (
    sdate, business_name, sto_val, gmv, sto_val_lm, gmv_lm, sto_val_budget, gmv_budget, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.sto_val, t.gmv, t.sto_val_lm, t.gmv_lm, f.stock_amount sto_val_budget, f.gmv * @d / @d_m gmv_budget, @add_user add_user
  FROM
    fe_dwd.sto_sal_tmp t
    LEFT JOIN fe_dm.dm_op_stock_forecast_insert f
      ON t.business_name = f.business_area
      AND f.month_id = @y_m;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_factor_tmp;
  CREATE TEMPORARY TABLE fe_dwd.area_factor_tmp (PRIMARY KEY (business_name))
  SELECT
    t.business_name, t.sto_val_budget / t.sto_val sto_val_factor, t.gmv_budget / t.gmv gmv_factor
  FROM
    fe_dm.dm_op_stock_area t
  WHERE t.sdate = @sdate
    AND ! ISNULL(t.business_name);
  DELETE
  FROM
    fe_dm.dm_op_stock_shelf
  WHERE sdate >= @month_start
    AND sdate < @add_day;
  INSERT INTO fe_dm.dm_op_stock_shelf (
    sdate, business_name, shelf_id, sto_val, gmv, sto_val_lm, gmv_lm, sto_val_budget, gmv_budget, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_id, t.sto_val, t.gmv, t.sto_val_lm, t.gmv_lm, t.sto_val * f.sto_val_factor sto_val_budget, t.gmv * gmv_factor gmv_budget, @add_user add_user
  FROM
    fe_dwd.s_sto_sal_tmp t
    LEFT JOIN fe_dwd.area_factor_tmp f
      ON t.business_name = f.business_name;
  DELETE
  FROM
    fe_dm.dm_op_stock_product
  WHERE sdate >= @month_start
    AND sdate < @add_day;
  INSERT INTO fe_dm.dm_op_stock_product (
    sdate, business_name, product_id, sto_qty,sto_val, gmv, sto_qty_lm,sto_val_lm, gmv_lm, sto_val_budget, gmv_budget, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.product_id, t.sto_qty,t.sto_val, t.gmv, t.sto_qty_lm,t.sto_val_lm, t.gmv_lm, t.sto_val * f.sto_val_factor sto_val_budget, t.gmv * gmv_factor gmv_budget, @add_user add_user
  FROM
    fe_dwd.p_sto_sal_tmp t
    LEFT JOIN fe_dwd.area_factor_tmp f
      ON t.business_name = f.business_name;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.addgmv_shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.addgmv_shelf_tmp (PRIMARY KEY (month_id, shelf_id))
  SELECT
    month_id, shelf_id, SUM(gmv) gmv
  FROM
    fe_dm.`dm_shelf_add_mgmv`
  GROUP BY month_id, shelf_id;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.addgmv_area_tmp;
  CREATE TEMPORARY TABLE fe_dwd.addgmv_area_tmp (
    PRIMARY KEY (month_id, business_name)
  )
  SELECT
    t.month_id, s.business_name, SUM(gmv) gmv
  FROM
    fe_dwd.addgmv_shelf_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY t.month_id, s.business_name;
  UPDATE
    fe_dm.dm_op_stock_area t
    JOIN fe_dwd.addgmv_area_tmp a
      ON t.business_name = a.business_name
      AND a.month_id = @y_m SET t.gmv = IFNULL(t.gmv, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  UPDATE
    fe_dm.dm_op_stock_area t
    JOIN fe_dwd.addgmv_area_tmp a
      ON t.business_name = a.business_name
      AND a.month_id = @y_m_lm SET t.gmv_lm = IFNULL(t.gmv_lm, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  UPDATE
    fe_dm.dm_op_stock_shelf t
    JOIN fe_dwd.addgmv_shelf_tmp a
      ON t.shelf_id = a.shelf_id
      AND a.month_id = @y_m SET t.gmv = IFNULL(t.gmv, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  UPDATE
    fe_dm.dm_op_stock_shelf t
    JOIN fe_dwd.addgmv_shelf_tmp a
      ON t.shelf_id = a.shelf_id
      AND a.month_id = @y_m_lm SET t.gmv_lm = IFNULL(t.gmv_lm, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_stock_three',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_stock_area','dm_op_stock_three','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_stock_product','dm_op_stock_three','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_stock_shelf','dm_op_stock_three','宋英南');
END