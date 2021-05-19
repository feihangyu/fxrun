CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_stock`()
BEGIN
  #run after sh_process.sp_op_product_shelf_stat
   SET @sdate := subdate(current_date, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @sdate_lm := SUBDATE(@sdate, INTERVAL 1 MONTH);
  SET @y_m_lm := DATE_FORMAT(@sdate_lm, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@add_day, @d);
  SET @month_start_lm := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @last_day := LAST_DAY(@sdate);
  SET @d_m := DAY(@last_day);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, b.business_name
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND t.shelf_status = 2
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sp_tmp;
  CREATE TEMPORARY TABLE feods.sp_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    feods.d_op_product_shelf_sto_month
  WHERE month_id = @y_m
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.d_op_product_shelf_sto_month
  WHERE month_id = @y_m_lm
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.d_op_product_shelf_sal_month
  WHERE month_id = @y_m
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.d_op_product_shelf_sal_month
  WHERE month_id = @y_m_lm
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sp_sto_sal_tmp;
  CREATE TEMPORARY TABLE feods.sp_sto_sal_tmp (
    PRIMARY KEY (shelf_id, product_id), KEY (product_id)
  )
  SELECT
    t.shelf_id, t.product_id, sto.qty_end * d.sale_price sto_val, sto_lm.qty_end * d.sale_price sto_val_lm, sal.gmv gmv, sal_lm.gmv gmv_lm
  FROM
    feods.sp_tmp t
    JOIN fe.sf_shelf_product_detail d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
      AND d.data_flag = 1
    LEFT JOIN feods.d_op_product_shelf_sto_month sto
      ON t.shelf_id = sto.shelf_id
      AND t.product_id = sto.product_id
      AND sto.month_id = @y_m
    LEFT JOIN feods.d_op_product_shelf_sto_month sto_lm
      ON t.shelf_id = sto_lm.shelf_id
      AND t.product_id = sto_lm.product_id
      AND sto_lm.month_id = @y_m_lm
    LEFT JOIN feods.d_op_product_shelf_sal_month sal
      ON t.shelf_id = sal.shelf_id
      AND t.product_id = sal.product_id
      AND sal.month_id = @y_m
    LEFT JOIN feods.d_op_product_shelf_sal_month sal_lm
      ON t.shelf_id = sal_lm.shelf_id
      AND t.product_id = sal_lm.product_id
      AND sal_lm.month_id = @y_m_lm
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.s_sto_sal_tmp;
  CREATE TEMPORARY TABLE feods.s_sto_sal_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, s.business_name, SUM(t.sto_val) sto_val, SUM(t.sto_val_lm) sto_val_lm, SUM(t.gmv) gmv, SUM(t.gmv_lm) gmv_lm
  FROM
    feods.sp_sto_sal_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.p_sto_sal_tmp;
  CREATE TEMPORARY TABLE feods.p_sto_sal_tmp (
    PRIMARY KEY (product_id, business_name)
  )
  SELECT
    t.product_id, s.business_name, SUM(t.sto_val) sto_val, SUM(t.sto_val_lm) sto_val_lm, SUM(t.gmv) gmv, SUM(t.gmv_lm) gmv_lm
  FROM
    feods.sp_sto_sal_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
  GROUP BY t.product_id, s.business_name;
  DROP TEMPORARY TABLE IF EXISTS feods.sto_sal_tmp;
  CREATE TEMPORARY TABLE feods.sto_sal_tmp (PRIMARY KEY (business_name))
  SELECT
    t.business_name, SUM(t.sto_val) sto_val, SUM(t.sto_val_lm) sto_val_lm, SUM(t.gmv) gmv, SUM(t.gmv_lm) gmv_lm
  FROM
    feods.p_sto_sal_tmp t
  WHERE ! ISNULL(t.business_name)
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.d_op_stock_area
  WHERE sdate >= @month_start
    AND sdate < @add_day;
  INSERT INTO feods.d_op_stock_area (
    sdate, business_name, sto_val, gmv, sto_val_lm, gmv_lm, sto_val_budget, gmv_budget, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.sto_val, t.gmv, t.sto_val_lm, t.gmv_lm, f.stock_amount sto_val_budget, f.gmv * @d / @d_m gmv_budget, @add_user add_user
  FROM
    feods.sto_sal_tmp t
    LEFT JOIN feods.d_op_stock_forecast f
      ON t.business_name = f.business_area
      AND f.month_id = @y_m;
  DROP TEMPORARY TABLE IF EXISTS feods.area_factor_tmp;
  CREATE TEMPORARY TABLE feods.area_factor_tmp (PRIMARY KEY (business_name))
  SELECT
    t.business_name, t.sto_val_budget / t.sto_val sto_val_factor, t.gmv_budget / t.gmv gmv_factor
  FROM
    feods.d_op_stock_area t
  WHERE t.sdate = @sdate
    AND ! ISNULL(t.business_name);
  DELETE
  FROM
    feods.d_op_stock_shelf
  WHERE sdate >= @month_start
    AND sdate < @add_day;
  INSERT INTO feods.d_op_stock_shelf (
    sdate, business_name, shelf_id, sto_val, gmv, sto_val_lm, gmv_lm, sto_val_budget, gmv_budget, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_id, t.sto_val, t.gmv, t.sto_val_lm, t.gmv_lm, t.sto_val * f.sto_val_factor sto_val_budget, t.gmv * gmv_factor gmv_budget, @add_user add_user
  FROM
    feods.s_sto_sal_tmp t
    LEFT JOIN feods.area_factor_tmp f
      ON t.business_name = f.business_name;
  DELETE
  FROM
    feods.d_op_stock_product
  WHERE sdate >= @month_start
    AND sdate < @add_day;
  INSERT INTO feods.d_op_stock_product (
    sdate, business_name, product_id, sto_val, gmv, sto_val_lm, gmv_lm, sto_val_budget, gmv_budget, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.product_id, t.sto_val, t.gmv, t.sto_val_lm, t.gmv_lm, t.sto_val * f.sto_val_factor sto_val_budget, t.gmv * gmv_factor gmv_budget, @add_user add_user
  FROM
    feods.p_sto_sal_tmp t
    LEFT JOIN feods.area_factor_tmp f
      ON t.business_name = f.business_name;
  DROP TEMPORARY TABLE IF EXISTS feods.addgmv_tmp;
  CREATE TEMPORARY TABLE feods.addgmv_tmp (KEY (month_id, shelf_id))
  SELECT
    month_id, shelf_id, payment_money gmv
  FROM
    feods.fjr_shelf_mgmv
  WHERE month_id IN (@y_m, @y_m_lm)
    AND payment_money > 0;
  INSERT INTO feods.addgmv_tmp
  SELECT
    DATE_FORMAT(apply_time, '%Y-%m') month_id, supplier_id shelf_id, SUM(total_price) gmv
  FROM
    fe.sf_product_fill_order
  WHERE apply_time >= @month_start_lm
    AND apply_time < @add_day
    AND order_status = 11
    AND sales_bussniess_channel = 1
    AND sales_order_status = 3
    AND sales_audit_status = 2
    AND fill_type = 13
    AND total_price > 0
    AND ! ISNULL(supplier_id)
  GROUP BY month_id, supplier_id;
  INSERT INTO feods.addgmv_tmp
  SELECT
    DATE_FORMAT(t.paytime, '%Y-%m') month_id, t.shelf_id, SUM(oi.price * oi.product_count) gmv
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
  WHERE t.data_flag = 1
    AND t.paytime >= @month_start_lm
    AND t.paytime < @add_day
    AND t.pay_status = 1
    AND ! ISNULL(t.shelf_id)
  GROUP BY month_id, t.shelf_id
  HAVING gmv > 0;
  DROP TEMPORARY TABLE IF EXISTS feods.addgmv_shelf_tmp;
  CREATE TEMPORARY TABLE feods.addgmv_shelf_tmp (PRIMARY KEY (month_id, shelf_id))
  SELECT
    month_id, shelf_id, SUM(gmv) gmv
  FROM
    feods.addgmv_tmp
  GROUP BY month_id, shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.addgmv_area_tmp;
  CREATE TEMPORARY TABLE feods.addgmv_area_tmp (
    PRIMARY KEY (month_id, business_name)
  )
  SELECT
    t.month_id, s.business_name, SUM(gmv) gmv
  FROM
    feods.addgmv_shelf_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY t.month_id, s.business_name;
  UPDATE
    feods.d_op_stock_area t
    JOIN feods.addgmv_area_tmp a
      ON t.business_name = a.business_name
      AND a.month_id = @y_m SET t.gmv = IFNULL(t.gmv, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  UPDATE
    feods.d_op_stock_area t
    JOIN feods.addgmv_area_tmp a
      ON t.business_name = a.business_name
      AND a.month_id = @y_m_lm SET t.gmv_lm = IFNULL(t.gmv_lm, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  UPDATE
    feods.d_op_stock_shelf t
    JOIN feods.addgmv_shelf_tmp a
      ON t.shelf_id = a.shelf_id
      AND a.month_id = @y_m SET t.gmv = IFNULL(t.gmv, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  UPDATE
    feods.d_op_stock_shelf t
    JOIN feods.addgmv_shelf_tmp a
      ON t.shelf_id = a.shelf_id
      AND a.month_id = @y_m_lm SET t.gmv_lm = IFNULL(t.gmv_lm, 0) + IFNULL(a.gmv, 0)
  WHERE t.sdate = @sdate;
  CALL feods.sp_task_log (
    'sp_op_stock', @sdate, CONCAT(
      'yingnansong_d_3855143168284bcba21f749576467e4c', @timestamp, @add_user
    )
  );
  COMMIT;
END