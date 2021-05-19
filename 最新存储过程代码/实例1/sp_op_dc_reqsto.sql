CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_dc_reqsto`()
BEGIN
  #run after sh_process.sp_erp_stock_daily
#run after sh_process.sp_op_sp_avgsal30
#run after sh_process.sh_outstock_day
#run after sh_process.sp_prewarehouse_stock_detail
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @sub_day := SUBDATE(@sdate, 1);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, pw.warehouse_id, b.business_name
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN fe.sf_prewarehouse_shelf_detail pw
      ON t.shelf_id = pw.shelf_id
      AND pw.data_flag = 1
  WHERE t.data_flag = 1
    AND t.shelf_type != 4
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.supplier_tmp;
  CREATE TEMPORARY TABLE feods.supplier_tmp (
    PRIMARY KEY (supplier_id), KEY (business_name), KEY (depot_code)
  )
  SELECT
    t.supplier_id, t.depot_code, b.business_area business_name
  FROM
    fe.sf_supplier t
    LEFT JOIN sserp.ZS_DC_BUSINESS_AREA b
      ON t.depot_code = b.dc_code
  WHERE t.data_flag = 1
    AND t.status = 2
    AND t.supplier_type = 2
    AND ! ISNULL(t.supplier_id);
  DROP TEMPORARY TABLE IF EXISTS feods.req_dc_tmp;
  CREATE TEMPORARY TABLE feods.req_dc_tmp (
    PRIMARY KEY (product_id, business_name)
  )
  SELECT
    s.business_name, t.product_id, SUM(t.qty_sal30 / t.days_sal_sto30) qty_req_dc
  FROM
    feods.d_op_sp_avgsal30 t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.qty_sal30 > 0
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY s.business_name, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.out_tmp;
  CREATE TEMPORARY TABLE feods.out_tmp (
    PRIMARY KEY (depot_code, product_code2)
  )
  SELECT
    t.warehouse_number depot_code, t.product_bar product_code2, t.fbaseqty
  FROM
    feods.PJ_OUTSTOCK2_DAY t
  WHERE t.fproducedate = @sub_day
    AND t.fbaseqty > 0
    AND ! ISNULL(t.warehouse_number)
    AND ! ISNULL(t.product_bar);
  DROP TEMPORARY TABLE IF EXISTS feods.product_tmp;
  CREATE TEMPORARY TABLE feods.product_tmp (
    product_code2 VARCHAR (100) CHARSET utf8mb4, product_id INT, PRIMARY KEY (product_code2)
  )
  SELECT
    product_code2, product_id
  FROM
    fe.sf_product
  WHERE data_flag = 1
    AND product_code2 != ''
    AND ! ISNULL(product_code2);
  DROP TEMPORARY TABLE IF EXISTS feods.qty_dc_tmp;
  CREATE TEMPORARY TABLE feods.qty_dc_tmp (
    PRIMARY KEY (product_id, supplier_id)
  )
  SELECT
    sup.business_name, sup.depot_code, sup.supplier_id, p.product_id, t.fbaseqty
  FROM
    feods.out_tmp t
    JOIN feods.supplier_tmp sup
      ON t.depot_code = sup.depot_code
    JOIN feods.product_tmp p
      ON t.product_code2 = p.product_code2
  WHERE ! ISNULL(sup.supplier_id)
    AND ! ISNULL(p.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.main_dc_tmp;
  CREATE TEMPORARY TABLE feods.main_dc_tmp (
    PRIMARY KEY (product_id, supplier_id)
  )
  SELECT
    product_id, supplier_id
  FROM
    feods.qty_dc_tmp
  UNION
  SELECT
    t.product_id, sup.supplier_id
  FROM
    feods.req_dc_tmp t
    JOIN feods.supplier_tmp sup
      ON t.business_name = sup.business_name;
  DELETE
  FROM
    feods.d_op_dc_reqsto
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_dc_reqsto (
    sdate, business_name, product_id, supplier_id, depot_code, qty_sto, qty_req, add_user
  )
  SELECT
    @sdate sdate, sup.business_name, t.product_id, t.supplier_id, sup.depot_code, IFNULL(dc.fbaseqty, 0) qty_sto, IFNULL(req.qty_req_dc, 0) qty_req, @add_user add_user
  FROM
    feods.main_dc_tmp t
    JOIN feods.supplier_tmp sup
      ON t.supplier_id = sup.supplier_id
    LEFT JOIN feods.qty_dc_tmp dc
      ON t.product_id = dc.product_id
      AND t.supplier_id = dc.supplier_id
    LEFT JOIN feods.req_dc_tmp req
      ON t.product_id = req.product_id
      AND req.business_name = sup.business_name;
  DROP TEMPORARY TABLE IF EXISTS feods.req_pwh_tmp;
  CREATE TEMPORARY TABLE feods.req_pwh_tmp (
    PRIMARY KEY (warehouse_id, product_id)
  )
  SELECT
    s.warehouse_id, t.product_id, SUM(t.qty_sal30 / t.days_sal_sto30) qty_req_pwh
  FROM
    feods.d_op_sp_avgsal30 t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.warehouse_id)
  WHERE t.qty_sal30 > 0
    AND ! ISNULL(s.warehouse_id)
    AND ! ISNULL(t.product_id)
  GROUP BY s.warehouse_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.qty_pwh_tmp;
  CREATE TEMPORARY TABLE feods.qty_pwh_tmp (
    PRIMARY KEY (warehouse_id, product_id)
  )
  SELECT
    t.warehouse_id, t.product_id, t.available_stock
  FROM
    feods.pj_prewarehouse_stock_detail t
  WHERE t.check_date = @sub_day
    AND t.available_stock > 0
    AND ! ISNULL(t.warehouse_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.main_pwh_tmp;
  CREATE TEMPORARY TABLE feods.main_pwh_tmp (
    PRIMARY KEY (warehouse_id, product_id)
  )
  SELECT
    warehouse_id, product_id
  FROM
    feods.qty_pwh_tmp
  UNION
  SELECT
    warehouse_id, product_id
  FROM
    feods.req_pwh_tmp t;
  DELETE
  FROM
    feods.d_op_pwh_reqsto
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_pwh_reqsto (
    sdate, business_name, product_id, warehouse_id, qty_sto, qty_req, add_user
  )
  SELECT
    @sdate sdate, s.business_name, t.product_id, t.warehouse_id, IFNULL(q.available_stock, 0) qty_sto, IFNULL(req.qty_req_pwh, 0) qty_req, @add_user add_user
  FROM
    feods.main_pwh_tmp t
    JOIN feods.shelf_tmp s
      ON t.warehouse_id = s.shelf_id
    LEFT JOIN feods.qty_pwh_tmp q
      ON t.product_id = q.product_id
      AND t.warehouse_id = q.warehouse_id
    LEFT JOIN feods.req_pwh_tmp req
      ON t.product_id = req.product_id
      AND t.warehouse_id = req.warehouse_id;
  CALL feods.sp_task_log (
    'sp_op_dc_reqsto', @sdate, CONCAT(
      'yingnansong_d_76798abd022c11e455c558a4943f746f', @timestamp, @add_user
    )
  );
  COMMIT;
END