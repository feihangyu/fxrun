CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_pwh_reqsto_two`()
BEGIN
   SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
   
  SET @sub_day := SUBDATE(@sdate, 1);
  SET @pre_day_30 := SUBDATE(@sub_day,30);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
  
  CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, pw.prewarehouse_id AS warehouse_id, t.business_name
  FROM
    fe_dwd.`dwd_shelf_base_day_all` t
    LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` pw
      ON t.shelf_id = pw.shelf_id
  WHERE t.shelf_type != 4
    AND ! ISNULL(t.shelf_id);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.supplier_tmp;
  CREATE TEMPORARY TABLE fe_dwd.supplier_tmp (
    PRIMARY KEY (supplier_id), KEY (business_name), KEY (depot_code)
  )
  SELECT
    t.supplier_id, t.depot_code, b.business_area business_name
  FROM
    fe_dwd.dwd_sf_supplier t
    LEFT JOIN fe_dwd.dwd_sserp_zs_dc_business_area b
      ON t.depot_code = b.dc_code
  WHERE t.status = 2
    AND t.supplier_type = 2
    AND ! ISNULL(t.supplier_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_op_sp_avgsal30;
  CREATE TEMPORARY TABLE fe_dwd.d_op_sp_avgsal30(
    PRIMARY KEY (shelf_id, product_id)
  )
SELECT
        shelf_id,
        product_id,
        sal_qty_day30 AS qty_sal30,
        stock_sal_day30 AS days_sal_sto30
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_day30`
;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.req_dc_tmp;
  CREATE TEMPORARY TABLE fe_dwd.req_dc_tmp (
    PRIMARY KEY (product_id, business_name)
  )
  SELECT
    s.business_name, t.product_id, SUM(t.qty_sal30 / t.days_sal_sto30) qty_req_dc
  FROM
    fe_dwd.d_op_sp_avgsal30 t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.qty_sal30 > 0
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY s.business_name, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.out_tmp;
  CREATE TEMPORARY TABLE fe_dwd.out_tmp (
    PRIMARY KEY (depot_code, product_code2)
  )
  SELECT
    t.warehouse_number depot_code, t.product_bar product_code2, t.fbaseqty
  FROM
    fe_dwd.dwd_pj_outstock2_day t
  WHERE t.fproducedate = @sub_day
    AND t.fbaseqty > 0
    AND ! ISNULL(t.warehouse_number)
    AND ! ISNULL(t.product_bar);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.product_tmp;
  CREATE TEMPORARY TABLE fe_dwd.product_tmp (
    product_code2 VARCHAR (100) CHARSET utf8mb4, product_id INT, PRIMARY KEY (product_code2)
  )
  SELECT
    product_code2, product_id
  FROM
    fe_dwd.`dwd_product_base_day_all`
  WHERE product_code2 != ''
    AND ! ISNULL(product_code2);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.qty_dc_tmp;
  CREATE TEMPORARY TABLE fe_dwd.qty_dc_tmp (
    PRIMARY KEY (product_id, supplier_id)
  )
  SELECT
    sup.business_name, sup.depot_code, sup.supplier_id, p.product_id, t.fbaseqty
  FROM
    fe_dwd.out_tmp t
    JOIN fe_dwd.supplier_tmp sup
      ON t.depot_code = sup.depot_code
    JOIN fe_dwd.product_tmp p
      ON t.product_code2 = p.product_code2
  WHERE ! ISNULL(sup.supplier_id)
    AND ! ISNULL(p.product_id);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.main_dc_tmp;
  CREATE TEMPORARY TABLE fe_dwd.main_dc_tmp (
    PRIMARY KEY (product_id, supplier_id)
  )
  SELECT
    product_id, supplier_id
  FROM
    fe_dwd.qty_dc_tmp
  UNION
  SELECT
    t.product_id, sup.supplier_id
  FROM
    fe_dwd.req_dc_tmp t
    JOIN fe_dwd.supplier_tmp sup
      ON t.business_name = sup.business_name;
	  
  DELETE
  FROM
    fe_dm.`dm_op_dc_reqsto`
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.`dm_op_dc_reqsto` (
    sdate, business_name, product_id, supplier_id, depot_code, qty_sto, qty_req, add_user
  )
  SELECT
    @sdate sdate, sup.business_name, t.product_id, t.supplier_id, sup.depot_code, IFNULL(dc.fbaseqty, 0) qty_sto, IFNULL(req.qty_req_dc, 0) qty_req, @add_user add_user
  FROM
    fe_dwd.main_dc_tmp t
    JOIN fe_dwd.supplier_tmp sup
      ON t.supplier_id = sup.supplier_id
    LEFT JOIN fe_dwd.qty_dc_tmp dc
      ON t.product_id = dc.product_id
      AND t.supplier_id = dc.supplier_id
    LEFT JOIN fe_dwd.req_dc_tmp req
      ON t.product_id = req.product_id
      AND req.business_name = sup.business_name;
	  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.req_pwh_tmp;
  CREATE TEMPORARY TABLE fe_dwd.req_pwh_tmp (
    PRIMARY KEY (warehouse_id, product_id)
  )
  SELECT
    s.warehouse_id, t.product_id, SUM(t.qty_sal30 / t.days_sal_sto30) qty_req_pwh
  FROM
    fe_dwd.d_op_sp_avgsal30 t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.warehouse_id)
  WHERE t.qty_sal30 > 0
    AND ! ISNULL(s.warehouse_id)
    AND ! ISNULL(t.product_id)
  GROUP BY s.warehouse_id, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.qty_pwh_tmp;
  CREATE TEMPORARY TABLE fe_dwd.qty_pwh_tmp (
    PRIMARY KEY (warehouse_id, product_id)
  )
  SELECT
    t.warehouse_id, t.product_id, t.available_stock
  FROM
    fe_dwd.dwd_sf_prewarehouse_stock_detail t
  WHERE t.available_stock > 0
    AND ! ISNULL(t.warehouse_id)
    AND ! ISNULL(t.product_id);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.main_pwh_tmp;
  CREATE TEMPORARY TABLE fe_dwd.main_pwh_tmp (
    PRIMARY KEY (warehouse_id, product_id)
  )
  SELECT
    warehouse_id, product_id
  FROM
    fe_dwd.qty_pwh_tmp
  UNION
  SELECT
    warehouse_id, product_id
  FROM
    fe_dwd.req_pwh_tmp t;
	
  DELETE
  FROM
    fe_dm.`dm_op_pwh_reqsto`
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.`dm_op_pwh_reqsto` (
    sdate, business_name, product_id, warehouse_id, qty_sto, qty_req, add_user
  )
  SELECT
    @sdate sdate, s.business_name, t.product_id, t.warehouse_id, IFNULL(q.available_stock, 0) qty_sto, IFNULL(req.qty_req_pwh, 0) qty_req, @add_user add_user
  FROM
    fe_dwd.main_pwh_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.warehouse_id = s.shelf_id
    LEFT JOIN fe_dwd.qty_pwh_tmp q
      ON t.product_id = q.product_id
      AND t.warehouse_id = q.warehouse_id
    LEFT JOIN fe_dwd.req_pwh_tmp req
      ON t.product_id = req.product_id
      AND t.warehouse_id = req.warehouse_id;
	  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_pwh_reqsto_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_dc_reqsto','dm_op_pwh_reqsto_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_pwh_reqsto','dm_op_pwh_reqsto_two','宋英南');
  COMMIT;
END