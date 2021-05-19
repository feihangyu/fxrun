CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_abnormal_package_product_two`()
BEGIN
   SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, t.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all t 
  WHERE ! ISNULL(t.shelf_id);
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.pack_business_tmp;
  CREATE TEMPORARY TABLE fe_dm.pack_business_tmp (PRIMARY KEY (package_id)) AS
 SELECT DISTINCT
    t.package_id, IFNULL(
      b.business_name, bl.business_name
    ) business_name
  FROM
    fe_dwd.dwd_package_information t
    LEFT JOIN fe_dwd.`dwd_city_business` b
      ON t.city = b.city
    LEFT JOIN
      (SELECT
        t.package_id, MIN(s.business_name) business_name
      FROM
        fe_dwd.dwd_sf_shelf_package_detail t
        JOIN fe_dwd.dwd_package_information pa
          ON t.package_id = pa.package_id
          AND IFNULL(pa.city, '') = ''
        JOIN fe_dm.shelf_tmp s
          ON t.shelf_id = s.shelf_id
      WHERE t.data_flag = 1
        AND ! ISNULL(t.package_id)
      GROUP BY t.package_id) bl
      ON t.package_id = bl.package_id
  HAVING ! ISNULL(business_name);
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.item_product_tmp;
  CREATE TEMPORARY TABLE fe_dm.item_product_tmp (PRIMARY KEY (item_id)) AS
  SELECT
    t.item_id, d.product_id
  FROM
    fe_dwd.dwd_package_information t
    JOIN fe_dwd.dwd_sf_supplier_product_detail d
      ON t.relation_id = d.detail_id
  WHERE ! ISNULL(t.item_id);
	
	
	
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_tmp;
  CREATE TEMPORARY TABLE fe_dm.sto_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, COUNT(*) shelfs, COUNT(t.shelf_id) shelfs_flag5
  FROM
    fe_dwd.dwd_shelf_product_day_all t 
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.stock_quantity > 0
	AND t.sales_flag = 5
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
  GROUP BY s.business_name, t.product_id;
  
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dc_tmp;
  CREATE TEMPORARY TABLE fe_dm.dc_tmp (
    PRIMARY KEY (business_name, product_code2)
  ) AS
  SELECT
    b.business_area business_name, m.fnumber product_code2, SUM(t.fbaseqty) fbaseqty
  FROM
    fe_dwd.`dwd_sserp_t_stk_inventory` t
    JOIN fe_dwd.`dwd_sserp_t_bd_material` m
      ON t.fmaterialid = m.fmaterialid
    JOIN fe_dwd.`dwd_sserp_t_bd_stock` s
      ON t.fstockid = s.fstockid
    JOIN fe_dwd.`dwd_sserp_zs_dc_business_area` b
      ON s.fnumber = b.dc_code
  WHERE ! ISNULL(b.business_area)
    AND ! ISNULL(m.fnumber)
  GROUP BY b.business_area, m.fnumber;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.dim_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_area business_name, t.product_id, t.product_fe product_code2, t.product_type, sto.shelfs, sto.shelfs_flag5, dc.fbaseqty, p.second_type_id IN (1, 2) if_st12, p.fill_model
  FROM
    fe_dwd.dwd_pub_product_dim_sserp t
    LEFT JOIN fe_dm.sto_tmp sto
      ON t.business_area = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN fe_dm.dc_tmp dc
      ON t.business_area = dc.business_name
      AND t.product_fe = dc.product_code2
    LEFT JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_id = p.product_id
  WHERE t.product_type IN (
      '原有', '新增（试运行）'
    )
    AND ! ISNULL(t.business_area)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_area, t.product_id;
  
  
  DELETE
  FROM
    fe_dm.dm_op_abnormal_package
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_abnormal_package (
    sdate, package_id, business_name, skus, skus0, skus12, shelf_fill_flags, fill_quantity, aim_sku, aim_quantity, if_sku, if_quantity, add_user
  )
  SELECT
    @sdate sdate, t.package_id, t.business_name, t.skus, t.skus0, t.skus12, t.shelf_fill_flags, t.quantity,
    CASE
      t.package_type_id
      WHEN 6
      THEN IF(t.pname_flag, t.skus, t.skus0)
      WHEN 7
      THEN t.skus12
      WHEN 3
      THEN t.skus
      WHEN 4
      THEN t.skus
      WHEN 5
      THEN t.skus
      ELSE 0
    END aim_sku,
    CASE
      t.package_type_id
      WHEN 6
      THEN IF(t.pname_flag, 250, 180)
      WHEN 7
      THEN 150
      WHEN 3
      THEN 360
      WHEN 4
      THEN 500
      WHEN 5
      THEN 650
      ELSE 0
    END aim_quantity,
    CASE
      t.package_type_id
      WHEN 6
      THEN t.shelf_fill_flags < IF(t.pname_flag, t.skus, t.skus0)
      WHEN 7
      THEN t.shelf_fill_flags < t.skus12
      WHEN 3
      THEN t.shelf_fill_flags < t.skus
      WHEN 4
      THEN t.shelf_fill_flags < t.skus
      WHEN 5
      THEN t.shelf_fill_flags < t.skus
      ELSE 0
    END if_sku,
    CASE
      t.package_type_id
      WHEN 6
      THEN t.quantity < IF(t.pname_flag, 250, 180)
      WHEN 7
      THEN t.quantity < 150
      WHEN 3
      THEN t.quantity < 360
      WHEN 4
      THEN t.quantity < 500
      WHEN 5
      THEN t.quantity < 650
      ELSE 0
    END if_quantity, @add_user add_user
  FROM
    (SELECT
      bl.business_name, t.package_id, t.package_name LIKE '%高配%' || t.package_name LIKE '%初始%' pname_flag, t.package_type_id, COUNT(d.product_id) skus,
	  IFNULL(SUM(d.product_type = '原有'), 0) skus0, IFNULL(SUM(d.if_st12), 0) skus12, IFNULL(SUM(t.shelf_fill_flag = 1), 0) shelf_fill_flags, IFNULL(
        SUM(
          (t.shelf_fill_flag = 1) * t.quantity
        ), 0
      ) quantity
    FROM
		fe_dwd.dwd_package_information t
      LEFT JOIN fe_dm.item_product_tmp ip
        ON t.item_id = ip.item_id
      LEFT JOIN fe_dm.pack_business_tmp bl
        ON t.package_id = bl.package_id
      LEFT JOIN fe_dm.dim_tmp d
        ON bl.business_name = d.business_name
        AND ip.product_id = d.product_id
    WHERE t.statu_flag = 1
      AND t.package_type_id NOT IN (8, 9)
    GROUP BY t.package_id
	) t;
	
	
  DELETE
  FROM
    fe_dm.dm_op_abnormal_package_product
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_abnormal_package_product (
    sdate, package_id, business_name, product_id, shelf_fill_flag, quantity, if_refill, if_unfill, if_quantity, if_upquantity, sto_qty_dc, product_type, shelfs_flag5, shelfs_sto, pquantity, add_user
  )
  SELECT
    @sdate sdate, t.package_id, bl.business_name, ip.product_id, t.shelf_fill_flag, t.quantity, IFNULL(
      d.fbaseqty > 200 && t.shelf_fill_flag = 2, 0
    ) if_refill, IFNULL(d.shelfs_flag5 > .8 * d.shelfs, 0) if_unfill, IFNULL(
      t.shelf_fill_flag = 1 && t.quantity < d.fill_model, 0
    ) if_quantity, IFNULL(
      t.shelf_fill_flag = 1 && t.quantity = 1, 0
    ) if_upquantity, IFNULL(d.fbaseqty, 0) fbaseqty, d.product_type, IFNULL(d.shelfs_flag5, 0) shelfs_flag5, IFNULL(d.shelfs, 0) shelfs, IFNULL(d.fill_model, 0) fill_model, @add_user add_user
  FROM
    fe_dwd.dwd_package_information t
    JOIN fe_dm.item_product_tmp ip
      ON t.item_id = ip.item_id
    LEFT JOIN fe_dm.pack_business_tmp bl
      ON t.package_id = bl.package_id
    LEFT JOIN fe_dm.dim_tmp d
      ON bl.business_name = d.business_name
      AND ip.product_id = d.product_id
  WHERE  t.statu_flag = 1
    AND t.package_type_id NOT IN (8, 9);
	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_abnormal_package_product_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_package','dm_op_abnormal_package_product_two','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_package_product','dm_op_abnormal_package_product_two','李世龙');
COMMIT;
    END