CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_abnormal_package`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, b.business_name
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.pack_business_tmp;
  CREATE TEMPORARY TABLE feods.pack_business_tmp (PRIMARY KEY (package_id)) AS
  SELECT
    t.package_id, IFNULL(
      b.business_name, bl.business_name
    ) business_name
  FROM
    fe.sf_package t
    LEFT JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN
      (SELECT
        t.package_id, MIN(s.business_name) business_name
      FROM
        fe.sf_shelf_package_detail t
        JOIN fe.sf_package pa
          ON t.package_id = pa.package_id
          AND pa.data_flag = 1
          AND IFNULL(pa.city, '') = ''
        JOIN feods.shelf_tmp s
          ON t.shelf_id = s.shelf_id
      WHERE t.data_flag = 1
        AND ! ISNULL(t.package_id)
      GROUP BY t.package_id) bl
      ON t.package_id = bl.package_id
  HAVING ! ISNULL(business_name);
  DROP TEMPORARY TABLE IF EXISTS feods.item_product_tmp;
  CREATE TEMPORARY TABLE feods.item_product_tmp (PRIMARY KEY (item_id)) AS
  SELECT
    t.item_id, d.product_id
  FROM
    fe.sf_package_item t
    JOIN fe.sf_supplier_product_detail d
      ON t.relation_id = d.detail_id
      AND t.data_flag = 1
  WHERE d.data_flag = 1
    AND ! ISNULL(t.item_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  CREATE TEMPORARY TABLE feods.sto_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, COUNT(*) shelfs, COUNT(f.shelf_id) shelfs_flag5
  FROM
    fe.sf_shelf_product_detail t
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id
      AND f.data_flag = 1
      AND f.sales_flag = 5
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.stock_quantity > 0
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
  GROUP BY s.business_name, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.dc_tmp;
  CREATE TEMPORARY TABLE feods.dc_tmp (
    PRIMARY KEY (business_name, product_code2)
  ) AS
  SELECT
    b.business_area business_name, m.fnumber product_code2, SUM(t.fbaseqty) fbaseqty
  FROM
    sserp.T_STK_INVENTORY t
    JOIN sserp.T_BD_MATERIAL m
      ON t.fmaterialid = m.fmaterialid
    JOIN sserp.T_BD_STOCK s
      ON t.fstockid = s.fstockid
    JOIN sserp.ZS_DC_BUSINESS_AREA b
      ON s.fnumber = b.dc_code
  WHERE ! ISNULL(b.business_area)
    AND ! ISNULL(m.fnumber)
  GROUP BY b.business_area, m.fnumber;
  DROP TEMPORARY TABLE IF EXISTS feods.dim_tmp;
  CREATE TEMPORARY TABLE feods.dim_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_area business_name, t.product_id, t.product_fe product_code2, t.product_type, sto.shelfs, sto.shelfs_flag5, dc.fbaseqty, p.second_type_id IN (1, 2) if_st12, p.fill_model
  FROM
    feods.zs_product_dim_sserp t
    LEFT JOIN feods.sto_tmp sto
      ON t.business_area = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN feods.dc_tmp dc
      ON t.business_area = dc.business_name
      AND t.product_fe = dc.product_code2
    LEFT JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
  WHERE t.product_type IN (
      '原有', '新增（试运行）'
    )
    AND ! ISNULL(t.business_area)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_area, t.product_id;
  DELETE
  FROM
    feods.fjr_abnormal_package
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_abnormal_package (
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
      bl.business_name, t.package_id, t.package_name LIKE '%高配%' || t.package_name LIKE '%初始%' pname_flag, t.package_type_id, COUNT(d.product_id) skus, IFNULL(SUM(d.product_type = '原有'), 0) skus0, IFNULL(SUM(d.if_st12), 0) skus12, IFNULL(SUM(pm.shelf_fill_flag = 1), 0) shelf_fill_flags, IFNULL(
        SUM(
          (pm.shelf_fill_flag = 1) * pm.quantity
        ), 0
      ) quantity
    FROM
      fe.sf_package t
      JOIN fe.sf_package_item pm
        ON t.package_id = pm.package_id
        AND pm.data_flag = 1
      LEFT JOIN feods.item_product_tmp ip
        ON pm.item_id = ip.item_id
      LEFT JOIN fe.sf_package_type pt
        ON t.package_type_id = pt.package_type_id
        AND pt.data_flag = 1
      LEFT JOIN feods.pack_business_tmp bl
        ON t.package_id = bl.package_id
      LEFT JOIN feods.dim_tmp d
        ON bl.business_name = d.business_name
        AND ip.product_id = d.product_id
    WHERE t.data_flag = 1
      AND t.statu_flag = 1
      AND t.package_type_id NOT IN (8, 9)
    GROUP BY t.package_id) t;
  DELETE
  FROM
    feods.fjr_abnormal_package_product
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_abnormal_package_product (
    sdate, package_id, business_name, product_id, shelf_fill_flag, quantity, if_refill, if_unfill, if_quantity, if_upquantity, sto_qty_dc, product_type, shelfs_flag5, shelfs_sto, pquantity, add_user
  )
  SELECT
    @sdate sdate, t.package_id, bl.business_name, ip.product_id, pm.shelf_fill_flag, pm.quantity, IFNULL(
      d.fbaseqty > 200 && pm.shelf_fill_flag = 2, 0
    ) if_refill, IFNULL(d.shelfs_flag5 > .8 * d.shelfs, 0) if_unfill, IFNULL(
      pm.shelf_fill_flag = 1 && pm.quantity < d.fill_model, 0
    ) if_quantity, IFNULL(
      pm.shelf_fill_flag = 1 && pm.quantity = 1, 0
    ) if_upquantity, IFNULL(d.fbaseqty, 0) fbaseqty, d.product_type, IFNULL(d.shelfs_flag5, 0) shelfs_flag5, IFNULL(d.shelfs, 0) shelfs, IFNULL(d.fill_model, 0) fill_model, @add_user add_user
  FROM
    fe.sf_package t
    JOIN fe.sf_package_item pm
      ON t.package_id = pm.package_id
      AND pm.data_flag = 1
    JOIN feods.item_product_tmp ip
      ON pm.item_id = ip.item_id
    LEFT JOIN feods.pack_business_tmp bl
      ON t.package_id = bl.package_id
    LEFT JOIN feods.dim_tmp d
      ON bl.business_name = d.business_name
      AND ip.product_id = d.product_id
  WHERE t.data_flag = 1
    AND t.statu_flag = 1
    AND t.package_type_id NOT IN (8, 9);
  CALL feods.sp_task_log (
    'sp_abnormal_package', @sdate, CONCAT(
      'fjr_d_c61267b15d92e51447c62aaf10f18f30', @timestamp, @add_user
    )
  );
  COMMIT;
END