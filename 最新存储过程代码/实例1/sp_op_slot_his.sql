CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_slot_his`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, b.business_name, p.package_id, m.machine_id
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN fe.sf_shelf_package_detail p
      ON t.shelf_id = p.shelf_id
      AND p.data_flag = 1
    LEFT JOIN fe.sf_shelf_machine m
      ON m.shelf_id = t.shelf_id
      AND m.data_flag = 1
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.second_tmp;
  CREATE TEMPORARY TABLE feods.second_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, dl.product_id, dl.stock_num
  FROM
    fe.sf_shelf_machine_second t
    JOIN fe.sf_shelf_machine_second_detail dl
      ON t.machine_second_id = dl.machine_second_id
      AND dl.data_flag = 1
      AND dl.stock_num != 0
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(dl.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.slot_tmp;
  CREATE TEMPORARY TABLE feods.slot_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.stock_num) stock_num, COUNT(*) slots, SUM(st.slot_capacity_limit) slot_capacity_limit, SUM(t.stock_num > 0) slots_sto
  FROM
    fe.sf_shelf_machine_slot t
    LEFT JOIN fe.sf_shelf_machine_slot_type st
      ON t.slot_type_id = st.slot_type_id
      AND st.data_flag = 1
  WHERE t.data_flag = 1
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.change_tmp;
  CREATE TEMPORARY TABLE feods.change_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT DISTINCT
    s.shelf_id, t.product_id
  FROM
    fe.sf_shelf_machine_product_change t
    JOIN feods.shelf_tmp s
      ON t.machine_id = s.machine_id
  WHERE t.data_flag = 1
    AND ! ISNULL(s.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.detail_tmp;
  CREATE TEMPORARY TABLE feods.detail_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, t.shelf_fill_flag, t.item_id, t.stock_quantity, t.sale_price, f.sales_flag, t.max_quantity
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.detail_id = f.detail_id
      AND f.data_flag = 1
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.main_tmp;
  CREATE TEMPORARY TABLE feods.main_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    feods.detail_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.slot_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.second_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.change_tmp;
  TRUNCATE feods.d_op_s7p_detail;
  INSERT INTO feods.d_op_s7p_detail (
    shelf_id, product_id, max_quantity, sales_flag, package_id, base_pack_flag, product_type_flag, shelf_fill_flag_pack, shelf_fill_flag, slots, slots_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, sale_price, add_user
  )
  SELECT
    t.shelf_id, t.product_id, IFNULL(d.max_quantity, 0) max_quantity, d.sales_flag, pm.package_id, IFNULL(s.package_id = pm.package_id, 0) base_pack_flag, ! ISNULL(dim.p_id) product_type_flag, IFNULL(pm.shelf_fill_flag = 1, 0) shelf_fill_flag_pack, IFNULL(d.shelf_fill_flag = 1, 0) shelf_fill_flag, IFNULL(sl.slots, 0) slots, IFNULL(sl.slots_sto, 0) slots_sto, IFNULL(d.stock_quantity, 0) stock_num, IFNULL(sl.slot_capacity_limit, 0) slot_capacity_limit, IFNULL(sl.stock_num, 0) stock_num_slot, IFNULL(se.stock_num, 0) stock_num_second, IFNULL(d.sale_price, 0) sale_price, @add_user add_user
  FROM
    feods.main_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.detail_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
    LEFT JOIN fe.sf_package_item pm
      ON d.item_id = pm.item_id
      AND pm.data_flag = 1
    LEFT JOIN feods.second_tmp se
      ON t.shelf_id = se.shelf_id
      AND t.product_id = se.product_id
    LEFT JOIN feods.slot_tmp sl
      ON t.shelf_id = sl.shelf_id
      AND t.product_id = sl.product_id
    LEFT JOIN feods.zs_product_dim_sserp dim
      ON t.product_id = dim.product_id
      AND s.business_name = dim.business_area
      AND dim.product_type IN (
        '原有', '新增（试运行）', '个性化商品'
      );
  DROP TEMPORARY TABLE IF EXISTS feods.slot_res_tmp;
  CREATE TEMPORARY TABLE feods.slot_res_tmp (
    PRIMARY KEY (slot_id), KEY (shelf_id, product_id)
  )
  SELECT
    t.slot_id, t.shelf_id, t.manufacturer_slot_code, t.slot_status, t.stock_num, ISNULL(ch.product_id) || ch.product_id = t.product_id same_product_flag, t.product_id, d.sales_flag, d.sale_price, IFNULL(d.base_pack_flag, 0) base_pack_flag, IFNULL(d.product_type_flag, 0) product_type_flag, IFNULL(d.shelf_fill_flag_pack, 0) shelf_fill_flag_pack, IFNULL(d.shelf_fill_flag, 0) shelf_fill_flag, IFNULL(ch.product_id, t.product_id) cproduct_id, IFNULL(dc.sales_flag, d.sales_flag) csales_flag, IFNULL(dc.sale_price, d.sale_price) csale_price, COALESCE(
      dc.base_pack_flag, d.base_pack_flag, 0
    ) cbase_pack_flag, COALESCE(
      dc.product_type_flag, d.product_type_flag, 0
    ) cproduct_type_flag, COALESCE(
      dc.shelf_fill_flag_pack, d.shelf_fill_flag_pack, 0
    ) cshelf_fill_flag_pack, COALESCE(
      dc.shelf_fill_flag, d.shelf_fill_flag, 0
    ) cshelf_fill_flag
  FROM
    fe.sf_shelf_machine_slot t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.d_op_s7p_detail d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
    LEFT JOIN fe.sf_shelf_machine_product_change ch
      ON t.slot_id = ch.slot_id
      AND ch.data_flag = 1
    LEFT JOIN feods.d_op_s7p_detail dc
      ON t.shelf_id = dc.shelf_id
      AND ch.product_id = dc.product_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.slot_id);
  DROP TEMPORARY TABLE IF EXISTS feods.same_product_tmp;
  CREATE TEMPORARY TABLE feods.same_product_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, COUNT(*) slots, SUM(t.same_product_flag) same_product_flags
  FROM
    feods.slot_res_tmp t
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id
  HAVING slots > 1
    AND same_product_flags > 0;
  DELETE
  FROM
    feods.d_op_slot_his
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_slot_his (
    sdate, slot_id, shelf_id, manufacturer_slot_code, slot_status, stock_num, same_product_flag, product_id, sales_flag, sale_price, base_pack_flag, product_type_flag, shelf_fill_flag_pack, shelf_fill_flag, cproduct_id, csales_flag, csale_price, cbase_pack_flag, cproduct_type_flag, cshelf_fill_flag_pack, cshelf_fill_flag, other_sta_slot_flag, concode, add_user
  )
  SELECT
    @sdate sdate, t.slot_id, t.shelf_id, t.manufacturer_slot_code, t.slot_status, t.stock_num, t.same_product_flag, t.product_id, t.sales_flag, t.sale_price, t.base_pack_flag, t.product_type_flag, t.shelf_fill_flag_pack, t.shelf_fill_flag, t.cproduct_id, t.csales_flag, t.csale_price, t.cbase_pack_flag, t.cproduct_type_flag, t.cshelf_fill_flag_pack, t.cshelf_fill_flag, ! ISNULL(sa.shelf_id) other_sta_slot_flag, CONCAT(
      t.same_product_flag, t.product_type_flag, t.shelf_fill_flag, t.cproduct_type_flag, t.cshelf_fill_flag, ! ISNULL(sa.shelf_id)
    ) concode, @add_user add_user
  FROM
    feods.slot_res_tmp t
    LEFT JOIN feods.same_product_tmp sa
      ON t.shelf_id = sa.shelf_id
      AND t.product_id = sa.product_id;
  DELETE
  FROM
    feods.d_op_s7p_nslot
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_s7p_nslot (
    sdate, shelf_id, product_id, base_pack_flag, sale_price, stock_num, stock_num_second, shelf_fill_flag_pack, shelf_fill_flag, product_type_flag, coutput, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, t.product_id, t.base_pack_flag, t.sale_price, t.stock_num, t.stock_num_second, t.shelf_fill_flag_pack, t.shelf_fill_flag, t.product_type_flag,
    CASE
      WHEN t.base_pack_flag && t.shelf_fill_flag && t.product_type_flag
      THEN '变更配置（提供售卖货道）或调走'
      WHEN t.base_pack_flag && t.shelf_fill_flag
      THEN '停止补货，调回前置仓或无人货架'
      ELSE '调回前置仓或无人货架'
    END coutput, @add_user add_user
  FROM
    feods.d_op_s7p_detail t
    LEFT JOIN feods.change_tmp c
      ON t.shelf_id = c.shelf_id
      AND t.product_id = c.product_id
  WHERE t.stock_num > 0
    AND t.slots = 0
    AND ISNULL(c.shelf_id);
  CALL feods.sp_task_log (
    'sp_op_slot_his', @sdate, CONCAT(
      'fjr_d_5698895aa305eb38a49decd70c4f4b04', @timestamp, @add_user
    )
  );
  COMMIT;
END