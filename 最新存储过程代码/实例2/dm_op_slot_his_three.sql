CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_slot_his_three`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, t.business_name, p.package_id, m.machine_id
  FROM
    fe_dwd.dwd_shelf_base_day_all t
    LEFT JOIN fe_dwd.dwd_sf_shelf_package_detail p
      ON t.shelf_id = p.shelf_id
    LEFT JOIN fe_dwd.dwd_shelf_machine_info m
      ON m.shelf_id = t.shelf_id
  WHERE  t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.second_tmp;
  CREATE TEMPORARY TABLE fe_dm.second_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, t.stock_num
  FROM
  fe_dwd.dwd_shelf_machine_second_info t
  WHERE t.stock_num != 0
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
	
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.slot_tmp;
  CREATE TEMPORARY TABLE fe_dm.slot_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.stock_num) stock_num, COUNT(*) slots, 
	SUM(t.slot_capacity_limit) slot_capacity_limit, SUM(t.stock_num > 0) slots_sto
  FROM
    fe_dwd.dwd_shelf_machine_slot_type t
  WHERE ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id, t.product_id;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.change_tmp;
  CREATE TEMPORARY TABLE fe_dm.change_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT DISTINCT
    s.shelf_id, t.product_id
  FROM
    fe_dwd.dwd_sf_shelf_machine_product_change t
    JOIN fe_dm.shelf_tmp s
      ON t.machine_id = s.machine_id
  WHERE t.data_flag = 1
    AND ! ISNULL(s.shelf_id)
    AND ! ISNULL(t.product_id);
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.detail_tmp;
  CREATE TEMPORARY TABLE fe_dm.detail_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, t.shelf_fill_flag, t.item_id, 
	t.stock_quantity, t.sale_price, t.sales_flag, t.max_quantity
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE  ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.main_tmp;
  CREATE TEMPORARY TABLE fe_dm.main_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.detail_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.slot_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.second_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.change_tmp;
	
  TRUNCATE fe_dm.dm_op_s7p_detail;  
  INSERT INTO fe_dm.dm_op_s7p_detail (
    shelf_id, product_id, max_quantity, sales_flag, package_id, base_pack_flag, product_type_flag, shelf_fill_flag_pack, shelf_fill_flag, slots, slots_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, sale_price, add_user
  )
  SELECT
    t.shelf_id, t.product_id, IFNULL(d.max_quantity, 0) max_quantity, d.sales_flag, pm.package_id, IFNULL(s.package_id = pm.package_id, 0) base_pack_flag, ! ISNULL(dim.p_id) product_type_flag, IFNULL(pm.shelf_fill_flag = 1, 0) shelf_fill_flag_pack, IFNULL(d.shelf_fill_flag = 1, 0) shelf_fill_flag, IFNULL(sl.slots, 0) slots, IFNULL(sl.slots_sto, 0) slots_sto, IFNULL(d.stock_quantity, 0) stock_num, IFNULL(sl.slot_capacity_limit, 0) slot_capacity_limit, IFNULL(sl.stock_num, 0) stock_num_slot, IFNULL(se.stock_num, 0) stock_num_second, IFNULL(d.sale_price, 0) sale_price, @add_user add_user
  FROM
    fe_dm.main_tmp t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dm.detail_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
    LEFT JOIN fe_dwd.dwd_package_information pm
      ON d.item_id = pm.item_id
    LEFT JOIN fe_dm.second_tmp se
      ON t.shelf_id = se.shelf_id
      AND t.product_id = se.product_id
    LEFT JOIN fe_dm.slot_tmp sl
      ON t.shelf_id = sl.shelf_id
      AND t.product_id = sl.product_id
    LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp dim
      ON t.product_id = dim.product_id
      AND s.business_name = dim.business_area
      AND dim.product_type IN (
        '原有', '新增（试运行）', '个性化商品'
      );
	  
	  
      
  DROP TEMPORARY TABLE IF EXISTS fe_dm.slot_res_tmp;
  CREATE TEMPORARY TABLE fe_dm.slot_res_tmp (
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
    fe_dwd.dwd_shelf_machine_slot_type t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dm.dm_op_s7p_detail d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
    LEFT JOIN fe_dwd.dwd_sf_shelf_machine_product_change ch
      ON t.slot_id = ch.slot_id
    LEFT JOIN fe_dm.dm_op_s7p_detail dc
      ON t.shelf_id = dc.shelf_id
      AND ch.product_id = dc.product_id
  WHERE ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.slot_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.same_product_tmp;
  CREATE TEMPORARY TABLE fe_dm.same_product_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, COUNT(*) slots, SUM(t.same_product_flag) same_product_flags
  FROM
    fe_dm.slot_res_tmp t
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id
  HAVING slots > 1
    AND same_product_flags > 0;
	
  DELETE
  FROM
    fe_dm.dm_op_slot_his
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_slot_his (
    sdate, slot_id, shelf_id, manufacturer_slot_code, slot_status, stock_num, same_product_flag, product_id, sales_flag, sale_price, base_pack_flag, product_type_flag, shelf_fill_flag_pack, shelf_fill_flag, cproduct_id, csales_flag, csale_price, cbase_pack_flag, cproduct_type_flag, cshelf_fill_flag_pack, cshelf_fill_flag, other_sta_slot_flag, concode, add_user
  )
  SELECT
    @sdate sdate, t.slot_id, t.shelf_id, t.manufacturer_slot_code, t.slot_status, t.stock_num, t.same_product_flag, t.product_id, t.sales_flag, t.sale_price, t.base_pack_flag, t.product_type_flag, t.shelf_fill_flag_pack, t.shelf_fill_flag, t.cproduct_id, t.csales_flag, t.csale_price, t.cbase_pack_flag, t.cproduct_type_flag, t.cshelf_fill_flag_pack, t.cshelf_fill_flag, ! ISNULL(sa.shelf_id) other_sta_slot_flag, CONCAT(
      t.same_product_flag, t.product_type_flag, t.shelf_fill_flag, t.cproduct_type_flag, t.cshelf_fill_flag, ! ISNULL(sa.shelf_id)
    ) concode, @add_user add_user
  FROM
    fe_dm.slot_res_tmp t
    LEFT JOIN fe_dm.same_product_tmp sa
      ON t.shelf_id = sa.shelf_id
      AND t.product_id = sa.product_id;
	  
  DELETE
  FROM
    fe_dm.dm_op_s7p_nslot
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_s7p_nslot (
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
    fe_dm.dm_op_s7p_detail t
    LEFT JOIN fe_dm.change_tmp c
      ON t.shelf_id = c.shelf_id
      AND t.product_id = c.product_id
  WHERE t.stock_num > 0
    AND t.slots = 0
    AND ISNULL(c.shelf_id);
	
	
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_slot_his_three',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_s7p_detail','dm_op_slot_his_three','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_slot_his','dm_op_slot_his_three','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_s7p_nslot','dm_op_slot_his_three','李世龙');
COMMIT;
    END