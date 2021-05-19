CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sf_fillorder_requirement_his`()
BEGIN
  SET @sdate := CURRENT_DATE,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  set @d := DAY(@sdate);
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sf_fillorder_requirement_his TRUNCATE PARTITION d",
    @d
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  INSERT INTO feods.d_op_sf_fillorder_requirement_his (
    sday,
    requirement_id,
    shelf_id,
    supplier_id,
    supplier_type,
    supplier_name,
    suggest_fill_num,
    total_price,
    weight,
    stock_ration,
    turn_rate,
    category_add_num,
    category_out_num,
    add_user
  )
  SELECT
    @d sday,
    requirement_id,
    shelf_id,
    supplier_id,
    supplier_type,
    supplier_name,
    suggest_fill_num,
    total_price,
    weight,
    stock_ration,
    turn_rate,
    category_add_num,
    category_out_num,
    @add_user add_user
  FROM
    fe.sf_fillorder_requirement
  WHERE data_flag = 1;
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sf_fillorder_requirement_item_his TRUNCATE PARTITION d",
    @d
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  INSERT INTO feods.d_op_sf_fillorder_requirement_item_his (
    sday,
    requirement_item_id,
    requirement_id,
    detail_id,
    shelf_id,
    product_id,
    purchase_price,
    onshelf_stock,
    onway_stock,
    max_quantity,
    week_sale_num,
    suggest_fill_num,
    actual_apply_num,
    weight,
    add_user
  )
  SELECT
    @d sday,
    requirement_item_id,
    requirement_id,
    detail_id,
    shelf_id,
    product_id,
    purchase_price,
    onshelf_stock,
    onway_stock,
    max_quantity,
    week_sale_num,
    suggest_fill_num,
    actual_apply_num,
    weight,
    @add_user add_user
  FROM
    fe.sf_fillorder_requirement_item
  WHERE data_flag = 1;
  CALL feods.sp_task_log (
    'sp_op_sf_fillorder_requirement_his',
    @sdate,
    CONCAT(
      'yingnansong_d_2f89b15ef1095195e06c24a63a389bf9',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END