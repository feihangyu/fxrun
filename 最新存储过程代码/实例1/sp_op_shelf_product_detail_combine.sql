CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_product_detail_combine`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  set @d := DAY(@sdate), @table := 'feods.d_op_shelf_product_detail_combine';
  set @table_new := CONCAT(@table, @d);
  set @sql_str := CONCAT("truncate ", @table_new);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO ", @table_new, " ( detail_id, item_id, product_id, shelf_id, max_quantity, alarm_quantity, stock_quantity, sale_price, purchase_price, shelf_fill_flag, package_flag, near_date, production_date, risk_source, danger_flag, first_fill_time, sales_flag, new_flag, near_days, sales_status, manager_fill_flag, near_date_source_flag, operate_sale_reason, business_status, allow_fill_status, operate_fill_status, operate_fill_reason, allow_sale_status, operate_sale_status, add_user ) SELECT t.detail_id, t.item_id, t.product_id, t.shelf_id, t.max_quantity, t.alarm_quantity, t.stock_quantity, t.sale_price, t.purchase_price, t.shelf_fill_flag, t.package_flag, f.near_date, f.production_date, f.risk_source, f.danger_flag, f.first_fill_time, f.sales_flag, f.new_flag, f.near_days, f.sales_status, f.manager_fill_flag, f.near_date_source_flag, f.operate_sale_reason, f.business_status, f.allow_fill_status, f.operate_fill_status, f.operate_fill_reason, f.allow_sale_status, f.operate_sale_status, @add_user add_user FROM fe.sf_shelf_product_detail t left JOIN fe.sf_shelf_product_detail_flag f ON t.detail_id = f.detail_id AND f.data_flag = 1 WHERE t.data_flag = 1"
    ) INTO @sql_str;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_op_shelf_product_detail_combine', @sdate, CONCAT(
      'fjr_d_b9f2e47bac89339ac42437d9ac001f8d', @timestamp, @add_user
    )
  );
  COMMIT;
END