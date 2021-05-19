CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_sto_val_rate`()
begin
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @d := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, b.business_name, b.region_name
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON b.city = t.city
  WHERE t.data_flag = 1
    AND t.shelf_status = 2
    AND t.shelf_type IN (1, 2, 3, 4, 5);
  DELETE
  FROM
    feods.fjr_kpi_sto_val_rate
  WHERE sdate = @sdate;
  set @sql_str := concat(
    "INSERT INTO feods.fjr_kpi_sto_val_rate ( sdate, region, business_area, product_id, sto_val, sto_val_flag5, sto_val_out, add_user ) SELECT @sdate sdate, s.region_name region, s.business_name business_area, t.product_id, SUM(t.sale_price * t.stock_quantity) sto_val, SUM( IF( t.sales_flag = 5 && (ISNULL(t.new_flag) || t.new_flag = 2), t.sale_price * t.stock_quantity, 0 ) ) sto_val_flag5, SUM( IF( ISNULL(pd.product_id), 0, t.sale_price * t.stock_quantity ) ) sto_val_out, @add_user add_user FROM feods.d_op_shelf_product_detail_combine", @d, " t JOIN feods.shelf_tmp s ON t.shelf_id = s.shelf_id LEFT JOIN feods.zs_product_dim_sserp pd ON t.product_id = pd.product_id AND pd.business_area = s.business_name AND pd.product_type = '淘汰（替补）' GROUP BY s.business_name, t.product_id HAVING sto_val != 0"
  );
  prepare sql_exe from @sql_str;
  execute sql_exe;
  CALL feods.sp_task_log (
    'sp_kpi_sto_val_rate', @sdate, CONCAT(
      'fjr_d_f57a5fe98567f37a36194420e9a1a5ed', @timestamp, @add_user
    )
  );
  commit;
end