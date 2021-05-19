CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_np_flag5_sto`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    fe_dm.dm_op_kpi_np_flag5_sto  
  WHERE sdate = @sdate;
  
  INSERT INTO fe_dm.dm_op_kpi_np_flag5_sto (
    sdate, region, business_area, product_id, product_fe, product_name, sales_flag, stoqty, stoval, add_user
  )
  SELECT
    @sdate, b.region_name, b.business_name, d.product_id, pd.product_fe, pd.product_name,
	IFNULL(d.sales_flag, 0), IFNULL(SUM(d.stock_quantity), 0) stock_quantity, IFNULL(
      SUM(d.stock_quantity * d.sale_price), 0
    ) sto_val, @add_user
  FROM
  fe_dwd.dwd_shelf_product_day_all d  
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON d.shelf_id = s.shelf_id
      AND s.shelf_status = 2
      AND s.shelf_type IN (1, 2, 3, 4, 5)
      AND s.activate_time < SUBDATE(@sdate, 14)
    JOIN fe_dwd.dwd_city_business b
      ON s.city = b.city
    JOIN fe_dwd.dwd_pub_product_dim_sserp pd  
      ON b.business_name = pd.business_area
      AND d.product_id = pd.product_id
      AND pd.product_type = '新增（试运行）'
  WHERE d.new_flag = 2
  GROUP BY b.business_name, d.product_id, IFNULL(d.sales_flag, 0);
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi_np_flag5_sto',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_np_flag5_sto','dm_op_kpi_np_flag5_sto','李世龙');
COMMIT;
    END