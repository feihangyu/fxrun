CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_sale_vs_stock_week`(in_week_end DATE)
BEGIN
  SET @week_end := SUBDATE(
    in_week_end,
    DAYOFWEEK(in_week_end) - 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  
  SET @add_day := ADDDATE(@week_end, 1),
  @week_start := SUBDATE(@week_end, 6),
  @week_d2 := SUBDATE(@week_end, 5),
  @week_d3 := SUBDATE(@week_end, 4),
  @week_d4 := SUBDATE(@week_end, 3),
  @week_d5 := SUBDATE(@week_end, 2),
  @week_d6 := SUBDATE(@week_end, 1);
  SET @y_m1 := DATE_FORMAT(@week_start, '%Y-%m'),
  @y_m2 := DATE_FORMAT(@week_d2, '%Y-%m'),
  @y_m3 := DATE_FORMAT(@week_d3, '%Y-%m'),
  @y_m4 := DATE_FORMAT(@week_d4, '%Y-%m'),
  @y_m5 := DATE_FORMAT(@week_d5, '%Y-%m'),
  @y_m6 := DATE_FORMAT(@week_d6, '%Y-%m'),
  @y_m7 := DATE_FORMAT(@week_end, '%Y-%m');
  SET @d1 := DAY(@week_start),
  @d2 := DAY(@week_d2),
  @d3 := DAY(@week_d3),
  @d4 := DAY(@week_d4),
  @d5 := DAY(@week_d5),
  @d6 := DAY(@week_d6),
  @d7 := DAY(@week_end);
  
  SET @time_1 := CURRENT_TIMESTAMP();
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp AS
  SELECT
    s.shelf_id,
    b.business_name
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1;
  CREATE INDEX idx_shelf_id
  ON feods.shelf_tmp (shelf_id);
  
  SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_1--@time_2",@time_1,@time_2);
  DROP TEMPORARY TABLE IF EXISTS feods.sal_tmp;
  CREATE TEMPORARY TABLE feods.sal_tmp AS
  SELECT
    s.business_name,
    oi.product_id,
    COUNT(DISTINCT t.shelf_id) shelfs_sal
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.order_status = 2
    AND t.order_date >= @week_start
    AND t.order_date < @add_day
  GROUP BY s.business_name,
    oi.product_id;
	
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_2--@time_3",@time_2,@time_3);
	
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  SET @str_create := " create temporary table feods.sto_tmp as ";
  SET @str_body1 := CONCAT(
    " select ",
    "   t.shelf_id, ",
    "   t.product_id ",
    " from ",
    "   fe.sf_shelf_product_stock_detail t ",
    " where t.stat_date = '"
  );
  SET @str_body2 := "' and t.day";
  SET @str_body3 := "_quantity > 0 ";
  SET @str_union := " union ";
  SET @str := CONCAT(
    @str_create,
    @str_body1,
    @y_m1,
    @str_body2,
    @d1,
    @str_body3,
    @str_union,
    @str_body1,
    @y_m2,
    @str_body2,
    @d2,
    @str_body3,
    @str_union,
    @str_body1,
    @y_m3,
    @str_body2,
    @d3,
    @str_body3,
    @str_union,
    @str_body1,
    @y_m4,
    @str_body2,
    @d4,
    @str_body3,
    @str_union,
    @str_body1,
    @y_m5,
    @str_body2,
    @d5,
    @str_body3,
    @str_union,
    @str_body1,
    @y_m6,
    @str_body2,
    @d6,
    @str_body3,
    @str_union,
    @str_body1,
    @y_m7,
    @str_body2,
    @d7,
    @str_body3
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_shelf_id
  ON feods.sto_tmp (shelf_id);
  
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_3--@time_4",@time_3,@time_4);
  DROP TEMPORARY TABLE IF EXISTS feods.sto_stat_tmp;
  CREATE TEMPORARY TABLE feods.sto_stat_tmp AS
  SELECT
    s.business_name,
    t.product_id,
    COUNT(*) shelfs_sto
  FROM
    feods.sto_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY s.business_name,
    t.product_id;
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_4--@time_5",@time_4,@time_5);	
	
  DELETE
  FROM
    feods.fjr_kpi2_sale_vs_stock_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi2_sale_vs_stock_week (
    week_end,
    business_name,
    product_id,
    shelfs_sal,
    shelfs_sto,
    add_user
  )
  SELECT
    @week_end week_end,
    t.business_name,
    t.product_id,
    SUM(t.shelfs_sal) shelfs_sal,
    SUM(t.shelfs_sto) shelfs_sto,
    @add_user add_user
  FROM
    (SELECT
      t.business_name,
      t.product_id,
      t.shelfs_sal,
      0 shelfs_sto
    FROM
      feods.sal_tmp t
    UNION
    ALL
    SELECT
      t.business_name,
      t.product_id,
      0 shelfs_sal,
      t.shelfs_sto
    FROM
      feods.sto_stat_tmp t) t
  GROUP BY t.business_name,
    t.product_id;
	
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_5--@time_6",@time_5,@time_6);	
	
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 103;
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @week_end sdate,
    'w' indicate_type,
    103 indicate_id,
    'fjr_kpi2_sale_vs_stock_week' indicate_name,
    ROUND(
      SUM(t.shelfs_sal) / SUM(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  FROM
    feods.fjr_kpi2_sale_vs_stock_week t
  WHERE t.week_end = @week_end;
  
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_6--@time_7",@time_6,@time_7);  
  
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 103;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate,
    business_name,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @week_end sdate,
    t.business_name,
    'w' indicate_type,
    103 indicate_id,
    'fjr_kpi2_sale_vs_stock_week' indicate_name,
    ROUND(
      SUM(t.shelfs_sal) / SUM(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  FROM
    feods.fjr_kpi2_sale_vs_stock_week t
  WHERE t.week_end = @week_end
  GROUP BY business_name;
  
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_7--@time_8",@time_7,@time_8); 
  
  CALL feods.sp_task_log (
    'sp_kpi2_sale_vs_stock_week',
    @week_end,
    CONCAT(
      'fjr_w_907f1d0d9841f43d3c67d51121d634f5',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END