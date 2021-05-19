CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_flags_area_product_week`(in_sdate DATE)
BEGIN
  #run after sh_process.sp_op_order_and_item
   SET @sdate := in_sdate, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1);
  SET @add_day := ADDDATE(@week_end, 1);
  SET @week_start := SUBDATE(@week_end, 6);
  SET @last_week_end := SUBDATE(@week_end, 7);
  SET @last_fill_day := SUBDATE(@week_end, 6+14);
  SET @y_m_add := DATE_FORMAT(@add_day, '%Y-%m');
  SET @y_m_start := DATE_FORMAT(@week_start, '%Y-%m');
  
SET @time_1 := CURRENT_TIMESTAMP();
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  SET @sqlstr := CONCAT(
    "CREATE TEMPORARY TABLE feods.sto_tmp(PRIMARY KEY (shelf_id, product_id)) AS select t.shelf_id,t.product_id,t.day", DAY(@add_day), "_quantity sto_qty_e from fe.sf_shelf_product_stock_detail t where t.stat_date = @y_m_add AND !ISNULL(t.shelf_id) AND !ISNULL(t.product_id) and t.day", DAY(@add_day), "_quantity>0 ;"
  );
  PREPARE sqlstr FROM @sqlstr;
  EXECUTE sqlstr;
  
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_1--@time_2",@time_1,@time_2);
  DROP TEMPORARY TABLE IF EXISTS feods.ssto_tmp;
  SET @sqlstr := CONCAT(
    "CREATE TEMPORARY TABLE feods.ssto_tmp(PRIMARY KEY (shelf_id, product_id)) AS select t.shelf_id,t.product_id,t.day", DAY(@week_start), "_quantity sto_qty_s from fe.sf_shelf_product_stock_detail t where t.stat_date = @y_m_start AND !ISNULL(t.shelf_id) AND !ISNULL(t.product_id) and t.day", DAY(@week_start), "_quantity>0 ;"
  );
  PREPARE sqlstr FROM @sqlstr;
  EXECUTE sqlstr;
  
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_2--@time_3",@time_2,@time_3);
  DROP TEMPORARY TABLE IF EXISTS feods.flag_tmp;
  CREATE TEMPORARY TABLE feods.flag_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id, t.product_id, SUM(t.sales_flag_s) sales_flag_s, SUM(t.sales_flag_e) sales_flag_e
  FROM
    (SELECT
      t.shelf_id, t.product_id, t.sales_flag sales_flag_s, 0 sales_flag_e
    FROM
      fe.sf_shelf_product_weeksales_detail t
    WHERE t.stat_date = @last_week_end
    UNION
    ALL
    SELECT
      t.shelf_id, t.product_id, 0 sales_flag_s, t.sales_flag sales_flag_e
    FROM
      fe.sf_shelf_product_weeksales_detail t
    WHERE t.stat_date = @week_end) t
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_3--@time_4",@time_3,@time_4);
  DROP TEMPORARY TABLE IF EXISTS feods.sal_tmp;
  CREATE TEMPORARY TABLE feods.sal_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id, t.product_id, SUM(t.quantity_act) sal_qty, SUM(t.quantity_act * t.sale_price) gmv
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
  WHERE t.pay_date >= @week_start
    AND t.pay_date < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_4--@time_5",@time_4,@time_5);
 
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    f.shelf_id, fi.product_id, SUM(fi.actual_sign_num) actual_sign_num
  FROM
    fe.sf_product_fill_order_item fi
    JOIN fe.sf_product_fill_order f
      ON fi.order_id = f.order_id
      AND f.data_flag = 1
      AND f.order_status IN (3, 4)
      AND f.fill_time >= @week_start
      AND f.fill_time < @add_day
  WHERE fi.data_flag = 1
    AND ! ISNULL(f.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY f.shelf_id, fi.product_id
  HAVING actual_sign_num != 0;
  
 SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_5--@time_6",@time_5,@time_6);
 
  DROP TEMPORARY TABLE IF EXISTS feods.detail_tmp;
  CREATE TEMPORARY TABLE feods.detail_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id, t.product_id, t.sale_price, IFNULL(
      f.first_fill_time >= @last_fill_day, 0
    ) first_fill_flag
  FROM
    fe.sf_shelf_product_detail t
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id
      AND f.data_flag = 1
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
	
 SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_6--@time_7",@time_6,@time_7);
	
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.shelf_id, b.region_name, b.business_name
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1
    AND ! ISNULL(s.shelf_id);
	
 SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_7--@time_8",@time_7,@time_8);
	
  DELETE
  FROM
    feods.fjr_flags_area_product
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_flags_area_product (
    week_end, region_name, business_name, second_type_id, product_id, first_fill_flag, sales_flag_s, sales_flag_e, sku_num, sal_qty, gmv, sto_qty_s, sto_val_s, sto_qty_e, sto_val_e, actual_sign_num, actual_sign_val, add_user
  )
  SELECT
    @week_end, s.region_name, s.business_name, p.second_type_id, flag.product_id, f.first_fill_flag, IFNULL(flag.sales_flag_s, 0), IFNULL(flag.sales_flag_e, 0), COUNT(*), IFNULL(SUM(sal.sal_qty), 0), IFNULL(SUM(sal.gmv), 0), IFNULL(SUM(ssto.sto_qty_s), 0), IFNULL(
      SUM(ssto.sto_qty_s * f.sale_price), 0
    ), IFNULL(SUM(sto.sto_qty_e), 0), IFNULL(
      SUM(sto.sto_qty_e * f.sale_price), 0
    ), IFNULL(SUM(fill.actual_sign_num), 0), IFNULL(
      SUM(
        fill.actual_sign_num * f.sale_price
      ), 0
    ), @add_user
  FROM
    feods.flag_tmp flag
    JOIN feods.shelf_tmp s
      ON flag.shelf_id = s.shelf_id
    JOIN fe.sf_product p
      ON flag.product_id = p.product_id
      AND p.data_flag = 1
    JOIN feods.detail_tmp f
      ON flag.shelf_id = f.shelf_id
      AND flag.product_id = f.product_id
    LEFT JOIN feods.sal_tmp sal
      ON flag.shelf_id = sal.shelf_id
      AND flag.product_id = sal.product_id
    LEFT JOIN feods.ssto_tmp ssto
      ON flag.shelf_id = ssto.shelf_id
      AND flag.product_id = ssto.product_id
    LEFT JOIN feods.sto_tmp sto
      ON flag.shelf_id = sto.shelf_id
      AND flag.product_id = sto.product_id
    LEFT JOIN feods.fill_tmp fill
      ON flag.shelf_id = fill.shelf_id
      AND flag.product_id = fill.product_id
  GROUP BY s.business_name, flag.product_id, first_fill_flag, flag.sales_flag_s, flag.sales_flag_e;
  
	
 SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_8--@time_9",@time_8,@time_9);
 
  DELETE
  FROM
    feods.fjr_flags_res_area
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_flags_res_area (
    week_end, region_name, business_name, first_fill_flag, sales_flag_s, sales_flag_e, sku_num, sal_qty, gmv, sto_qty_s, sto_val_s, sto_qty_e, sto_val_e, actual_sign_num, actual_sign_val, add_user
  )
  SELECT
    @week_end, t.region_name, t.business_name, t.first_fill_flag, t.sales_flag_s, t.sales_flag_e, SUM(t.sku_num), SUM(t.sal_qty), SUM(t.gmv), SUM(t.sto_qty_s), SUM(t.sto_val_s), SUM(t.sto_qty_e), SUM(t.sto_val_e), SUM(t.actual_sign_num), SUM(t.actual_sign_val), @add_user
  FROM
    feods.fjr_flags_area_product t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name, t.first_fill_flag, t.sales_flag_s, t.sales_flag_e;
  
 SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_flags_area_product_week","@time_9--@time_10",@time_9,@time_10);
  
  CALL feods.sp_task_log (
    'sp_flags_area_product_week', @week_end, CONCAT(
      'yingnansong_w_81620485ddd38e1ee8f5d8a3e9648607', @timestamp, @add_user
    )
  );
  COMMIT;
END