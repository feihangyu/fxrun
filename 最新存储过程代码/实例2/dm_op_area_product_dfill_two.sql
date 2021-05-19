CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_dfill_two`()
BEGIN
  SET @run_date := SUBDATE(CURRENT_DATE, 1), @user := CURRENT_USER, @stime := CURRENT_TIMESTAMP;
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_end_last := SUBDATE(@month_start, 1);
  SET @w := WEEKDAY(@sdate);
  SET @week_start := SUBDATE(@sdate, @w);
  SET @week_end := ADDDATE(@week_start, 6);
  
  DELETE
  FROM
    fe_dm.dm_op_product_area_shelftype_dfill
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_product_area_shelftype_dfill (
    sdate, product_id, business_name, shelf_type, supplier_type, fill_type, qty_fill, val_fill, add_user
  )
  SELECT
    @sdate sdate, t.product_id, s.business_name, s.shelf_type, t.supplier_type, t.fill_type, 
	SUM(t.actual_fill_num) qty_sal, SUM(
      t.actual_fill_num * t.purchase_price
    ) val_fill, @add_user add_user
  FROM
    fe_dwd.dwd_fill_day_inc t
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON t.shelf_id = s.shelf_id
  WHERE t.order_status IN (3, 4)
    AND t.fill_time >= @sdate
    AND t.fill_time < @add_day
  GROUP BY t.product_id, s.business_name, s.shelf_type, t.supplier_type, t.fill_type
  HAVING qty_sal != 0;
  
  
  DELETE
  FROM
    fe_dm.dm_op_area_product_dfill
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_area_product_dfill (
    sdate, product_id, business_name, qty_fill, val_fill, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, SUM(t.qty_fill) qty_fill, SUM(t.val_fill) val_fill, @add_user
  FROM
    fe_dm.dm_op_product_area_shelftype_dfill t
  WHERE t.sdate = @sdate
  GROUP BY t.product_id, t.business_name;
  
  
  
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_area_product_dfill_two',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('李世龙@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_area_shelftype_dfill','dm_op_area_product_dfill_two','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_dfill','dm_op_area_product_dfill_two','李世龙');
COMMIT;
END