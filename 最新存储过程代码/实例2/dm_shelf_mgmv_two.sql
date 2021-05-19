CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_shelf_mgmv_two`()
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
-- 有GMVV 和补付款的货架  需要补数据
  DELETE
  FROM
    fe_dm.dm_shelf_wgmv
  WHERE sdate = @week_end;
  INSERT INTO fe_dm.dm_shelf_wgmv (
    sdate, shelf_id, gmv, payment_money, orders, qty_sal, o_product_total_amount, o_discount_amount, o_coupon_amount, o_third_discount_amount, add_user
  )
  SELECT
    @week_end sdate, t.shelf_id, IFNULL(SUM(t.gmv), 0) gmv, IFNULL(SUM(t.AFTER_PAYMENT_MONEY), 0) payment_money, 
	IFNULL(SUM(t.orders), 0) orders, IFNULL(SUM(t.sal_qty), 0) qty_sal, 
	IFNULL(SUM(t.o_product_total_amount), 0) o_product_total_amount, 
	IFNULL(SUM(t.o_discount_amount), 0) o_discount_amount, IFNULL(SUM(t.o_coupon_amount), 0) o_coupon_amount,
	IFNULL(SUM(t.o_third_discount_amount), 0) o_third_discount_amount, @add_user add_user
  FROM
    fe_dwd.dwd_shelf_day_his t
  WHERE t.sdate >= @week_start
    AND t.sdate < @add_day
	and (t.gmv > 0 or t.AFTER_PAYMENT_MONEY >0)
  GROUP BY t.shelf_id;
  
  
  
  DELETE
  FROM
    fe_dm.dm_shelf_mgmv
  WHERE month_id = @y_m;
  INSERT INTO fe_dm.dm_shelf_mgmv (
    month_id, shelf_id, gmv, payment_money, orders, qty_sal, o_product_total_amount, o_discount_amount, o_coupon_amount, o_third_discount_amount, add_user
  )
  SELECT
    @y_m month_id, t.shelf_id, IFNULL(SUM(t.gmv), 0) gmv, IFNULL(SUM(t.AFTER_PAYMENT_MONEY), 0) payment_money, 
	IFNULL(SUM(t.orders), 0) orders, IFNULL(SUM(t.sal_qty), 0) qty_sal, 
	IFNULL(SUM(t.o_product_total_amount), 0) o_product_total_amount, 
	IFNULL(SUM(t.o_discount_amount), 0) o_discount_amount, IFNULL(SUM(t.o_coupon_amount), 0) o_coupon_amount, 
	IFNULL(SUM(t.o_third_discount_amount), 0) o_third_discount_amount, @add_user add_user
  FROM
    fe_dwd.dwd_shelf_day_his t
  WHERE t.sdate >= @month_start
    AND t.sdate < @add_day
	and (t.gmv > 0 or t.AFTER_PAYMENT_MONEY >0)
  GROUP BY t.shelf_id;
  
  
  
  
CALL sh_process.`sp_sf_dw_task_log` ('dm_shelf_mgmv_two',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('李世龙@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_wgmv','dm_shelf_mgmv_two','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_mgmv','dm_shelf_mgmv_two','李世龙');
COMMIT;
END