CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_dgmv`(in_sdate DATE)
BEGIN
  #run after sh_process.sp_op_order_and_item
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_end_last := SUBDATE(@month_start, 1);
  SET @w := WEEKDAY(@sdate);
  SET @week_start := SUBDATE(@sdate, @w);
  SET @week_end := ADDDATE(@week_start, 6);
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_tmp;
  CREATE TEMPORARY TABLE feods.gmv_tmp (PRIMARY KEY (shelf_id))
   SELECT
    t.shelf_id, SUM(IF(t.refund_amount>0,t.quantity_act,t.`QUANTITY`) * t.`SALE_PRICE`) gmv,
    -- SUM(t.ogmv) gmv, 
	COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.user_id) users, 
	SUM(t.quantity_act) qty_sal, COUNT(DISTINCT t.product_id) skus, 
       SUM(
      t.o_product_total_amount * t.sale_price * t.quantity_act / t.ogmv
    ) o_product_total_amount,   -- 取折算后的
	SUM(
      t.o_discount_amount  * t.sale_price * t.quantity_act / t.ogmv
    ) o_discount_amount,  -- 取折算后的
	SUM(
      t.o_coupon_amount * t.sale_price * t.quantity_act / t.ogmv
    ) o_coupon_amount,   -- 取折算后的
	SUM(
      t.o_third_discount_amount * t.sale_price * t.quantity_act / t.ogmv
    ) o_third_discount_amount    -- 取折算后的 
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
  GROUP BY t.shelf_id; 
  
  
  
  
  DROP TEMPORARY TABLE IF EXISTS feods.after_tmp;
  CREATE TEMPORARY TABLE feods.after_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.payment_money) payment_money
  FROM
    fe.sf_after_payment t
  WHERE t.payment_status = 2
    AND t.payment_date >= @sdate
    AND t.payment_date < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_id_tmp;
  CREATE TEMPORARY TABLE feods.shelf_id_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id
  FROM
    feods.after_tmp t
    LEFT JOIN feods.gmv_tmp g
      ON t.shelf_id = g.shelf_id
  WHERE ISNULL(g.shelf_id)
    AND ! ISNULL(t.shelf_id);
  INSERT INTO feods.gmv_tmp (shelf_id)
  SELECT
    shelf_id
  FROM
    feods.shelf_id_tmp;
  DELETE
  FROM
    feods.fjr_shelf_dgmv
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_shelf_dgmv (
    sdate, shelf_id, gmv, payment_money, orders, users, qty_sal, skus, o_product_total_amount, o_discount_amount, o_coupon_amount, o_third_discount_amount, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, IFNULL(t.gmv, 0) gmv, IFNULL(af.payment_money, 0) payment_money, IFNULL(t.orders, 0) orders, IFNULL(t.users, 0) users, IFNULL(t.qty_sal, 0) qty_sal, IFNULL(t.skus, 0) skus, IFNULL(t.o_product_total_amount, 0) o_product_total_amount, IFNULL(t.o_discount_amount, 0) o_discount_amount, IFNULL(t.o_coupon_amount, 0) o_coupon_amount, IFNULL(t.o_third_discount_amount, 0) o_third_discount_amount, @add_user add_user
  FROM
    feods.gmv_tmp t
    LEFT JOIN feods.after_tmp af
      ON t.shelf_id = af.shelf_id;
  DELETE
  FROM
    feods.fjr_shelf_wgmv
  WHERE sdate = @week_end;
  INSERT INTO feods.fjr_shelf_wgmv (
    sdate, shelf_id, gmv, payment_money, orders, qty_sal, o_product_total_amount, o_discount_amount, o_coupon_amount, o_third_discount_amount, add_user
  )
  SELECT
    @week_end sdate, t.shelf_id, IFNULL(SUM(t.gmv), 0) gmv, IFNULL(SUM(t.payment_money), 0) payment_money, IFNULL(SUM(t.orders), 0) orders, IFNULL(SUM(t.qty_sal), 0) qty_sal, IFNULL(SUM(t.o_product_total_amount), 0) o_product_total_amount, IFNULL(SUM(t.o_discount_amount), 0) o_discount_amount, IFNULL(SUM(t.o_coupon_amount), 0) o_coupon_amount, IFNULL(SUM(t.o_third_discount_amount), 0) o_third_discount_amount, @add_user add_user
  FROM
    feods.fjr_shelf_dgmv t
  WHERE t.sdate >= @week_start
    AND t.sdate < @add_day
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.fjr_shelf_mgmv
  WHERE month_id = @y_m;
  INSERT INTO feods.fjr_shelf_mgmv (
    month_id, shelf_id, gmv, payment_money, orders, qty_sal, o_product_total_amount, o_discount_amount, o_coupon_amount, o_third_discount_amount, add_user
  )
  SELECT
    @y_m month_id, t.shelf_id, IFNULL(SUM(t.gmv), 0) gmv, IFNULL(SUM(t.payment_money), 0) payment_money, IFNULL(SUM(t.orders), 0) orders, IFNULL(SUM(t.qty_sal), 0) qty_sal, IFNULL(SUM(t.o_product_total_amount), 0) o_product_total_amount, IFNULL(SUM(t.o_discount_amount), 0) o_discount_amount, IFNULL(SUM(t.o_coupon_amount), 0) o_coupon_amount, IFNULL(SUM(t.o_third_discount_amount), 0) o_third_discount_amount, @add_user add_user
  FROM
    feods.fjr_shelf_dgmv t
  WHERE t.sdate >= @month_start
    AND t.sdate < @add_day
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.d_op_product_area_shelftype_mgmv
  WHERE month_id = @y_m;
  INSERT INTO feods.d_op_product_area_shelftype_mgmv (
    month_id, product_id, business_name, shelf_type, qty_sal, gmv, discount, coupon, add_user
  )
  SELECT
    @y_m month_id, product_id, business_name, shelf_type, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, SUM(coupon) coupon, @add_user add_user
  FROM
    feods.d_op_product_area_shelftype_dgmv
  WHERE sdate BETWEEN @month_start
    AND @sdate
  GROUP BY product_id, business_name, shelf_type;
  DELETE
  FROM
    feods.fjr_area_product_mgmv
  WHERE month_id = @y_m;
  INSERT INTO feods.fjr_area_product_mgmv (
    month_id, product_id, business_name, qty_sal, gmv, discount, add_user
  )
  SELECT
    month_id, product_id, business_name, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, @add_user add_user
  FROM
    feods.d_op_product_area_shelftype_mgmv
  WHERE month_id = @y_m
  GROUP BY product_id, business_name;
  DELETE
  FROM
    feods.d_op_product_area_shelftype_wgmv
  WHERE sdate = @week_end;
  INSERT INTO feods.d_op_product_area_shelftype_wgmv (
    sdate, product_id, business_name, shelf_type, qty_sal, gmv, discount, coupon, add_user
  )
  SELECT
    @week_end sdate, product_id, business_name, shelf_type, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, SUM(coupon) coupon, CURRENT_USER add_user
  FROM
    feods.d_op_product_area_shelftype_dgmv
  WHERE sdate BETWEEN @week_start
    AND @week_end
  GROUP BY product_id, business_name, shelf_type;
  DELETE
  FROM
    feods.fjr_area_product_wgmv
  WHERE sdate = @week_end;
  INSERT INTO feods.fjr_area_product_wgmv (
    sdate, product_id, business_name, qty_sal, gmv, discount, add_user
  )
  SELECT
    sdate, product_id, business_name, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, CURRENT_USER add_user
  FROM
    feods.d_op_product_area_shelftype_wgmv
  WHERE sdate = @week_end
  GROUP BY product_id, business_name;
  CALL feods.sp_task_log (
    'sp_shelf_dgmv', @sdate, CONCAT(
      'fjr_d_2d8704e8782a3e19e8fc0b6c25369fe8', @timestamp, @add_user
    )
  );
  COMMIT;
END