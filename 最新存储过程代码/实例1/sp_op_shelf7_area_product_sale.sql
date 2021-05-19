CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf7_area_product_sale`(in_sdate DATE)
BEGIN
  #run after sh_process.dwd_order_item_refund_day_inc
   SET @sdate := DATE(in_sdate), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1), @week_flag := (WEEKDAY(@sdate) = 6), @week_start := SUBDATE(@sdate, WEEKDAY(@sdate)), @month_flag := (@sdate = LAST_DAY(@sdate)), @month_start := SUBDATE(@sdate, DAY(@sdate) - 1), @y_m := DATE_FORMAT(@sdate, '%Y-%m'), @d := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, b.business_name
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
  DELETE
  FROM
    feods.d_op_shelf7_area_product_sale_day
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_shelf7_area_product_sale_day (
    sdate, business_name, product_id, orders_normal, orders_fail, quantity, gmv, discount_amount, coupon_amount, quantity_shipped, gmv_shipped, discount_amount_shipped, coupon_amount_shipped, add_user
  )
  SELECT
    @sdate sdate, s.business_name, t.product_id, COUNT(
      DISTINCT IF(
        t.order_status = 6, NULL, t.order_id
      )
    ) orders_normal, COUNT(
      DISTINCT IF(
        t.order_status = 6, t.order_id, NULL
      )
    ) orders_fail, SUM(t.quantity) quantity, SUM(t.quantity * t.sale_price) gmv, SUM(
      t.o_discount_amount * t.quantity * t.sale_price / t.ogmv
    ) discount_amount, SUM(
      t.o_coupon_amount * t.quantity * t.sale_price / t.ogmv
    ) coupon_amount, SUM(t.quantity_act) quantity_act, SUM(t.quantity_act * t.sale_price) gmv_shipped, SUM(
      t.o_discount_amount * t.quantity_act * t.sale_price / t.ogmv
    ) discount_amount_shipped, SUM(
      t.o_coupon_amount * t.quantity_act * t.sale_price / t.ogmv
    ) coupon_amount_shipped, @add_user add_user
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND t.order_type = 3
  GROUP BY s.business_name, t.product_id;
  DELETE
  FROM
    feods.d_op_shelf7_area_product_sale_week
  WHERE @week_flag
    AND sdate = @sdate;
  INSERT INTO feods.d_op_shelf7_area_product_sale_week (
    sdate, business_name, product_id, orders_normal, orders_fail, quantity, gmv, discount_amount, coupon_amount, quantity_shipped, gmv_shipped, discount_amount_shipped, coupon_amount_shipped, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.product_id, SUM(t.orders_normal) orders_normal, SUM(t.orders_fail) orders_fail, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.quantity_shipped) quantity_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.discount_amount_shipped) discount_amount_shipped, SUM(t.coupon_amount_shipped) coupon_amount_shipped, @add_user add_user
  FROM
    feods.d_op_shelf7_area_product_sale_day t
  WHERE @week_flag
    AND t.sdate >= @week_start
    AND t.sdate < @add_day
  GROUP BY t.business_name, t.product_id;
  DELETE
  FROM
    feods.d_op_shelf7_area_product_sale_month
  WHERE @month_flag
    AND sdate = @sdate;
  INSERT INTO feods.d_op_shelf7_area_product_sale_month (
    sdate, month_id, business_name, product_id, orders_normal, orders_fail, quantity, gmv, discount_amount, coupon_amount, quantity_shipped, gmv_shipped, discount_amount_shipped, coupon_amount_shipped, add_user
  )
  SELECT
    @sdate sdate, @y_m month_id, t.business_name, t.product_id, SUM(t.orders_normal) orders_normal, SUM(t.orders_fail) orders_fail, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.quantity_shipped) quantity_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.discount_amount_shipped) discount_amount_shipped, SUM(t.coupon_amount_shipped) coupon_amount_shipped, @add_user add_user
  FROM
    feods.d_op_shelf7_area_product_sale_day t
  WHERE @month_flag
    AND t.sdate >= @month_start
    AND t.sdate < @add_day
  GROUP BY t.business_name, t.product_id;
  CALL feods.sp_task_log (
    'sp_op_shelf7_area_product_sale', @sdate, CONCAT(
      'fjr_d_88562e5422d9936cf732ab2e3e5d00d9', @timestamp, @add_user
    )
  );
  COMMIT;
END