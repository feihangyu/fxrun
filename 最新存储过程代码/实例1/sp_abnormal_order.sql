CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_abnormal_order`(in_sdate date)
BEGIN
  set @sdate := DATE(in_sdate), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  set @add_day := ADDDATE(@sdate, 1);
   DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp AS
 SELECT
    t.order_id, t.pay_date as order_date, t.user_id, t.shelf_id, 
	IFNULL(t.payment_type_gateway, '') payment_type_gateway,
	t.platform, 
	t.product_id, SUM(t.quantity) quantity, 
	SUM(t.quantity * t.sale_price) gmv, 
	SUM(t.quantity * t.sale_price) / SUM(t.quantity) sale_price,
	SUM(
      t.quantity * IFNULL(
        t.purchase_price, t.cost_price
      )
    ) val_cost, 
	SUM(t.discount_amount) oi_discount_amount, t.discount_amount,
	IFNULL(t.coupon_amount, 0) coupon_amount, t.order_type,
	IFNULL(t.third_discount_amount, 0) third_discount_amount, @add_user
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
  WHERE t.order_status IN (2, 6, 7)
    AND t.pay_date >= @sdate
    AND t.pay_date < @add_day
   GROUP BY t.order_id,t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.ou_tmp;
  CREATE TEMPORARY TABLE feods.ou_tmp AS
  SELECT
    t.user_id, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.third_discount_amount) third_discount_amount
  FROM
    feods.oi_tmp t
  GROUP BY t.user_id;
  DELETE
  FROM
    feods.fjr_abnormal_order_over100
  WHERE order_date >= @sdate
    AND order_date < @add_day;
  INSERT INTO feods.fjr_abnormal_order_over100 (
    order_id, order_date, user_id, shelf_id, payment_type_gateway, platform, skus, quantity, gmv, val_cost, discount_amount, coupon_amount, order_type, third_discount_amount, product_detail, quantity_detail, add_user
  )
  SELECT
    t.order_id, t.order_date, t.user_id, t.shelf_id, t.payment_type_gateway, t.platform, COUNT(DISTINCT t.product_id) skus, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.val_cost) val_cost, t.discount_amount, t.coupon_amount, t.order_type, t.third_discount_amount, GROUP_CONCAT(t.product_id) product_detail, GROUP_CONCAT(t.quantity) quantity_detail, @add_user
  FROM
    feods.oi_tmp t
  GROUP BY t.order_id
  HAVING SUM(t.gmv) > 100;
  DELETE
  FROM
    feods.fjr_abnormal_order_product_qty
  WHERE order_date >= @sdate
    AND order_date < @add_day;
  INSERT INTO feods.fjr_abnormal_order_product_qty (
    order_id, order_date, user_id, shelf_id, payment_type_gateway, platform, product_id, quantity, gmv, val_cost, discount_amount, coupon_amount, order_type, third_discount_amount, add_user
  )
  SELECT
    t.order_id, t.order_date, t.user_id, t.shelf_id, t.payment_type_gateway, t.platform, t.product_id, t.quantity, t.gmv, t.val_cost, t.discount_amount, t.coupon_amount, t.order_type, t.third_discount_amount, @add_user
  FROM
    feods.oi_tmp t
  WHERE t.quantity > IF(t.sale_price > 3, 5, 10);
  DELETE
  FROM
    feods.fjr_abnormal_order_shelf_product
  WHERE sdate >= @sdate
    AND sdate < @add_day;
  INSERT INTO feods.fjr_abnormal_order_shelf_product (
    sdate, shelf_id, product_id, orders, users, quantity, gmv, val_cost, oi_discount_amount, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, t.product_id, COUNT(*) orders, COUNT(DISTINCT t.user_id) users, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.val_cost) val_cost, SUM(t.oi_discount_amount) oi_discount_amount, @add_user
  FROM
    feods.oi_tmp t
  GROUP BY t.shelf_id, t.product_id
  HAVING orders > 50;
  DELETE
  FROM
    feods.fjr_abnormal_order_user
  WHERE sdate >= @sdate
    AND sdate < @add_day;
  INSERT INTO feods.fjr_abnormal_order_user (
    sdate, user_id, orders, shelfs, skus, quantity, gmv, val_cost, discount_amount, coupon_amount, third_discount_amount, add_user
  )
  SELECT
    @sdate sdate, t.user_id, t.orders, t.shelfs, t.skus, t.quantity, t.gmv, t.val_cost, u.discount_amount, u.coupon_amount, u.third_discount_amount, @add_user add_user
  FROM
    (SELECT
      t.user_id, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.shelf_id) shelfs, COUNT(DISTINCT t.product_id) skus, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.val_cost) val_cost, SUM(t.oi_discount_amount) oi_discount_amount
    FROM
      feods.oi_tmp t
    GROUP BY t.user_id
    HAVING SUM(t.gmv) > 100) t
    JOIN feods.ou_tmp u
      ON t.user_id = u.user_id;
  CALL feods.sp_task_log (
    'sp_abnormal_order', @sdate, CONCAT(
      'fjr_d_a8e6f66c443fa91a08dc9c704c6558c4', @timestamp, @add_user
    )
  );
  COMMIT;
END