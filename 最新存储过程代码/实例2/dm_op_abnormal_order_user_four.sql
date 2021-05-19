CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_abnormal_order_user_four`(in in_sdate DATE)
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := DATE(in_sdate), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
   DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp AS
 SELECT
    t.order_id, t.pay_date AS order_date, t.user_id, t.shelf_id, 
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
  DROP TEMPORARY TABLE IF EXISTS fe_dm.ou_tmp;
  CREATE TEMPORARY TABLE fe_dm.ou_tmp AS
  SELECT
    t.user_id, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.third_discount_amount) third_discount_amount
  FROM
    fe_dm.oi_tmp t
  GROUP BY t.user_id;
  DELETE
  FROM
    fe_dm.dm_op_abnormal_order_over100
  WHERE order_date >= @sdate
    AND order_date < @add_day;
  INSERT INTO fe_dm.dm_op_abnormal_order_over100 (
    order_id, order_date, user_id, shelf_id, payment_type_gateway, platform, skus, quantity, gmv, val_cost, discount_amount, coupon_amount, order_type, third_discount_amount, product_detail, quantity_detail, add_user
  )
  SELECT
    t.order_id, t.order_date, t.user_id, t.shelf_id, t.payment_type_gateway, t.platform, COUNT(DISTINCT t.product_id) skus, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.val_cost) val_cost, t.discount_amount, t.coupon_amount, t.order_type, t.third_discount_amount, GROUP_CONCAT(t.product_id) product_detail, GROUP_CONCAT(t.quantity) quantity_detail, @add_user
  FROM
    fe_dm.oi_tmp t
  GROUP BY t.order_id
  HAVING SUM(t.gmv) > 100;
  DELETE
  FROM
    fe_dm.dm_op_abnormal_order_product_qty
  WHERE order_date >= @sdate
    AND order_date < @add_day;
  INSERT INTO fe_dm.dm_op_abnormal_order_product_qty (
    order_id, order_date, user_id, shelf_id, payment_type_gateway, platform, product_id, quantity, gmv, val_cost, discount_amount, coupon_amount, order_type, third_discount_amount, add_user
  )
  SELECT
    t.order_id, t.order_date, t.user_id, t.shelf_id, t.payment_type_gateway, t.platform, t.product_id, t.quantity, t.gmv, t.val_cost, t.discount_amount, t.coupon_amount, t.order_type, t.third_discount_amount, @add_user
  FROM
    fe_dm.oi_tmp t
  WHERE t.quantity > IF(t.sale_price > 3, 5, 10);
  DELETE
  FROM
    fe_dm.dm_op_abnormal_order_shelf_product
  WHERE sdate >= @sdate
    AND sdate < @add_day;
  INSERT INTO fe_dm.dm_op_abnormal_order_shelf_product (
    sdate, shelf_id, product_id, orders, users, quantity, gmv, val_cost, oi_discount_amount, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, t.product_id, COUNT(*) orders, COUNT(DISTINCT t.user_id) users, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.val_cost) val_cost, SUM(t.oi_discount_amount) oi_discount_amount, @add_user
  FROM
    fe_dm.oi_tmp t
  GROUP BY t.shelf_id, t.product_id
  HAVING orders > 50;
  DELETE
  FROM
    fe_dm.dm_op_abnormal_order_user
  WHERE sdate >= @sdate
    AND sdate < @add_day;
  INSERT INTO fe_dm.dm_op_abnormal_order_user (
    sdate, user_id, orders, shelfs, skus, quantity, gmv, val_cost, discount_amount, coupon_amount, third_discount_amount, add_user
  )
  SELECT
    @sdate sdate, t.user_id, t.orders, t.shelfs, t.skus, t.quantity, t.gmv, t.val_cost, u.discount_amount, u.coupon_amount, u.third_discount_amount, @add_user add_user
  FROM
    (SELECT
      t.user_id, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.shelf_id) shelfs, COUNT(DISTINCT t.product_id) skus, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.val_cost) val_cost, SUM(t.oi_discount_amount) oi_discount_amount
    FROM
      fe_dm.oi_tmp t
    GROUP BY t.user_id
    HAVING SUM(t.gmv) > 100) t
    JOIN fe_dm.ou_tmp u
      ON t.user_id = u.user_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_abnormal_order_user_four',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_order_shelf_product','dm_op_abnormal_order_user_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_order_user','dm_op_abnormal_order_user_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_order_over100','dm_op_abnormal_order_user_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_order_product_qty','dm_op_abnormal_order_user_four','李世龙');
END