CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_order_and_item`(in_sdate DATE)
BEGIN
#  SET @sdate := in_sdate,
#  @add_user := CURRENT_USER,
#  @timestamp := CURRENT_TIMESTAMP;
#  SET @add_day := ADDDATE(@sdate, 1);
#  DELETE
#  FROM
#    feods.d_op_order_and_item
#  WHERE pay_date >= @sdate
#    AND pay_date < @add_day;
#  INSERT INTO feods.d_op_order_and_item (
#    order_id,
#    order_status,
#    order_type,
#    order_date,
#    pay_date,
#    user_id,
#    payment_type_gateway,
#    shelf_id,
#    product_id,
#    quantity,
#    quantity_act,
#    sale_price,
#    purchase_price,
#    discount_amount,
#    ogmv,
#    o_product_total_amount,
#    o_discount_amount,
#    o_coupon_amount,
#    o_third_discount_amount,
#    add_user
#  )
#  SELECT
#    t.order_id,
#    t.order_status,
#    t.order_type,
#    t.order_date,
#    t.pay_date,
#    t.user_id,
#    t.payment_type_gateway,
#    t.shelf_id,
#    oi.product_id,
#    oi.quantity,
#    IF(
#      t.order_status = 6,
#      oi.quantity_shipped,
#      oi.quantity
#    ) quantity_act,
#    oi.sale_price,
#    IFNULL(
#      oi.purchase_price,
#      oi.cost_price
#    ) purchase_price,
#    oi.discount_amount,
#    t.product_total_amount + t.discount_amount + IFNULL(t.coupon_amount, 0) + IFNULL(t.third_discount_amount, 0) ogmv,
#    t.product_total_amount o_product_total_amount,
#    t.discount_amount o_discount_amount,
#    t.coupon_amount o_coupon_amount,
#    IFNULL(t.third_discount_amount, 0) o_third_discount_amount,
#    @add_user add_user
#  FROM
#    fe.sf_order t
#    JOIN fe.sf_order_item oi
#      ON t.order_id = oi.order_id
#  WHERE t.order_status IN (2, 6, 7)
#    AND t.pay_date >= @sdate
#    AND t.pay_date < @add_day;
#  CALL feods.sp_task_log (
#    'sp_op_order_and_item',
#    @sdate,
#    CONCAT(
#      'fjr_d_5ad1472f4938c802fee932a32f62997e',
#      @timestamp,
#      @add_user
#    )
#  );
  CALL sh_process.sp_op_sp_stock_detail (
    DATE_FORMAT(CURRENT_DATE, '%Y-%m')
  );
  COMMIT;
END