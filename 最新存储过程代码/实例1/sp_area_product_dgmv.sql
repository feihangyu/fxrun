CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_area_product_dgmv`(in_sdate DATE)
BEGIN
  #run after sh_process.dwd_order_item_refund_day_inc
   SET @sdate := DATE(in_sdate), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, b.business_name, t.shelf_type
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id);
  DELETE
  FROM
    feods.d_op_product_area_shelftype_dgmv
  WHERE sdate >= @sdate;
  INSERT INTO feods.d_op_product_area_shelftype_dgmv (
    sdate, product_id, business_name, shelf_type, qty_sal, gmv, discount, coupon, add_user
  )
  SELECT
    @sdate sdate, t.product_id, s.business_name, s.shelf_type, SUM(t.quantity_act) qty_sal, SUM(t.quantity_act * t.sale_price) gmv, SUM(
      t.discount_amount * t.quantity_act / t.quantity
    ) discount, SUM(
      t.o_coupon_amount * t.quantity_act * t.sale_price / t.ogmv
    ) coupon, @add_user add_user
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
  GROUP BY t.product_id, s.business_name, s.shelf_type;
  DELETE
  FROM
    feods.fjr_area_product_dgmv
  WHERE sdate >= @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.cum_tmp;
  CREATE TEMPORARY TABLE feods.cum_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    t.product_id, t.business_name, t.qty_sal_cum, t.gmv_cum, t.discount_cum
  FROM
    feods.fjr_area_product_dgmv t
    JOIN
      (SELECT
        t.product_id, t.business_name, MAX(t.sdate) sdate
      FROM
        feods.fjr_area_product_dgmv t
      GROUP BY t.product_id, t.business_name) l
      ON t.sdate = l.sdate
      AND t.product_id = l.product_id
      AND t.business_name = l.business_name
  where ! ISNULL(t.product_id)
    AND ! ISNULL(t.business_name);
  INSERT INTO feods.fjr_area_product_dgmv (
    sdate, product_id, business_name, qty_sal, qty_sal_cum, gmv, gmv_cum, discount, discount_cum, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, t.qty_sal, t.qty_sal + IFNULL(c.qty_sal_cum, 0) qty_sal_cum, t.gmv, t.gmv + IFNULL(c.gmv_cum, 0) gmv_cum, t.discount, t.discount + IFNULL(c.discount_cum, 0) discount_cum, @add_user
  FROM
    (SELECT
      t.product_id, t.business_name, SUM(t.qty_sal) qty_sal, SUM(t.gmv) gmv, SUM(t.discount) discount
    FROM
      feods.d_op_product_area_shelftype_dgmv t
    WHERE t.sdate = @sdate
    GROUP BY t.product_id, t.business_name) t
    LEFT JOIN feods.cum_tmp c
      ON t.business_name = c.business_name
      AND t.product_id = c.product_id;
  DELETE
  FROM
    feods.d_op_product_area_shelftype_dfill
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_product_area_shelftype_dfill (
    sdate, product_id, business_name, shelf_type, supplier_type, fill_type, qty_fill, val_fill, add_user
  )
  SELECT
    @sdate sdate, fi.product_id, s.business_name, s.shelf_type, t.supplier_type, t.fill_type, SUM(fi.actual_fill_num) qty_sal, SUM(
      fi.actual_fill_num * fi.purchase_price
    ) val_fill, @add_user add_user
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.order_status IN (3, 4)
    AND t.fill_time >= @sdate
    AND t.fill_time < @add_day
  GROUP BY fi.product_id, s.business_name, s.shelf_type, t.supplier_type, t.fill_type
  HAVING qty_sal != 0;
  DELETE
  FROM
    feods.fjr_area_product_dfill
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_area_product_dfill (
    sdate, product_id, business_name, qty_fill, val_fill, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, SUM(t.qty_fill) qty_fill, SUM(t.val_fill) val_fill, @add_user
  FROM
    feods.d_op_product_area_shelftype_dfill t
  WHERE t.sdate = @sdate
  GROUP BY t.product_id, t.business_name;
  CALL feods.sp_task_log (
    'sp_area_product_dgmv', @sdate, CONCAT(
      'fjr_d_019aa084a4ee1fb20a323be16179caf3', @timestamp, @add_user
    )
  );
  CALL sh_process.sp_op_product_area_disrate ();
  COMMIT;
END