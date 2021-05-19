CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_ds7p_sal_fil`(in_sdate DATE)
BEGIN
  #run after sh_process.dwd_order_item_refund_day_inc
   SET @sdate := in_sdate;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id
  FROM
    fe.sf_shelf t
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.quantity_act) quantity_act, SUM(t.quantity_act * t.sale_price) gmv
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, fi.product_id, SUM(
      IF(
        t.fill_type IN (1, 2, 3, 4, 7, 8, 9), fi.actual_sign_num, 0
      )
    ) actual_sign_num_in, SUM(
      IF(
        t.fill_type IN (5, 6, 11), fi.actual_sign_num, 0
      )
    ) actual_sign_num_out
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.order_status IN (3, 4)
    AND t.fill_time >= @sdate
    AND t.fill_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY t.shelf_id, fi.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.main_tmp;
  CREATE TEMPORARY TABLE feods.main_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    feods.oi_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.fill_tmp;
  DELETE
  FROM
    feods.d_op_ds7p_sal_fil
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_ds7p_sal_fil (
    sdate, shelf_id, product_id, quantity_act, gmv, actual_sign_num_in, actual_sign_num_out, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, t.product_id, IFNULL(o.quantity_act, 0) quantity_act, IFNULL(o.gmv, 0) gmv, IFNULL(f.actual_sign_num_in, 0) actual_sign_num_in, IFNULL(f.actual_sign_num_out, 0) actual_sign_num_out, @add_user add_user
  FROM
    feods.main_tmp t
    LEFT JOIN feods.oi_tmp o
      ON t.shelf_id = o.shelf_id
      AND t.product_id = o.product_id
    LEFT JOIN feods.fill_tmp f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id;
  CALL feods.sp_task_log (
    'sp_op_ds7p_sal_fil', @sdate, CONCAT(
      'fjr_d_3b70112ee034c72ca244c029c7d04d7c', @timestamp, @add_user
    )
  );
  COMMIT;
END