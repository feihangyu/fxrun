CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sp_shelf7_stock3`()
BEGIN
  SET @sdate := CURRENT_DATE;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
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
  DROP TEMPORARY TABLE IF EXISTS feods.detail_tmp;
  CREATE TEMPORARY TABLE feods.detail_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, t.stock_quantity, t.shelf_fill_flag, t.sale_price
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.slot_tmp;
  CREATE TEMPORARY TABLE feods.slot_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.stock_num) slot_stock_num
  FROM
    fe.sf_shelf_machine_slot t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.second_tmp;
  CREATE TEMPORARY TABLE feods.second_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, msd.product_id, SUM(msd.stock_num) second_stock_num
  FROM
    fe.sf_shelf_machine_second t
    JOIN fe.sf_shelf_machine_second_detail msd
      ON t.machine_second_id = msd.machine_second_id
      AND msd.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(msd.product_id)
  GROUP BY t.shelf_id, msd.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.onway_tmp;
  CREATE TEMPORARY TABLE feods.onway_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, fi.product_id, SUM(fi.actual_apply_num) onway_num
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.order_status IN (1, 2)
    AND t.apply_time >= SUBDATE(@sdate, INTERVAL 1 MONTH)
    AND t.fill_type IN (1, 2, 3, 4, 7, 8, 9)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY t.shelf_id, fi.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.main_tmp;
  CREATE TEMPORARY TABLE feods.main_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    feods.detail_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.slot_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.second_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.onway_tmp;
  TRUNCATE feods.d_op_sp_shelf7_stock3;
  INSERT INTO feods.d_op_sp_shelf7_stock3 (
    shelf_id, product_id, qty_sto, qty_sto_slot, qty_sto_sec, shelf_fill_flag, sale_price, onway_num, add_user
  )
  SELECT
    t.shelf_id, t.product_id, IFNULL(de.stock_quantity, 0) qty_sto, IFNULL(sl.slot_stock_num, 0) qty_sto_slot, IFNULL(se.second_stock_num, 0) qty_sto_sec, IFNULL(de.shelf_fill_flag, 1) shelf_fill_flag, IFNULL(de.sale_price, 1) sale_price, IFNULL(ow.onway_num, 0) onway_num, @add_user add_user
  FROM
    feods.main_tmp t
    LEFT JOIN feods.detail_tmp de
      ON t.shelf_id = de.shelf_id
      AND t.product_id = de.product_id
    LEFT JOIN feods.slot_tmp sl
      ON t.shelf_id = sl.shelf_id
      AND t.product_id = sl.product_id
    LEFT JOIN feods.second_tmp se
      ON t.shelf_id = se.shelf_id
      AND t.product_id = se.product_id
    LEFT JOIN feods.onway_tmp ow
      ON t.shelf_id = ow.shelf_id
      AND t.product_id = ow.product_id;
  CALL feods.sp_task_log (
    'sp_op_sp_shelf7_stock3', @sdate, CONCAT(
      'fjr_d_8c869d06421bbf2ae2476cf97bb695b5', @timestamp, @add_user
    )
  );
  COMMIT;
END