CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_abnormal_nsale_shelf_product`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := SUBDATE(@sdate, DAYOFMONTH(@sdate) - 1);
  SET @last_month_start := SUBDATE(@month_start, INTERVAL 1 MONTH), @last2_month_start := SUBDATE(@month_start, INTERVAL 2 MONTH);
  SET @y_m := DATE_FORMAT(@month_start, '%Y-%m'), @last_y_m := DATE_FORMAT(@last_month_start, '%Y-%m'), @last2_y_m := DATE_FORMAT(@last2_month_start, '%Y-%m');
  DROP TEMPORARY TABLE IF EXISTS feods.sal_tmp;
  CREATE TEMPORARY TABLE feods.sal_tmp AS
  SELECT
    t.shelf_id, t.product_id
  FROM
    fe.sf_statistics_shelf_product_sale t
  WHERE t.create_date >= @month_start
  UNION
  SELECT
    t.shelf_id, t.product_id
  FROM
    fe.sf_statistics_shelf_product_sale_month t
  WHERE t.create_date >= @last2_month_start;
  CREATE INDEX idx_shelf_id_product_id
  ON feods.sal_tmp (shelf_id, product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  CREATE TEMPORARY TABLE feods.sto_tmp AS
  SELECT
    t.shelf_id, t.product_id, t.stock_quantity, t.sale_price, t.shelf_fill_flag, t.package_flag, f.danger_flag, f.first_fill_time, f.new_flag
  FROM
    fe.sf_shelf_product_detail t
    LEFT JOIN feods.sal_tmp sal
      ON t.shelf_id = sal.shelf_id
      AND t.product_id = sal.product_id
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id
      AND f.data_flag = 1
  WHERE t.data_flag = 1
    AND t.stock_quantity > 0
    AND sal.shelf_id IS NULL;
  CREATE INDEX idx_shelf_id_product_id
  ON feods.sto_tmp (shelf_id, product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sal_stat_tmp;
  CREATE TEMPORARY TABLE feods.sal_stat_tmp AS
  SELECT
    t.shelf_id, t.product_id, DATE(MIN(t.create_date)) first_sal_day, DATE(MAX(t.create_date)) last_sal_day, SUM(t.sale_amount) gmv, SUM(t.quantity) qty_sal, SUM(t.order_num) orders
  FROM
    fe.sf_statistics_shelf_product_sale t
    JOIN feods.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
      AND t.product_id = sto.product_id
  GROUP BY t.shelf_id, t.product_id;
  CREATE INDEX idx_shelf_id_product_id
  ON feods.sal_stat_tmp (shelf_id, product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.days_sto_tmp;
  CREATE TEMPORARY TABLE feods.days_sto_tmp AS
  SELECT
    t.shelf_id, t.product_id, SUM(
      (day1_quantity > 0) + (day2_quantity > 0) + (day3_quantity > 0) + (day4_quantity > 0) + (day5_quantity > 0) + (day6_quantity > 0) + (day7_quantity > 0) + (day8_quantity > 0) + (day9_quantity > 0) + (day10_quantity > 0) + (day11_quantity > 0) + (day12_quantity > 0) + (day13_quantity > 0) + (day14_quantity > 0) + (day15_quantity > 0) + (day16_quantity > 0) + (day17_quantity > 0) + (day18_quantity > 0) + (day19_quantity > 0) + (day20_quantity > 0) + (day21_quantity > 0) + (day22_quantity > 0) + (day23_quantity > 0) + (day24_quantity > 0) + (day25_quantity > 0) + (day26_quantity > 0) + (day27_quantity > 0) + (day28_quantity > 0) + (day29_quantity > 0) + (day30_quantity > 0) + (day31_quantity > 0)
    ) days_sto
  FROM
    fe.sf_shelf_product_stock_detail t
    JOIN feods.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
      AND t.product_id = sto.product_id
  WHERE t.stat_date IN (@y_m, @last_y_m, @last2_y_m)
  GROUP BY t.shelf_id, t.product_id;
  CREATE INDEX idx_shelf_id_product_id
  ON feods.days_sto_tmp (shelf_id, product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp AS
  SELECT
    t.shelf_id, fi.product_id, SUM(t.fill_type IN (1, 2, 7, 8, 9)) orders_in, SUM(
      IF(
        t.fill_type IN (1, 2, 7, 8, 9), fi.actual_fill_num, 0
      )
    ) actual_fill_num_in, SUM(t.fill_type IN (6, 11)) orders_out, SUM(
      IF(
        t.fill_type IN (6, 11), fi.actual_fill_num, 0
      )
    ) actual_fill_num_out
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
      AND fi.product_id = sto.product_id
  WHERE t.data_flag = 1
    AND t.order_status IN (3, 4)
    AND t.fill_type IN (1, 2, 6, 7, 8, 9, 11)
    AND t.fill_time >= @last2_month_start
  GROUP BY t.shelf_id, fi.product_id;
  CREATE INDEX idx_shelf_id_product_id
  ON feods.fill_tmp (shelf_id, product_id);
  TRUNCATE TABLE feods.fjr_abnormal_nsale_shelf_product;
  INSERT INTO feods.fjr_abnormal_nsale_shelf_product (
    shelf_id, product_id, first_sal_day, last_sal_day, orders, qty_sal, gmv, stock_quantity, sale_price, first_fill_time, danger_flag, new_flag, shelf_fill_flag, package_flag, days_sto, orders_in, actual_fill_num_in, orders_out, actual_fill_num_out, add_user
  )
  SELECT
    t.shelf_id, t.product_id, s.first_sal_day, s.last_sal_day, IFNULL(s.orders, 0) orders, IFNULL(s.qty_sal, 0) qty_sal, IFNULL(s.gmv, 0) gmv, IFNULL(t.stock_quantity, 0) stock_quantity, IFNULL(t.sale_price, 0) sale_price, t.first_fill_time, IFNULL(t.danger_flag, 0) danger_flag, IFNULL(t.new_flag, 0) new_flag, IFNULL(t.shelf_fill_flag, 0) shelf_fill_flag, IFNULL(t.package_flag, 0) package_flag, IFNULL(d.days_sto, 0) days_sto, IFNULL(f.orders_in, 0) orders_in, IFNULL(f.actual_fill_num_in, 0) actual_fill_num_in, IFNULL(f.orders_out, 0) orders_out, IFNULL(f.actual_fill_num_out, 0) actual_fill_num_out, @add_user add_user
  FROM
    feods.sto_tmp t
    LEFT JOIN feods.sal_stat_tmp s
      ON t.shelf_id = s.shelf_id
      AND t.product_id = s.product_id
    LEFT JOIN feods.days_sto_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
    LEFT JOIN feods.fill_tmp f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id;
  CALL feods.sp_task_log (
    'sp_abnormal_nsale_shelf_product', @sdate, CONCAT(
      'fjr_d_6613d08115976963ec70daad9f1ced38', @timestamp, @add_user
    )
  );
  COMMIT;
END