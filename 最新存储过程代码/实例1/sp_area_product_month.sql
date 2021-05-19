CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_area_product_month`(in_month_id CHAR(7))
BEGIN
  SET @month_id := in_month_id, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_first_day := CONCAT(@month_id, '-01');
  SET @month_last_day := LAST_DAY(@month_first_day);
  SET @month_add_day := ADDDATE(@month_last_day, 1);
  SET @month_last_weekend := SUBDATE(
    @month_last_day, DAYOFWEEK(@month_last_day) - 1
  );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_area_tmp, feods.stat_inventory_tmp, feods.fil_tmp, feods.sto_tmp, feods.order_month_tmp, feods.order_tmp, feods.order_re_tmp, feods.order_area_tmp;
  CREATE TEMPORARY TABLE feods.shelf_area_tmp AS
  SELECT
    s.shelf_id, b.business_name
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1;
  CREATE INDEX idx_shelf_area_tmp_shelf_id
  ON feods.shelf_area_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.stat_inventory_tmp AS
  SELECT
    b.business_name, t.product_id, SUM(t.pre_stock_num) pre_stock_num, SUM(t.curr_fill_num) curr_fill_num, SUM(t.curr_actual_stock) curr_actual_stock, SUM(t.curr_should_stock) curr_should_stock
  FROM
    fe.sf_statistics_product_inventory t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.stat_month = @month_id
  GROUP BY b.business_name, t.product_id;
  CREATE INDEX idx_stat_inventory_tmp_business_name_product_id
  ON feods.stat_inventory_tmp (business_name, product_id);
  CREATE TEMPORARY TABLE feods.fil_tmp AS
  SELECT
    s.business_name, fi.product_id, SUM(fi.actual_fill_num) actual_fill_num3
  FROM
    fe.sf_product_fill_order f
    JOIN fe.sf_product_fill_order_item fi
      ON fi.order_id = f.order_id
      AND fi.data_flag = 1
    JOIN feods.shelf_area_tmp s
      ON s.shelf_id = f.shelf_id
  WHERE f.data_flag = 1
    AND f.order_status IN (3, 4)
    AND f.fill_type = 3
    AND f.fill_time >= @month_first_day
    AND f.fill_time < @month_add_day
  GROUP BY s.business_name, fi.product_id;
  CREATE INDEX idx_fil_tmp_business_name_product_id
  ON feods.fil_tmp (business_name, product_id);
  CREATE TEMPORARY TABLE feods.sto_tmp AS
  SELECT
    sa.business_name, t.product_id, SUM(
      (t.day1_quantity > 0)
      OR (t.day2_quantity > 0)
      OR (t.day3_quantity > 0)
      OR (t.day4_quantity > 0)
      OR (t.day5_quantity > 0)
      OR (t.day6_quantity > 0)
      OR (t.day7_quantity > 0)
      OR (t.day8_quantity > 0)
      OR (t.day9_quantity > 0)
      OR (t.day10_quantity > 0)
      OR (t.day11_quantity > 0)
      OR (t.day12_quantity > 0)
      OR (t.day13_quantity > 0)
      OR (t.day14_quantity > 0)
      OR (t.day15_quantity > 0)
      OR (t.day16_quantity > 0)
      OR (t.day17_quantity > 0)
      OR (t.day18_quantity > 0)
      OR (t.day19_quantity > 0)
      OR (t.day20_quantity > 0)
      OR (t.day21_quantity > 0)
      OR (t.day22_quantity > 0)
      OR (t.day23_quantity > 0)
      OR (t.day24_quantity > 0)
      OR (t.day25_quantity > 0)
      OR (t.day26_quantity > 0)
      OR (t.day27_quantity > 0)
      OR (t.day28_quantity > 0)
      OR (t.day29_quantity > 0)
      OR (t.day30_quantity > 0)
      OR (t.day31_quantity > 0)
    ) shelfs, SUM(
      (t.day1_quantity > 0) + (t.day2_quantity > 0) + (t.day3_quantity > 0) + (t.day4_quantity > 0) + (t.day5_quantity > 0) + (t.day6_quantity > 0) + (t.day7_quantity > 0) + (t.day8_quantity > 0) + (t.day9_quantity > 0) + (t.day10_quantity > 0) + (t.day11_quantity > 0) + (t.day12_quantity > 0) + (t.day13_quantity > 0) + (t.day14_quantity > 0) + (t.day15_quantity > 0) + (t.day16_quantity > 0) + (t.day17_quantity > 0) + (t.day18_quantity > 0) + (t.day19_quantity > 0) + (t.day20_quantity > 0) + (t.day21_quantity > 0) + (t.day22_quantity > 0) + (t.day23_quantity > 0) + (t.day24_quantity > 0) + (t.day25_quantity > 0) + (t.day26_quantity > 0) + (t.day27_quantity > 0) + (t.day28_quantity > 0) + (t.day29_quantity > 0) + (t.day30_quantity > 0) + (t.day31_quantity > 0)
    ) skudays
  FROM
    fe.sf_shelf_product_stock_detail t
    JOIN feods.shelf_area_tmp sa
      ON t.shelf_id = sa.shelf_id
  WHERE t.stat_date = @month_id
  GROUP BY sa.business_name, t.product_id;
  CREATE INDEX idx_sto_tmp_business_name_product_id
  ON feods.sto_tmp (business_name, product_id);
  CREATE TEMPORARY TABLE feods.order_month_tmp AS
  SELECT
    o.order_id, o.order_date, o.user_id, o.shelf_id, oi.product_id, oi.quantity, oi.sale_price, IFNULL(
      oi.purchase_price, oi.cost_price
    ) purchase_price, oi.discount_amount, IFNULL(o.product_total_amount, 0) + IFNULL(o.discount_amount, 0) + IFNULL(o.coupon_amount, 0) ogmv, o.discount_amount o_discount_amount, o.coupon_amount o_coupon_amount
  FROM
    fe.sf_order_item oi
    JOIN fe.sf_order o
      ON oi.order_id = o.order_id
      AND o.order_status = 2
      AND o.order_date >= @month_first_day
      AND o.order_date < @month_add_day;
  CREATE INDEX idx_order_month_tmp_shelf_id_product_id
  ON feods.order_month_tmp (shelf_id, product_id);
  CREATE INDEX idx_order_month_tmp_user_id
  ON feods.order_month_tmp (user_id);
  CREATE TEMPORARY TABLE feods.order_tmp AS
  SELECT
    sa.business_name, t.product_id, SUM(t.sale_price * t.quantity) gmv, SUM(t.quantity) quantity, SUM(t.discount_amount) discount_amount, COUNT(DISTINCT t.user_id) users, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.shelf_id) shelfs, COUNT(
      DISTINCT
      CASE
        WHEN t.ogmv > t.sale_price * t.quantity + .1
        THEN t.order_id
      END
    ) orders_related
  FROM
    feods.order_month_tmp t
    JOIN feods.shelf_area_tmp sa
      ON t.shelf_id = sa.shelf_id
  GROUP BY sa.business_name, t.product_id;
  CREATE INDEX idx_order_tmp_business_name_product_id
  ON feods.order_tmp (business_name, product_id);
  CREATE TEMPORARY TABLE feods.order_re_tmp AS
  SELECT
    t.business_name, t.product_id, COUNT(*) users_re
  FROM
    (SELECT
      sa.business_name, t.product_id
    FROM
      feods.order_month_tmp t
      JOIN feods.shelf_area_tmp sa
        ON t.shelf_id = sa.shelf_id
    GROUP BY sa.business_name, t.product_id, t.user_id
    HAVING COUNT(DISTINCT t.order_id) > 1) t
  GROUP BY t.business_name, t.product_id;
  CREATE INDEX idx_order_re_tmp_business_name_product_id
  ON feods.order_re_tmp (business_name, product_id);
  CREATE TEMPORARY TABLE feods.order_area_tmp AS
  SELECT
    sa.business_name, COUNT(DISTINCT t.user_id) users
  FROM
    feods.order_month_tmp t
    JOIN feods.shelf_area_tmp sa
      ON t.shelf_id = sa.shelf_id
  GROUP BY sa.business_name;
  CREATE INDEX idx_order_area_tmp_business_name
  ON feods.order_area_tmp (business_name);
  DELETE
  FROM
    feods.fjr_area_product_month
  WHERE month_id = @month_id;
  INSERT INTO feods.fjr_area_product_month (
    month_id, region_name, business_name, product_id, product_code2, product_name, second_type_id, second_type_name, product_type, gmv, quantity, discount_amount, pre_stock_num, curr_fill_num, initial_fill_num, curr_actual_stock, curr_should_stock, shelfs_sto, shelfs_sal, skudays, orders, orders_related, users, users_area, users_re, add_user
  )
  SELECT
    @month_id, b.region_name, t.business_name, t.product_id, p.product_code2, p.product_name, p.second_type_id, pt.type_name, pdh.product_type, IFNULL(o.gmv, 0), IFNULL(o.quantity, 0), IFNULL(o.discount_amount, 0), IFNULL(t.pre_stock_num, 0), IFNULL(t.curr_fill_num, 0), IFNULL(fil.actual_fill_num3, 0), IFNULL(t.curr_actual_stock, 0), IFNULL(t.curr_should_stock, 0), IFNULL(sto.shelfs, 0), IFNULL(o.shelfs, 0), IFNULL(sto.skudays, 0), IFNULL(o.orders, 0), IFNULL(o.orders_related, 0), IFNULL(o.users, 0), IFNULL(oa.users, 0) users_area, IFNULL(oe.users_re, 0), @add_user
  FROM
    feods.stat_inventory_tmp t
    LEFT JOIN feods.fil_tmp fil
      ON t.business_name = fil.business_name
      AND t.product_id = fil.product_id
    LEFT JOIN feods.sto_tmp sto
      ON t.business_name = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN feods.order_tmp o
      ON t.business_name = o.business_name
      AND t.product_id = o.product_id
    LEFT JOIN feods.order_re_tmp oe
      ON t.business_name = oe.business_name
      AND t.product_id = oe.product_id
    LEFT JOIN feods.order_area_tmp oa
      ON t.business_name = oa.business_name
    JOIN
      (SELECT DISTINCT
        b.region_name, b.business_name
      FROM
        feods.fjr_city_business b) b
      ON t.business_name = b.business_name
    JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
    LEFT JOIN fe.sf_product_type pt
      ON p.second_type_id = pt.type_id
      AND pt.data_flag = 1
    LEFT JOIN feods.zs_product_dim_sserp_his pdh
      ON t.business_name = pdh.business_area
      AND t.product_id = pdh.product_id
      AND pdh.version =
      (SELECT
        vv.version_id
      FROM
        feods.d_op_dim_date vv              -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
      WHERE vv.sdate <= @month_last_day     -- vv.min_date <= @month_last_day
        AND vv.edate > @month_last_day);    -- vv.max_date > @month_last_day)
  CALL feods.sp_task_log (
    'sp_area_product_month', @month_id, CONCAT(
      'fjr_m_051c3b929c086424044946062b53870b', @timestamp, @add_user
    )
  );
  COMMIT;
END