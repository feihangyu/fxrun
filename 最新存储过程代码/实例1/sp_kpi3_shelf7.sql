CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi3_shelf7`(in_sdate DATE)
BEGIN
  #run after sh_process.sh_dynamic_weighted_purchase_price
#run after sh_process.dwd_order_item_refund_day_inc
   SET @sdate := DATE(in_sdate), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @add_day := ADDDATE(@sdate, 1), @sub_day := SUBDATE(@sdate, 1), @w := WEEKDAY(@sdate), @wadj := (
      CASE
        @w
        WHEN 0
        THEN 2
        WHEN 6
        THEN 1
        ELSE 0
      END
    ) x1, @sub_day_adjweek := SUBDATE(@sub_day, @wadj), @sub_day_adjwork :=
    (SELECT
      MAX(t.sdate)
    FROM
      feods.fjr_work_days t
    WHERE t.sdate < @sdate
      AND t.if_work_day = 1) x2, @if_work_day :=
    (SELECT
      t.if_work_day
    FROM
      feods.fjr_work_days t
    WHERE t.sdate = @sdate) x3, @week_flag := (@w = 6), @week_start := SUBDATE(@sdate, WEEKDAY(@sdate)), @month_flag := (@sdate = LAST_DAY(@sdate)), @month_start := SUBDATE(@sdate, DAY(@sdate) - 1), @y_m := DATE_FORMAT(@month_start, '%Y-%m'), @d := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, t.city, t.revoke_time, t.shelf_name LIKE '%测试%' is_test
  FROM
    fe.sf_shelf t
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.price_tmp;
  CREATE TEMPORARY TABLE feods.price_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id, t.product_id, t.sale_price
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, COUNT(DISTINCT t.order_id) filltimes, SUM(fi.actual_fill_num) fill_qty, SUM(
      fi.actual_fill_num * fi.purchase_price
    ) fill_val, SUM(
      fi.actual_fill_num * p.sale_price
    ) fill_sval, SUM(
      IF(
        t.fill_type IN (5, 6, 11), fi.actual_fill_num, 0
      )
    ) fillout_qty, SUM(
      IF(
        t.fill_type IN (5, 6, 11), fi.actual_fill_num * fi.purchase_price, 0
      )
    ) fillout_val, SUM(
      IF(
        t.fill_type IN (5, 6, 11), fi.actual_fill_num * p.sale_price, 0
      )
    ) fillout_sval
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.price_tmp p
      ON t.shelf_id = p.shelf_id
      AND fi.product_id = p.product_id
  WHERE t.data_flag = 1
    AND t.order_status IN (3, 4)
    AND t.fill_time >= @sdate
    AND t.fill_time < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id
  HAVING fill_qty != 0;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_inf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_inf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, f.firstfill activate_date, DATE(IFNULL(t.revoke_time, @add_day)) revoke_date, b.business_name, t.is_test
  FROM
    feods.shelf_tmp t
    LEFT JOIN feods.d_op_shelf_firstfill f
      ON t.shelf_id = f.shelf_id
    LEFT JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.cost_tmp;
  CREATE TEMPORARY TABLE feods.cost_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    s.shelf_id, p.product_id, t.purchase_price
  FROM
    feods.wt_monthly_manual_purchase_price t
    JOIN feods.shelf_inf_tmp s
      ON t.business_area = s.business_name
    JOIN fe.sf_product p
      ON t.product_code2 = p.product_code2
      AND p.data_flag = 1
  WHERE @month_flag
    AND t.stat_month = @sdate
    AND ! ISNULL(s.shelf_id)
    AND ! ISNULL(p.product_id);
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_day
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_shelf_sale_day (
    sdate, shelf_id, orders, users, product_total_amount, discount_amount, coupon_amount, orders_shipped, users_shipped, product_total_amount_shipped, discount_amount_shipped, coupon_amount_shipped, quantity, gmv, quantity_shipped, gmv_shipped, oi_discount_amount, oi_real_total_price, oi_discount_amount_shipped, oi_real_total_price_shipped, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, COUNT(*) orders, COUNT(DISTINCT t.user_id) users, SUM(t.product_total_amount) product_total_amount, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.rate_shipped >= .99) orders_shipped, COUNT(
      DISTINCT IF(
        t.rate_shipped >= .99, t.user_id, NULL
      )
    ) users_shipped, SUM(
      t.product_total_amount * t.rate_shipped
    ) product_total_amount_shipped, SUM(
      t.discount_amount * t.rate_shipped
    ) discount_amount_shipped, SUM(t.coupon_amount * t.rate_shipped) coupon_amount_shipped, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.quantity_shipped) quantity_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.oi_discount_amount) oi_discount_amount, SUM(t.oi_real_total_price) oi_real_total_price, SUM(t.oi_discount_amount_shipped) oi_discount_amount_shipped, SUM(t.oi_real_total_price_shipped) oi_real_total_price_shipped, @add_user add_user
  FROM
    (SELECT
      t.order_id, t.shelf_id, t.user_id, t.order_status, t.product_total_amount, t.discount_amount, IFNULL(t.coupon_amount, 0) coupon_amount, SUM(oi.quantity) quantity, SUM(oi.quantity * oi.sale_price) gmv, SUM(oi.quantity_shipped) quantity_shipped, SUM(
        oi.quantity_shipped * oi.sale_price
      ) gmv_shipped, SUM(oi.discount_amount) oi_discount_amount, SUM(oi.real_total_price) oi_real_total_price, SUM(
        oi.discount_amount * oi.quantity_shipped / oi.quantity
      ) oi_discount_amount_shipped, SUM(
        oi.real_total_price * oi.quantity_shipped / oi.quantity
      ) oi_real_total_price_shipped, SUM(
        oi.quantity_shipped * oi.sale_price
      ) / SUM(oi.quantity * oi.sale_price) rate_shipped
    FROM
      fe.sf_order t
      JOIN fe.sf_order_item oi
        ON t.order_id = oi.order_id
        AND oi.data_flag = 1
    WHERE t.order_type = 3
      AND t.order_status IN (2, 6, 7)
      AND t.order_date >= @sdate
      AND t.order_date < @add_day
      AND ! ISNULL(t.shelf_id)
    GROUP BY t.order_id) t
  GROUP BY t.shelf_id
  HAVING orders > 0;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_week
  WHERE @week_flag
    AND sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_shelf_sale_week (
    sdate, shelf_id, orders, product_total_amount, discount_amount, coupon_amount, orders_shipped, product_total_amount_shipped, discount_amount_shipped, coupon_amount_shipped, quantity, gmv, quantity_shipped, gmv_shipped, oi_discount_amount, oi_real_total_price, oi_discount_amount_shipped, oi_real_total_price_shipped, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, SUM(t.orders) orders, SUM(t.product_total_amount) product_total_amount, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.orders_shipped) orders_shipped, SUM(t.product_total_amount_shipped) product_total_amount_shipped, SUM(t.discount_amount_shipped) discount_amount_shipped, SUM(t.coupon_amount_shipped) coupon_amount_shipped, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.quantity_shipped) quantity_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.oi_discount_amount) oi_discount_amount, SUM(t.oi_real_total_price) oi_real_total_price, SUM(t.oi_discount_amount_shipped) oi_discount_amount_shipped, SUM(t.oi_real_total_price_shipped) oi_real_total_price_shipped, @add_user add_user
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_day t
  WHERE @week_flag
    AND t.sdate >= @week_start
    AND t.sdate < @add_day
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_month_tmp;
  CREATE TEMPORARY TABLE feods.oi_month_tmp AS
  SELECT DISTINCT
    t.shelf_id, t.order_status, t.product_id, t.user_id
  FROM
    fe_dwd.`dwd_order_item_refund_day` t
  WHERE @month_flag
    AND t.pay_date >= @month_start
    AND t.pay_date < @add_day
    AND t.order_type = 3;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_month_user_tmp;
  CREATE TEMPORARY TABLE feods.oi_month_user_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, COUNT(DISTINCT t.user_id) users, COUNT(
      DISTINCT IF(t.order_status = 6, NULL, t.user_id)
    ) users_shipped
  FROM
    feods.oi_month_tmp t
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_month_sku_flag_tmp;
  CREATE TEMPORARY TABLE feods.oi_month_sku_flag_tmp AS
  SELECT DISTINCT
    t.shelf_id, t.product_id, 'sal' flag
  FROM
    feods.oi_month_tmp t;
  INSERT INTO feods.oi_month_sku_flag_tmp (shelf_id, product_id, flag)
  SELECT
    t.shelf_id, t.product_id, 'sto' flag
  FROM
    fe.sf_shelf_product_stock_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE @month_flag
    AND t.stat_date = @y_m
    AND (
      t.day1_quantity > 0
      OR t.day2_quantity > 0
      OR t.day3_quantity > 0
      OR t.day4_quantity > 0
      OR t.day5_quantity > 0
      OR t.day6_quantity > 0
      OR t.day7_quantity > 0
      OR t.day8_quantity > 0
      OR t.day9_quantity > 0
      OR t.day10_quantity > 0
      OR t.day11_quantity > 0
      OR t.day12_quantity > 0
      OR t.day13_quantity > 0
      OR t.day14_quantity > 0
      OR t.day15_quantity > 0
      OR t.day16_quantity > 0
      OR t.day17_quantity > 0
      OR t.day18_quantity > 0
      OR t.day19_quantity > 0
      OR t.day20_quantity > 0
      OR t.day21_quantity > 0
      OR t.day22_quantity > 0
      OR t.day23_quantity > 0
      OR t.day24_quantity > 0
      OR t.day25_quantity > 0
      OR t.day26_quantity > 0
      OR t.day27_quantity > 0
      OR t.day28_quantity > 0
      OR t.day29_quantity > 0
      OR t.day30_quantity > 0
      OR t.day31_quantity > 0
    );
  DROP TEMPORARY TABLE IF EXISTS feods.oi_month_sku_tmp;
  CREATE TEMPORARY TABLE feods.oi_month_sku_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(t.flag = 'sal') skus_sal, COUNT(DISTINCT t.product_id) skus_salsto
  FROM
    feods.oi_month_sku_flag_tmp t
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_month
  WHERE @month_flag
    AND sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_shelf_sale_month (
    month_id, sdate, shelf_id, orders, users, product_total_amount, discount_amount, coupon_amount, orders_shipped, users_shipped, product_total_amount_shipped, discount_amount_shipped, coupon_amount_shipped, quantity, gmv, quantity_shipped, gmv_shipped, oi_discount_amount, oi_real_total_price, oi_discount_amount_shipped, oi_real_total_price_shipped, add_user
  )
  SELECT
    @y_m month_id, @sdate sdate, t.shelf_id, t.orders, u.users, t.product_total_amount, t.discount_amount, t.coupon_amount, t.orders_shipped, u.users_shipped, t.product_total_amount_shipped, t.discount_amount_shipped, t.coupon_amount_shipped, t.quantity, t.gmv, t.quantity_shipped, t.gmv_shipped, t.oi_discount_amount, t.oi_real_total_price, t.oi_discount_amount_shipped, t.oi_real_total_price_shipped, @add_user add_user
  FROM
    (SELECT
      t.shelf_id, SUM(t.orders) orders, SUM(t.product_total_amount) product_total_amount, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUM(t.orders_shipped) orders_shipped, SUM(t.product_total_amount_shipped) product_total_amount_shipped, SUM(t.discount_amount_shipped) discount_amount_shipped, SUM(t.coupon_amount_shipped) coupon_amount_shipped, SUM(t.quantity) quantity, SUM(t.gmv) gmv, SUM(t.quantity_shipped) quantity_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.oi_discount_amount) oi_discount_amount, SUM(t.oi_real_total_price) oi_real_total_price, SUM(t.oi_discount_amount_shipped) oi_discount_amount_shipped, SUM(t.oi_real_total_price_shipped) oi_real_total_price_shipped
    FROM
      feods.fjr_kpi3_shelf7_shelf_sale_day t
    WHERE @month_flag
      AND t.sdate >= @month_start
      AND t.sdate < @add_day
    GROUP BY t.shelf_id) t
    LEFT JOIN feods.oi_month_user_tmp u
      ON t.shelf_id = u.shelf_id;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_fill_nday
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_fill_nday (sdate, shelf_id, add_user)
  SELECT
    @sdate sdate, t.shelf_id, @add_user add_user
  FROM
    feods.shelf_inf_tmp t;
  UPDATE
    feods.fjr_kpi3_shelf7_fill_nday t
    JOIN
      (SELECT
        t.shelf_id, COUNT(*) forders, SUM(t.send_time < @sub_day) forders_ot, SUM(t.send_time < @sub_day_adjweek) forders_ot_adjweek, SUM(t.send_time < @sub_day_adjwork) forders_ot_adjwork
      FROM
        fe.sf_product_fill_order t
        JOIN feods.shelf_inf_tmp s
          ON t.shelf_id = s.shelf_id
      WHERE t.fill_time >= @sdate
        AND t.fill_time < @add_day
      GROUP BY t.shelf_id
      HAVING forders > 0) f
      ON t.shelf_id = f.shelf_id SET t.forders = f.forders, t.forders_ot = f.forders_ot, t.forders_ot_adjweek = f.forders_ot_adjweek, t.forders_ot_adjwork = f.forders_ot_adjwork
  WHERE t.sdate = @sdate;
  UPDATE
    feods.fjr_kpi3_shelf7_fill_nday t
    JOIN
      (SELECT
        t.shelf_id, SUM(t.send_time >= @sub_day) sorders, SUM(
          t.send_time >= @sub_day && IFNULL(t.fill_time, @add_day) >= @add_day
        ) sorders_ot, SUM(
          ! @wadj && t.send_time >= @sub_day_adjweek
        ) sorders_adjweek, SUM(
          ! @wadj && t.send_time >= @sub_day_adjweek && IFNULL(t.fill_time, @add_day) >= @add_day
        ) sorders_ot_adjweek, SUM(
          @if_work_day && t.send_time >= @sub_day_adjwork
        ) sorders_adjwork, SUM(
          @if_work_day && t.send_time >= @sub_day_adjwork && IFNULL(t.fill_time, @add_day) >= @add_day
        ) sorders_ot_adjwork
      FROM
        fe.sf_product_fill_order t
        JOIN feods.shelf_inf_tmp s
          ON t.shelf_id = s.shelf_id
      WHERE t.send_time >= LEAST(
          @sub_day_adjweek, @sub_day_adjwork
        )
        AND t.send_time < @sdate
      GROUP BY t.shelf_id
      HAVING sorders > 0
        OR sorders_adjweek > 0
        OR sorders_adjwork > 0) s
      ON t.shelf_id = s.shelf_id SET t.sorders = s.sorders, t.sorders_ot = s.sorders_ot, t.sorders_adjweek = s.sorders_adjweek, t.sorders_ot_adjweek = s.sorders_ot_adjweek, t.sorders_adjwork = s.sorders_adjwork, t.sorders_ot_adjwork = s.sorders_ot_adjwork
  WHERE t.sdate = @sdate;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_fill_nday
  WHERE sdate = @sdate
    AND forders = 0
    AND sorders = 0
    AND sorders_adjweek = 0
    AND sorders_adjwork = 0;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_stock_his
  WHERE sdate = @add_day;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @d_tmp := DAY(@add_day), @y_m_tmp := DATE_FORMAT(@add_day, '%Y-%m'), @sql_str := CONCAT(
      "INSERT INTO feods.fjr_kpi3_shelf7_shelf_stock_his ( sdate, shelf_id, sto_qty, psto_qty, sto_val, psto_val, add_user ) SELECT @add_day sdate, t.shelf_id, SUM(t.day", @d_tmp, "_quantity) sto_qty, SUM( IF( t.day", @d_tmp, "_quantity > 0, t.day", @d_tmp, "_quantity, 0 ) ) psto_qty, SUM(t.day", @d_tmp, "_quantity * p.sale_price) sto_val, SUM( IF( t.day", @d_tmp, "_quantity > 0, t.day", @d_tmp, "_quantity * p.sale_price, 0 ) ) psto_val, @add_user add_user FROM fe.sf_shelf_product_stock_detail t JOIN feods.price_tmp p ON t.shelf_id = p.shelf_id AND t.product_id = p.product_id WHERE t.stat_date = @y_m_tmp group by t.shelf_id having sto_qty!=0 or psto_qty>0;"
    ) x1;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.refund_tmp;
  CREATE TEMPORARY TABLE feods.refund_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(t.refund_amount) refund_amount
  FROM
    fe.sf_order_refund_order t
  WHERE t.data_flag = 1
    AND t.refund_status = 5
    AND t.apply_time >= @sdate
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.after_tmp;
  CREATE TEMPORARY TABLE feods.after_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(t.payment_money) payment_after
  FROM
    fe.sf_after_payment t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.payment_status = 2
    AND t.payment_date >= @sdate
    AND t.payment_date < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id
  HAVING payment_after != 0;
  DROP TEMPORARY TABLE IF EXISTS feods.check_tmp;
  CREATE TEMPORARY TABLE feods.check_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(
      cd.audit_error_num * cd.sale_price
    ) audit_lost_val
  FROM
    fe.sf_shelf_check t
    JOIN fe.sf_shelf_check_detail cd
      ON t.check_id = cd.check_id
      AND cd.data_flag = 1
      AND cd.audit_status = 2
      AND cd.error_reason IN (1, 2, 4)
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.operate_time >= @sdate
    AND t.operate_time < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id
  HAVING audit_lost_val != 0;
  DROP TEMPORARY TABLE IF EXISTS feods.fault_tmp;
  CREATE TEMPORARY TABLE feods.fault_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, COUNT(*) fault, SUM(t.fault_type = 4) fault4
  FROM
    fe.sf_shelf_machine_fault t
  WHERE t.data_flag = 1
    AND t.report_time >= @sdate
    AND t.report_time < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.sku_flag_tmp;
  CREATE TEMPORARY TABLE feods.sku_flag_tmp AS
  SELECT DISTINCT
    t.shelf_id, oi.product_id, 'sal' flag
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
      AND oi.data_flag = 1
  WHERE t.order_type = 3
    AND t.order_status IN (2, 6, 7)
    AND t.order_date >= @sdate
    AND t.order_date < @add_day;
  SET @sql_str := CONCAT(
    "INSERT INTO feods.sku_flag_tmp (shelf_id, product_id, flag) SELECT t.shelf_id, t.PRODUCT_ID, 'sto' flag FROM fe.sf_shelf_product_stock_detail t JOIN feods.shelf_tmp p ON t.shelf_id = p.shelf_id WHERE t.stat_date = @y_m AND t.day", @d, "_quantity > 0;"
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.sku_tmp;
  CREATE TEMPORARY TABLE feods.sku_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(t.flag = 'sal') skus_sal, COUNT(DISTINCT t.product_id) skus_salsto
  FROM
    feods.sku_flag_tmp t
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_day
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_shelf_stat_day (
    sdate, shelf_id, sto_qty, pre_sto_qty, sto_val, pre_sto_val, filltimes, fill_qty, fill_val, fill_sval, fillout_qty, fillout_val, fillout_sval, sal_qty, gmv, sal_qty_shipped, gmv_shipped, refund_amount, payment_after, audit_lost_val, skus_sal, skus_salsto, fault, fault4, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, sto1.psto_qty sto_qty, sto0.psto_qty pre_sto_qty, sto1.psto_val sto_val, sto0.psto_val pre_sto_val, fill.filltimes, fill.fill_qty, fill.fill_val, fill.fill_sval, fill.fillout_qty, fill.fillout_val, fill.fillout_sval, sal.quantity sal_qty, sal.gmv, sal.quantity_shipped sal_qty_shipped, sal.gmv_shipped, ref.refund_amount, aft.payment_after, ck.audit_lost_val, sku.skus_sal, sku.skus_salsto, ft.fault, ft.fault4, @add_user add_user
  FROM
    feods.shelf_tmp t
    LEFT JOIN feods.fjr_kpi3_shelf7_shelf_stock_his sto1
      ON t.shelf_id = sto1.shelf_id
      AND sto1.sdate = @add_day
    LEFT JOIN feods.fjr_kpi3_shelf7_shelf_stock_his sto0
      ON t.shelf_id = sto0.shelf_id
      AND sto0.sdate = @sdate
    LEFT JOIN feods.fill_tmp fill
      ON t.shelf_id = fill.shelf_id
    LEFT JOIN feods.fjr_kpi3_shelf7_shelf_sale_day sal
      ON t.shelf_id = sal.shelf_id
      AND sal.sdate = @sdate
    LEFT JOIN feods.refund_tmp ref
      ON t.shelf_id = ref.shelf_id
    LEFT JOIN feods.after_tmp aft
      ON t.shelf_id = aft.shelf_id
    LEFT JOIN feods.check_tmp ck
      ON t.shelf_id = ck.shelf_id
    LEFT JOIN feods.sku_tmp sku
      ON t.shelf_id = sku.shelf_id
    LEFT JOIN feods.fault_tmp ft
      ON t.shelf_id = ft.shelf_id
  WHERE sto1.psto_qty != 0
    OR sto0.psto_qty != 0
    OR fill.fill_qty != 0
    OR sal.quantity != 0
    OR ref.refund_amount != 0
    OR aft.payment_after != 0
    OR ck.audit_lost_val != 0
    OR sku.skus_salsto != 0
    OR ft.fault != 0;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_week
  WHERE @week_flag
    AND sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_shelf_stat_week (
    sdate, shelf_id, sto_qty, pre_sto_qty, sto_val, pre_sto_val, fill_qty, fill_val, fill_sval, fillout_qty, fillout_val, fillout_sval, sal_qty, gmv, sal_qty_shipped, gmv_shipped, refund_amount, payment_after, audit_lost_val, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, SUM(IF(t.sdate = @sdate, t.sto_qty, 0)) sto_qty, SUM(
      IF(
        t.sdate = @week_start, t.pre_sto_qty, 0
      )
    ) pre_sto_qty, SUM(IF(t.sdate = @sdate, t.sto_val, 0)) sto_val, SUM(
      IF(
        t.sdate = @week_start, t.pre_sto_val, 0
      )
    ) pre_sto_val, SUM(t.fill_qty) fill_qty, SUM(t.fill_val) fill_val, SUM(t.fill_sval) fill_sval, SUM(t.fillout_qty) fillout_qty, SUM(t.fillout_val) fillout_val, SUM(t.fillout_sval) fillout_sval, SUM(t.sal_qty) sal_qty, SUM(t.gmv) gmv, SUM(t.sal_qty_shipped) sal_qty_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.refund_amount) refund_amount, SUM(t.payment_after) payment_after, SUM(t.audit_lost_val) audit_lost_val, @add_user add_user
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_day t
  WHERE @week_flag
    AND t.sdate >= @week_start
    AND t.sdate < @add_day
  GROUP BY t.shelf_id;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_month
  WHERE @month_flag
    AND sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_shelf_stat_month (
    sdate, month_id, shelf_id, sto_qty, pre_sto_qty, sto_val, pre_sto_val, filltimes, fill_qty, fill_val, fill_sval, fillout_qty, fillout_val, fillout_sval, sal_qty, gmv, sal_qty_shipped, gmv_shipped, refund_amount, payment_after, audit_lost_val, skus_sal, skus_salsto, add_user
  )
  SELECT
    @sdate sdate, @y_m month_id, t.shelf_id, t.sto_qty, t.pre_sto_qty, t.sto_val, t.pre_sto_val, t.filltimes, t.fill_qty, t.fill_val, t.fill_sval, t.fillout_qty, t.fillout_val, t.fillout_sval, t.sal_qty, t.gmv, t.sal_qty_shipped, t.gmv_shipped, t.refund_amount, t.payment_after, t.audit_lost_val, s.skus_sal, s.skus_salsto, @add_user add_user
  FROM
    (SELECT
      t.shelf_id, SUM(IF(t.sdate = @sdate, t.sto_qty, 0)) sto_qty, SUM(
        IF(
          t.sdate = @month_start, t.pre_sto_qty, 0
        )
      ) pre_sto_qty, SUM(IF(t.sdate = @sdate, t.sto_val, 0)) sto_val, SUM(
        IF(
          t.sdate = @month_start, t.pre_sto_val, 0
        )
      ) pre_sto_val, SUM(t.filltimes) filltimes, SUM(t.fill_qty) fill_qty, SUM(t.fill_val) fill_val, SUM(t.fill_sval) fill_sval, SUM(t.fillout_qty) fillout_qty, SUM(t.fillout_val) fillout_val, SUM(t.fillout_sval) fillout_sval, SUM(t.sal_qty) sal_qty, SUM(t.gmv) gmv, SUM(t.sal_qty_shipped) sal_qty_shipped, SUM(t.gmv_shipped) gmv_shipped, SUM(t.refund_amount) refund_amount, SUM(t.payment_after) payment_after, SUM(t.audit_lost_val) audit_lost_val
    FROM
      feods.fjr_kpi3_shelf7_shelf_stat_day t
    WHERE @month_flag
      AND t.sdate >= @month_start
      AND t.sdate < @add_day
    GROUP BY t.shelf_id) t
    LEFT JOIN feods.oi_month_sku_tmp s
      ON t.shelf_id = s.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_day_cost_tmp;
  CREATE TEMPORARY TABLE feods.shelf_day_cost_tmp (PRIMARY KEY (sdate, shelf_id)) AS
  SELECT
    DATE(t.order_date) sdate, t.shelf_id, SUM(oi.quantity * c.purchase_price) gmv_cost, SUM(
      oi.quantity_shipped * c.purchase_price
    ) gmv_cost_shipped
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
      AND oi.data_flag = 1
    JOIN feods.cost_tmp c
      ON t.shelf_id = c.shelf_id
      AND oi.product_id = c.product_id
  WHERE @month_flag
    AND t.order_type = 3
    AND t.order_status IN (2, 6, 7)
    AND t.order_date >= @month_start
    AND t.order_date < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.order_date)
  GROUP BY sdate, t.shelf_id;
  UPDATE
    feods.fjr_kpi3_shelf7_shelf_stat_day t
    JOIN feods.shelf_day_cost_tmp c
      ON t.sdate = c.sdate
      AND t.shelf_id = c.shelf_id SET t.gmv_cost = c.gmv_cost, t.gmv_cost_shipped = c.gmv_cost_shipped
  WHERE @month_flag
    AND t.sdate >= @month_start
    AND t.sdate < @add_day;
  UPDATE
    feods.fjr_kpi3_shelf7_shelf_stat_month t
    JOIN
      (SELECT
        t.shelf_id, SUM(t.gmv_cost) gmv_cost, SUM(t.gmv_cost_shipped) gmv_cost_shipped
      FROM
        feods.shelf_day_cost_tmp t
      WHERE @month_flag
      GROUP BY t.shelf_id) c
      ON t.shelf_id = c.shelf_id SET t.gmv_cost = c.gmv_cost, t.gmv_cost_shipped = c.gmv_cost_shipped
  WHERE @month_flag
    AND t.sdate = @sdate;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_monitor
  WHERE sdate = @sdate
    AND indicate_name IN (
      '自然日日机均GMV', '实收', '有销售设备占比', '日均GMV>65元设备数', '自动贩卖机投放数量', '0销设备数', '低销设备数', '次日上架率', '订单故障率', '销量故障率', '盗损金额', '盗损率', '货损金额', '退款率', '上架费', '折扣金额', '用券', '销售提成', '商品成本'
    );
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @shelfs_day := SUM(t.sdate >= @sdate), @shelfs_week := SUM(t.sdate >= @week_start), @shelfs_month := SUM(t.sdate >= @month_start), @shelf_week := COUNT(
      DISTINCT IF(
        t.sdate >= @week_start, s.shelf_id, NULL
      )
    ) x1, @shelf_month := COUNT(
      DISTINCT IF(
        t.sdate >= @month_start, s.shelf_id, NULL
      )
    ) x2
  FROM
    feods.fjr_work_days t
    JOIN feods.shelf_inf_tmp s
      ON t.sdate >= s.activate_date
      AND t.sdate < s.revoke_date
      AND ! s.is_test
  WHERE t.sdate >= LEAST(@month_start, @week_start)
    AND t.sdate < @add_day;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @shelf_add_day := SUM(t.activate_date = @sdate), @shelf_add_week := SUM(
      t.activate_date >= @week_start && t.activate_date < @add_day
    ) x1, @shelf_add_month := SUM(
      t.activate_date >= @month_start && t.activate_date < @add_day
    ) x2
  FROM
    feods.shelf_inf_tmp t
  WHERE ! t.is_test;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @indicate_value_d1 := SUM(t.gmv_shipped) / @shelfs_day x1, @indicate_value_d2 := SUM(t.product_total_amount_shipped) x2, @indicate_value_d3 := COUNT(*) / @shelfs_day x3, @indicate_value_d4 := SUM(t.gmv_shipped > 65) x4, @indicate_value_d5 := @shelfs_day - COUNT(*) x5, @indicate_value_d6 := SUM(t.gmv_shipped < 30) x6, @indicate_value_d7 := 1- SUM(t.orders_shipped) / SUM(t.orders) x7, @indicate_value_d71 := 1- SUM(t.quantity_shipped) / SUM(t.quantity) x71, @indicate_value_d8 := SUM(t.discount_amount_shipped) x8, @indicate_value_d9 := SUM(t.coupon_amount_shipped) x9
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_day t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE t.sdate = @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @indicate_value_w1 := SUM(t.gmv_shipped) / @shelfs_week x1, @indicate_value_w2 := SUM(t.product_total_amount_shipped) x2, @indicate_value_w3 := COUNT(*) / @shelf_week x3, @indicate_value_w4 := SUM(t.gmv_shipped > 65 * 7) x4, @indicate_value_w5 := @shelf_week - COUNT(*) x5, @indicate_value_w6 := SUM(t.gmv_shipped < 200) x6, @indicate_value_w7 := 1- SUM(t.orders_shipped) / SUM(t.orders) x7, @indicate_value_w71 := 1- SUM(t.quantity_shipped) / SUM(t.quantity) x71, @indicate_value_w8 := SUM(t.discount_amount_shipped) x8, @indicate_value_w9 := SUM(t.coupon_amount_shipped) x9
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_week t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE @week_flag
    AND t.sdate = @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @indicate_value_m1 := SUM(t.gmv_shipped) / @shelfs_month x1, @indicate_value_m2 := SUM(t.product_total_amount_shipped) x2, @indicate_value_m3 := COUNT(*) / @shelf_month x3, @indicate_value_m4 := SUM(t.gmv_shipped > 65 * @d) x4, @indicate_value_m5 := @shelf_month - COUNT(*) x5, @indicate_value_m6 := SUM(t.gmv_shipped < 1000) x6, @indicate_value_m7 := 1- SUM(t.orders_shipped) / SUM(t.orders) x7, @indicate_value_m71 := 1- SUM(t.quantity_shipped) / SUM(t.quantity) x71, @indicate_value_m8 := SUM(t.discount_amount_shipped) x8, @indicate_value_m9 := SUM(t.coupon_amount_shipped) x9
  FROM
    feods.fjr_kpi3_shelf7_shelf_sale_month t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE @month_flag
    AND t.sdate = @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @indicate_value_sd1 := SUM(t.refund_amount) / (
      SUM(t.refund_amount + t.gmv_shipped)
    ) x1, @indicate_value_sd2 := SUM(t.fill_qty) * .15, @indicate_value_sd3 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval
    ) x2, @indicate_value_sd4 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
    ) x3, @indicate_value_sd5 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
    ) / (
      SUM(t.gmv_shipped) + SUM(
        ABS(
          t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
        )
      )
    ) x4
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_day t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE t.sdate = @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @indicate_value_sw1 := SUM(t.refund_amount) / (
      SUM(t.refund_amount + t.gmv_shipped)
    ) x1, @indicate_value_sw2 := SUM(t.fill_qty) * .15 x2, @indicate_value_sw3 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval
    ) x3, @indicate_value_sw4 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
    ) x4, @indicate_value_sw5 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
    ) / (
      SUM(t.gmv_shipped) + SUM(
        ABS(
          t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
        )
      )
    ) x5
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_week t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE @week_flag
    AND t.sdate = @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.nuse_tmp;
  CREATE TEMPORARY TABLE feods.nuse_tmp
  SELECT
    @indicate_value_sm1 := SUM(t.refund_amount) / (
      SUM(t.refund_amount + t.gmv_shipped)
    ) x1, @indicate_value_sm2 := SUM(t.fill_qty) * .15 x2, @indicate_value_sm3 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval
    ) x3, @indicate_value_sm4 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
    ) x4, @indicate_value_sm5 := SUM(
      t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
    ) / (
      SUM(t.gmv_shipped) + SUM(
        ABS(
          t.sto_val + t.gmv_shipped - t.pre_sto_val - t.fill_sval + t.payment_after - t.audit_lost_val
        )
      )
    ) x5, @indicate_value_sm6 := SUM(
      CASE
        WHEN t.gmv_shipped <= 1e3
        THEN 0
        WHEN t.gmv_shipped <= 2e3
        THEN .02 * (t.gmv_shipped - 1e3)
        WHEN t.gmv_shipped <= 3e3
        THEN .03 * (t.gmv_shipped - 2e3) + 20
        WHEN t.gmv_shipped <= 4e3
        THEN .05 * (t.gmv_shipped - 3e3) + 50
        WHEN t.gmv_shipped <= 5e3
        THEN .06 * (t.gmv_shipped - 4e3) + 100
        ELSE .07 * (t.gmv_shipped - 5e3) + 160
      END
    ) x6, @indicate_value_sm7 := SUM(t.gmv_cost_shipped) x7
  FROM
    feods.fjr_kpi3_shelf7_shelf_stat_month t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE @month_flag
    AND t.sdate = @sdate;
  SELECT
    1- SUM(t.sorders_ot_adjweek) / SUM(t.sorders_adjweek) into @fill_not_rate_day
  FROM
    feods.fjr_kpi3_shelf7_fill_nday t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE ! @wadj
    AND t.sdate = @sdate;
  SELECT
    1- SUM(t.sorders_ot_adjweek) / SUM(t.sorders_adjweek) into @fill_not_rate_week
  FROM
    feods.fjr_kpi3_shelf7_fill_nday t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE @week_flag
    AND t.sdate BETWEEN @week_start
    AND @sdate;
  SELECT
    1- SUM(t.sorders_ot_adjweek) / SUM(t.sorders_adjweek) into @fill_not_rate_month
  FROM
    feods.fjr_kpi3_shelf7_fill_nday t
    JOIN feods.shelf_inf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! s.is_test
  WHERE @month_flag
    AND t.sdate BETWEEN @month_start
    AND @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.monitor_tmp;
  CREATE TEMPORARY TABLE feods.monitor_tmp (
    indicate_type CHAR(1), indicate_name VARCHAR (48), indicate_value DECIMAL (18, 6), PRIMARY KEY (indicate_type, indicate_name)
  );
  set @sql_str := CONCAT(
    "INSERT INTO feods.monitor_tmp(indicate_type,indicate_name,indicate_value)VALUES", "('d','自然日日机均GMV',@indicate_value_d1),('w','自然日日机均GMV',@indicate_value_w1),('m','自然日日机均GMV',@indicate_value_m1),", "('d','实收',@indicate_value_d2),('w','实收',@indicate_value_w2),('m','实收',@indicate_value_m2),", "('d','有销售设备占比',@indicate_value_d3),('w','有销售设备占比',@indicate_value_w3),('m','有销售设备占比',@indicate_value_m3),", "('d','日均GMV>65元设备数',@indicate_value_d4),('w','日均GMV>65元设备数',@indicate_value_w4),('m','日均GMV>65元设备数',@indicate_value_m4),", "('d','0销设备数',@indicate_value_d5),('w','0销设备数',@indicate_value_w5),('m','0销设备数',@indicate_value_m5),", "('d','低销设备数',@indicate_value_d6),('w','低销设备数',@indicate_value_w6),('m','低销设备数',@indicate_value_m6),", "('d','订单故障率',@indicate_value_d7),('w','订单故障率',@indicate_value_w7),('m','订单故障率',@indicate_value_m7),", "('d','销量故障率',@indicate_value_d71),('w','销量故障率',@indicate_value_w71),('m','销量故障率',@indicate_value_m71),", "('d','折扣金额',@indicate_value_d8),('w','折扣金额',@indicate_value_w8),('m','折扣金额',@indicate_value_m8),", "('d','用券',@indicate_value_d9),('w','用券',@indicate_value_w9),('m','用券',@indicate_value_m9),", "('d','自动贩卖机投放数量',@shelf_add_day),('w','自动贩卖机投放数量',@shelf_add_week),('m','自动贩卖机投放数量',@shelf_add_month),", "('d','次日上架率',@fill_not_rate_day),('w','次日上架率',@fill_not_rate_week),('m','次日上架率',@fill_not_rate_month),", "('m','销售提成',@indicate_value_sm6),", "('m','商品成本',@indicate_value_sm7),", "('d','退款率',@indicate_value_sd1),('w','退款率',@indicate_value_sw1),('m','退款率',@indicate_value_sm1),", "('d','上架费',@indicate_value_sd2),('w','上架费',@indicate_value_sw2),('m','上架费',@indicate_value_sm2),", "('d','货损金额',@indicate_value_sd3),('w','货损金额',@indicate_value_sw3),('m','货损金额',@indicate_value_sm3),", "('d','盗损金额',@indicate_value_sd4),('w','盗损金额',@indicate_value_sw4),('m','盗损金额',@indicate_value_sm4),", "('d','盗损率',@indicate_value_sd5),('w','盗损率',@indicate_value_sw5),('m','盗损率',@indicate_value_sm5);"
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  INSERT INTO feods.fjr_kpi3_shelf7_monitor (
    indicate_type, sdate, indicate_name, indicate_value, add_user
  )
  SELECT
    t.indicate_type, @sdate sdate, t.indicate_name, t.indicate_value, @add_user add_user
  FROM
    feods.monitor_tmp t
  WHERE ! ISNULL(t.indicate_value)
    AND (
      t.indicate_type = 'd'
      OR (
        @week_flag
        AND t.indicate_type = 'w'
      )
      OR (
        @month_flag
        AND t.indicate_type = 'm'
      )
    );
  CALL feods.sp_task_log (
    'sp_kpi3_shelf7', @sdate, CONCAT(
      'fjr_d_701fd1066868f2a7e472e836bcfd33e2', @timestamp, @add_user
    )
  );
  COMMIT;
END