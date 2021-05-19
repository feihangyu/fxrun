CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi3_shelf7_stosal`(in_date DATE)
BEGIN
  SET @sdate := in_date, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1), @week_flag := (WEEKDAY(@sdate) = 6), @week_start := SUBDATE(@sdate, WEEKDAY(@sdate)), @month_flag := (@sdate = LAST_DAY(@sdate)), @month_start := SUBDATE(@sdate, DAY(@sdate) - 1), @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (
    PRIMARY KEY (shelf_id), KEY (shelf_code)
  ) AS
  SELECT
    t.shelf_id, t.shelf_code, IFNULL(b.business_name, '') business_name, IFNULL(m.machine_type_id, 0) machine_type_id
  FROM
    fe.sf_shelf t
    LEFT JOIN fe.sf_shelf_machine m
      ON t.shelf_id = m.shelf_id
      AND m.data_flag = 1
    LEFT JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.psale_day_tmp;
  CREATE TEMPORARY TABLE feods.psale_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    oi.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_sale, COUNT(
      DISTINCT IF(
        oi.quantity_shipped > 0, t.shelf_id, NULL
      )
    ) shelfs_sale_shipped
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
      AND oi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.machine_type_id)
  WHERE t.order_type = 3
    AND t.order_status IN (2, 6, 7)
    AND t.order_date >= @sdate
    AND t.order_date < @add_day
    AND ! ISNULL(oi.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY oi.product_id, s.business_name, s.machine_type_id;
  DROP TEMPORARY TABLE IF EXISTS feods.psale_week_tmp;
  CREATE TEMPORARY TABLE feods.psale_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    oi.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_sale, COUNT(
      DISTINCT IF(
        oi.quantity_shipped > 0, t.shelf_id, NULL
      )
    ) shelfs_sale_shipped
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
      AND oi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.machine_type_id)
  WHERE @week_flag
    AND t.order_type = 3
    AND t.order_status IN (2, 6, 7)
    AND t.order_date >= @week_start
    AND t.order_date < @add_day
    AND ! ISNULL(oi.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY oi.product_id, s.business_name, s.machine_type_id;
  DROP TEMPORARY TABLE IF EXISTS feods.psale_month_tmp;
  CREATE TEMPORARY TABLE feods.psale_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    oi.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_sale, COUNT(
      DISTINCT IF(
        oi.quantity_shipped > 0, t.shelf_id, NULL
      )
    ) shelfs_sale_shipped
  FROM
    fe.sf_order t
    JOIN fe.sf_order_item oi
      ON t.order_id = oi.order_id
      AND oi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.machine_type_id)
  WHERE @month_flag
    AND t.order_type = 3
    AND t.order_status IN (2, 6, 7)
    AND t.order_date >= @month_start
    AND t.order_date < @add_day
    AND ! ISNULL(oi.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY oi.product_id, s.business_name, s.machine_type_id;
  DROP TEMPORARY TABLE IF EXISTS feods.psale_yht_day_tmp;
  CREATE TEMPORARY TABLE feods.psale_yht_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    oi.goods_id product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT s.shelf_id) shelfs_sale, COUNT(DISTINCT s.shelf_id) shelfs_sale_shipped
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
    JOIN feods.shelf_tmp s
      ON (
        ! ISNULL(t.shelf_id)
        AND t.shelf_id = s.shelf_id
      )
      OR (
        ISNULL(t.shelf_id)
        AND t.asset_id = s.shelf_code
      )
  WHERE t.payTime >= @sdate
    AND t.payTime < @add_day
    AND t.data_flag = 1
    AND ! ISNULL(oi.goods_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY product_id, s.business_name, s.machine_type_id;
  DROP TEMPORARY TABLE IF EXISTS feods.psale_yht_week_tmp;
  CREATE TEMPORARY TABLE feods.psale_yht_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    oi.goods_id product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT s.shelf_id) shelfs_sale, COUNT(DISTINCT s.shelf_id) shelfs_sale_shipped
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
    JOIN feods.shelf_tmp s
      ON (
        ! ISNULL(t.shelf_id)
        AND t.shelf_id = s.shelf_id
      )
      OR (
        ISNULL(t.shelf_id)
        AND t.asset_id = s.shelf_code
      )
  WHERE @week_flag
    AND t.payTime >= @week_start
    AND t.payTime < @add_day
    AND t.data_flag = 1
    AND ! ISNULL(oi.goods_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY product_id, s.business_name, s.machine_type_id;
  DROP TEMPORARY TABLE IF EXISTS feods.psale_yht_month_tmp;
  CREATE TEMPORARY TABLE feods.psale_yht_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    oi.goods_id product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT s.shelf_id) shelfs_sale, COUNT(DISTINCT s.shelf_id) shelfs_sale_shipped
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
    JOIN feods.shelf_tmp s
      ON (
        ! ISNULL(t.shelf_id)
        AND t.shelf_id = s.shelf_id
      )
      OR (
        ISNULL(t.shelf_id)
        AND t.asset_id = s.shelf_code
      )
  WHERE @month_flag
    AND t.payTime >= @month_start
    AND t.payTime < @add_day
    AND t.data_flag = 1
    AND ! ISNULL(oi.goods_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY product_id, s.business_name, s.machine_type_id;
  DELETE
    t
  FROM
    feods.psale_day_tmp t
    JOIN feods.psale_yht_day_tmp yht
      ON t.product_id = yht.product_id
      AND t.business_name = yht.business_name
      AND t.machine_type_id = yht.machine_type_id;
  DELETE
    t
  FROM
    feods.psale_week_tmp t
    JOIN feods.psale_yht_week_tmp yht
      ON t.product_id = yht.product_id
      AND t.business_name = yht.business_name
      AND t.machine_type_id = yht.machine_type_id
  WHERE @week_flag;
  DELETE
    t
  FROM
    feods.psale_month_tmp t
    JOIN feods.psale_yht_month_tmp yht
      ON t.product_id = yht.product_id
      AND t.business_name = yht.business_name
      AND t.machine_type_id = yht.machine_type_id
  WHERE @month_flag;
  INSERT INTO feods.psale_day_tmp
  SELECT
    *
  FROM
    feods.psale_yht_day_tmp;
  INSERT INTO feods.psale_week_tmp
  SELECT
    *
  FROM
    feods.psale_yht_week_tmp;
  INSERT INTO feods.psale_month_tmp
  SELECT
    *
  FROM
    feods.psale_yht_month_tmp;
  DROP TEMPORARY TABLE IF EXISTS feods.sp_stock_tmp;
  CREATE TEMPORARY TABLE feods.sp_stock_tmp (product_id INT, shelf_id INT);
  SET @sdate_tmp := @sdate;
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str1 := 'INSERT INTO feods.sp_stock_tmp(product_id,shelf_id) SELECT t.product_id,t.shelf_id FROM fe.sf_shelf_product_stock_detail t JOIN feods.shelf_tmp s ON t.shelf_id=s.shelf_id WHERE t.stat_date=@y_m_tmp AND t.day';
  SET @sql_str2 := '_quantity>0;';
  SET @sql_str2f := '_quantity>0 and @week_flag;';
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.pstock_day_tmp;
  CREATE TEMPORARY TABLE feods.pstock_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(*) shelfs_stock
  FROM
    feods.sp_stock_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  where ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id;
  SET @sdate_tmp := SUBDATE(@sdate_tmp, 1);
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2f);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sdate_tmp := SUBDATE(@sdate_tmp, 1);
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2f);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sdate_tmp := SUBDATE(@sdate_tmp, 1);
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2f);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sdate_tmp := SUBDATE(@sdate_tmp, 1);
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2f);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sdate_tmp := SUBDATE(@sdate_tmp, 1);
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2f);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sdate_tmp := SUBDATE(@sdate_tmp, 1);
  SET @y_m_tmp := DATE_FORMAT(@sdate_tmp, '%Y-%m');
  SET @d_tmp := DAY(@sdate_tmp);
  SET @sql_str := CONCAT(@sql_str1, @d_tmp, @sql_str2f);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.pstock_week_tmp;
  CREATE TEMPORARY TABLE feods.pstock_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_stock
  FROM
    feods.sp_stock_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE @week_flag
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id
  HAVING @week_flag;
  DROP TEMPORARY TABLE IF EXISTS feods.pstock_month_tmp;
  CREATE TEMPORARY TABLE feods.pstock_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(*) shelfs_stock
  FROM
    fe.sf_shelf_product_stock_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(t.product_id)
      AND ! ISNULL(s.business_name)
      AND ! ISNULL(s.machine_type_id)
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
    )
  GROUP BY t.product_id, s.business_name, s.machine_type_id
  HAVING @month_flag;
  DROP TEMPORARY TABLE IF EXISTS feods.product_day_tmp, feods.product_week_tmp, feods.product_month_tmp;
  CREATE TEMPORARY TABLE feods.product_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    feods.psale_day_tmp t
  UNION
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    feods.pstock_day_tmp t;
  CREATE TEMPORARY TABLE feods.product_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    feods.psale_week_tmp t
  UNION
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    feods.pstock_week_tmp t;
  CREATE TEMPORARY TABLE feods.product_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    feods.psale_month_tmp t
  UNION
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    feods.pstock_month_tmp t;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_product_sale_stock_day
  WHERE sdate = @sdate;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_product_sale_stock_week
  WHERE @week_flag
    AND sdate = @sdate;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_product_sale_stock_month
  WHERE @month_flag
    AND sdate = @sdate;
  INSERT INTO feods.fjr_kpi3_shelf7_product_sale_stock_day (
    sdate, product_id, business_name, machine_type_id, shelfs_sale, shelfs_sale_shipped, shelfs_stock, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, t.machine_type_id, sal.shelfs_sale, sal.shelfs_sale_shipped, sto.shelfs_stock, @add_user add_user
  FROM
    feods.product_day_tmp t
    LEFT JOIN feods.psale_day_tmp sal
      ON t.product_id = sal.product_id
      AND t.business_name = sal.business_name
      AND t.machine_type_id = sal.machine_type_id
    LEFT JOIN feods.pstock_day_tmp sto
      ON t.product_id = sto.product_id
      AND t.business_name = sto.business_name
      AND t.machine_type_id = sto.machine_type_id;
  INSERT INTO feods.fjr_kpi3_shelf7_product_sale_stock_week (
    sdate, product_id, business_name, machine_type_id, shelfs_sale, shelfs_sale_shipped, shelfs_stock, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, t.machine_type_id, sal.shelfs_sale, sal.shelfs_sale_shipped, sto.shelfs_stock, @add_user add_user
  FROM
    feods.product_week_tmp t
    LEFT JOIN feods.psale_week_tmp sal
      ON t.product_id = sal.product_id
      AND t.business_name = sal.business_name
      AND t.machine_type_id = sal.machine_type_id
    LEFT JOIN feods.pstock_week_tmp sto
      ON t.product_id = sto.product_id
      AND t.business_name = sto.business_name
      AND t.machine_type_id = sto.machine_type_id
  WHERE @week_flag;
  INSERT INTO feods.fjr_kpi3_shelf7_product_sale_stock_month (
    sdate, month_id, product_id, business_name, machine_type_id, shelfs_sale, shelfs_sale_shipped, shelfs_stock, add_user
  )
  SELECT
    @sdate sdate, @y_m month_id, t.product_id, t.business_name, t.machine_type_id, sal.shelfs_sale, sal.shelfs_sale_shipped, sto.shelfs_stock, @add_user add_user
  FROM
    feods.product_month_tmp t
    LEFT JOIN feods.psale_month_tmp sal
      ON t.product_id = sal.product_id
      AND t.business_name = sal.business_name
      AND t.machine_type_id = sal.machine_type_id
    LEFT JOIN feods.pstock_month_tmp sto
      ON t.product_id = sto.product_id
      AND t.business_name = sto.business_name
      AND t.machine_type_id = sto.machine_type_id
  WHERE @month_flag;
  DELETE
  FROM
    feods.fjr_kpi3_shelf7_monitor
  WHERE sdate = @sdate
    AND indicate_name IN ('商品动销率');
  INSERT INTO feods.fjr_kpi3_shelf7_monitor (
    indicate_type, sdate, indicate_name, indicate_value, add_user
  )
  SELECT
    t.indicate_type, @sdate sdate, '商品动销率' indicate_name, t.indicate_value, @add_user add_user
  FROM
    (SELECT
      'd' indicate_type, SUM(t.shelfs_sale_shipped) / SUM(t.shelfs_stock) indicate_value
    FROM
      feods.fjr_kpi3_shelf7_product_sale_stock_day t
    WHERE t.sdate = @sdate
    UNION
    ALL
    SELECT
      'w' indicate_type, SUM(t.shelfs_sale_shipped) / SUM(t.shelfs_stock) indicate_value
    FROM
      feods.fjr_kpi3_shelf7_product_sale_stock_week t
    WHERE @week_flag
      AND t.sdate = @sdate
    UNION
    ALL
    SELECT
      'm' indicate_type, SUM(t.shelfs_sale_shipped) / SUM(t.shelfs_stock) indicate_value
    FROM
      feods.fjr_kpi3_shelf7_product_sale_stock_month t
    WHERE @month_flag
      AND t.sdate = @sdate) t
  WHERE ! ISNULL(t.indicate_value);
  CALL feods.sp_task_log (
    'sp_kpi3_shelf7_stosal', @sdate, CONCAT(
      'fjr_d_fdacc47980022ecfcb6c844c16372936', @timestamp, @add_user
    )
  );
  COMMIT;
END