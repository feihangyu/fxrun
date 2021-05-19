CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_effective_stock`()
BEGIN
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @high_tot := 6;
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_start_last := SUBDATE(@month_start, INTERVAL 1 MONTH), @month_start_last2 := SUBDATE(@month_start, INTERVAL 2 MONTH), @month_start_last3 := SUBDATE(@month_start, INTERVAL 3 MONTH);
  SET @y_m := DATE_FORMAT(@month_start, '%Y-%m'), @y_m_last := DATE_FORMAT(@month_start_last, '%Y-%m'), @y_m_last2 := DATE_FORMAT(@month_start_last2, '%Y-%m'), @y_m_last3 := DATE_FORMAT(@month_start_last3, '%Y-%m');
  SET @ym := DATE_FORMAT(@month_start, '%Y%m'), @ym_last := DATE_FORMAT(@month_start_last, '%Y%m'), @ym_last2 := DATE_FORMAT(@month_start_last2, '%Y%m'), @ym_last3 := DATE_FORMAT(@month_start_last3, '%Y%m');
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_tot_stat TRUNCATE PARTITION p", @ym_last3, ",p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_tot_stat(month_id, shelf_id, product_id, tot, qty_sal, days_sal, days_tot, add_user)SELECT @y_m month_id,shelf_id,product_id,tot,SUM(sal)qty_sal,SUM(sal>0)days_sal,COUNT(*)days_tot,@add_user add_user FROM(", GROUP_CONCAT(
        "SELECT shelf_id,product_id,s", number, " sal,t", number, " tot FROM feods.d_op_sp_sal_sto_detail WHERE month_id =@y_m AND t", number, ">0" SEPARATOR ' union all '
      ), " )t GROUP BY shelf_id,product_id,tot"
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.dst_tmp;
  CREATE TEMPORARY TABLE feods.dst_tmp (KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id, tot, SUM(days_tot) days_tot, SUM(days_sal) days_sal, SUM(qty_sal) qty_sal
  FROM
    feods.d_op_tot_stat
  GROUP BY shelf_id, product_id, tot;
  DROP TEMPORARY TABLE IF EXISTS feods.dst_ct_tmp;
  CREATE TEMPORARY TABLE feods.dst_ct_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id, COUNT(*) ct, CEILING(COUNT(*) / 3) ct3, SUM(days_tot) days_tott, SUM(days_sal) days_salt, SUM(qty_sal) qty_salt, SUM(tot * days_tot) qty_tott, SUM(IF(tot >= @high_tot, days_tot, 0)) days_toth, SUM(IF(tot >= @high_tot, days_sal, 0)) days_salh, SUM(IF(tot >= @high_tot, qty_sal, 0)) qty_salh, SUM(
      IF(tot >= @high_tot, tot * days_tot, 0)
    ) qty_toth
  FROM
    feods.dst_tmp
  GROUP BY shelf_id, product_id;
  SET @shelf_id := NULL, @product_id := NULL, @order_num := NULL;
  DROP TEMPORARY TABLE IF EXISTS feods.dst_re_tmp;
  CREATE TEMPORARY TABLE feods.dst_re_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, MIN(t.tot) effective_stock
  FROM
    (SELECT
      @order_num := IF(
        @shelf_id = t.shelf_id && @product_id = t.product_id, @order_num, 0
      ) + 1 order_num, @shelf_id := t.shelf_id shelf_id, @product_id := t.product_id product_id, t.tot, t.avgsal
    FROM
      (SELECT
        shelf_id, product_id, tot, qty_sal / days_tot avgsal
      FROM
        feods.dst_tmp
      ORDER BY shelf_id, product_id, avgsal DESC) t) t
    JOIN feods.dst_ct_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
      AND t.order_num <= d.ct3
  GROUP BY t.shelf_id, t.product_id;
  TRUNCATE feods.d_op_effective_stock;
  INSERT INTO feods.d_op_effective_stock (
    shelf_id, product_id, effective_stock, qty_sal, qty_tot, days_sal, days_tot, qty_salh, qty_toth, days_salh, days_toth, tots, qty_salt, qty_tott, days_salt, days_tott, add_user
  )
  SELECT
    t.shelf_id, t.product_id, r.effective_stock, d.qty_sal, d.tot * d.days_tot qty_tot, d.days_sal, d.days_tot, t.qty_salh, t.qty_toth, t.days_salh, t.days_toth days_toth, t.ct tots, t.qty_salt, t.qty_tott, t.days_salt, t.days_tott days_tott, @add_user add_user
  FROM
    feods.dst_ct_tmp t
    JOIN feods.dst_re_tmp r
      ON t.shelf_id = r.shelf_id
      AND t.product_id = r.product_id
    JOIN feods.dst_tmp d
      ON r.shelf_id = d.shelf_id
      AND r.product_id = d.product_id
      AND r.effective_stock = d.tot;
  CALL feods.sp_task_log (
    'sp_op_effective_stock', @sdate, CONCAT(
      'yingnansong_d_f89af2c14cabcf521e9638c65f378957', @timestamp, @add_user
    )
  );
  COMMIT;
END