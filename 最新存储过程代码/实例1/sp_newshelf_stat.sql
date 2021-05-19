CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_newshelf_stat`()
BEGIN
  #run after sh_process.sh_zs_goods_damaged
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @threshhold := 21, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @act_date := SUBDATE(@sdate, @threshhold - 1), @add_date := ADDDATE(@sdate, 1), @month_id := DATE_FORMAT(@sdate, '%Y%m'), @if_lmonth := DATE_FORMAT(
    SUBDATE(
      @sdate, INTERVAL (DAY(@sdate) < @threshhold) MONTH
    ), '%Y%m'
  );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp, feods.oi_tmp, feods.oi_day_tmp, feods.oi_stat_tmp, feods.oi_day_stat_tmp, feods.sto_tmp, feods.fil_tmp, feods.dam_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp AS
  SELECT
    s.shelf_id, date(s.activate_time) act_date, COUNT(*) duration_days, SUM(w.if_work_day) duration_days_wd
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_work_days w
      ON w.sdate >= date(s.activate_time)
      AND w.sdate < IFNULL(s.revoke_time, @add_date)
  WHERE s.data_flag = 1
    AND s.shelf_type IN (1, 2, 3, 5)
    AND s.activate_time >= @act_date
    AND s.activate_time < @add_date
  GROUP BY s.shelf_id;
  CREATE INDEX idx_shelf_tmp_shelf_id
  ON feods.shelf_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.oi_tmp AS
  SELECT
    o.order_id, date(o.order_date) sdate, o.user_id, o.shelf_id, s.act_date, oi.product_id, oi.quantity * oi.sale_price gmv
  FROM
    fe.sf_order o
    JOIN fe.sf_order_item oi
      ON o.order_id = oi.order_id
    JOIN feods.shelf_tmp s
      ON o.shelf_id = s.shelf_id
      AND o.order_date >= s.act_date
  WHERE o.order_status = 2
    AND o.order_date < @add_date;
  CREATE INDEX idx_oi_tmp_shelf_id_product_id
  ON feods.oi_tmp (shelf_id, product_id);
  CREATE INDEX idx_oi_tmp_user_id
  ON feods.oi_tmp (user_id);
  CREATE TEMPORARY TABLE feods.oi_day_tmp AS
  SELECT
    t.sdate, t.shelf_id, t.act_date, SUM(t.users) users, SUM(t.orders) orders, SUM(t.pgmv) pgmv
  FROM
    (SELECT
      t.sdate, t.shelf_id, t.act_date, COUNT(DISTINCT t.user_id) users, COUNT(DISTINCT t.order_id) orders, SUM(t.gmv) pgmv
    FROM
      feods.oi_tmp t
    GROUP BY t.sdate, t.shelf_id
    UNION
    ALL
    SELECT
      date(a.payment_date) sdate, a.shelf_id, s.act_date, 0 users, 0 orders, SUM(a.payment_money) pgmv
    FROM
      fe.sf_after_payment a
      JOIN feods.shelf_tmp s
        ON a.shelf_id = s.shelf_id
        AND a.payment_date >= s.act_date
    WHERE a.payment_status = 2
      AND a.payment_date < @add_date
    GROUP BY sdate, a.shelf_id) t
  GROUP BY t.sdate, t.shelf_id;
  CREATE INDEX idx_oi_day_tmp_shelf_id
  ON feods.oi_day_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.oi_stat_tmp AS
  SELECT
    t.shelf_id, COUNT(DISTINCT t.user_id) users, COUNT(DISTINCT t.product_id) skus, COUNT(
      DISTINCT
      CASE
        WHEN t.sdate < ADDDATE(t.act_date, 7)
        THEN t.user_id
      END
    ) users_fw, COUNT(
      DISTINCT
      CASE
        WHEN t.sdate >= ADDDATE(t.act_date, 7)
        AND t.sdate < ADDDATE(t.act_date, 7 * 2)
        THEN t.user_id
      END
    ) users_sw
  FROM
    feods.oi_tmp t
  GROUP BY t.shelf_id;
  CREATE INDEX idx_oi_stat_tmp_shelf_id
  ON feods.oi_stat_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.oi_day_stat_tmp AS
  SELECT
    t.shelf_id, SUM(t.orders) orders, SUM(t.pgmv) pgmv, MAX(t.pgmv) pgmv_max, SUM(
      CASE
        t.sdate
        WHEN t.act_date
        THEN t.pgmv
      END
    ) pgmv_fd, SUM(
      CASE
        t.sdate
        WHEN t.act_date
        THEN t.users
      END
    ) users_fd, SUM(
      CASE
        t.sdate
        WHEN ADDDATE(t.act_date, 1)
        THEN t.pgmv
      END
    ) pgmv_sd, SUM(
      CASE
        t.sdate
        WHEN ADDDATE(t.act_date, 1)
        THEN t.users
      END
    ) users_sd, SUM(
      CASE
        WHEN t.sdate < ADDDATE(t.act_date, 7)
        THEN t.pgmv
      END
    ) pgmv_fw, SUM(
      CASE
        WHEN t.sdate >= ADDDATE(t.act_date, 7)
        AND t.sdate < ADDDATE(t.act_date, 7 * 2)
        THEN t.pgmv
      END
    ) pgmv_sw
  FROM
    feods.oi_day_tmp t
  GROUP BY t.shelf_id;
  CREATE INDEX idx_oi_day_stat_tmp_shelf_id
  ON feods.oi_day_stat_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.sto_tmp AS
  SELECT
    t.shelf_id, SUM(t.stock_quantity > 0) skus_sto, SUM(t.stock_quantity < 0) skus_nsto, SUM(t.shelf_fill_flag = 1) skus_fflag, SUM(t.stock_quantity) stock_quantity, SUM(t.stock_quantity * t.sale_price) stock_value
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
  GROUP BY t.shelf_id;
  CREATE INDEX idx_sto_tmp_shelf_id
  ON feods.sto_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.fil_tmp AS
  SELECT
    f.shelf_id, COUNT(DISTINCT DAY(f.fill_time)) days_fil, COUNT(*) orders_fil, SUM(f.product_num) product_num, SUM(f.total_price) total_price, SUM(f.fill_type = 3) > 0 if_first_fill
  FROM
    fe.sf_product_fill_order f
    JOIN feods.shelf_tmp s
      ON f.shelf_id = s.shelf_id
      AND f.fill_time >= s.act_date
  WHERE f.data_flag = 1
    AND f.order_status IN (3, 4)
    AND f.fill_time < @add_date
  GROUP BY f.shelf_id;
  CREATE INDEX idx_fil_tmp_shelf_id
  ON feods.fil_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.dam_tmp AS
  SELECT
    d.shelf_id, IFNULL(SUM(d.huosun), 0) + IFNULL(SUM(d.bk_money), 0) - IFNULL(SUM(d.total_error_value), 0) daosun_value
  FROM
    feods.pj_zs_goods_damaged d
    JOIN feods.shelf_tmp s
      ON d.shelf_id = s.shelf_id
  WHERE d.smonth IN (@month_id, @if_lmonth)
  GROUP BY d.shelf_id;
  CREATE INDEX idx_dam_tmp_shelf_id
  ON feods.dam_tmp (shelf_id);
  DELETE
  FROM
    feods.fjr_newshelf_stat
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_newshelf_stat (
    sdate, shelf_id, shelf_code, shelf_name, business_name, shelf_type, shelf_status, revoke_status, whether_close, exploit_type, activate_date, duration_days, duration_days_wd, users, orders, pgmv, pgmv_max, pgmv_fd, users_fd, pgmv_sd, users_sd, pgmv_fw, users_fw, pgmv_sw, users_sw, skus_sto, skus_fflag, skus_sal, skus_nsto, stock_quantity, stock_value, days_fil, orders_fil, product_num, total_price, daosun_value, if_first_fill, add_user
  )
  SELECT
    @sdate, s.shelf_id, s.shelf_code, s.shelf_name, b.business_name, s.shelf_type, s.shelf_status, s.revoke_status, s.whether_close, s.exploit_type, st.act_date, st.duration_days, st.duration_days_wd, IFNULL(os.users, 0), IFNULL(ods.orders, 0), IFNULL(ods.pgmv, 0), IFNULL(ods.pgmv_max, 0), IFNULL(ods.pgmv_fd, 0), IFNULL(ods.users_fd, 0), IFNULL(ods.pgmv_sd, 0), IFNULL(ods.users_sd, 0), IFNULL(ods.pgmv_fw, 0), IFNULL(os.users_fw, 0), IFNULL(ods.pgmv_sw, 0), IFNULL(os.users_sw, 0), IFNULL(sto.skus_sto, 0), IFNULL(sto.skus_fflag, 0), IFNULL(os.skus, 0), IFNULL(sto.skus_nsto, 0), IFNULL(sto.stock_quantity, 0), IFNULL(sto.stock_value, 0), IFNULL(fil.days_fil, 0), IFNULL(fil.orders_fil, 0), IFNULL(fil.product_num, 0), IFNULL(fil.total_price, 0), IFNULL(dam.daosun_value, 0), IFNULL(fil.if_first_fill, 0), @add_user
  FROM
    fe.sf_shelf s
    JOIN feods.shelf_tmp st
      ON s.shelf_id = st.shelf_id
    LEFT JOIN feods.fjr_city_business b
      ON s.city = b.city
    LEFT JOIN feods.oi_stat_tmp os
      ON s.shelf_id = os.shelf_id
    LEFT JOIN feods.oi_day_stat_tmp ods
      ON s.shelf_id = ods.shelf_id
    LEFT JOIN feods.sto_tmp sto
      ON s.shelf_id = sto.shelf_id
    LEFT JOIN feods.fil_tmp fil
      ON s.shelf_id = fil.shelf_id
    LEFT JOIN feods.dam_tmp dam
      ON s.shelf_id = dam.shelf_id;
  CALL feods.sp_task_log (
    'sp_newshelf_stat', @sdate, CONCAT(
      'fjr_d_f5b3e55324434f3b5bdbdfbb5a85e015', @timestamp, @add_user
    )
  );
  COMMIT;
END