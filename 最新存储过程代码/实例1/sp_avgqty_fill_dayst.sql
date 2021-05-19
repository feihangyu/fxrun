CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_avgqty_fill_dayst`(in_date DATE)
BEGIN
  SET @sdate := in_date, @str := '', @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_date := ADDDATE(@sdate, 1), @month_id := DATE_FORMAT(@sdate, '%Y-%m'), @day_id := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    b.region_name, b.business_name, s.shelf_id
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON b.city = s.city
  WHERE s.data_flag = 1;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp (
    KEY (
      product_id, business_name, first_fill_date
    )
  )
  SELECT
    st.region_name, st.business_name, t.product_id, DATE(f.first_fill_time) first_fill_date, COUNT(DISTINCT t.shelf_id) shelfs_sal, SUM(t.quantity_act) salqty
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN feods.shelf_tmp st
      ON st.shelf_id = t.shelf_id
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON f.shelf_id = t.shelf_id
      AND f.product_id = t.product_id
      AND f.data_flag = 1
      AND f.first_fill_time < @add_date
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_date
  GROUP BY t.product_id, st.business_name, first_fill_date;
  DROP TEMPORARY TABLE IF EXISTS feods.dayst_tmp;
  CREATE TEMPORARY TABLE feods.dayst_tmp (
    KEY (
      product_id, business_name, first_fill_date
    )
  )
  SELECT
    t.region_name, t.business_name, t.product_id, t.first_fill_date, DATEDIFF(@sdate, t.first_fill_date) ddiff, SUM(t.salqty) salqty, SUM(t.shelfs_sal) shelfs_sal
  FROM
    feods.oi_tmp t
  GROUP BY t.product_id, t.business_name, t.first_fill_date;
  DELETE
  FROM
    feods.fjr_avgqty_fill_dayst
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_avgqty_fill_dayst (
    sdate, region_name, business_name, product_id, fill_date, salqty, shelfs_sal, add_user
  )
  SELECT
    @sdate, t.region_name, t.business_name, t.product_id, t.first_fill_date, t.salqty, t.shelfs_sal, @add_user
  FROM
    feods.dayst_tmp t;
  SELECT
    COUNT(*) = 0 INTO @nrun_flag
  FROM
    feods.sf_dw_task_log t
  WHERE t.createtime > CURRENT_DATE
    AND t.task_name = 'sp_avgqty_fill_dayst';
  UPDATE
    feods.fjr_avgqty_fill_dayst_stat t
    JOIN feods.dayst_tmp d
      ON t.product_id = d.product_id
      AND t.business_name = d.business_name
      AND t.ddiff = d.ddiff SET t.salqty = t.salqty + d.salqty, t.shelfs_sal = t.shelfs_sal + d.shelfs_sal
  WHERE @nrun_flag;
  INSERT INTO feods.fjr_avgqty_fill_dayst_stat (
    region_name, business_name, product_id, ddiff, salqty, shelfs_sal, add_user
  )
  SELECT
    t.region_name, t.business_name, t.product_id, t.ddiff, t.salqty, t.shelfs_sal, @add_user add_user
  FROM
    feods.dayst_tmp t
    LEFT JOIN feods.fjr_avgqty_fill_dayst_stat d
      ON t.product_id = d.product_id
      AND t.business_name = d.business_name
      AND t.ddiff = d.ddiff
  WHERE @nrun_flag
    AND ISNULL(d.product_id);
  CALL feods.sp_task_log (
    'sp_avgqty_fill_dayst', @sdate, CONCAT(
      'fjr_d_53e2b8e4726a9f68829e9e1393269dce', @timestamp, @add_user
    )
  );
  COMMIT;
END