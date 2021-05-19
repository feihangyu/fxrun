CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_product_price_tag`()
BEGIN
  #run after sh_process.sp_subtype_price_salqty_week
   SET @week_end := SUBDATE(
    CURRENT_DATE, DAYOFWEEK(CURRENT_DATE) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @week_add := ADDDATE(@week_end, 1), @add_day := CURRENT_DATE;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.shelf_id, b.business_name
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1
    AND s.shelf_status = 2 #AND s.shelf_type NOT IN (6, 7)
     AND ! ISNULL(s.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.wgmv_tmp;
  CREATE TEMPORARY TABLE feods.wgmv_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, t.sale_price, SUM(t.sale_price * t.salqty) gmv
  FROM
    feods.fjr_product_price_salqty t
  GROUP BY t.business_name, t.product_id, t.sale_price;
  INSERT INTO feods.wgmv_tmp (
    business_name, product_id, sale_price, gmv
  )
  SELECT
    s.business_name, oi.product_id, oi.sale_price, SUM(oi.quantity * oi.sale_price) gmv
  FROM
    fe.sf_order o
    JOIN fe.sf_order_item oi
      ON o.order_id = oi.order_id
    JOIN feods.shelf_tmp s
      ON o.shelf_id = s.shelf_id
  WHERE o.order_status = 2
    AND o.order_date >= @week_add
    AND o.order_date < @add_day
  GROUP BY s.business_name, oi.product_id, oi.sale_price;
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_tmp;
  CREATE TEMPORARY TABLE feods.gmv_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, t.sale_price, SUM(t.gmv) gmv
  FROM
    feods.wgmv_tmp t
  GROUP BY t.business_name, t.product_id, t.sale_price;
  DROP TEMPORARY TABLE IF EXISTS feods.max_gmv_tmp;
  CREATE TEMPORARY TABLE feods.max_gmv_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_name, t.product_id, MAX(t.gmv) mgmv
  FROM
    feods.gmv_tmp t
  where ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.rec_price_tmp;
  CREATE TEMPORARY TABLE feods.rec_price_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_name, t.product_id, MAX(t.sale_price) rec_price
  FROM
    feods.gmv_tmp t
    JOIN feods.max_gmv_tmp m
      ON t.business_name = m.business_name
      AND t.product_id = m.product_id
      AND t.gmv = m.mgmv
  where ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_product_tmp;
  CREATE TEMPORARY TABLE feods.shelf_product_tmp (
    PRIMARY KEY (shelf_id, product_id), KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.shelf_id, t.product_id, pm.package_id, t.sale_price
  FROM
    fe.sf_shelf_product_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe.sf_package_item pm
      ON t.item_id = pm.item_id
      AND pm.data_flag = 1
  WHERE t.data_flag = 1
    and ! ISNULL(s.shelf_id)
    AND ! ISNULL(t.product_id)
    AND t.stock_quantity > 0;
  DROP TEMPORARY TABLE IF EXISTS feods.area_price_tmp;
  CREATE TEMPORARY TABLE feods.area_price_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, t.sale_price, COUNT(1) ct
  FROM
    feods.shelf_product_tmp t
  GROUP BY t.business_name, t.product_id, t.sale_price;
  DROP TEMPORARY TABLE IF EXISTS feods.max_count_tmp;
  CREATE TEMPORARY TABLE feods.max_count_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, MAX(t.ct) ct
  FROM
    feods.area_price_tmp t
  GROUP BY t.business_name, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.most_price_tmp;
  CREATE TEMPORARY TABLE feods.most_price_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_name, t.product_id, MAX(t.sale_price) most_price
  FROM
    feods.area_price_tmp t
    JOIN feods.max_count_tmp m
      ON t.business_name = m.business_name
      AND t.product_id = m.product_id
      AND t.ct = m.ct
  where ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
  TRUNCATE TABLE feods.fjr_shelf_product_price_tag;
  INSERT INTO feods.fjr_shelf_product_price_tag (
    business_name, shelf_id, product_id, PACKAGE_ID, sale_price, rec_price, most_price, price_tag, add_user
  )
  SELECT
    t.business_name, t.shelf_id, t.product_id, t.PACKAGE_ID, t.sale_price, rp.rec_price, mp.most_price, 2 * (t.sale_price = mp.most_price) + (t.sale_price > rp.rec_price) price_tag, @add_user add_user
  FROM
    feods.shelf_product_tmp t
    JOIN feods.rec_price_tmp rp
      ON t.business_name = rp.business_name
      AND t.product_id = rp.product_id
      AND t.sale_price != rp.rec_price
    JOIN feods.most_price_tmp mp
      ON t.business_name = mp.business_name
      AND t.product_id = mp.product_id;
  CALL feods.sp_task_log (
    'sp_shelf_product_price_tag', @week_end, CONCAT(
      'fjr_w_b89111b127ed3a784ebb007e4eb31ac4', @timestamp, @add_user
    )
  );
  COMMIT;
END