CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_area_product_pq4`()
BEGIN
  #run after sh_process.sp_subtype_price_salqty_week
   SET @week_end := SUBDATE(
    CURRENT_DATE, DAYOFWEEK(CURRENT_DATE) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.price_sal_qty_tmp;
  CREATE TEMPORARY TABLE feods.price_sal_qty_tmp AS
  SELECT
    t.business_name, t.product_id, t.sale_price, SUM(t1.salqty) salqty
  FROM
    feods.fjr_product_price_salqty t
    JOIN feods.fjr_product_price_salqty t1
      ON t.business_name = t1.business_name
      AND t.product_id = t1.product_id
      AND t.sale_price >= t1.sale_price
      AND t1.week_end = @week_end
  WHERE t.week_end = @week_end
  GROUP BY t.business_name, t.product_id, t.sale_price;
  DROP TEMPORARY TABLE IF EXISTS feods.max_tmp;
  CREATE TEMPORARY TABLE feods.max_tmp AS
  SELECT
    t.business_name, t.product_id, MAX(t.salqty) salqty
  FROM
    feods.price_sal_qty_tmp t
  GROUP BY t.business_name, t.product_id;
  TRUNCATE TABLE feods.fjr_area_product_pq4;
  INSERT INTO feods.fjr_area_product_pq4 (
    business_name, product_id, sale_price, add_user
  )
  SELECT
    t.business_name, t.product_id, MIN(t.sale_price) sale_price, @add_user add_user
  FROM
    feods.price_sal_qty_tmp t
    JOIN feods.max_tmp m
      ON t.business_name = m.business_name
      AND t.product_id = m.product_id
      AND t.salqty >= m.salqty * .25
  GROUP BY t.business_name, t.product_id;
  CALL feods.sp_task_log (
    'sp_area_product_pq4', @week_end, CONCAT(
      'fjr_w_74869a1a855822593ea21f4e8a97edb9', @timestamp, @add_user
    )
  );
  COMMIT;
END