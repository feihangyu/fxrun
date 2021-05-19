CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_city_unsalable_his`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  delete
  from
    feods.pj_city_unsalable_his
  where stat_date = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    );
  INSERT INTO feods.pj_city_unsalable_his (
    stat_date,
    city,
    city_name,
    product_id,
    product_name,
    sum_amount
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ),
    t3.city,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(t3.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city_name,
    t1.PRODUCT_ID,
    p.PRODUCT_NAME,
    SUM(
      t1.SALE_PRICE * t1.STOCK_QUANTITY
    ) sum_amount
  FROM
    fe.sf_shelf_product_detail t1,
    fe.sf_shelf_product_detail_flag t2,
    fe.sf_shelf t3,
    fe.sf_product p
  WHERE t1.SHELF_ID = t2.SHELF_ID
    AND t1.PRODUCT_ID = t2.PRODUCT_ID
    AND t1.product_id = p.product_id
    AND t1.SHELF_ID = t3.SHELF_ID
    AND t1.DATA_FLAG = 1
    AND t2.DATA_FLAG = 1
    AND t2.SALES_FLAG = 5
  GROUP BY DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ),
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(t3.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ),
    t1.PRODUCT_ID,
    p.PRODUCT_NAME
  HAVING SUM(
      t1.SALE_PRICE * t1.STOCK_QUANTITY
    ) != 0;
    
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_city_unsalable_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));    
    
  COMMIT;
END