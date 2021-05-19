CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_order_item_twomonth`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  TRUNCATE TABLE feods.wt_order_item_twomonth_temp;
  INSERT INTO feods.wt_order_item_twomonth_temp (
    ORDER_ITEM_ID,
    ORDER_ID,
    SHELF_ID,
    SHELF_ID_SHARD,
    SUPPLIER_ID,
    PRODUCT_ID,
    QUANTITY,
    COST_PRICE,
    PURCHASE_PRICE,
    SALE_PRICE,
    DISCOUNT_AMOUNT,
    REAL_TOTAL_PRICE,
    PRODUCT_NAME,
    LIMIT_BUY_ID,
    ORDER_DATE,
    order_status
  )
  SELECT
    t1.ORDER_ITEM_ID,
    t1.ORDER_ID,
    t2.SHELF_ID,
    t1.SHELF_ID_SHARD,
    t1.SUPPLIER_ID,
    t1.PRODUCT_ID,
    CASE
      WHEN t2.order_status = 2
      THEN t1.quantity
      ELSE t1.quantity_shipped
    END AS quantity,
    t1.COST_PRICE,
    t1.PURCHASE_PRICE,
    t1.SALE_PRICE,
    t1.DISCOUNT_AMOUNT,
    t1.REAL_TOTAL_PRICE,
    t1.PRODUCT_NAME,
    t1.LIMIT_BUY_ID,
    t2.pay_date AS order_date,
    t2.order_status
  FROM
    fe.sf_order_item t1,
    fe.sf_order t2
  WHERE t2.order_id = t1.order_id
    AND t2.pay_date >= DATE_SUB(CURDATE(), INTERVAL 62 DAY)
    AND t2.pay_date < CURDATE()
    AND t2.order_status IN (2, 6, 7);
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_order_item_twomonth',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));   
 
  COMMIT;
END