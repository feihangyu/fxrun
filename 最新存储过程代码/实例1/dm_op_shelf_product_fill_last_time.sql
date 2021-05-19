CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_shelf_product_fill_last_time`()
BEGIN
   SET @end_date = CURDATE();
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_fill_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_fill_tmp (
KEY idx_shelf_id_product_id (shelf_id,product_id)
) AS
SELECT 
    SHELF_ID,
    PRODUCT_ID,
    MAX(FILL_TIME) AS FILL_TIME,
    SUBSTRING_INDEX(GROUP_CONCAT(FILL_TYPE ORDER BY fill_time DESC),',',1) AS FILL_TYPE,
    SUBSTRING_INDEX(GROUP_CONCAT(ACTUAL_FILL_NUM ORDER BY fill_time DESC),',',1) AS ACTUAL_FILL_NUM,
    SUBSTRING_INDEX(GROUP_CONCAT(supplier_type ORDER BY fill_time DESC),',',1) AS supplier_type,
    SUBSTRING_INDEX(GROUP_CONCAT(SALE_PRICE ORDER BY fill_time DESC),',',1) AS SALE_PRICE,
    SUBSTRING_INDEX(GROUP_CONCAT(PURCHASE_PRICE ORDER BY fill_time DESC),',',1) AS PURCHASE_PRICE,
	SUBSTRING_INDEX(GROUP_CONCAT(apply_time ORDER BY fill_time DESC SEPARATOR ","),",",1) AS apply_time,
	SUBSTRING_INDEX(GROUP_CONCAT(WEEK_SALE_NUM ORDER BY fill_time DESC SEPARATOR ","),",",1) AS WEEK_SALE_NUM,
	SUBSTRING_INDEX(GROUP_CONCAT(SUPPLIER_ID ORDER BY fill_time DESC SEPARATOR ","),",",1) AS SUPPLIER_ID,
    SUBSTRING_INDEX(GROUP_CONCAT(STOCK_NUM ORDER BY fill_time DESC),',',1) AS STOCK_NUM
FROM
    `fe_dwd`.`dwd_fill_day_inc`
WHERE FILL_TYPE IN (1,2,3,4,7,8,9)
    AND order_status = 4
    AND fill_time < @end_date
    AND fill_time >= @start_date
GROUP BY shelf_id,product_id
;
-- 更新历史数据
UPDATE fe_dm.dm_op_shelf_product_fill_last_time a
JOIN fe_dwd.shelf_product_fill_tmp b
ON a.shelf_id = b.shelf_id AND a.product_id = b.product_id
SET a.FILL_TIME = b.FILL_TIME,
    a.FILL_TYPE = b.FILL_TYPE,
    a.ACTUAL_FILL_NUM = b.ACTUAL_FILL_NUM,
    a.supplier_type = b.supplier_type,
    a.SALE_PRICE = b.SALE_PRICE,
    a.PURCHASE_PRICE = b.PURCHASE_PRICE,
    a.STOCK_NUM = b.STOCK_NUM,
	a.apply_time = b.apply_time,
	a.WEEK_SALE_NUM = b.WEEK_SALE_NUM,
	a.SUPPLIER_ID = b.SUPPLIER_ID,
	a.load_time = @run_date
;
-- 插入新增的货架商品
INSERT INTO fe_dm.dm_op_shelf_product_fill_last_time
(
    SHELF_ID,
    PRODUCT_ID,
    FILL_TIME,
	apply_time,
    FILL_TYPE,
    ACTUAL_FILL_NUM,
    supplier_type,
    SALE_PRICE,
    PURCHASE_PRICE,
    STOCK_NUM,
	WEEK_SALE_NUM,
	SUPPLIER_ID
)
SELECT
    a.SHELF_ID,
    a.PRODUCT_ID,
    a.FILL_TIME,
	a.apply_time,
    a.FILL_TYPE,
    a.ACTUAL_FILL_NUM,
    a.supplier_type,
    a.SALE_PRICE,
    a.PURCHASE_PRICE,
    a.STOCK_NUM,
	a.WEEK_SALE_NUM,
	a.SUPPLIER_ID
FROM
    fe_dwd.shelf_product_fill_tmp a
    LEFT JOIN fe_dm.dm_op_shelf_product_fill_last_time b
        ON a.shelf_id = b.shelf_id
        AND a.product_id = b.product_id
WHERE b.shelf_id IS NULL
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_op_shelf_product_fill_last_time',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
COMMIT;
END