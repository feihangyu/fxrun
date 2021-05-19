CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_product_day_all_update`()
BEGIN
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
-- 用于更新货架商品的标识数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_day_all_tmp;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_day_all_tmp AS 
SELECT 
a.pid,
a.DETAIL_ID,
a.ITEM_ID,
a.PRODUCT_ID,
a.SHELF_ID,
a.MAX_QUANTITY,
a.ALARM_QUANTITY,
a.STOCK_QUANTITY,
a.SALE_PRICE,
IFNULL(b.FIRST_FILL_TIME,a.FIRST_FILL_TIME) AS FIRST_FILL_TIME,
a.PURCHASE_PRICE,
IFNULL(b.manager_fill_flag,a.manager_fill_flag) AS manager_fill_flag,
a.SHELF_FILL_FLAG,
a.PACKAGE_FLAG,
IFNULL(b.NEAR_DATE,a.NEAR_DATE) AS NEAR_DATE,
IFNULL(b.DANGER_FLAG,a.DANGER_FLAG) AS DANGER_FLAG,
IFNULL(b.production_date,a.production_date) AS production_date,
IFNULL(b.risk_source,a.risk_source) AS risk_source,
IFNULL(b.SALES_FLAG,a.SALES_FLAG) AS SALES_FLAG,
IFNULL(b.NEW_FLAG,a.NEW_FLAG) AS NEW_FLAG,
IFNULL(b.NEAR_DAYS,a.NEAR_DAYS) AS NEAR_DAYS,
IFNULL(b.SALES_STATUS,a.SALES_STATUS) AS SALES_STATUS,
IFNULL(b.NEAR_DATE_SOURCE_FLAG,a.NEAR_DATE_SOURCE_FLAG) AS NEAR_DATE_SOURCE_FLAG,
IFNULL(b.operate_sale_reason,a.operate_sale_reason) AS operate_sale_reason,
IFNULL(b.business_status,a.business_status) AS business_status,
IFNULL(b.allow_fill_status,a.allow_fill_status) AS allow_fill_status,
IFNULL(b.operate_fill_status,a.operate_fill_status) AS operate_fill_status,
IFNULL(b.operate_fill_reason,a.operate_fill_reason) AS operate_fill_reason,
IFNULL(b.allow_sale_status,a.allow_sale_status) AS allow_sale_status,
IFNULL(b.operate_sale_status,a.operate_sale_status) AS operate_sale_status,
IFNULL(b.smart_fill_status,a.smart_fill_status) AS smart_fill_status,
b.add_time,
b.add_user_id,
a.load_time
FROM fe_dwd.dwd_shelf_product_day_all a 
LEFT JOIN fe.sf_shelf_product_detail_flag b 
ON a.DETAIL_ID = b.DETAIL_ID;
TRUNCATE TABLE fe_dwd.dwd_shelf_product_day_all;
INSERT INTO fe_dwd.dwd_shelf_product_day_all
(
DETAIL_ID,
ITEM_ID,
PRODUCT_ID,
SHELF_ID,
MAX_QUANTITY,
ALARM_QUANTITY,
STOCK_QUANTITY,
SALE_PRICE,
FIRST_FILL_TIME,
PURCHASE_PRICE,
MANAGER_FILL_FLAG,
SHELF_FILL_FLAG,
PACKAGE_FLAG,
NEAR_DATE,
DANGER_FLAG,
production_date,
risk_source,
SALES_FLAG,
NEW_FLAG,
NEAR_DAYS,
SALES_STATUS,
NEAR_DATE_SOURCE_FLAG,
operate_sale_reason,
business_status,
allow_fill_status,
operate_fill_status,
operate_fill_reason,
allow_sale_status,
operate_sale_status,
smart_fill_status,
add_time,
add_user_id
)
SELECT 
DETAIL_ID,
ITEM_ID,
PRODUCT_ID,
SHELF_ID,
MAX_QUANTITY,
ALARM_QUANTITY,
STOCK_QUANTITY,
SALE_PRICE,
FIRST_FILL_TIME,
PURCHASE_PRICE,
MANAGER_FILL_FLAG,
SHELF_FILL_FLAG,
PACKAGE_FLAG,
NEAR_DATE,
DANGER_FLAG,
production_date,
risk_source,
SALES_FLAG,
NEW_FLAG,
NEAR_DAYS,
SALES_STATUS,
NEAR_DATE_SOURCE_FLAG,
operate_sale_reason,
business_status,
allow_fill_status,
operate_fill_status,
operate_fill_reason,
allow_sale_status,
operate_sale_status,
smart_fill_status,
add_time,
add_user_id
FROM fe_dwd.dwd_shelf_product_day_all_tmp;
-- 执行记录日志
 CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_product_day_all_update',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
 
END