CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_product_last_status`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 找出历史和最新的交叉的货架商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_last_status_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_last_status_1 
(PRIMARY KEY idx_shelf_product(SHELF_ID, PRODUCT_ID)) AS
SELECT 
a.SHELF_ID,a.PRODUCT_ID
FROM 
fe_dwd.dwd_shelf_product_last_status a
JOIN 
fe_dwd.dwd_shelf_product_day_all b
ON a.SHELF_ID = b.SHELF_ID
AND a.PRODUCT_ID = b.PRODUCT_ID;
-- 删除交叉的货架商品
DELETE a.* FROM fe_dwd.dwd_shelf_product_last_status a 
JOIN fe_dwd.dwd_shelf_product_last_status_1 b
ON a.SHELF_ID = b.SHELF_ID
and a.PRODUCT_ID = b.PRODUCT_ID;  
-- 找出新增的货架商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_last_status_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_last_status_2
(PRIMARY KEY idx_shelf_product_2(SHELF_ID, PRODUCT_ID)) AS
SELECT 
t.SHELF_ID,
t.PRODUCT_ID
FROM 
(
SELECT 
a.SHELF_ID,a.PRODUCT_ID,b.PRODUCT_ID PRODUCT_ID_2
FROM 
fe_dwd.dwd_shelf_product_day_all a
LEFT JOIN 
fe_dwd.dwd_shelf_product_last_status b
ON a.SHELF_ID = b.SHELF_ID
AND a.PRODUCT_ID = b.PRODUCT_ID
) t
WHERE t.PRODUCT_ID_2 IS NULL ;
-- 插入最新状态的货架商品信息
INSERT INTO fe_dwd.dwd_shelf_product_last_status
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
smart_fill_status
)
SELECT 
a.DETAIL_ID,
a.ITEM_ID,
a.PRODUCT_ID,
a.SHELF_ID,
a.MAX_QUANTITY,
a.ALARM_QUANTITY,
a.STOCK_QUANTITY,
a.SALE_PRICE,
a.FIRST_FILL_TIME,
a.PURCHASE_PRICE PURCHASE_PRICE,
a.manager_fill_flag,
a.SHELF_FILL_FLAG,
a.PACKAGE_FLAG,
a.NEAR_DATE,
a.DANGER_FLAG,
a.production_date,
a.risk_source,
a.SALES_FLAG,
a.NEW_FLAG,
a.NEAR_DAYS,
a.SALES_STATUS,
a.NEAR_DATE_SOURCE_FLAG,
a.operate_sale_reason,
a.business_status,
a.allow_fill_status,
a.operate_fill_status,
a.operate_fill_reason,
a.allow_sale_status,
a.operate_sale_status,
a.smart_fill_status
FROM
fe_dwd.dwd_shelf_product_day_all a
JOIN
fe_dwd.dwd_shelf_product_last_status_2 b
ON a.SHELF_ID = b.SHELF_ID
AND a.PRODUCT_ID = b.PRODUCT_ID
;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_product_last_status',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_last_status','dwd_shelf_product_last_status','李世龙');
  COMMIT;	
END