CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_replenish_day_inc`()
BEGIN
SET @end_date = CURDATE();
SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);  -- 当前前一天
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dwd.dwd_fill_day_inc WHERE SUBSTR(apply_time,1,10) = @start_date;
## 需要同步更新dwd_update_dwd_table_info 里面的脚本
DROP TEMPORARY TABLE IF EXISTS fe_dwd.replenish_lsl_tmp;
CREATE TEMPORARY TABLE fe_dwd.replenish_lsl_tmp AS
SELECT 
a.apply_time,
a.order_id,
b.ORDER_ITEM_ID,
b.PRODUCT_ID,
a.SEND_TIME,
a.FILL_TIME,
a.fill_type,
a.FILL_RESULT,
a.SHELF_ID,
b.SHELF_DETAIL_ID,
b.actual_apply_num,
b.actual_send_num,
b.actual_sign_num,
b.actual_fill_num,
a.order_status,
a.SUPPLIER_ID,
a.supplier_type,
b.SALE_PRICE,
b.PURCHASE_PRICE,
a.audit_status,
a.surplus_reason,
a.sale_faulty_type,
b.STOCK_NUM,
b.WEEK_SALE_NUM
,a.PRODUCT_TYPE_NUM
,a.PRODUCT_NUM
,a.TOTAL_PRICE
,a.FILL_USER_ID
,a.FILL_USER_NAME
,a.FILL_AUDIT_STATUS
,a.FILL_AUDIT_USER_ID
,a.FILL_AUDIT_USER_NAME
,a.FILL_AUDIT_TIME
,a.APPLY_USER_ID
,a.APPLY_USER_NAME
,a.RECEIVER_ID
,a.RECEIVER_NAME
,a.RECEIVER_PHONE
,a.BACK_STOCK_TIME
,a.BACK_STOCK_STATUS
,b.ERROR_NUM
,b.QUALITY_STOCK_NUM
,b.DEFECTIVE_STOCK_NUM
,b.ERROR_REASON
,b.FILL_ITEM_AUDIT_STATUS
,b.AUDIT_ERROR_NUM
,b.STOCK_STATUS 
,a.ADD_USER_ID
,a.CANCEL_REMARK
,a.last_update_time
FROM
fe.sf_product_fill_order a 
JOIN fe.sf_product_fill_order_item b
ON a.order_id = b.order_id
AND a.`DATA_FLAG` = 1
AND b.`DATA_FLAG` = 1
WHERE a.apply_time  >= @start_date
    AND a.apply_time < @end_date;
	
	
CREATE INDEX idx_replenish_tmp_1
ON fe_dwd.replenish_lsl_tmp (PRODUCT_ID,SHELF_ID);
INSERT INTO fe_dwd.dwd_fill_day_inc
(
apply_time,
order_id,
ORDER_ITEM_ID,
PRODUCT_ID,
SEND_TIME,
FILL_TIME,
FILL_TYPE,
FILL_RESULT,
SHELF_ID,
SHELF_DETAIL_ID,
actual_apply_num,
actual_send_num,
actual_sign_num,
ACTUAL_FILL_NUM,
order_status,
SUPPLIER_ID,
supplier_type,
SALE_PRICE,
PURCHASE_PRICE,
audit_status,
surplus_reason,
sale_faulty_type,
STOCK_NUM,
ALARM_QUANTITY,
WEEK_SALE_NUM,
NEW_FLAG,
SALES_FLAG
,PRODUCT_TYPE_NUM
,PRODUCT_NUM
,TOTAL_PRICE
,FILL_USER_ID
,FILL_USER_NAME
,FILL_AUDIT_STATUS
,FILL_AUDIT_USER_ID
,FILL_AUDIT_USER_NAME
,FILL_AUDIT_TIME
,APPLY_USER_ID
,APPLY_USER_NAME
,RECEIVER_ID
,RECEIVER_NAME
,RECEIVER_PHONE
,BACK_STOCK_TIME
,BACK_STOCK_STATUS
,ERROR_NUM
,QUALITY_STOCK_NUM
,DEFECTIVE_STOCK_NUM
,ERROR_REASON
,FILL_ITEM_AUDIT_STATUS
,AUDIT_ERROR_NUM
,STOCK_STATUS
,ADD_USER_ID
,CANCEL_REMARK
,last_update_time
)
SELECT
a.apply_time,
a.order_id,
a.ORDER_ITEM_ID,
a.PRODUCT_ID,
a.SEND_TIME,
a.FILL_TIME,
a.fill_type,
a.FILL_RESULT,
a.SHELF_ID,
a.SHELF_DETAIL_ID,
a.actual_apply_num,
a.actual_send_num,
a.actual_sign_num,
a.actual_fill_num,
a.order_status,
a.SUPPLIER_ID,
a.supplier_type,
IFNULL(a.SALE_PRICE,f.SALE_PRICE) SALE_PRICE,
a.PURCHASE_PRICE,
a.audit_status,
a.surplus_reason,
a.sale_faulty_type,
a.STOCK_NUM,
i.ALARM_QUANTITY,
a.WEEK_SALE_NUM,
c.NEW_FLAG,
c.SALES_FLAG,
a.PRODUCT_TYPE_NUM
,a.PRODUCT_NUM
,a.TOTAL_PRICE
,a.FILL_USER_ID
,a.FILL_USER_NAME
,a.FILL_AUDIT_STATUS
,a.FILL_AUDIT_USER_ID
,a.FILL_AUDIT_USER_NAME
,a.FILL_AUDIT_TIME
,a.APPLY_USER_ID
,a.APPLY_USER_NAME
,a.RECEIVER_ID
,a.RECEIVER_NAME
,a.RECEIVER_PHONE
,a.BACK_STOCK_TIME
,a.BACK_STOCK_STATUS
,a.ERROR_NUM
,a.QUALITY_STOCK_NUM
,a.DEFECTIVE_STOCK_NUM
,a.ERROR_REASON
,a.FILL_ITEM_AUDIT_STATUS
,a.AUDIT_ERROR_NUM
,a.STOCK_STATUS
,a.ADD_USER_ID
,a.CANCEL_REMARK
,a.last_update_time
FROM fe_dwd.replenish_lsl_tmp a 
LEFT JOIN fe.sf_shelf_product_detail_flag c 
    ON a.SHELF_ID=c.SHELF_ID 
    AND a.PRODUCT_ID=c.PRODUCT_ID 
    AND c.DATA_FLAG = 1
LEFT JOIN fe.`sf_shelf_product_detail` f
    ON a.`SHELF_ID` = f.SHELF_ID
    AND a.`PRODUCT_ID` = f.PRODUCT_ID
    AND f.DATA_FLAG = 1
LEFT JOIN fe.`sf_package_item` i
    ON i.ITEM_ID= f.ITEM_ID
    AND i.DATA_FLAG = 1;
    
    
-- 删除同时超过62天的
 DELETE FROM fe_dwd.`dwd_fill_day_inc_recent_two_month` WHERE FILL_TIME < SUBDATE(CURDATE(),62) AND apply_time < SUBDATE(CURDATE(),62) AND send_TIME < SUBDATE(CURDATE(),62);
 
 
-- 删除当日新增的
  DELETE FROM fe_dwd.`dwd_fill_day_inc_recent_two_month` 
  WHERE apply_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY);
   
  DELETE FROM fe_dwd.`dwd_fill_day_inc_recent_two_month`   
    WHERE FILL_TIME >= DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    
  DELETE FROM fe_dwd.`dwd_fill_day_inc_recent_two_month`      
     WHERE send_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    
-- 添加其中一个前一天的数据    
INSERT INTO fe_dwd.dwd_fill_day_inc_recent_two_month
(
apply_time,
order_id,
ORDER_ITEM_ID,
PRODUCT_ID,
SEND_TIME,
FILL_TIME,
FILL_TYPE,
FILL_RESULT,
SHELF_ID,
SHELF_DETAIL_ID,
actual_apply_num,
actual_send_num,
actual_sign_num,
ACTUAL_FILL_NUM,
order_status,
SUPPLIER_ID,
supplier_type,
SALE_PRICE,
PURCHASE_PRICE,
audit_status,
surplus_reason,
sale_faulty_type,
STOCK_NUM,
ALARM_QUANTITY,
WEEK_SALE_NUM,
NEW_FLAG,
SALES_FLAG
,PRODUCT_TYPE_NUM
,PRODUCT_NUM
,TOTAL_PRICE
,FILL_USER_ID
,FILL_USER_NAME
,FILL_AUDIT_STATUS
,FILL_AUDIT_USER_ID
,FILL_AUDIT_USER_NAME
,FILL_AUDIT_TIME
,APPLY_USER_ID
,APPLY_USER_NAME
,RECEIVER_ID
,RECEIVER_NAME
,RECEIVER_PHONE
,BACK_STOCK_TIME
,BACK_STOCK_STATUS
,ERROR_NUM
,QUALITY_STOCK_NUM
,DEFECTIVE_STOCK_NUM
,ERROR_REASON
,FILL_ITEM_AUDIT_STATUS
,AUDIT_ERROR_NUM
,STOCK_STATUS
,ADD_USER_ID
,CANCEL_REMARK
,last_update_time
,load_time
)
SELECT
apply_time,
order_id,
ORDER_ITEM_ID,
PRODUCT_ID,
SEND_TIME,
FILL_TIME,
FILL_TYPE,
FILL_RESULT,
SHELF_ID,
SHELF_DETAIL_ID,
actual_apply_num,
actual_send_num,
actual_sign_num,
ACTUAL_FILL_NUM,
order_status,
SUPPLIER_ID,
supplier_type,
SALE_PRICE,
PURCHASE_PRICE,
audit_status,
surplus_reason,
sale_faulty_type,
STOCK_NUM,
ALARM_QUANTITY,
WEEK_SALE_NUM,
NEW_FLAG,
SALES_FLAG
,PRODUCT_TYPE_NUM
,PRODUCT_NUM
,TOTAL_PRICE
,FILL_USER_ID
,FILL_USER_NAME
,FILL_AUDIT_STATUS
,FILL_AUDIT_USER_ID
,FILL_AUDIT_USER_NAME
,FILL_AUDIT_TIME
,APPLY_USER_ID
,APPLY_USER_NAME
,RECEIVER_ID
,RECEIVER_NAME
,RECEIVER_PHONE
,BACK_STOCK_TIME
,BACK_STOCK_STATUS
,ERROR_NUM
,QUALITY_STOCK_NUM
,DEFECTIVE_STOCK_NUM
,ERROR_REASON
,FILL_ITEM_AUDIT_STATUS
,AUDIT_ERROR_NUM
,STOCK_STATUS
,ADD_USER_ID
,CANCEL_REMARK
,last_update_time
,load_time
FROM fe_dwd.dwd_fill_day_inc 
  WHERE apply_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    OR FILL_TIME >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
     OR send_time >= DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    
  --   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_replenish_day_inc',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  ); 
  
COMMIT;
END