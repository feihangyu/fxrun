CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_fill_day_inc_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 补货订单宽表数据从fe_temp 到 fe_dwd 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_fill_day_inc_tmp1;
CREATE TEMPORARY TABLE fe_dwd.dwd_fill_day_inc_tmp1 AS
SELECT a.order_id,a.order_item_id    
FROM 
fe_temp.dwd_fill_day_inc b -- 小表   小表要放在前面作为驱动表，加快速度
 JOIN fe_dwd.dwd_fill_day_inc a   -- 大表 
    ON a.order_id = b.order_id AND a.order_item_id=b.order_item_id
    AND b.load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY);   
CREATE INDEX idx_order_item_id
ON fe_dwd.dwd_fill_day_inc_tmp1(order_id,order_item_id);
DELETE a.* FROM fe_dwd.dwd_fill_day_inc a   -- 先删除共同的部分  按照订单号删除即可
JOIN  fe_dwd.dwd_fill_day_inc_tmp1  b
    ON a.order_id = b.order_id AND a.order_item_id=b.order_item_id;
INSERT INTO fe_dwd.dwd_fill_day_inc(
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
SALES_FLAG,
PRODUCT_TYPE_NUM,
PRODUCT_NUM,
TOTAL_PRICE,
FILL_USER_ID,
FILL_USER_NAME,
FILL_AUDIT_STATUS,
FILL_AUDIT_USER_ID,
FILL_AUDIT_USER_NAME,
FILL_AUDIT_TIME,
APPLY_USER_ID,
APPLY_USER_NAME,
RECEIVER_ID,
RECEIVER_NAME,
RECEIVER_PHONE,
BACK_STOCK_TIME,
BACK_STOCK_STATUS,
ERROR_NUM,
QUALITY_STOCK_NUM,
DEFECTIVE_STOCK_NUM,
ERROR_REASON,
FILL_ITEM_AUDIT_STATUS,
AUDIT_ERROR_NUM,
STOCK_STATUS,
ADD_USER_ID,
CANCEL_REMARK,
LAST_UPDATE_TIME,
load_time)
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
SALES_FLAG,
PRODUCT_TYPE_NUM,
PRODUCT_NUM,
TOTAL_PRICE,
FILL_USER_ID,
FILL_USER_NAME,
FILL_AUDIT_STATUS,
FILL_AUDIT_USER_ID,
FILL_AUDIT_USER_NAME,
FILL_AUDIT_TIME,
APPLY_USER_ID,
APPLY_USER_NAME,
RECEIVER_ID,
RECEIVER_NAME,
RECEIVER_PHONE,
BACK_STOCK_TIME,
BACK_STOCK_STATUS,
ERROR_NUM,
QUALITY_STOCK_NUM,
DEFECTIVE_STOCK_NUM,
ERROR_REASON,
FILL_ITEM_AUDIT_STATUS,
AUDIT_ERROR_NUM,
STOCK_STATUS,
ADD_USER_ID,
CANCEL_REMARK,
LAST_UPDATE_TIME,
load_time
FROM fe_temp.dwd_fill_day_inc 
WHERE load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;   
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_fill_day_inc_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
 -- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_fill_day_inc','dwd_fill_day_inc_to_dwd','李世龙');
  COMMIT;	
END