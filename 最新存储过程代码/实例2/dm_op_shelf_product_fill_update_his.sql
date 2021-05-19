CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_product_fill_update_his`()
BEGIN
SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
delete from fe_dm.dm_op_shelf_product_fill_update_his where cdate = @run_date or cdate < subdate(@run_date,interval 30 day);
insert into fe_dm.dm_op_shelf_product_fill_update_his
(ID,
cdate,
PRODUCT_ID,
SHELF_ID,
NEW_FLAG,
SALES_FLAG,
FILL_MODEL,
ALARM_QUANTITY,
STOCK_NUM,
ONWAY_NUM,
warehouse_stock,
whether_push_order,
fill_cycle,
fill_days,
day_sale_qty,
safe_stock_qty,
shelf_stock_upper_limit,
stock_total_qty,
suspect_false_stock_qty,
SUGGEST_FILL_NUM,
reduce_suggest_fill_num,
reduce_suggest_fill_ceiling_num,
add_time,
last_update_time)
select 
ID,
cdate,
PRODUCT_ID,
SHELF_ID,
NEW_FLAG,
SALES_FLAG,
FILL_MODEL,
ALARM_QUANTITY,
STOCK_NUM,
ONWAY_NUM,
warehouse_stock,
whether_push_order,
fill_cycle,
fill_days,
day_sale_qty,
safe_stock_qty,
shelf_stock_upper_limit,
stock_total_qty,
suspect_false_stock_qty,
SUGGEST_FILL_NUM,
reduce_suggest_fill_num,
reduce_suggest_fill_ceiling_num,
add_time,
last_update_time
from fe_dm.dm_op_shelf_product_fill_update;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_shelf_product_fill_update_his',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('宋英南@', @user), @stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_product_fill_update_his','dm_op_shelf_product_fill_update_his','宋英南');
END