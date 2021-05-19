CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_pub_activity_order_shelf_product_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 消费端活动宽表数据从fe_temp 到 fe_dwd ,因为没有其他任务修改此表的数据，所以直接插入当天新增的数据即可
DELETE FROM fe_dwd.dwd_pub_activity_order_shelf_product WHERE load_time>=CURRENT_DATE;
INSERT INTO fe_dwd.dwd_pub_activity_order_shelf_product(
activity_id,
activity_name,
cost_dept,
platform,
platform_business_type,
discount_type,
discount_name,
discount_value,
activity_type,
start_date,
end_date,
order_id,
ORDER_ITEM_ID,
pay_date,
shelf_id,
product_id,
quantity,
quantity_act,
sale_price,
DISCOUNT_AMOUNT,
load_time)
SELECT 
activity_id,
activity_name,
cost_dept,
platform,
platform_business_type,
discount_type,
discount_name,
discount_value,
activity_type,
start_date,
end_date,
order_id,
ORDER_ITEM_ID,
pay_date,
shelf_id,
product_id,
quantity,
quantity_act,
sale_price,
DISCOUNT_AMOUNT,
load_time
FROM fe_temp.dwd_pub_activity_order_shelf_product 
WHERE load_time >= CURRENT_DATE ;   -- 修复数据，从02-27号开始：此处需要改为 load_time>=subdate(current_date,interval 3 day) and load_time<subdate(current_date,interval 2 day)
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_pub_activity_order_shelf_product_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pub_activity_order_shelf_product','dwd_pub_activity_order_shelf_product_to_dwd','李世龙');
 
  COMMIT;	
END