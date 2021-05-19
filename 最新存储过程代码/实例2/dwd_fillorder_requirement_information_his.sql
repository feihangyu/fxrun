CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_fillorder_requirement_information_his`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  
SET @sdate := CURRENT_DATE;
  SET @d := DAY(@sdate);
  -- 按照分区删除数据
  SET @sql_str := CONCAT(
    "ALTER TABLE fe_dwd.dwd_fillorder_requirement_information_his TRUNCATE PARTITION d",
    @d
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
 -- 插入数据
  INSERT INTO fe_dwd.dwd_fillorder_requirement_information_his (
sday
,requirement_id
,shelf_id
,supplier_id
,supplier_type
,supplier_name
,suggest_fill_num
,total_price
,weight
,stock_ration
,turn_rate
,category_add_num
,category_out_num
,whether_push_order
,requirement_item_id
,detail_id
,product_id
,purchase_price
,onshelf_stock
,onway_stock
,max_quantity
,week_sale_num
,detail_suggest_fill_num
,actual_apply_num
,detail_weight
,load_time
) 
  SELECT 
@d sday
,requirement_id
,shelf_id
,supplier_id
,supplier_type
,supplier_name
,suggest_fill_num
,total_price
,weight
,stock_ration
,turn_rate
,category_add_num
,category_out_num
,whether_push_order
,requirement_item_id
,detail_id
,product_id
,purchase_price
,onshelf_stock
,onway_stock
,max_quantity
,week_sale_num
,detail_suggest_fill_num
,actual_apply_num
,detail_weight
,load_time
FROM fe_dwd.dwd_fillorder_requirement_information;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_fillorder_requirement_information_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_fillorder_requirement_information_his','dwd_fillorder_requirement_information_his','宋英南');
END