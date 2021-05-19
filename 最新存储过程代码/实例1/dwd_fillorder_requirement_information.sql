CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_fillorder_requirement_information`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_fillorder_requirement_information_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_fillorder_requirement_information_test LIKE fe_dwd.dwd_fillorder_requirement_information;
insert into fe_dwd.dwd_fillorder_requirement_information_test
(
`requirement_id`
,`supplier_id`
,`supplier_type`
,`supplier_name`
,`suggest_fill_num`
,`total_price`
,`weight`
,`stock_ration`
,`turn_rate`
,`category_add_num`
,`category_out_num`
,`whether_push_order`
,`requirement_item_id`
,`detail_id`
,`shelf_id`
,`product_id`
,`purchase_price`
,`onshelf_stock`
,`onway_stock`
,`max_quantity`
,`week_sale_num`
,`detail_suggest_fill_num`
,`actual_apply_num`
,`detail_weight`
)
select 
a.`requirement_id`
,a.`supplier_id`
,a.`supplier_type`
,a.`supplier_name`
,a.`suggest_fill_num`
,a.`total_price`
,a.`weight`
,a.`stock_ration`
,a.`turn_rate`
,a.`category_add_num`
,a.`category_out_num`
,a.`whether_push_order`
,b.`requirement_item_id`
,b.`detail_id`
,b.`shelf_id`
,b.`product_id`
,b.`purchase_price`
,b.`onshelf_stock`
,b.`onway_stock`
,b.`max_quantity`
,b.`week_sale_num`
,b.`suggest_fill_num` as detail_suggest_fill_num
,b.`actual_apply_num`
,b.`weight` as detail_weight
FROM fe.sf_fillorder_requirement_item b
LEFT JOIN 
fe.sf_fillorder_requirement a 
on a.requirement_id = b.requirement_id 
and a.data_flag =1 
and b.data_flag =1;
truncate table fe_dwd.dwd_fillorder_requirement_information;
insert into fe_dwd.dwd_fillorder_requirement_information
(
`requirement_id`
,`supplier_id`
,`supplier_type`
,`supplier_name`
,`suggest_fill_num`
,`total_price`
,`weight`
,`stock_ration`
,`turn_rate`
,`category_add_num`
,`category_out_num`
,`whether_push_order`
,`requirement_item_id`
,`detail_id`
,`shelf_id`
,`product_id`
,`purchase_price`
,`onshelf_stock`
,`onway_stock`
,`max_quantity`
,`week_sale_num`
,`detail_suggest_fill_num`
,`actual_apply_num`
,`detail_weight`
)
select 
a.`requirement_id`
,a.`supplier_id`
,a.`supplier_type`
,a.`supplier_name`
,a.`suggest_fill_num`
,a.`total_price`
,a.`weight`
,a.`stock_ration`
,a.`turn_rate`
,a.`category_add_num`
,a.`category_out_num`
,a.`whether_push_order`
,b.`requirement_item_id`
,b.`detail_id`
,b.`shelf_id`
,b.`product_id`
,b.`purchase_price`
,b.`onshelf_stock`
,b.`onway_stock`
,b.`max_quantity`
,b.`week_sale_num`
,b.`suggest_fill_num` as detail_suggest_fill_num
,b.`actual_apply_num`
,b.`weight` as detail_weight
FROM fe.sf_fillorder_requirement_item b
LEFT JOIN 
fe.sf_fillorder_requirement a 
ON a.requirement_id = b.requirement_id 
AND b.data_flag =1
WHERE a.data_flag =1;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_fillorder_requirement_information',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END