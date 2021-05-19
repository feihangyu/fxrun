CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_automachine_slot_product_template`()
BEGIN
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_automachine_slot_product_template_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_automachine_slot_product_template_test LIKE fe_dwd.dwd_automachine_slot_product_template;
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
INSERT INTO fe_dwd.dwd_automachine_slot_product_template_test
(
`product_template_id`
,`slot_template_id`
,`template_name`
,`machine_type`
,`template_no`
,`slot_template_item_id`
,`manufacturer_slot_code`
,`product_id`
,`product_threshold`
,`actual_sale_price`
,`template_item_capacity`
)
SELECT
a.`product_template_id`
,a.`slot_template_id`
,a.`template_name`
,a.`machine_type`
,a.`template_no`
,b.`slot_template_item_id`
,b.`manufacturer_slot_code`
,b.`product_id`
,b.`product_threshold`
,b.`actual_sale_price`
,b.`template_item_capacity`
FROM fe.sf_shelf_machine_product_template_item b 
LEFT JOIN
 fe.sf_shelf_machine_product_template a
ON a.product_template_id = b.product_template_id
AND b.data_flag =1
WHERE a.data_flag =1
;
TRUNCATE TABLE fe_dwd.dwd_automachine_slot_product_template;
INSERT INTO fe_dwd.dwd_automachine_slot_product_template
(
`product_template_id`
,`slot_template_id`
,`template_name`
,`machine_type`
,`template_no`
,`slot_template_item_id`
,`manufacturer_slot_code`
,`product_id`
,`product_threshold`
,`actual_sale_price`
,`template_item_capacity`
)
SELECT
a.`product_template_id`
,a.`slot_template_id`
,a.`template_name`
,a.`machine_type`
,a.`template_no`
,b.`slot_template_item_id`
,b.`manufacturer_slot_code`
,b.`product_id`
,b.`product_threshold`
,b.`actual_sale_price`
,b.`template_item_capacity`
FROM fe.sf_shelf_machine_product_template_item b 
LEFT JOIN
 fe.sf_shelf_machine_product_template a
ON a.product_template_id = b.product_template_id
AND b.data_flag =1
WHERE a.data_flag =1
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_automachine_slot_product_template',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
COMMIT;
END