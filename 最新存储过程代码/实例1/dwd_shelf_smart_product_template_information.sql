CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_smart_product_template_information`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_smart_product_template_information_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_smart_product_template_information_test LIKE fe_dwd.dwd_shelf_smart_product_template_information;
insert into fe_dwd.dwd_shelf_smart_product_template_information_test
(
`template_id`
,`template_name`
,`machine_type_id`
,`third_party_template_id`
,`template_status`
,`remark`
,`product_id`
,`sale_price`
,`standard_quantity`
,`shelf_id`
,`use_status`
,`relation_remark`
)
SELECT 
a.`template_id`
,a.`template_name`
,a.`machine_type_id`
,a.`third_party_template_id`
,a.`template_status`
,a.`remark`
,IFNULL(b.`product_id`,-1) AS product_id
,b.`sale_price`
,b.`standard_quantity`
,c.`shelf_id`
,c.`use_status`
,c.remark AS `relation_remark`
FROM fe.sf_shelf_smart_product_template a
LEFT JOIN fe.sf_shelf_smart_product_template_item b 
ON a.template_id = b.template_id
AND b.data_flag = 1
LEFT JOIN fe.sf_shelf_smart_template_relation c
ON a.template_id = c.template_id
AND c.data_flag = 1
WHERE a.data_flag =1 ;
truncate table fe_dwd.dwd_shelf_smart_product_template_information;
insert into fe_dwd.dwd_shelf_smart_product_template_information
(
`template_id`
,`template_name`
,`machine_type_id`
,`third_party_template_id`
,`template_status`
,`remark`
,`product_id`
,`sale_price`
,`standard_quantity`
,`shelf_id`
,`use_status`
,`relation_remark`
)
SELECT 
a.`template_id`
,a.`template_name`
,a.`machine_type_id`
,a.`third_party_template_id`
,a.`template_status`
,a.`remark`
,ifnull(b.`product_id`,-1) as product_id
,b.`sale_price`
,b.`standard_quantity`
,c.`shelf_id`
,c.`use_status`
,c.remark AS `relation_remark`
FROM fe.sf_shelf_smart_product_template a
LEFT JOIN fe.sf_shelf_smart_product_template_item b 
ON a.template_id = b.template_id
AND b.data_flag = 1
LEFT JOIN fe.sf_shelf_smart_template_relation c
ON a.template_id = c.template_id
AND c.data_flag = 1
WHERE a.data_flag =1 ;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_smart_product_template_information',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END