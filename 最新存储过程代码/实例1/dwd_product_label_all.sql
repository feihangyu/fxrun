CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_product_label_all`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
-- 防止异常报错
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_product_label_all_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_product_label_all_test LIKE fe_dwd.dwd_product_label_all;
insert into  fe_dwd.dwd_product_label_all_test
(
label_id
,label_code
,label_name
,product_id
,parent_id
,parent_label_name 
)
SELECT distinct 
a.label_id
,a.label_code
,a.label_name
,d.product_id
,a.parent_id
,b.label_name  AS parent_label_name
FROM fe.sf_product_label a
LEFT JOIN 
fe.sf_product_label b 
ON a.parent_id=b.label_id
AND b.data_flag =1
LEFT JOIN 
fe.sf_product_label_detail d
ON a.label_id=d.label_id
AND d.data_flag =1
WHERE a.data_flag =1;
truncate table fe_dwd.dwd_product_label_all;
insert into  fe_dwd.dwd_product_label_all
(
label_id
,label_code
,label_name
,product_id
,parent_id
,parent_label_name 
)
SELECT distinct
a.label_id
,a.label_code
,a.label_name
,d.product_id
,a.parent_id
,b.label_name  AS parent_label_name
FROM fe.sf_product_label a
LEFT JOIN 
fe.sf_product_label b 
ON a.parent_id=b.label_id
AND b.data_flag =1
LEFT JOIN 
fe.sf_product_label_detail d
ON a.label_id=d.label_id
AND d.data_flag =1
WHERE a.data_flag =1;
 --   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_product_label_all',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END