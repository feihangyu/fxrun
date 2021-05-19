CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_pub_school_shelf_infornation`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
-- 防止异常报错
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_pub_school_shelf_infornation_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_school_shelf_infornation_test LIKE fe_dwd.dwd_pub_school_shelf_infornation;
insert into  fe_dwd.dwd_pub_school_shelf_infornation_test
(
`campus_id`
,`campus_code`
,`campus_name`
,`school_id`
,`address_id`
,`school_name`
,`school_level`
,`shelf_id`
,`dormitory_area`
,`dormitory_dong`
,`room_no`
,`delivery_area`
)
SELECT 
a.`campus_id`
,a.`campus_code`
,a.`campus_name`
,a.`school_id`
,a.`address_id`
,b.`school_name`
,b.`school_level`
,c.`shelf_id`
,c.`dormitory_area`
,c.`dormitory_dong`
,c.`room_no`
,c.`delivery_area`
FROM fe.sf_pub_school_campus a
LEFT JOIN fe.sf_pub_school b
ON a.school_id = b.school_id 
AND b.data_flag =1
LEFT JOIN fe.`sf_shelf_campus` c
ON a.campus_id = c.campus_id
AND c.data_flag =1
WHERE a.data_flag =1;
	
truncate table fe_dwd.dwd_pub_school_shelf_infornation;
insert into  fe_dwd.dwd_pub_school_shelf_infornation
(
`campus_id`
,`campus_code`
,`campus_name`
,`school_id`
,`address_id`
,`school_name`
,`school_level`
,`shelf_id`
,`dormitory_area`
,`dormitory_dong`
,`room_no`
,`delivery_area`
)
SELECT 
a.`campus_id`
,a.`campus_code`
,a.`campus_name`
,a.`school_id`
,a.`address_id`
,b.`school_name`
,b.`school_level`
,c.`shelf_id`
,c.`dormitory_area`
,c.`dormitory_dong`
,c.`room_no`
,c.`delivery_area`
FROM fe.sf_pub_school_campus a
LEFT JOIN fe.sf_pub_school b
ON a.school_id = b.school_id 
AND b.data_flag =1
LEFT JOIN fe.`sf_shelf_campus` c
ON a.campus_id = c.campus_id
AND c.data_flag =1
WHERE a.data_flag =1;	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_pub_school_shelf_infornation',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END