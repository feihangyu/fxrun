CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_scope_area_all`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_scope_area_all_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_scope_area_all_test LIKE fe_dwd.dwd_shelf_scope_area_all;
insert into  fe_dwd.dwd_shelf_scope_area_all_test
(
`shelf_scope_id`
,`scope_name`
,`scope_type`
,`business_type`
,`shelf_area_type`
,`shelf_tag`
,`shelf_tag_bin`
,`exclude_scope_id`
,`estimate_num`
,`region_area_id`
,`city`
)
select 
a.`shelf_scope_id`
,a.`scope_name`
,a.`scope_type`
,a.`business_type`
,a.`shelf_area_type`
,a.`shelf_tag`
,a.`shelf_tag_bin`
,a.`exclude_scope_id`
,a.`estimate_num`
,IFNULL(b.`region_area_id`,-1)
,b.city
from fe.sf_shelf_scope a 
left join 
fe.sf_shelf_scope_area b 
on a.shelf_scope_id = b.shelf_scope_id 
and a.data_flag = 1 
and b.data_flag =1;
truncate table fe_dwd.dwd_shelf_scope_area_all;
insert into  fe_dwd.dwd_shelf_scope_area_all
(
`shelf_scope_id`
,`scope_name`
,`scope_type`
,`business_type`
,`shelf_area_type`
,`shelf_tag`
,`shelf_tag_bin`
,`exclude_scope_id`
,`estimate_num`
,`region_area_id`
,`city`
)
select 
a.`shelf_scope_id`
,a.`scope_name`
,a.`scope_type`
,a.`business_type`
,a.`shelf_area_type`
,a.`shelf_tag`
,a.`shelf_tag_bin`
,a.`exclude_scope_id`
,a.`estimate_num`
,ifnull(b.`region_area_id`,-1)
,b.city
from fe.sf_shelf_scope a 
left join 
fe.sf_shelf_scope_area b 
on a.shelf_scope_id = b.shelf_scope_id 
and a.data_flag = 1 
and b.data_flag =1;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_scope_area_all',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END