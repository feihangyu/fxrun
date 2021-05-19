CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_en_fx_distribute_to_bdp`()
BEGIN
   SET @start_date = SUBDATE(CURDATE(),INTERVAL 1 DAY);  -- 当天前一天
 
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
   ## 初始化数据
/*
insert INTO fe_dwd.`dwd_en_fx_distribute_to_bdp`
(
detail_id,
distribute_period,
distribute_time,
group_customer_id,
group_name,
job_number,
emp_user_name,
distribute_amount,
distribute_remark
)
SELECT	
   d.detail_id,	
   d.distribute_period ,
   d.add_time ,
   p.group_customer_id ,
   c.group_name ,			
   e.job_number ,			
   e.emp_user_name ,					
   d.distribute_amount,
   d.distribute_remark 		
FROM			
   `fe_group`.sf_group_distribute_plan_detail d			
JOIN fe_group.sf_group_distribute_plan p ON p.distribute_plan_id = d.distribute_plan_id			
JOIN fe_group.sf_group_customer c ON c.group_customer_id = p.group_customer_id			
JOIN fe_group.sf_group_emp e ON e.emp_user_id = d.emp_user_id			
JOIN fe_group.sf_group_dictionary_item i ON i.item_id = d.welfare_item_dict_id			
JOIN fe_group.sf_group_auth a ON a.bind_group_customer_id = c.group_customer_id	
JOIN fe_group.`sf_group_customer` b ON b.group_customer_id = a.group_customer_id
WHERE d.`data_flag` = 1
AND p.`data_flag` = 1
AND c.`data_flag` = 1
AND b.data_flag = 1	
AND d.add_time  >= '2019-09-24'   
AND d.add_time < CURDATE()
*/
## 按天增量添加派发数据
REPLACE INTO fe_dwd.`dwd_en_fx_distribute_to_bdp`
(
detail_id,
distribute_period,
distribute_time,
group_customer_id,
group_name,
job_number,
emp_user_name,
distribute_amount,
distribute_remark
)
SELECT	
   d.detail_id,	
   d.distribute_period ,
   d.add_time ,
   p.group_customer_id ,
   c.group_name ,			
   e.job_number ,			
   e.emp_user_name ,					
   d.distribute_amount,
   d.distribute_remark 		
FROM			
   `fe_group`.sf_group_distribute_plan_detail d			
JOIN fe_group.sf_group_distribute_plan p ON p.distribute_plan_id = d.distribute_plan_id			
JOIN fe_group.sf_group_customer c ON c.group_customer_id = p.group_customer_id			
JOIN fe_group.sf_group_emp e ON e.emp_user_id = d.emp_user_id			
JOIN fe_group.sf_group_dictionary_item i ON i.item_id = d.welfare_item_dict_id			
JOIN fe_group.sf_group_auth a ON a.bind_group_customer_id = c.group_customer_id	
JOIN fe_group.`sf_group_customer` b ON b.group_customer_id = a.group_customer_id
WHERE d.`data_flag` = 1
AND p.`data_flag` = 1
AND c.`data_flag` = 1
AND b.data_flag = 1	
AND d.add_time  >= @start_date   
AND d.add_time < CURDATE()
	
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_en_fx_distribute_to_bdp',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('linihe@', @user, @timestamp)
  );
COMMIT;
END