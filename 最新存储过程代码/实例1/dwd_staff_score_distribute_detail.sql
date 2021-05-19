CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_staff_score_distribute_detail`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
SET @sday := SUBDATE(CURRENT_DATE,1);
REPLACE INTO fe_dwd.dwd_staff_score_distribute_detail 
SELECT		
   d.detail_id,
   d.distribute_plan_id,
   d.last_update_time,#最后修改时间	
   d.receive_time,#领取时间
   d.add_time,#添加时间
   p.distribute_time,#派发时间
   d.distribute_period,#派发所属周期
   p.group_customer_id,	#企业编码
   c.group_name,#人事名称			
   d.emp_user_id,#员工ID				
   e.job_number,#员工号
   e.emp_user_name,#员工姓名
   e.duties,#职务
   e.bindtime,#绑定时间
   e.cancel_flag,#是否离职
   e.joindate,#入职日期
   e.cancel_date,#离职日期
   d.customer_user_id,#消费端ID
   CONCAT(SUBSTRING_INDEX(e.dept, '分部',1),'分部') dept,#部门
   d.distribute_remark,#奖励类型
   i.item_name item_name,#类型说明			
   CASE WHEN d.distribute_status = 2 THEN d.distribute_amount ELSE 0 END AS got_amount,#领取金额
   CASE WHEN d.distribute_status = 2 THEN '已领取' ELSE '未领取' END AS is_got,	#是否已领取
   d.distribute_amount, #派发金额
   p.service_id,#派发业务类型id
   d.project_code,
   e.bind_status,#绑定状态  
   d.distribute_status,
   d.add_user_id,#'添加的管理员ID'
   d.last_update_user_id, #'最后修改的企业管理员ID'
   i.item_id, #派发积分字典ID
   i.dictionary_id,#派发积分字典编码id
   i.item_value #派发积分明细值
FROM fe_group.sf_group_distribute_plan_detail d
JOIN fe_group.sf_group_distribute_plan p ON p.distribute_plan_id = d.distribute_plan_id							
JOIN fe_group.sf_group_customer c ON c.group_customer_id = p.group_customer_id							
JOIN fe_group.sf_group_emp e ON e.emp_user_id = d.emp_user_id							
JOIN fe_group.sf_group_dictionary_item i ON i.item_id = d.welfare_item_dict_id							
JOIN fe_group.sf_group_auth a ON a.bind_group_customer_id = c.group_customer_id							
JOIN fe_group.sf_group_customer b ON b.group_customer_id = a.group_customer_id							
WHERE d.`data_flag` = 1							
AND p.`data_flag` = 1							
AND c.`data_flag` = 1							
AND b.data_flag = 1							
AND d.distribute_status != 3	
AND d.`last_update_time` >= @sday		 -- d.`add_time` >='2019-09-24'					
AND d.`last_update_time` < CURRENT_DATE();
TRUNCATE TABLE fe_dwd.dwd_staff_score_distribute_result;
INSERT INTO fe_dwd.dwd_staff_score_distribute_result
SELECT			
  t.emp_user_id,#员工ID					
  t.group_customer_id,#企业id,	
  t.job_number,#工号,														
  SUM(IFNULL(t.distribute_amount,0)) distribute_amount #派发积分				
 FROM
  fe_dwd.dwd_staff_score_distribute_detail t WHERE distribute_status!='0'
GROUP BY t.job_number,t.emp_user_id;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_staff_score_distribute_detail',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('郑志省@', @user, @timestamp)
  );
  
COMMIT;	
END