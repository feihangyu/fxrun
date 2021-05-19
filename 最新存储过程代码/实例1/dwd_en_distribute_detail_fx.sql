CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_en_distribute_detail_fx`()
BEGIN
    SET  @sdate := SUBDATE(CURRENT_DATE,1);
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
   
DELETE FROM fe_dwd.dwd_en_distribute_detail_fx  WHERE  @sdate = DATE(add_time_detail); 
## 1.先更新昨天之前的领取状态有变动的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`d_en_distribute_id_status`;
CREATE TEMPORARY TABLE fe_dwd.`d_en_distribute_id_status`(KEY(detail_id))
AS
SELECT detail_id
FROM `fe_group`.sf_group_distribute_plan_detail
WHERE last_update_time >= SUBDATE(CURDATE(),INTERVAL 1 DAY)
AND add_time < SUBDATE(CURDATE(),INTERVAL 1 DAY);
 UPDATE fe_dwd.`dwd_en_distribute_detail_fx` t1
 JOIN
 ( 
SELECT	
   d.detail_id,						
   (			
      CASE			
      WHEN d.distribute_status = 2 THEN '已领取'			
			WHEN d.distribute_status = 1 THEN '待派发'
			WHEN d.distribute_status = 3 THEN '已取消'
      WHEN d.distribute_status = 6 THEN '未领用'			
			WHEN d.distribute_status = 7 THEN '待手动领取'
      ELSE '' END			
   )  distribute_status_desc, -- 领取状态,
   d.distribute_status
FROM			
   `fe_group`.sf_group_distribute_plan_detail d	
JOIN fe_dwd.`d_en_distribute_id_status` d1 ON d1.detail_id = d.detail_id		
)t2 ON t1.detail_id = t2.detail_id
SET t1.distribute_status_desc = t2.distribute_status_desc, t1.distribute_status = t2.distribute_status
;
 ## 将昨天派发数据添加   
INSERT INTO fe_dwd.`dwd_en_distribute_detail_fx`
(
detail_id,
distribute_period,
receive_time,
add_time_detail,
group_customer_id,
group_name,
group_customer_id_sub,
group_name_sub,
job_number,
emp_user_id,
customer_user_id,
job_number_8,
emp_user_name,
cancel_flag,
data_flag,
dept,
item_name,
distribute_status_desc,
distribute_status,
distribute_amount_detail,
distribute_remark,
distribute_plan_id,
add_user_id,
emp_user_name_add,
add_time_plan,
actual_distribute_starttime,
actual_distribute_endtime,
distribute_time,
distribute_emp_count,
per_amount,
distribute_amount,
success_amount,
success_emp_count
)
SELECT	distinct
   d.detail_id,	
   d.distribute_period , -- AS 归属月份,
   d.`receive_time`,--  派发领取时间,
   d.add_time add_time_detail,-- 派发时间,
   a.`group_customer_id` ,-- 父级企业id,
   b.group_name ,-- 父级企业名称,
   p.group_customer_id group_customer_id_sub ,-- 公司id,
   c.group_name group_name_sub, -- 公司名称,			
   e.job_number ,-- 工号,
   d.emp_user_id,
   d.customer_user_id,
   LPAD(e.job_number,8,'0') job_number_8 , --  工号补零, 
   e.emp_user_name, -- 员工姓名,
   e.cancel_flag,
   e.data_flag,			
   e.dept ,-- 部门,			
   i.item_name, --  "积分类别",						
   (			
      CASE			
      WHEN d.distribute_status = 2 THEN '已领取'			
			WHEN d.distribute_status = 1 THEN '待派发'
			WHEN d.distribute_status = 3 THEN '已取消'
      WHEN d.distribute_status = 6 THEN '未领用'			
			WHEN d.distribute_status = 7 THEN '待手动领取'
      ELSE '' END			
   )  distribute_status_desc, -- 领取状态,
   d.distribute_status,
   d.distribute_amount distribute_amount_detail , -- 派发金额 ,
   d.distribute_remark , -- 派发备注	,
   p.distribute_plan_id	,		
   p.add_user_id ,-- '派发任务创建的企业管理员ID',			
   b1.emp_user_name  emp_user_name_add,-- 派发任务创建的企业管理员,			
   p.add_time add_time_plan,-- '数据导入时间',			
   p.actual_distribute_starttime,--  实际派发开始时间,			
   p.actual_distribute_endtime , -- 实际派发结束时间	,
   p.distribute_time ,
   p.distribute_emp_count,
   p.per_amount,
   p.distribute_amount,
   p.success_amount,
   p.success_emp_count
FROM			
   `fe_group`.sf_group_distribute_plan_detail d			
JOIN fe_group.sf_group_distribute_plan p ON p.distribute_plan_id = d.distribute_plan_id			
JOIN fe_group.sf_group_customer c ON c.group_customer_id = p.group_customer_id			
JOIN fe_group.sf_group_emp e ON e.emp_user_id = d.emp_user_id			
JOIN fe_group.sf_group_dictionary_item i ON i.item_id = d.welfare_item_dict_id			
JOIN fe_group.sf_group_auth a ON a.bind_group_customer_id = c.group_customer_id	
JOIN fe_group.`sf_group_customer` b ON b.group_customer_id = a.group_customer_id
JOIN `fe_group`.`sf_group_emp` b1 ON b1.emp_user_id = p.add_user_id
WHERE d.`data_flag` = 1
AND p.`data_flag` = 1
AND c.`data_flag` = 1
AND b.data_flag = 1	
AND d.add_time >= @sdate
AND d.add_time < CURDATE()
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_en_distribute_detail_fx',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
 
COMMIT;
END