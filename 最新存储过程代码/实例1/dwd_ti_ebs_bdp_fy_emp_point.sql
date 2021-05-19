CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_ti_ebs_bdp_fy_emp_point`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP(), @current_batch := DATE_FORMAT(CURRENT_TIMESTAMP,'%Y%m%d%H%i'),@stime := CURRENT_TIMESTAMP;
TRUNCATE TABLE fe_dwd.dwd_ti_ebs_bdp_fy_emp_point ;
INSERT INTO fe_dwd.dwd_ti_ebs_bdp_fy_emp_point 
(batch_no,group_name,belong_code,accounting_code,emp_code,emp_name,category_code,POINT,remark,project_code,create_emp,create_tm,modify_emp,modify_tm,upload_time)
SELECT 
#CONCAT('P81',DATE_FORMAT(CURRENT_DATE,'%Y%m'),
#	CASE WHEN DAY(CURRENT_DATE)<10 THEN CONCAT('00',DAY(CURRENT_DATE)) ELSE CONCAT('0',DAY(CURRENT_DATE)) END) batch_no, #批次号,
CONCAT('P81',@current_batch) batch_no, #批次号,修改成获取当前的年月日时分,
d.group_name group_name , #企业名称,
d.distribute_period belong_code, # 所属月份, 
d.distribute_period accounting_code, # 结算月份,
d.job_number emp_code, # 工号,
d.emp_user_name emp_name, # 姓名,
d.item_value category_code, # 积分类别,
d.distribute_amount POINT, #积分,
d.distribute_remark remark,# 备注
d.project_code, #项目代码
d.add_user_id create_emp, #创建人
d.add_time create_tm, #创建时间
d.last_update_user_id modify_emp, #修改人
d.last_update_time modify_tm, #修改时间
@stime upload_time #记录上传时间
FROM fe_dwd.dwd_staff_score_distribute_detail d 
WHERE d.group_customer_id IN (6154,6100,6276,6311,6312)
#d.group_customer_id IN (6154)
AND d.distribute_status!=3 AND d.distribute_status!=0 
AND d.distribute_status!=4 AND d.distribute_status!=5 
AND d.distribute_status!=8
AND d.distribute_status!=9 AND d.distribute_status!=10
#根据实际的派发时间，增量抽取上个月的派发积分数据
#########################################################
AND d.add_time >= DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-01')			
AND d.add_time < DATE_FORMAT(CURRENT_DATE,'%Y-%m-01');
##########################################################
TRUNCATE TABLE fe_dwd.dwd_ti_ebs_bdp_fy_batch;
INSERT INTO fe_dwd.dwd_ti_ebs_bdp_fy_batch 
(batch_no,create_emp,create_tm,modify_emp,modify_tm,total_point,total_count,STATUS,upload_time)
SELECT 
d.batch_no, #批次号,
d.create_emp, #创建人
d.create_tm, #创建时间
d.modify_emp, #修改人
d.modify_tm, #修改时间
SUM(IFNULL(d.point,0)) total_point, #积分
COUNT(1) total_count,
NULL STATUS,
@stime upload_time #记录上传时间
FROM fe_dwd.dwd_ti_ebs_bdp_fy_emp_point d;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_ti_ebs_bdp_fy_emp_point',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('郑志省@', @user, @timestamp)
  );
  
COMMIT;	
END