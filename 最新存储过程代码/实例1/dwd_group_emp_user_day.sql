CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_group_emp_user_day`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 每天全量更新结果表
truncate table fe_dwd.dwd_group_emp_user_day;
-- group_customer_id = 4726 的需要data_flag =2的这部分数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1_1`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1_1  AS 
SELECT 
emp_user_id,
group_customer_id
FROM fe_group.sf_group_emp a 
WHERE a.group_customer_id = 4726;
-- 正常的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1_2`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1_2  AS 
SELECT 
emp_user_id,
group_customer_id
FROM fe_group.sf_group_emp a 
WHERE a.group_customer_id != 4726
AND  a.data_flag = 1;
-- 通过扫码支付的这部分，有些data_flag =2 的也能下单，需要补上这些用户。
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1_3_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1_3_tmp  AS 
SELECT DISTINCT b.order_user_id
FROM fe_goods.sf_scan_order b 
UNION 
SELECT DISTINCT a.order_user_id
FROM fe_goods.sf_group_order a;
 
 
CREATE INDEX idx_dwd_group_emp_user_1_3_tmp
ON fe_dwd.dwd_group_emp_user_1_3_tmp  (order_user_id);
 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1_3`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1_3  AS  
SELECT 
a.emp_user_id,
a.group_customer_id
FROM fe_dwd.dwd_group_emp_user_1_3_tmp aa
JOIN fe_group.sf_group_emp a  -- 员工信息
ON aa.order_user_id = a.customer_user_id
WHERE a.data_flag =2;
 
 /*  派发表中有的，员工表一定有数据。 派发表中的员工公司都是子公司
 -- 只要企业派发了的用户也需要添加上
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1_4`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1_4  AS    
SELECT DISTINCT d.emp_user_id,p.`group_customer_id`
FROM `fe_group`.sf_group_distribute_plan_detail d
JOIN fe_group.sf_group_distribute_plan p ON p.distribute_plan_id = d.distribute_plan_id	 
JOIN `fe_group`.`sf_group_customer` c ON c.`group_customer_id` = p.`group_customer_id` ;
 
 */
 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1_5`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1_5  AS 
SELECT * FROM fe_dwd.dwd_group_emp_user_1_1
UNION 
SELECT * FROM fe_dwd.dwd_group_emp_user_1_2
UNION  
SELECT * FROM fe_dwd.dwd_group_emp_user_1_3
-- UNION
-- SELECT * FROM fe_dwd.dwd_group_emp_user_1_4
;
CREATE INDEX idx_dwd_group_emp_user_1_5
ON fe_dwd.dwd_group_emp_user_1_5  (emp_user_id);
-- 这些数据里面的主键是emp_user_id 。有些data_flag =2的也是正常数据。所以去掉data_flag =1的这些限制
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_1`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_1  AS 
SELECT 
 aa.emp_user_id
,a.customer_user_id  -- 丰声号ID
,aa.group_customer_id   -- 4726
,d.group_name 
,e.group_customer_id AS group_customer_id_sub  -- 企业id 
,e.group_name AS group_name_sub                -- 企业名称
,a.user_role
,a.bind_status
,a.manager_status
,a.emp_user_name
,a.sex
,a.mobile
,a.mobile_encrypt
,a.dept_id
,a.duties
,a.dept
-- ,a.dept_higher
,a.professional_level
,a.job_number
,a.ltrim_job_number
,a.group_email
,a.birthday
,a.joindate
,a.cancel_flag
,a.cancel_date
,a.add_time
,a.data_flag
,a.from_type
,a.bindtime
,a.remark
,a.nick
FROM fe_dwd.dwd_group_emp_user_1_5 aa
LEFT JOIN fe_group.sf_group_emp a  -- 员工信息
ON aa.emp_user_id = a.emp_user_id
LEFT JOIN fe_group.sf_group_emp_extend b ON a.emp_user_id = b.emp_user_id --  AND b.data_flag = 1                         -- 员工信息扩展表 员工id关联 不发散
LEFT JOIN fe_group.sf_group_auth c ON c.auth_id=b.auth_id -- AND c.data_flag=1                                           -- 人事权限 人事权限id关联 不发散
LEFT JOIN fe_group.`sf_group_customer` d ON a.group_customer_id = d.group_customer_id -- AND d.data_flag=1               -- 企业信息表 企业ID关联 不发散
LEFT JOIN fe_group.`sf_group_customer` e ON c.bind_group_customer_id = e.group_customer_id  -- AND e.data_flag=1         -- 企业信息表
;
CREATE INDEX idx_dwd_group_emp_user_1 
ON fe_dwd.dwd_group_emp_user_1  (job_number);
CREATE INDEX idx_dwd_group_emp_user_1_1 
ON fe_dwd.dwd_group_emp_user_1  (customer_user_id);
-- 获取员工岗位 组织及分部信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_2`; 
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_2  AS 
SELECT DISTINCT a.emp_code job_number,     -- 工号
a.position_attr,                           -- 岗位属性
a.org_name,                                -- 组织全称
a.parent_org_name,                         -- 上级组织全称
a.division_code,                           -- 分部代码
a.division_name ,                        -- 分部名称
a.bukrs_txt
FROM fe_dwd.dwd_group_emp_user_1 b
JOIN feods.`sap_pmp_hos_emp_base_info` a   -- SAP/PMP/HOS员工基础信息表
-- ON b.job_number = a.emp_code;
ON LPAD(b.job_number, 8, '0')= a.emp_code;
CREATE INDEX idx_dwd_group_emp_user_2
ON fe_dwd.dwd_group_emp_user_2  (job_number);
-- 获取平台用户信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_3`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_3  AS 
SELECT DISTINCT a.user_id customer_user_id,a.mobile_phone,a.open_id,
a.OPEN_TYPE 
FROM 
  (SELECT user_id,mobile_phone,open_id,GROUP_CONCAT(open_type SEPARATOR '/') AS OPEN_TYPE 
   FROM fe.pub_user_open   -- 平台用户信息   
   WHERE data_flag = 1
   GROUP BY user_id
   ) a
JOIN fe_dwd.dwd_group_emp_user_1 b
ON a.user_id = b.customer_user_id
;
CREATE INDEX idx_dwd_group_emp_user_3
ON fe_dwd.dwd_group_emp_user_3  (customer_user_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_emp_user_4`; 
CREATE TEMPORARY TABLE fe_dwd.dwd_group_emp_user_4  AS 
SELECT distinct
a.emp_user_id
,a.customer_user_id  -- 丰声号ID
,a.group_customer_id
,a.group_name 
,a.group_customer_id_sub 
,a.group_name_sub 
,a.user_role
,a.bind_status
,a.manager_status
,a.emp_user_name
,a.sex
,a.mobile
,a.mobile_encrypt
,c.OPEN_TYPE
,c.open_id
,a.dept_id
,a.duties
,IFNULL(a.dept,b.org_name) dept  -- 员工表提取不到组织信息，取大网信息。但是dept_id两者不同，不能替换
,b.parent_org_name
,b.division_code
,b.division_name
,a.professional_level
,b.position_attr
,b.bukrs_txt
,a.job_number
,a.ltrim_job_number
,a.group_email
,a.birthday
,a.joindate
,a.cancel_flag
,a.cancel_date
,a.add_time
,a.data_flag
,a.from_type
,a.bindtime
,a.remark
,a.nick
FROM fe_dwd.dwd_group_emp_user_1 a
LEFT JOIN fe_dwd.dwd_group_emp_user_2 b      -- 获取员工岗位 组织及分部信息
ON a.job_number = b.job_number               
LEFT JOIN fe_dwd.dwd_group_emp_user_3 c      -- 获取平台用户信息
ON a.customer_user_id = c.customer_user_id
;
INSERT INTO fe_dwd.dwd_group_emp_user_day
(
 emp_user_id
,customer_user_id
,group_customer_id
,group_name
,group_customer_id_sub
,group_name_sub
,user_role
,bind_status
,manager_status
,emp_user_name
,sex
,mobile
,OPEN_TYPE
,OPEN_ID
,dept_id
,duties
,dept
,parent_org_name
,division_code
,division_name
,professional_level
,position_attr
,bukrs_txt
,job_number
,ltrim_job_number
,group_email
,birthday
,joindate
,cancel_flag
,cancel_date
,add_time
,data_flag
,from_type
,bindtime
,remark
,nick
)
SELECT 
 a.emp_user_id
,a.customer_user_id
,a.group_customer_id
,a.group_name
,IFNULL(a.group_customer_id_sub,a.group_customer_id) group_customer_id_sub
,IFNULL(a.group_name_sub,a.group_name) group_name_sub
,a.user_role
,a.bind_status
,a.manager_status
,a.emp_user_name
,a.sex
,a.mobile
,a.OPEN_TYPE
,a.OPEN_ID
,a.dept_id
,a.duties
,a.dept
,a.parent_org_name
,a.division_code
,a.division_name
,a.professional_level
,a.position_attr
,a.bukrs_txt
,a.job_number
,a.ltrim_job_number
,a.group_email
,a.birthday
,a.joindate
,a.cancel_flag
,a.cancel_date
,a.add_time
,a.data_flag
,a.from_type
,a.bindtime
,a.remark
,a.nick
FROM fe_dwd.dwd_group_emp_user_4 a ;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_group_emp_user_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('李世龙@', @user, @timestamp));
 
  COMMIT;
END