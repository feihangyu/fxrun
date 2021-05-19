CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_en_fx_distribute_consume_area_month`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
SET @last_month_start := CONCAT(DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m'),'-01');  # 上月第一天
SET @last_month_end := LAST_DAY(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH));  #上月最后一天
SET @last_month_id := DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m');
#派发与领取
#根据add_time进行增量抽取，抽取上个月的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_paifa_emp;							
CREATE TEMPORARY TABLE fe_dwd.d_en_paifa_emp (KEY(customer_user_id),KEY(job_number),KEY(receive_time))							
AS									
SELECT  t.job_number,#工号,				
	t.emp_user_name,#员工姓名,		
	GROUP_CONCAT(DISTINCT t.cancel_flag) AS  cancel_flag,					
	t.group_customer_id, #企业id,					
	t.group_name,#公司名称,				
	t.dept,#分部,		
	t.customer_user_id,				
	t.emp_user_id,
	DATE_FORMAT(t.receive_time,'%Y-%m') receive_time,
	DATE_FORMAT(t.last_update_time,'%Y-%m') add_time,		
	SUM(ifnull(t.got_amount,0)) got_amount, #已领取积分,											
	SUM(t.distribute_amount) distribute_amount#派发积分				
FROM fe_dwd.dwd_staff_score_distribute_detail t WHERE t.last_update_time >= @last_month_start AND t.last_update_time <= @last_month_end
GROUP BY t.job_number, t.customer_user_id,DATE_FORMAT(t.receive_time,'%Y-%m');		
#上月派发积分
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_paifa_emp_last;							
CREATE TEMPORARY TABLE fe_dwd.d_en_paifa_emp_last (KEY(customer_user_id),KEY(job_number),KEY(receive_time))							
AS									
SELECT  t.job_number,#工号,				
	t.emp_user_name,#员工姓名,		
	t.cancel_flag,					
	t.group_customer_id, #企业id,					
	t.group_name,#公司名称,				
	t.dept,#分部,		
	t.customer_user_id,				
	t.emp_user_id,
	DATE_FORMAT(DATE_ADD(CONCAT(t.receive_time,'-01'), INTERVAL 1 MONTH),'%Y-%m') receive_time,
	DATE_FORMAT(DATE_ADD(CONCAT(t.add_time,'-01'), INTERVAL 1 MONTH),'%Y-%m') add_time,		
	t.got_amount got_amount_last, #已领取积分,											
	t.distribute_amount distribute_amount_last#派发积分				
FROM fe_dwd.d_en_paifa_emp t;		
#派发汇总表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_all_paifa_emp;							
CREATE TEMPORARY TABLE fe_dwd.d_en_all_paifa_emp (KEY(customer_user_id),KEY(job_number),KEY(receive_time))							
AS
SELECT 
	p.job_number,#工号,				
	p.emp_user_name,#员工姓名,		
	p.cancel_flag,					
	p.group_customer_id, #企业id,					
	p.group_name,#公司名称,				
	p.dept,#分部,		
	p.customer_user_id,				
	p.emp_user_id,
	p.receive_time,
	p.add_time,		
	p.got_amount,#已领取积分,	
	l.got_amount_last, #上月已领取积分,											
	p.distribute_amount,#派发积分
	l.distribute_amount_last#上月派发积分
FROM fe_dwd.d_en_paifa_emp p 
LEFT JOIN fe_dwd.d_en_paifa_emp_last l ON p.job_number=l.job_number AND p.receive_time=l.receive_time
AND p.group_customer_id=l.group_customer_id;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_zhanghao_emp;							
CREATE TEMPORARY TABLE fe_dwd.d_en_zhanghao_emp (KEY(customer_user_id))							
AS							
SELECT job_number,							
       customer_user_id							
FROM fe_dwd.d_en_paifa_emp							
WHERE 	customer_user_id IS NOT NULL;	
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_zhanghao_emp;						
CREATE TEMPORARY TABLE fe_dwd.d_en_zhanghao_emp (KEY(customer_user_id))							
AS							
SELECT job_number,							
       customer_user_id							
FROM fe_dwd.d_en_paifa_emp							
WHERE 	customer_user_id IS NOT NULL ;	
						
## 账号消费金额
#根据PAY_DATE进行增量抽取，抽取上个月的数据							
DROP TEMPORARY TABLE IF EXISTS 	fe_dwd.d_en_emp_xiaofei;						
CREATE TEMPORARY TABLE fe_dwd.d_en_emp_xiaofei(KEY(job_number),KEY(PAY_DATE))							
AS							
SELECT  
DATE_FORMAT(PAY_DATE,'%Y-%m') PAY_DATE,
SUM(AMOUNTS) wallet_AMOUNTS, 
bb.job_number job_number				
FROM fe.`user_member_wallet_log` aa							
JOIN fe_dwd.d_en_zhanghao_emp bb ON aa.WALLET_ID = bb.customer_user_id							
WHERE aa.DATA_FLAG = 1							
AND TRADE_STATUS = 1							
AND TRADE_TYPE = 2					
AND PAY_DATE >= @last_month_start
AND PAY_DATE <= @last_month_end						
GROUP BY job_number,DATE_FORMAT(PAY_DATE,'%Y-%m') ;
## 上月账号消费金额							
DROP TEMPORARY TABLE IF EXISTS  fe_dwd.d_en_emp_xiaofei_last;						
CREATE TEMPORARY TABLE fe_dwd.d_en_emp_xiaofei_last(KEY(job_number),KEY(PAY_DATE))							
AS							
SELECT  
DATE_FORMAT(DATE_ADD(CONCAT(t.PAY_DATE,'-01'), INTERVAL 1 MONTH),'%Y-%m') PAY_DATE,
t.wallet_AMOUNTS wallet_AMOUNTS_last, 
t.job_number				
FROM fe_dwd.d_en_emp_xiaofei t;
#消费汇总表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_all_xiaofei_emp;							
CREATE TEMPORARY TABLE fe_dwd.d_en_all_xiaofei_emp (KEY(job_number),KEY(PAY_DATE))							
AS
SELECT 
f.PAY_DATE,
f.job_number,
f.wallet_AMOUNTS,
l.wallet_AMOUNTS_last
FROM fe_dwd.d_en_emp_xiaofei f 
LEFT JOIN fe_dwd.d_en_emp_xiaofei_last l ON f.job_number=l.job_number AND f.PAY_DATE=l.PAY_DATE;
#派发与消费汇总表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_en_all_result_1;							
CREATE TEMPORARY TABLE fe_dwd.d_en_all_result_1 (KEY(job_number),KEY(group_customer_id),KEY(customer_user_id))
SELECT 
p.job_number,#工号,				
p.emp_user_name,#员工姓名,		
p.cancel_flag,					
p.group_customer_id, #企业id,					
p.group_name,#公司名称,				
p.dept,#分部,		
p.customer_user_id,				
p.emp_user_id,
p.receive_time,
p.add_time,		
p.got_amount,#已领取积分,	
p.got_amount_last, #上月已领取积分,											
p.distribute_amount,#派发积分
p.distribute_amount_last,#上月派发积分
f.wallet_AMOUNTS, #消费积分
f.wallet_AMOUNTS_last #上月消费积分
FROM fe_dwd.d_en_all_paifa_emp p 
LEFT JOIN fe_dwd.d_en_all_xiaofei_emp f ON p.job_number=f.job_number AND p.receive_time=f.PAY_DATE;
DELETE FROM fe_dwd.dwd_en_fx_receive_rate_report WHERE report_time >= @last_month_id;
# 描述：丰享领取率报表
INSERT INTO fe_dwd.dwd_en_fx_receive_rate_report(
auth_id,auth_create_time,report_time,total_distribute_score,total_receive_score,
score_receive_rate,score_receive_relate_rate,total_distribute_person,total_receive_person,
person_receive_rate,person_receive_relate_rate,data_flag,
add_time,add_user_id,last_update_time,last_update_user_id
) 
SELECT 
r.group_customer_id auth_id, #人事权限id
SYSDATE() auth_create_time, #人事权限创建时间
r.add_time report_time,#'报告所属年月（如2018-05）',
SUM(IFNULL(r.distribute_amount,0)) total_distribute_score, #派发总积分
SUM(IFNULL(r.got_amount,0)) total_receive_score, #领取总积分
CASE WHEN SUM(IFNULL(r.distribute_amount,0)) IS NOT NULL OR SUM(IFNULL(r.distribute_amount,0))<>0
	THEN ROUND(SUM(IFNULL(r.got_amount,0))/SUM(IFNULL(r.distribute_amount,0)),4)	
	ELSE 0 END AS score_receive_rate,#积分领取率（0.9867）
	
CASE WHEN SUM(IFNULL(r.distribute_amount,0)) IS NOT NULL OR SUM(IFNULL(r.distribute_amount,0))<>0
	THEN ROUND(SUM(IFNULL(r.got_amount_last,0))/SUM(IFNULL(r.distribute_amount,0)),4)	
	ELSE 0 END AS score_receive_relate_rate,#积分领取率环比上月（如-0.0623）
COUNT(r.job_number) total_distribute_person, #派发总人数
COUNT(r.got_amount) total_receive_person, #领取总人数
CASE WHEN COUNT(r.distribute_amount) IS NOT NULL AND COUNT(r.distribute_amount)<>0
	THEN ROUND(COUNT(r.got_amount)/COUNT(r.distribute_amount),4)
     ELSE 0 END AS person_receive_rate,#人数领取率
 CASE WHEN COUNT(r.job_number) IS NOT NULL AND COUNT(r.job_number)<>0
	THEN ROUND(COUNT(r.got_amount_last)/COUNT(r.job_number),4)
     ELSE 0 END AS person_receive_relate_rate, #人数领取率环比上月
1 data_flag,
SYSDATE() add_time,
1 add_user_id,
SYSDATE() last_update_time,
1 last_update_user_id    
FROM fe_dwd.d_en_all_result_1 r
GROUP BY r.group_customer_id,r.add_time;
DELETE FROM fe_dwd.dwd_en_fx_consumption_rate_report WHERE report_time >= @last_month_id;
# 描述：丰享消费率报表
INSERT INTO fe_dwd.dwd_en_fx_consumption_rate_report(
auth_id,auth_create_time,report_time,total_receive_score,
total_consume_score,score_consume_rate,score_consume_relate_rate,
total_receive_person,total_consume_person,person_consume_rate,
person_consume_relate_rate,data_flag,add_time,
add_user_id,last_update_time,last_update_user_id
) 
SELECT 
r.group_customer_id auth_id, #人事权限id
SYSDATE() auth_create_time, #人事权限创建时间
r.add_time report_time,#'报告所属年月（如2018-05）',
SUM(IFNULL(r.got_amount,0)) total_receive_score, #领取总积分
SUM(IFNULL(r.wallet_AMOUNTS,0)) total_consume_score, #消费总积分
CASE WHEN SUM(IFNULL(r.got_amount,0)) IS NOT NULL OR SUM(IFNULL(r.got_amount,0))<>0
	THEN ROUND(SUM(IFNULL(r.wallet_AMOUNTS,0))/SUM(IFNULL(r.got_amount,0)),4)	
	ELSE 0 END AS score_consume_rate,#积分消费率（0.9867）
CASE WHEN SUM(IFNULL(r.got_amount,0)) IS NOT NULL OR SUM(IFNULL(r.got_amount,0))<>0
	THEN ROUND(SUM(IFNULL(r.wallet_AMOUNTS_last,0))/SUM(IFNULL(r.got_amount,0)),4)	
	ELSE 0 END AS score_consume_relate_rate,#积分消费率环比上月（如-0.0623）
COUNT(r.got_amount) total_receive_person, #领取总人数
COUNT(r.wallet_AMOUNTS) total_consume_person, #消费总人数
CASE WHEN COUNT(r.got_amount) IS NOT NULL AND COUNT(r.got_amount)<>0
	THEN ROUND(COUNT(r.wallet_AMOUNTS)/COUNT(r.got_amount),4)
     ELSE 0 END AS person_consume_rate,#人数消费率
 CASE WHEN COUNT(r.got_amount) IS NOT NULL AND COUNT(r.got_amount)<>0
	THEN ROUND(COUNT(r.wallet_AMOUNTS_last)/COUNT(r.got_amount),4)
     ELSE 0 END AS person_consume_relate_rate, #人数消费率环比上月
1 data_flag,
SYSDATE() add_time,
1 add_user_id,
SYSDATE() last_update_time,
1 last_update_user_id    
FROM fe_dwd.d_en_all_result_1 r
GROUP BY r.group_customer_id,r.add_time;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_en_fx_distribute_consume_area_month',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('郑志省@', @user, @timestamp)
  );
  
COMMIT;	
END