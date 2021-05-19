CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_sap_pmp_hos_emp_base_info`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 INT DEFAULT 0;
DECLARE v_temp2 INT DEFAULT 0;
DECLARE v_interval int  DEFAULT 20000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(16000) DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM feods.sap_pmp_hos_emp_base_info where update_time > date_sub(now(),interval 2 day) ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
/* 因为数据源是采用增量更新，没有用truncate操作，这里无需比较删除
DELETE a FROM fe_data.sap_pmp_hos_emp_base_info a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.sap_pmp_hos_emp_base_info);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
*/  
REPEAT 
#select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.sap_pmp_hos_emp_base_info  where update_time > date_sub(now(),interval 2 day) and  pid> v_temp2 limit v_interval) t ;
select min(pid) temp1,max(pid) temp2   into v_temp1,v_temp2
 from
(
select pid from (
select pid
 from (
  select pid from  feods.sap_pmp_hos_emp_base_info 
  where update_time > date_sub(now(),interval 2 day) 
  and pid> v_temp2
  )t0
  order by pid
  )t1
  limit v_interval
  )t2;
  
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.sap_pmp_hos_emp_base_info(
pid,
emp_code,
emp_name,
sex,
birth,
email,
werks,
org_type,
btrtl,
bukrs,
org_id,
job_id,
org_name,
hire_date,
dept_code,
dept_name,
area_code,
area_name,
city_code,
city_name,
werks_txt,
bukrs_txt,
btrtl_txt,
emp_source,
station_id,
stattion_name,
org_code,
org_yjzz,
job_name,
cancel_flag,
cancel_date,
position_id,
position_attr,
position_name,
org_id_parent,
parent_org_name,
super_emp_code,
super_emp_name,
division_code,
division_name,
add_time,
update_time,
phone
)
SELECT 
pid,
emp_code,
emp_name,
sex,
birth,
email,
werks,
org_type,
btrtl,
bukrs,
org_id,
job_id,
org_name,
hire_date,
dept_code,
dept_name,
area_code,
area_name,
city_code,
city_name,
werks_txt,
bukrs_txt,
btrtl_txt,
emp_source,
station_id,
stattion_name,
org_code,
org_yjzz,
job_name,
cancel_flag,
cancel_date,
position_id,
position_attr,
position_name,
org_id_parent,
parent_org_name,
super_emp_code,
super_emp_name,
division_code,
division_name,
add_time,
update_time,
phone
FROM feods.sap_pmp_hos_emp_base_info 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
  AND    update_time > date_sub(now(),interval 2 day)
ON DUPLICATE KEY UPDATE
pid=VALUES(pid),
emp_code=VALUES(emp_code),
emp_name=VALUES(emp_name),
sex=VALUES(sex),
birth=VALUES(birth),
email=VALUES(email),
werks=VALUES(werks),
org_type=VALUES(org_type),
btrtl=VALUES(btrtl),
bukrs=VALUES(bukrs),
org_id=VALUES(org_id),
job_id=VALUES(job_id),
org_name=VALUES(org_name),
hire_date=VALUES(hire_date),
dept_code=VALUES(dept_code),
dept_name=VALUES(dept_name),
area_code=VALUES(area_code),
area_name=VALUES(area_name),
city_code=VALUES(city_code),
city_name=VALUES(city_name),
werks_txt=VALUES(werks_txt),
bukrs_txt=VALUES(bukrs_txt),
btrtl_txt=VALUES(btrtl_txt),
emp_source=VALUES(emp_source),
station_id=VALUES(station_id),
stattion_name=VALUES(stattion_name),
org_code=VALUES(org_code),
org_yjzz=VALUES(org_yjzz),
job_name=VALUES(job_name),
cancel_flag=VALUES(cancel_flag),
cancel_date=VALUES(cancel_date),
position_id=VALUES(position_id),
position_attr=VALUES(position_attr),
position_name=VALUES(position_name),
org_id_parent=VALUES(org_id_parent),
parent_org_name=VALUES(parent_org_name),
super_emp_code=VALUES(super_emp_code),
super_emp_name=VALUES(super_emp_name),
division_code=VALUES(division_code),
division_name=VALUES(division_name),
add_time=VALUES(add_time),
update_time=VALUES(update_time),
phone=VALUES(phone);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'recorde:',rpad(concat(v_tag-v_interval+1,'-',v_tag),20,' '),' range:',rpad(concat(v_temp1,'-',v_temp2),20,' '),' update error',';  effect count:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'recorde:',rpad(concat(v_tag-v_interval+1,'-',v_tag),20,' '),' range:',rpad(concat(v_temp1,'-',v_temp2),20,' '),' update success',';effect count:',v_effect_count,char(13));        
END IF;  
 UNTIL v_tag>=v_count
END REPEAT;
SET v_msg=concat(v_msg,'v_tag=',v_tag,';v_count=',v_count,char(13));  
SET v_msg=concat(v_msg,'sync data take seconds:',TIMESTAMPDIFF(second,v_start_date,now()));  
select v_msg; 
SET autocommit=1; 
 END