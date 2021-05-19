CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_d_en_emp_org`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 varchar(20) DEFAULT '';
DECLARE v_temp2 varchar(20) DEFAULT '';
DECLARE v_interval int  DEFAULT 20000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(16000) DEFAULT '';
#DECLARE v_msg mediumtext DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM feods.d_en_emp_org  where update_time > date_sub(now(),interval 2 day) ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
/* 因为数据源是采用增量更新，没有用truncate操作，这里无需比较删除
DELETE a FROM fe_data.d_en_emp_org a  
WHERE a.emp_code NOT IN 
(SELECT emp_code FROM  feods.d_en_emp_org);
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
#select min(emp_code) emp_code,max(emp_code) emp_code  into v_temp1,v_temp2  from (select emp_code from  feods.d_en_emp_org  where update_time > date_sub(now(),interval 2 day)and  emp_code> v_temp2 order by emp_code limit v_interval) t ;
select min(emp_code) temp1,max(emp_code) temp2  into v_temp1,v_temp2 
 from
(
select emp_code from (
select emp_code
 from (
  select emp_code from  feods.d_en_emp_org 
  where update_time > date_sub(now(),interval 2 day)and  emp_code> ''
  )t0
  order by emp_code
  )t1
  limit v_interval
  )t2;
  
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.d_en_emp_org(
emp_id,
emp_code,
emp_name,
curr_org_name,
org_name,
org_code,
dept_code,
curr_area,
sex,
stell,
STATUS,
edu_level,
speciality,
date_of_birth,
job_date_from,
personal_phone,
marital_status,
add_time,
update_time
)
SELECT 
emp_id,
emp_code,
emp_name,
curr_org_name,
org_name,
org_code,
dept_code,
curr_area,
sex,
stell,
STATUS,
edu_level,
speciality,
date_of_birth,
job_date_from,
personal_phone,
marital_status,
add_time,
update_time
FROM feods.d_en_emp_org 
  WHERE  emp_code>=v_temp1
  AND    emp_code<=v_temp2
  AND    update_time > date_sub(now(),interval 2 day)
ON DUPLICATE KEY UPDATE
emp_id=VALUES(emp_id),
emp_code=VALUES(emp_code),
emp_name=VALUES(emp_name),
curr_org_name=VALUES(curr_org_name),
org_name=VALUES(org_name),
org_code=VALUES(org_code),
dept_code=VALUES(dept_code),
curr_area=VALUES(curr_area),
sex=VALUES(sex),
stell=VALUES(stell),
STATUS=VALUES(STATUS),
edu_level=VALUES(edu_level),
speciality=VALUES(speciality),
date_of_birth=VALUES(date_of_birth),
job_date_from=VALUES(job_date_from),
personal_phone=VALUES(personal_phone),
marital_status=VALUES(marital_status),
add_time=VALUES(add_time),
update_time=VALUES(update_time);
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