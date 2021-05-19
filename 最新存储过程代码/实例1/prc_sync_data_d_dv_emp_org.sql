CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_d_dv_emp_org`()
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
SELECT count(*) into v_count FROM feods.d_dv_emp_org  where update_time > date_sub(now(),interval 2 day) ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
/* 因为数据源是采用增量更新，没有用truncate操作，这里无需比较删除
DELETE a FROM fe_data.d_dv_emp_org a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.d_dv_emp_org);
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
#select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.d_dv_emp_org  where update_time > date_sub(now(),interval 2 day) and pid> v_temp2 limit v_interval) t ;
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  
from
(
select pid from (
select pid
 from (
  select pid from  feods.d_dv_emp_org 
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
INSERT INTO fe_data.d_dv_emp_org(
pid,
emp_num,
zzcreated_on,
last_name,
date_of_birth,
sex,
edu_level,
zhrsfbcz,
hire_date,
mail_address,
personal_phone,
orgeh_lv1,
orgtx_lv1,
speciality,
person_type,
zhrgzd,
zhrssqy,
bukrs_txt,
zhrzzqc,
net_code,
curr_org_name,
zhrlzlx,
zhrzgzt_txt,
stell,
stell_txt,
zhrgzdmc,
btrtl_txt,
cancel_flag,
cancel_date,
zhrgzdmc_up,
marital_status,
werks,
werks_txt,
curr_org_id,  
job_name,  
job_id,  
position_id,  
position_name,  
add_time,
update_time
)
SELECT 
pid,
emp_num,
zzcreated_on,
last_name,
date_of_birth,
sex,
edu_level,
zhrsfbcz,
hire_date,
mail_address,
personal_phone,
orgeh_lv1,
orgtx_lv1,
speciality,
person_type,
zhrgzd,
zhrssqy,
bukrs_txt,
zhrzzqc,
net_code,
curr_org_name,
zhrlzlx,
zhrzgzt_txt,
stell,
stell_txt,
zhrgzdmc,
btrtl_txt,
cancel_flag,
cancel_date,
zhrgzdmc_up,
marital_status,
werks,
werks_txt,
curr_org_id,  
job_name,  
job_id,  
position_id,  
position_name,  
add_time,
update_time
FROM feods.d_dv_emp_org 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
  AND    update_time > date_sub(now(),interval 2 day)
ON DUPLICATE KEY UPDATE
pid=VALUES(pid),      
emp_num=VALUES(emp_num), 
zzcreated_on=VALUES(zzcreated_on), 
last_name=VALUES(last_name), 
date_of_birth=VALUES(date_of_birth), 
sex=VALUES(sex), 
edu_level=VALUES(edu_level), 
zhrsfbcz=VALUES(zhrsfbcz), 
hire_date=VALUES(hire_date), 
mail_address=VALUES(mail_address), 
personal_phone=VALUES(personal_phone), 
orgeh_lv1=VALUES(orgeh_lv1), 
orgtx_lv1=VALUES(orgtx_lv1), 
speciality=VALUES(speciality), 
person_type=VALUES(person_type), 
zhrgzd=VALUES(zhrgzd), 
zhrssqy=VALUES(zhrssqy), 
bukrs_txt=VALUES(bukrs_txt), 
zhrzzqc=VALUES(zhrzzqc), 
net_code=VALUES(net_code), 
curr_org_name=VALUES(curr_org_name), 
zhrlzlx=VALUES(zhrlzlx), 
zhrzgzt_txt=VALUES(zhrzgzt_txt), 
stell=VALUES(stell), 
stell_txt=VALUES(stell_txt), 
zhrgzdmc=VALUES(zhrgzdmc), 
btrtl_txt=VALUES(btrtl_txt), 
cancel_flag=VALUES(cancel_flag), 
cancel_date=VALUES(cancel_date), 
zhrgzdmc_up=VALUES(zhrgzdmc_up), 
marital_status=VALUES(marital_status), 
werks=VALUES(werks), 
werks_txt=VALUES(werks_txt),
curr_org_id=VALUES(curr_org_id), 
job_name=VALUES(job_name), 
job_id=VALUES(job_id), 
position_id=VALUES(position_id), 
position_name=VALUES(position_name),  
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