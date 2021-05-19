CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_d_dv_sf_org`()
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
SELECT count(*) into v_count FROM feods.d_dv_sf_org ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.d_dv_sf_org a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.d_dv_sf_org);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.d_dv_sf_org  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.d_dv_sf_org(
pid,
mandt,
serno,
org_id,
zzcreated_by,
zzcreated_on,
zzcreated_ts,
org_id_parent,
org_name,
internal_address,
org_type,
org_code,
date_from,
date_to,
manager,
manager_name,
zhrzzcj,
zhrzzgn,
loc_id,
zhrxzzz,
kostl,
kostl_txt,
net_code,
persa,
zhrzzdj,
zhrzzdj_txt,
org_yjzz,
zsuv_pernr,
zsuv_plans,
zsuv_plstx,
zhrxzzz_bz,
stext,
zhrglx,
zhrsfssw,
ds_create_tm,
inc_day,
add_time,
update_time
)
SELECT 
pid,
mandt,
serno,
org_id,
zzcreated_by,
zzcreated_on,
zzcreated_ts,
org_id_parent,
org_name,
internal_address,
org_type,
org_code,
date_from,
date_to,
manager,
manager_name,
zhrzzcj,
zhrzzgn,
loc_id,
zhrxzzz,
kostl,
kostl_txt,
net_code,
persa,
zhrzzdj,
zhrzzdj_txt,
org_yjzz,
zsuv_pernr,
zsuv_plans,
zsuv_plstx,
zhrxzzz_bz,
stext,
zhrglx,
zhrsfssw,
ds_create_tm,
inc_day,
add_time,
update_time
FROM feods.d_dv_sf_org 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
ON DUPLICATE KEY UPDATE
pid=VALUES(pid), 
mandt=VALUES(mandt), 
serno=VALUES(serno), 
org_id=VALUES(org_id), 
zzcreated_by=VALUES(zzcreated_by), 
zzcreated_on=VALUES(zzcreated_on), 
zzcreated_ts=VALUES(zzcreated_ts), 
org_id_parent=VALUES(org_id_parent), 
org_name=VALUES(org_name), 
internal_address=VALUES(internal_address), 
org_type=VALUES(org_type), 
org_code=VALUES(org_code), 
date_from=VALUES(date_from), 
date_to=VALUES(date_to), 
manager=VALUES(manager), 
manager_name=VALUES(manager_name), 
zhrzzcj=VALUES(zhrzzcj), 
zhrzzgn=VALUES(zhrzzgn), 
loc_id=VALUES(loc_id), 
zhrxzzz=VALUES(zhrxzzz), 
kostl=VALUES(kostl), 
kostl_txt=VALUES(kostl_txt), 
net_code=VALUES(net_code), 
persa=VALUES(persa), 
zhrzzdj=VALUES(zhrzzdj), 
zhrzzdj_txt=VALUES(zhrzzdj_txt), 
org_yjzz=VALUES(org_yjzz), 
zsuv_pernr=VALUES(zsuv_pernr), 
zsuv_plans=VALUES(zsuv_plans), 
zsuv_plstx=VALUES(zsuv_plstx), 
zhrxzzz_bz=VALUES(zhrxzzz_bz), 
stext=VALUES(stext), 
zhrglx=VALUES(zhrglx), 
zhrsfssw=VALUES(zhrsfssw), 
ds_create_tm=VALUES(ds_create_tm), 
inc_day=VALUES(inc_day), 
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