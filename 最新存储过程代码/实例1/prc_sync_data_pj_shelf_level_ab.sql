CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_pj_shelf_level_ab`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 INT DEFAULT 0;
DECLARE v_temp2 INT DEFAULT 0;
DECLARE v_interval int  DEFAULT 50000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(10000) DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM feods.pj_shelf_level_ab ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.pj_shelf_level_ab a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.pj_shelf_level_ab);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.pj_shelf_level_ab  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.pj_shelf_level_ab(
pid,
smonth,
city_name,
BRANCH_CODE,
BRANCH_NAME,
SF_CODE,
REAL_NAME,
shelf_type,
shelf_id,
ACTIVATE_date,
ACTIVATE_date_old,
REVOKE_date,
REVOKE_date_old,
SHELF_STATUS,
day_long,
if_band,
SHELF_HANDLE_STATUS,
gmv,
order_num,
shelf_level_t,
shelf_level
)
SELECT 
pid,
smonth,
city_name,
BRANCH_CODE,
BRANCH_NAME,
SF_CODE,
REAL_NAME,
shelf_type,
shelf_id,
ACTIVATE_date,
ACTIVATE_date_old,
REVOKE_date,
REVOKE_date_old,
SHELF_STATUS,
day_long,
if_band,
SHELF_HANDLE_STATUS,
gmv,
order_num,
shelf_level_t,
shelf_level
FROM feods.pj_shelf_level_ab 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
ON DUPLICATE KEY UPDATE
pid=VALUES(pid),
smonth=VALUES(smonth),
city_name=VALUES(city_name),
BRANCH_CODE=VALUES(BRANCH_CODE),
BRANCH_NAME=VALUES(BRANCH_NAME),
SF_CODE=VALUES(SF_CODE),
REAL_NAME=VALUES(REAL_NAME),
shelf_type=VALUES(shelf_type),
shelf_id=VALUES(shelf_id),
ACTIVATE_date=VALUES(ACTIVATE_date),
ACTIVATE_date_old=VALUES(ACTIVATE_date_old),
REVOKE_date=VALUES(REVOKE_date),
REVOKE_date_old=VALUES(REVOKE_date_old),
SHELF_STATUS=VALUES(SHELF_STATUS),
day_long=VALUES(day_long),
if_band=VALUES(if_band),
SHELF_HANDLE_STATUS=VALUES(SHELF_HANDLE_STATUS),
gmv=VALUES(gmv),
order_num=VALUES(order_num),
shelf_level_t=VALUES(shelf_level_t),
shelf_level=VALUES(shelf_level);
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