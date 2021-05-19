CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_shelf_check_for_month_label`()
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
SELECT count(*) into v_count FROM feods.D_LO_shelf_check_for_month_label ;
IF v_count =0 or DAY(DATE(DATE_SUB(NOW(),INTERVAL 1 HOUR)))<20 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据或当前日期小于20号不同步';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.shelf_check_for_month_label a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.D_LO_shelf_check_for_month_label);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.D_LO_shelf_check_for_month_label  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.shelf_check_for_month_label(
pid,
stat_date,
check_id,
operate_time,
business_area,
shelf_id,
shelf_type,
operator_ID,
operator_name,
SF_CODE,
month_end_operate,
add_time,
last_update_time
)
SELECT 
pid,
stat_date,
check_id,
operate_time,
business_area,
shelf_id,
shelf_type,
operator_ID,
operator_name,
SF_CODE,
month_end_operate,
add_time,
last_update_time
FROM feods.D_LO_shelf_check_for_month_label 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
ON DUPLICATE KEY UPDATE
pid=VALUES(pid), 
stat_date=VALUES(stat_date), 
check_id=VALUES(check_id), 
operate_time=VALUES(operate_time), 
business_area=VALUES(business_area), 
shelf_id=VALUES(shelf_id), 
shelf_type=VALUES(shelf_type), 
operator_ID=VALUES(operator_ID), 
operator_name=VALUES(operator_name), 
SF_CODE=VALUES(SF_CODE), 
month_end_operate=VALUES(month_end_operate), 
add_time=VALUES(add_time), 
last_update_time=VALUES(last_update_time);
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