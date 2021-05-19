CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_stat_group_distribute_dept_rank`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 INT DEFAULT 0;
DECLARE v_temp2 INT DEFAULT 0;
DECLARE v_interval int  DEFAULT 50000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(16000) DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM fe_dm.dm_en_group_distribute_dept_rank;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.stat_group_distribute_dept_rank a  
WHERE a.emp_rank_id NOT IN 
(SELECT emp_rank_id FROM  fe_dm.dm_en_group_distribute_dept_rank);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(emp_rank_id) temp1,max(emp_rank_id) temp2  into v_temp1,v_temp2  from (select emp_rank_id from  fe_dm.dm_en_group_distribute_dept_rank  where emp_rank_id> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.stat_group_distribute_dept_rank(
emp_rank_id,
group_customer_id,
dept_id,
year,
unit_type,
unit_time,
distributed_amount,
recycled_amount,
rank_amount,
data_flag,
add_time,
add_user_id,
last_update_time,
last_update_user_id
)
SELECT 
emp_rank_id,
group_customer_id,
dept_id,
year,
unit_type,
unit_time,
distributed_amount,
recycled_amount,
rank_amount,
data_flag,
add_time,
add_user_id,
last_update_time,
last_update_user_id
FROM fe_dm.dm_en_group_distribute_dept_rank 
  WHERE  emp_rank_id>=v_temp1
  AND    emp_rank_id<=v_temp2
ON DUPLICATE KEY UPDATE
emp_rank_id=VALUES(emp_rank_id),
group_customer_id=VALUES(group_customer_id),
dept_id=VALUES(dept_id),
year=VALUES(year),
unit_type=VALUES(unit_type),
unit_time=VALUES(unit_time),
distributed_amount=VALUES(distributed_amount),
recycled_amount=VALUES(recycled_amount),
rank_amount=VALUES(rank_amount),
data_flag=VALUES(data_flag),
add_time=VALUES(add_time),
add_user_id=VALUES(add_user_id),
last_update_time=VALUES(last_update_time),
last_update_user_id=VALUES(last_update_user_id);
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