CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_fulltime_manager_star_assessment_score_detail`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 INT DEFAULT 0;
DECLARE v_temp2 INT DEFAULT 0;
DECLARE v_interval int  DEFAULT 20000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(15000) DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM feods.D_LO_fulltime_manager_star_assessment_score_detail ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.fulltime_manager_star_assessment_score_detail a  
WHERE a.item_id NOT IN 
(SELECT item_id FROM  feods.D_LO_fulltime_manager_star_assessment_score_detail);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(item_id) temp1,max(item_id) temp2  into v_temp1,v_temp2  from (select item_id from  feods.D_LO_fulltime_manager_star_assessment_score_detail  where item_id> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.fulltime_manager_star_assessment_score_detail(
item_id,              
statis_time,
manager_id,
statis_type,
statis_type_name,
task_finish_rate,
weight,
score,
DATA_FLAG
)
SELECT 
item_id,              
statis_time,
manager_id,
statis_type,
statis_type_name,
task_finish_rate,
weight,
score,
DATA_FLAG
FROM feods.D_LO_fulltime_manager_star_assessment_score_detail 
  WHERE  item_id>=v_temp1
  AND    item_id<=v_temp2
ON DUPLICATE KEY UPDATE
item_id=VALUES(item_id),         
statis_time=VALUES(statis_time),
manager_id=VALUES(manager_id),
statis_type=VALUES(statis_type),
statis_type_name=VALUES(statis_type_name),
task_finish_rate=VALUES(task_finish_rate),
weight=VALUES(weight),
score=VALUES(score),
DATA_FLAG=VALUES(DATA_FLAG);
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