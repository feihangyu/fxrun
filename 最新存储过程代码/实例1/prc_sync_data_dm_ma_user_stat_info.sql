CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_dm_ma_user_stat_info`()
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
SELECT count(*) into v_count FROM fe_dm.dm_ma_user_stat_info;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.dm_ma_user_stat_info a  
WHERE a.user_id NOT IN 
(SELECT user_id FROM  fe_dm.dm_ma_user_stat_info);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(user_id) temp1,max(user_id) temp2  into v_temp1,v_temp2  from (select user_id from  fe_dm.dm_ma_user_stat_info  where user_id> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.dm_ma_user_stat_info(
user_id,
product_id_top5,
birthday,
ext_int1,
ext_int2,
ext_int3,
ext_int4,
ext_int5,
ext_bin_1,
ext_bin_2,
ext_bin_3,
ext_bin_4,
ext_bin_5,
add_time,
last_update_time
)
SELECT 
user_id,
product_id_top5,
birthday,
ext_int1,
ext_int2,
ext_int3,
ext_int4,
ext_int5,
ext_bin_1,
ext_bin_2,
ext_bin_3,
ext_bin_4,
ext_bin_5,
add_time,
last_update_time
FROM fe_dm.dm_ma_user_stat_info 
  WHERE  user_id>=v_temp1
  AND    user_id<=v_temp2
ON DUPLICATE KEY UPDATE
user_id=VALUES(user_id),
product_id_top5=VALUES(product_id_top5),
birthday=VALUES(birthday),
ext_int1=VALUES(ext_int1),
ext_int2=VALUES(ext_int2),
ext_int3=VALUES(ext_int3),
ext_int4=VALUES(ext_int4),
ext_int5=VALUES(ext_int5),
ext_bin_1=VALUES(ext_bin_1),
ext_bin_2=VALUES(ext_bin_2),
ext_bin_3=VALUES(ext_bin_3),
ext_bin_4=VALUES(ext_bin_4),
ext_bin_5=VALUES(ext_bin_5),
add_time=VALUES(add_time);
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