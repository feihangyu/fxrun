CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_pj_manager_shelf_statistic_result`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 INT DEFAULT 0;
DECLARE v_temp2 INT DEFAULT 0;
DECLARE v_interval int  DEFAULT 500000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(10000) DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM feods.pj_manager_shelf_statistic_result ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.pj_manager_shelf_statistic_result a  
WHERE a.sequence NOT IN 
(SELECT sequence FROM  feods.pj_manager_shelf_statistic_result);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(sequence) temp1,max(sequence) temp2  into v_temp1,v_temp2  from (select sequence from  feods.pj_manager_shelf_statistic_result  where sequence> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.pj_manager_shelf_statistic_result(
sequence,                   
shelf_id,                   
shelf_name,                 
real_name,                  
manager_id,                
sf_code,                    
business_area,              
manager_category,           
date_category,             
statis_date,                
shelf_gmv,                  
filling_amount_rate,        
shelf_qty,                  
tor_added_rate,             
shelf_avg_gmv_finish_rate,  
remove_shelf_rate,          
filling_amount_recover_rate)
SELECT 
sequence,                   
shelf_id,                   
shelf_name,                 
real_name,                  
manager_id,                
sf_code,                    
business_area,              
manager_category,           
date_category,             
statis_date,                
shelf_gmv,                  
filling_amount_rate,        
shelf_qty,                  
tor_added_rate,             
shelf_avg_gmv_finish_rate,  
remove_shelf_rate,          
filling_amount_recover_rate
FROM feods.pj_manager_shelf_statistic_result 
  WHERE  sequence>=v_temp1
  AND    sequence<=v_temp2
ON DUPLICATE KEY UPDATE
sequence=VALUES(sequence),                   
shelf_id=VALUES(shelf_id),                
shelf_name=VALUES(shelf_name),
real_name=VALUES(real_name),             
manager_id=VALUES(manager_id),           
sf_code=VALUES(sf_code),             
business_area=VALUES(business_area),
manager_category=VALUES(manager_category),
date_category=VALUES(date_category),  
statis_date=VALUES(statis_date),
shelf_gmv=VALUES(shelf_gmv),
filling_amount_rate=VALUES(filling_amount_rate),
shelf_qty=VALUES(shelf_qty),
tor_added_rate=VALUES(tor_added_rate),
shelf_avg_gmv_finish_rate=VALUES(shelf_avg_gmv_finish_rate),
remove_shelf_rate=VALUES(remove_shelf_rate),
filling_amount_recover_rate=VALUES(filling_amount_recover_rate);
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