CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_pj_zs_goods_damaged`()
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
SELECT count(*) into v_count FROM feods.pj_zs_goods_damaged ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.pj_zs_goods_damaged a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.pj_zs_goods_damaged);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.pj_zs_goods_damaged  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.pj_zs_goods_damaged(
pid,
smonth,  
city_name,          
shelf_id,
shelf_code ,        
sf_code, 
real_name ,         
shelf_status,       
activate_time ,     
revoke_time ,       
stock_value_old,    
in_value,
sale_value,         
stock_value_now,    
huosun_qty,         
huosun,  
bk_money ,          
gmv,
user_qty  ,         
operate_time ,      
damaged_value ,     
damaged_value_aduit,
overdue_value ,     
overdue_value_aduit,
quality_value ,     
quality_value_aduit,
total_error_value  )
SELECT
pid,
smonth,  
city_name,          
shelf_id,
shelf_code ,        
sf_code, 
real_name ,         
shelf_status,       
activate_time ,     
revoke_time ,       
stock_value_old,    
in_value,
sale_value,         
stock_value_now,    
huosun_qty,         
huosun,  
bk_money ,          
gmv,
user_qty  ,         
operate_time ,      
damaged_value ,     
damaged_value_aduit,
overdue_value ,     
overdue_value_aduit,
quality_value ,     
quality_value_aduit,
total_error_value  
FROM
  feods.pj_zs_goods_damaged
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2  
  ON DUPLICATE KEY UPDATE   
pid=VALUES(pid),
smonth=VALUES(smonth),     
city_name=VALUES(city_name),   
shelf_id=VALUES(shelf_id),   
shelf_code=VALUES(shelf_code),
sf_code=VALUES(sf_code),
real_name=VALUES(real_name),
shelf_status=VALUES(shelf_status),   
activate_time=VALUES(activate_time),
revoke_time=VALUES(revoke_time), 
stock_value_old=VALUES(stock_value_old),
in_value=VALUES(in_value),   
sale_value=VALUES(sale_value),
stock_value_now=VALUES(stock_value_now),
huosun_qty=VALUES(huosun_qty),   
huosun=VALUES(huosun),
bk_money =VALUES(bk_money),  
gmv=VALUES(gmv),    
user_qty=VALUES(user_qty),
operate_time =VALUES(operate_time),   
damaged_value =VALUES(damaged_value),
damaged_value_aduit=VALUES(damaged_value_aduit),
overdue_value =VALUES(overdue_value),
overdue_value_aduit=VALUES(overdue_value_aduit),
quality_value=VALUES(quality_value), 
quality_value_aduit=VALUES(quality_value_aduit),
total_error_value =VALUES(total_error_value);
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