CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_zs_shelf_manager_check_monitor_point`()
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
SELECT count(*) into v_count FROM feods.zs_shelf_manager_check_monitor_point ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.zs_shelf_manager_check_monitor_point a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.zs_shelf_manager_check_monitor_point);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.zs_shelf_manager_check_monitor_point  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.zs_shelf_manager_check_monitor_point(
pid,
check_id,
shelf_id,
business_area,
operate_time,
operate_period,
operate_stock,
suspect_fake_operate,
operate_sku,
operate_error_num,
shelf_type,
operator_name,
operator_ID
)
SELECT 
pid,
check_id,
shelf_id,
business_area,
operate_time,
operate_period,
operate_stock,
suspect_fake_operate,
operate_sku,
operate_error_num,
shelf_type,
operator_name,
operator_ID
FROM feods.zs_shelf_manager_check_monitor_point 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
ON DUPLICATE KEY UPDATE
pid=VALUES(pid),
check_id=VALUES(check_id),
shelf_id=VALUES(shelf_id),
business_area=VALUES(business_area),
operate_time=VALUES(operate_time),
operate_period=VALUES(operate_period),
operate_stock=VALUES(operate_stock),
suspect_fake_operate=VALUES(suspect_fake_operate),
operate_sku=VALUES(operate_sku),
operate_error_num=VALUES(operate_error_num),
shelf_type=VALUES(shelf_type),
operator_name=VALUES(operator_name),
operator_ID=VALUES(operator_ID);
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