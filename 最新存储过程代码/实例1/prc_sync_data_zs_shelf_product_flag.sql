CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_zs_shelf_product_flag`()
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
SELECT count(*) into v_count FROM feods.zs_shelf_product_flag ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
  END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
#删除不存在的数据,不用
DELETE a FROM fe_data.zs_shelf_product_flag a  
left join feods.zs_shelf_product_flag b on a.pid=b.pid 
where b.pid is null;
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.zs_shelf_product_flag  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
#插入或和修改数据
INSERT INTO fe_data.zs_shelf_product_flag(
pid,
shelf_id,
product_id,
sales_level,
danger_level,
#stock_level,
new_flag,
clean_time,
ext1,
ext2,
ext3,
ext4,
ext5,
ext6,
ext7,
ext8,
ext9,
ext_bin_1,
ext_bin_2,
ext_bin_3,
ext_bin_4,
ext_bin_5,
data_flag,
add_time,
add_user_id,
last_update_time,
last_update_user_id
)
SELECT 
pid,
shelf_id,
product_id,
sales_level,
danger_level,
#stock_level,
new_flag,
clean_time,
ext1,
ext2,
ext3,
ext4,
ext5,
ext6,
ext7,
ext8,
ext9,
ext_bin_1,
ext_bin_2,
ext_bin_3,
ext_bin_4,
ext_bin_5,
data_flag,
add_time,
add_user_id,
last_update_time,
last_update_user_id
FROM feods.zs_shelf_product_flag 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
ON DUPLICATE KEY UPDATE
pid=VALUES(pid),
shelf_id=VALUES(shelf_id),
product_id=VALUES(product_id),
sales_level=VALUES(sales_level),
danger_level=VALUES(danger_level),
#stock_level=VALUES(stock_level),
new_flag=VALUES(new_flag),
clean_time=VALUES(clean_time),
ext1=VALUES(ext1),
ext2=VALUES(ext2),
ext3=VALUES(ext3),
ext4=VALUES(ext4),
ext5=VALUES(ext5),
ext6=VALUES(ext6),
ext7=VALUES(ext7),
ext8=VALUES(ext8),
ext9=VALUES(ext9),
ext_bin_1=VALUES(ext_bin_1),
ext_bin_2=VALUES(ext_bin_2),
ext_bin_3=VALUES(ext_bin_3),
ext_bin_4=VALUES(ext_bin_4),
ext_bin_5=VALUES(ext_bin_5),
data_flag=VALUES(data_flag),
add_user_id=VALUES(add_user_id),
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