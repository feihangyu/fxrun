CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_zs_shelf_flag`()
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
SELECT count(*) into v_count FROM feods.zs_shelf_flag ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.zs_shelf_flag a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.zs_shelf_flag);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.zs_shelf_flag  where pid> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.zs_shelf_flag(
  pid,
  shelf_id,
  daoshun_level,
  GMV_level,
  sale_qty_level,
  if_dixiao_shelf,
  cooperation_type,
  city,
  shelf_type,
  users_potential,
  users_saturability,
  users_quanlity ,
  reorder_rate,
  users_active ,
  users_chg ,
  shelf_value ,
  orders_per_user,  
  ext1,
  ext2,
  ext3,
  ext4,
  ext5,
  ext6,
  ext7,
  ext8,
  ext9,
  ext10,
  ext11,
  ext12,
  ext13,
  ext14,
  ext15,
  ext16,
  ext17,
  ext18,
  ext19,
  ext20,
  ext21,
  ext22,
  ext23,
  ext24,
  ext25,
  ext26,
  ext27,
  ext28,
  ext29,
  ext30,
  ext31,
  ext32,
  ext33,
  ext34,
  ext_bin_1,
  ext_bin_2,
  ext_bin_3,
  ext_bin_4,
  ext_bin_5
  )
SELECT
  pid,
  shelf_id,
  daoshun_level,
  GMV_level,
  sale_qty_level,
  if_dixiao_shelf,
  cooperation_type,
  city,
  shelf_type,
  users_potential,
  users_saturability,
  users_quanlity ,
  reorder_rate,
  users_active ,
  users_chg ,
  shelf_value ,
  orders_per_user,  
  ext1,
  ext2,
  ext3,
  ext4,
  ext5,
  ext6,
  ext7,
  ext8,
  ext9,
  ext10,
  ext11,
  ext12,
  ext13,
  ext14,
  ext15,
  ext16,
  ext17,
  ext18,
  ext19,
  ext20,
  ext21,
  ext22,
  ext23,
  ext24,
  ext25,
  ext26,
  ext27,
  ext28,
  ext29,
  ext30,
  ext31,
  ext32,
  ext33,
  ext34,  
  ext_bin_1,
  ext_bin_2,
  ext_bin_3,
  ext_bin_4,
  ext_bin_5 
FROM
  feods.zs_shelf_flag t
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
  ON DUPLICATE KEY UPDATE 
  pid=VALUES(pid),
  shelf_id=VALUES(shelf_id),
  daoshun_level=VALUES(daoshun_level),
  GMV_level=VALUES(GMV_level),
  if_dixiao_shelf=VALUES(if_dixiao_shelf),
  if_dixiao_shelf=VALUES(if_dixiao_shelf),
  cooperation_type=VALUES(cooperation_type),
  city=VALUES(city),
  shelf_type=VALUES(shelf_type),
  users_potential=VALUES(users_potential),
  users_saturability=VALUES(users_saturability),
  users_quanlity=VALUES(users_quanlity),
  reorder_rate=VALUES(reorder_rate),
  users_active=VALUES(users_active),
  users_chg =VALUES(users_chg),
  shelf_value =VALUES(shelf_value),
  orders_per_user=VALUES(orders_per_user),
ext1=VALUES(ext1),
ext2=VALUES(ext2),
ext3=VALUES(ext3),
ext4=VALUES(ext4),
ext5=VALUES(ext5),
ext6=VALUES(ext6),
ext7=VALUES(ext7),
ext8=VALUES(ext8),
ext9=VALUES(ext9),
ext10=VALUES(ext10),
ext11=VALUES(ext11),
ext12=VALUES(ext12),
ext13=VALUES(ext13),
ext14=VALUES(ext14),
ext15=VALUES(ext15),
ext16=VALUES(ext16),
ext17=VALUES(ext17),
ext18=VALUES(ext18),
ext19=VALUES(ext19),
ext20=VALUES(ext20),
ext21=VALUES(ext21),
ext22=VALUES(ext22),
ext23=VALUES(ext23),
ext24=VALUES(ext24),
ext25=VALUES(ext25),
ext26=VALUES(ext26),
ext27=VALUES(ext27),
ext28=VALUES(ext28),
ext29=VALUES(ext29),
ext30=VALUES(ext30),
ext31=VALUES(ext31),
ext32=VALUES(ext32),
ext33=VALUES(ext33),
ext34=VALUES(ext34),
ext_bin_1=VALUES(ext_bin_1),
ext_bin_2=VALUES(ext_bin_2),
ext_bin_3=VALUES(ext_bin_3),
ext_bin_4=VALUES(ext_bin_4),
ext_bin_5=VALUES(ext_bin_5);  
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