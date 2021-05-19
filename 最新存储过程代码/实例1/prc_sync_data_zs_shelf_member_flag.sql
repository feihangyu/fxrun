CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_zs_shelf_member_flag`()
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
SELECT count(*) into v_count FROM feods.zs_shelf_member_flag ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
delete a from fe_data.zs_shelf_member_flag a  
 where a.user_id not in (
select user_id  from feods.zs_shelf_member_flag );
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
repeat 
select min(user_id) temp1,max(user_id) temp2  into v_temp1,v_temp2  from (select user_id from  feods.zs_shelf_member_flag  where user_id> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.zs_shelf_member_flag(
  sdate,
  user_id,
  last_buy_time,
  order_qty,
  gender,
  age_level,
  age,
  belong_industry,
  mobile_phone,
  reg_channel,
  user_type_buy,
  user_type_activity,
  user_type_buy_time,
  user_type_buy_kds,
  member_level,
  shelf_status,
  shelf_id,
  province,
  city,
  district,
  company_name,
  company_address,
  company_type,
  business_characteristics,
  user_buy_value,
  user_buy_type,
  user_buy_hour,
  user_buy_if_weekend,
  user_buy_product_id,
  user_kouwei,
  user_gongneng,
  user_texing,
  user_buy_goods_type,
  user_huodongmingan,
  user_yxgj,
  if_buy_this_week,
  user_buy_goods_type_bin,
  user_buy_hour_bin,
  if_revoke,
  if_shelf_admin,
  reg_client,
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
  user_life_cycle_genera,
  recent_buy_status,
  if_new_register,
  pct_level,
  last_week_order_qty_level,
  birthday_if_this_week,
  birthday_weekday,
  if_unsalable_user,
  if_full_stock_user,
  if_coupon,
  if_wechat_subscribe,
  ext_bin_1,
  ext_bin_2,
  ext_bin_3,
  ext_bin_4,
  ext_bin_5,  
  if_revoke_update_time,
  coupon_type,
  discount_type,
  user_type_buy_new,
  user_type_buy_time_new,
  user_type_buy_kds_new 
  )
SELECT
  sdate,
  user_id,
  last_buy_time,
  order_qty,
  gender,
  age_level,
  age,
  belong_industry,
  mobile_phone,
  reg_channel,
  user_type_buy,
  user_type_activity,
  user_type_buy_time,
  user_type_buy_kds,
  member_level,
  shelf_status,
  shelf_id,
  province,
  city,
  district,
  company_name,
  company_address,
  company_type,
  business_characteristics,
  user_buy_value,
  user_buy_type,
  user_buy_hour,
  user_buy_if_weekend,
  user_buy_product_id,
  user_kouwei,
  user_gongneng,
  user_texing,
  user_buy_goods_type,
  user_huodongmingan,
  user_yxgj,
  if_buy_this_week,
  user_buy_goods_type_bin,
  user_buy_hour_bin,
  if_revoke,
  if_shelf_admin,
  reg_client,
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
  user_life_cycle_genera,
  recent_buy_status,
  if_new_register,
  pct_level,
  last_week_order_qty_level,
  birthday_if_this_week,
  birthday_weekday,
  if_unsalable_user,
  if_full_stock_user,
  if_coupon,
  if_wechat_subscribe,
  ext_bin_1,
  ext_bin_2,
  ext_bin_3,
  ext_bin_4,
  ext_bin_5,  
  if_revoke_update_time,
 coupon_type,
 discount_type,
 user_type_buy_new,
 user_type_buy_time_new,
 user_type_buy_kds_new
FROM
  feods.zs_shelf_member_flag 
  WHERE  user_id>=v_temp1
  AND    user_id<=v_temp2  
  ON DUPLICATE KEY UPDATE 
  user_id=VALUES(user_id),
  last_buy_time=VALUES(last_buy_time),
  order_qty=VALUES(order_qty),
  gender=VALUES(gender),
  age_level=VALUES(age_level),
  age=VALUES(age),
  belong_industry=VALUES(belong_industry),
  mobile_phone=VALUES(mobile_phone),
  reg_channel=VALUES(reg_channel),
  user_type_buy=VALUES(user_type_buy),
  user_type_activity=VALUES(user_type_activity),
  user_type_buy_time=VALUES(user_type_buy_time),
  user_type_buy_kds=VALUES(user_type_buy_kds),
  member_level=VALUES(member_level),
  shelf_status=VALUES(shelf_status),
  shelf_id=VALUES(shelf_id),
  province=VALUES(province),
  city=VALUES(city),
  district=VALUES(district),
  company_name=VALUES(company_name),
  company_address=VALUES(company_address),
  company_type=VALUES(company_type),
  business_characteristics=VALUES(business_characteristics),
  user_buy_value=VALUES(user_buy_value),
  user_buy_type=VALUES(user_buy_type),
  user_buy_hour=VALUES(user_buy_hour),
  user_buy_if_weekend=VALUES(user_buy_if_weekend),
  user_buy_product_id=VALUES(user_buy_product_id),
  user_kouwei=VALUES(user_kouwei),
  user_gongneng=VALUES(user_gongneng),
  user_texing=VALUES(user_texing),
  user_buy_goods_type=VALUES(user_buy_goods_type),
  user_huodongmingan=VALUES(user_huodongmingan),
  user_yxgj=VALUES(user_yxgj),
  if_buy_this_week=VALUES(if_buy_this_week),
  user_buy_goods_type_bin=VALUES(user_buy_goods_type_bin),
  user_buy_hour_bin=VALUES(user_buy_hour_bin),
  if_revoke=VALUES(if_revoke),
  if_shelf_admin=VALUES(if_shelf_admin),
  reg_client=VALUES(reg_client),
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
  user_life_cycle_genera=VALUES(user_life_cycle_genera),
  recent_buy_status=VALUES(recent_buy_status),
  if_new_register=VALUES(if_new_register),
  pct_level=VALUES(pct_level),
  last_week_order_qty_level=VALUES(last_week_order_qty_level),
  birthday_if_this_week=VALUES(birthday_if_this_week),
  birthday_weekday=VALUES(birthday_weekday),
  if_unsalable_user=VALUES(if_unsalable_user),
  if_full_stock_user=VALUES(if_full_stock_user),
  if_coupon=VALUES(if_coupon),
  if_wechat_subscribe=VALUES(if_wechat_subscribe),  
  ext_bin_1=VALUES(ext_bin_1),
  ext_bin_2=VALUES(ext_bin_2),
  ext_bin_3=VALUES(ext_bin_3),
  ext_bin_4=VALUES(ext_bin_4),
  ext_bin_5=VALUES(ext_bin_5),  
  if_revoke_update_time=values(if_revoke_update_time),
  coupon_type=VALUES(coupon_type),
  discount_type=VALUES(discount_type),
  user_type_buy_new=VALUES(user_type_buy_new),
  user_type_buy_time_new=VALUES(user_type_buy_time_new),
  user_type_buy_kds_new =VALUES(user_type_buy_kds_new);
  
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