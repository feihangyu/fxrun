CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_d_sc_preware_daily_report`()
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
SELECT count(*) into v_count FROM feods.d_sc_preware_daily_report  where  sdate> date_sub(now(),interval 7 day) and last_update_time > date_sub(now(),interval 2 day) ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0; 
/* 因为数据源是采用增量更新，没有用truncate操作，这里无需比较删除
DELETE a FROM fe_data.d_sc_preware_daily_report a  
WHERE a.pid NOT IN 
(SELECT pid FROM  feods.d_sc_preware_daily_report);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
*/  
REPEAT 
#select min(pid) temp1,max(pid) temp2  into v_temp1,v_temp2  from (select pid from  feods.d_sc_preware_daily_report  where  sdate> date_sub(now(),interval 7 day) and last_update_time > date_sub(now(),interval 2 day) and  pid> v_temp2 limit v_interval) t ;
select min(pid) temp1,max(pid) temp2   into v_temp1,v_temp2
 from
(
select pid from (
select pid
 from (
  select pid from  feods.d_sc_preware_daily_report 
  where sdate> date_sub(now(),interval 7 day) and last_update_time > date_sub(now(),interval 2 day) 
  and pid> v_temp2
  )t0
  order by pid
  )t1
  limit v_interval
  )t2;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
start transaction;
INSERT INTO fe_data.d_sc_preware_daily_report(
pid,
sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_name,
shelf_code,
product_id,
product_name,
product_code2,
product_type,
available_stock,
available_amount,
purchase_price,
actual_send_num,
actual_fill_num,
actual_send_forteen_qty,
actual_send_forteen_amount,
turn_over_day,
turn_over_level,
turn_over_level_flag,
require_amount,
seriously_stagnant_amount,
seriously_lack_amount,
cover_shelf_stock,
sale_flag,
sales_level,
sales_level_flag,
sale_in_fifteen_days,
per_shelf_dailysale,
sale_shelf_cnt,
stock_shelf_cnt,
cover_shelf_cnt,
qualityqty,
f_bgj_fboxedstandards,
satisfy,
fname,
total_stock,
total_stock_yesterday,
stock_in_theory,
avg_send_num,
avg_send_amount,
quantity,
GMV,
onload_num,
valid_turnover_days,
whether_close,
revoke_status,
suggest_fill,
warehouse_satitsfy_amend,
suggest_fill_qty,
min_suggest_qty,
max_suggest_qty,
suggest_fill_amend,
fill_priority,
add_time,
last_update_time
)
SELECT 
pid,
sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_name,
shelf_code,
product_id,
product_name,
product_code2,
product_type,
available_stock,
available_amount,
purchase_price,
actual_send_num,
actual_fill_num,
actual_send_forteen_qty,
actual_send_forteen_amount,
turn_over_day,
turn_over_level,
turn_over_level_flag,
require_amount,
seriously_stagnant_amount,
seriously_lack_amount,
cover_shelf_stock,
sale_flag,
sales_level,
sales_level_flag,
sale_in_fifteen_days,
per_shelf_dailysale,
sale_shelf_cnt,
stock_shelf_cnt,
cover_shelf_cnt,
qualityqty,
f_bgj_fboxedstandards,
satisfy,
fname,
total_stock,
total_stock_yesterday,
stock_in_theory,
avg_send_num,
avg_send_amount,
quantity,
GMV,
onload_num,
valid_turnover_days,
whether_close,
revoke_status,
suggest_fill,
warehouse_satitsfy_amend,
suggest_fill_qty,
min_suggest_qty,
max_suggest_qty,
suggest_fill_amend,
fill_priority,
add_time,
last_update_time
FROM feods.d_sc_preware_daily_report 
  WHERE  pid>=v_temp1
  AND    pid<=v_temp2
  AND    sdate> date_sub(now(),interval 7 day) and last_update_time > date_sub(now(),interval 2 day) 
ON DUPLICATE KEY UPDATE
pid=VALUES(pid),
sdate=VALUES(sdate),
region_area=VALUES(region_area),
business_area=VALUES(business_area),
warehouse_number=VALUES(warehouse_number),
warehouse_name=VALUES(warehouse_name),
warehouse_id=VALUES(warehouse_id),
shelf_name=VALUES(shelf_name),
shelf_code=VALUES(shelf_code),
product_id=VALUES(product_id),
product_name=VALUES(product_name),
product_code2=VALUES(product_code2),
product_type=VALUES(product_type),
available_stock=VALUES(available_stock),
available_amount=VALUES(available_amount),
purchase_price=VALUES(purchase_price),
actual_send_num=VALUES(actual_send_num),
actual_fill_num=VALUES(actual_fill_num),
actual_send_forteen_qty=VALUES(actual_send_forteen_qty),
actual_send_forteen_amount=VALUES(actual_send_forteen_amount),
turn_over_day=VALUES(turn_over_day),
turn_over_level=VALUES(turn_over_level),
turn_over_level_flag=VALUES(turn_over_level_flag),
require_amount=VALUES(require_amount),
seriously_stagnant_amount=VALUES(seriously_stagnant_amount),
seriously_lack_amount=VALUES(seriously_lack_amount),
cover_shelf_stock=VALUES(cover_shelf_stock),
sale_flag=VALUES(sale_flag),
sales_level=VALUES(sales_level),
sales_level_flag=VALUES(sales_level_flag),
sale_in_fifteen_days=VALUES(sale_in_fifteen_days),
per_shelf_dailysale=VALUES(per_shelf_dailysale),
sale_shelf_cnt=VALUES(sale_shelf_cnt),
stock_shelf_cnt=VALUES(stock_shelf_cnt),
cover_shelf_cnt=VALUES(cover_shelf_cnt),
qualityqty=VALUES(qualityqty),
f_bgj_fboxedstandards=VALUES(f_bgj_fboxedstandards),
satisfy=VALUES(satisfy),
fname=VALUES(fname),
total_stock=VALUES(total_stock),
total_stock_yesterday=VALUES(total_stock_yesterday),
stock_in_theory=VALUES(stock_in_theory),
avg_send_num=VALUES(avg_send_num),
avg_send_amount=VALUES(avg_send_amount),
quantity=VALUES(quantity),
GMV=VALUES(GMV),
onload_num=VALUES(onload_num),
valid_turnover_days=VALUES(valid_turnover_days),
whether_close=VALUES(whether_close),
revoke_status=VALUES(revoke_status),
suggest_fill=VALUES(suggest_fill),
warehouse_satitsfy_amend=VALUES(warehouse_satitsfy_amend),
suggest_fill_qty=VALUES(suggest_fill_qty),
min_suggest_qty=VALUES(min_suggest_qty),
max_suggest_qty=VALUES(max_suggest_qty),
suggest_fill_amend=VALUES(suggest_fill_amend),
fill_priority=VALUES(fill_priority),
add_time=VALUES(add_time),
last_update_time=VALUES(last_update_time);
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