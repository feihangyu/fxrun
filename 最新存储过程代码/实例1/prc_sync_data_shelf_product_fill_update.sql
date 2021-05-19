CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_shelf_product_fill_update`()
LABEL:BEGIN
DECLARE v_start_date datetime DEFAULT now();
DECLARE v_temp1 INT DEFAULT 0;
DECLARE v_temp2 INT DEFAULT 0;
DECLARE v_interval int  DEFAULT 10000;
DECLARE v_count BIGINT DEFAULT 0;
DECLARE v_tag BIGINT DEFAULT 0;
DECLARE v_effect_count BIGINT DEFAULT 0;
DECLARE v_error INT DEFAULT 0;
DECLARE v_msg varchar(16000) DEFAULT '';
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;  
SELECT count(*) into v_count FROM feods.d_op_shelf_product_fill_update ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.shelf_product_fill_update a  
WHERE a.id NOT IN 
(SELECT id FROM  feods.d_op_shelf_product_fill_update);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(id) temp1,max(id) temp2  into v_temp1,v_temp2  from (select id from  feods.d_op_shelf_product_fill_update  where id> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.shelf_product_fill_update(
id,
cdate,
DETAIL_ID,
ITEM_ID,
PRODUCT_ID,
product_name,
product_fe,
SHELF_ID,
SHELF_TYPE,
shelf_level,
SALE_PRICE,
NEW_FLAG,
SALES_FLAG,
FILL_MODEL,
ALARM_QUANTITY,
SHELF_FILL_FLAG,
STOCK_NUM,
ONWAY_NUM,
WEEK_SALE_NUM,
PRODUCT_TYPE,
warehouse_type,
warehouse_stock,
shelf_group,
fill_order_day,
whether_push_order,
fill_cycle,
fill_days,
is_holiday_stock_up,
holiday_stock_up_qty,
holiday_stock_up_ratio,
holiday_stock_up_cycle,
holiday_stock_up_datetime,
holiday_stop_fill_date,
holiday_recover_fill_date,
predict_day_sale_qty,
day_sale_qty,
season_factor,
safe_stock_qty,
shelf_stock_upper_limit,
stock_total_qty,
suspect_false_stock_qty,
SUGGEST_FILL_NUM,
reduce_suggest_fill_num,
reduce_suggest_fill_ceiling_num,
add_time,
last_update_time
)
SELECT 
id,
cdate,
DETAIL_ID,
ITEM_ID,
PRODUCT_ID,
product_name,
product_fe,
SHELF_ID,
SHELF_TYPE,
shelf_level,
SALE_PRICE,
NEW_FLAG,
SALES_FLAG,
FILL_MODEL,
ALARM_QUANTITY,
SHELF_FILL_FLAG,
STOCK_NUM,
ONWAY_NUM,
WEEK_SALE_NUM,
PRODUCT_TYPE,
warehouse_type,
warehouse_stock,
shelf_group,
fill_order_day,
whether_push_order,
fill_cycle,
fill_days,
is_holiday_stock_up,
holiday_stock_up_qty,
holiday_stock_up_ratio,
holiday_stock_up_cycle,
holiday_stock_up_datetime,
holiday_stop_fill_date,
holiday_recover_fill_date,
predict_day_sale_qty,
day_sale_qty,
season_factor,
safe_stock_qty,
shelf_stock_upper_limit,
stock_total_qty,
suspect_false_stock_qty,
SUGGEST_FILL_NUM,
reduce_suggest_fill_num,
reduce_suggest_fill_ceiling_num,
add_time,
last_update_time
FROM feods.d_op_shelf_product_fill_update 
  WHERE  id>=v_temp1
  AND    id<=v_temp2
ON DUPLICATE KEY UPDATE
id=VALUES(id), 
cdate=VALUES(cdate), 
DETAIL_ID=VALUES(DETAIL_ID), 
ITEM_ID=VALUES(ITEM_ID), 
PRODUCT_ID=VALUES(PRODUCT_ID), 
product_name=VALUES(product_name), 
product_fe=VALUES(product_fe), 
SHELF_ID=VALUES(SHELF_ID), 
SHELF_TYPE=VALUES(SHELF_TYPE), 
shelf_level=VALUES(shelf_level), 
SALE_PRICE=VALUES(SALE_PRICE), 
NEW_FLAG=VALUES(NEW_FLAG),
SALES_FLAG=VALUES(SALES_FLAG), 
FILL_MODEL=VALUES(FILL_MODEL), 
ALARM_QUANTITY=VALUES(ALARM_QUANTITY), 
SHELF_FILL_FLAG=VALUES(SHELF_FILL_FLAG), 
STOCK_NUM=VALUES(STOCK_NUM), 
ONWAY_NUM=VALUES(ONWAY_NUM), 
WEEK_SALE_NUM=VALUES(WEEK_SALE_NUM), 
PRODUCT_TYPE=VALUES(PRODUCT_TYPE), 
warehouse_type=VALUES(warehouse_type), 
warehouse_stock=VALUES(warehouse_stock), 
shelf_group=VALUES(shelf_group), 
fill_order_day=VALUES(fill_order_day), 
whether_push_order=VALUES(whether_push_order), 
fill_cycle=VALUES(fill_cycle), 
fill_days=VALUES(fill_days), 
is_holiday_stock_up=VALUES(is_holiday_stock_up), 
holiday_stock_up_qty=VALUES(holiday_stock_up_qty), 
holiday_stock_up_ratio=VALUES(holiday_stock_up_ratio), 
holiday_stock_up_cycle=VALUES(holiday_stock_up_cycle), 
holiday_stock_up_datetime=VALUES(holiday_stock_up_datetime), 
holiday_stop_fill_date=VALUES(holiday_stop_fill_date), 
holiday_recover_fill_date=VALUES(holiday_recover_fill_date), 
predict_day_sale_qty=VALUES(predict_day_sale_qty), 
day_sale_qty=VALUES(day_sale_qty), 
season_factor=VALUES(season_factor), 
safe_stock_qty=VALUES(safe_stock_qty), 
shelf_stock_upper_limit=VALUES(shelf_stock_upper_limit), 
stock_total_qty=VALUES(stock_total_qty), 
suspect_false_stock_qty=VALUES(suspect_false_stock_qty), 
SUGGEST_FILL_NUM=VALUES(SUGGEST_FILL_NUM), 
reduce_suggest_fill_num=VALUES(reduce_suggest_fill_num), 
reduce_suggest_fill_ceiling_num=VALUES(reduce_suggest_fill_ceiling_num), 
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