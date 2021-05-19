CREATE DEFINER=`fedatasync`@`%` PROCEDURE `prc_sync_data_dm_op_shelf_product_trans_out_list`()
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
SELECT count(*) into v_count FROM fe_dm.dm_op_shelf_product_trans_out_list ;
IF v_count =0 THEN
   SET v_msg='同步数据源为空,中止同步,保留旧数据';
   select v_msg;
   LEAVE LABEL; # 退出存储过程
END IF;
SET v_msg=concat(v_msg,'v_interval=:',v_interval,char(13));
SET v_msg=concat(v_msg,'total records:',v_count,char(13));
SET autocommit=0;
DELETE a FROM fe_data.dm_op_shelf_product_trans_out_list a  
WHERE a.id NOT IN 
(SELECT id FROM  fe_dm.dm_op_shelf_product_trans_out_list);
SELECT ROW_COUNT() into v_effect_count;
IF v_error=1 THEN  
        ROLLBACK; -- 事务回滚  
        SET v_msg=concat(v_msg,'delete error',';effect records:',v_effect_count,char(13));
ELSE  
        COMMIT;  -- 事务提交  
        SET v_msg=concat(v_msg,'delete success',';effect records:',v_effect_count,char(13));
END IF;  
REPEAT 
select min(id) temp1,max(id) temp2  into v_temp1,v_temp2  from (select id from  fe_dm.dm_op_shelf_product_trans_out_list  where id> v_temp2 limit v_interval) t ;
set v_tag=v_tag+v_interval;
#SET v_temp1=v_temp2;
#SET v_temp2=v_temp2+v_interval;
set v_error=0;
INSERT INTO fe_data.dm_op_shelf_product_trans_out_list(
id,
BUSINESS_AREA,
SHELF_ID,
SHELF_CODE,
SF_CODE,
REAL_NAME,
warehouse_id,
PRODUCT_ID,
PRODUCT_CODE2,
SALE_PRICE,
PRODUCT_NAME,
FILL_MODEL,
SALES_FLAG,
tday_Q,
yday_Q,
month_sale_qty_A,
remain_qty,
add_time,
last_update_time
)
SELECT 
id,
BUSINESS_AREA,
SHELF_ID,
SHELF_CODE,
SF_CODE,
REAL_NAME,
warehouse_id,
PRODUCT_ID,
PRODUCT_CODE2,
SALE_PRICE,
PRODUCT_NAME,
FILL_MODEL,
SALES_FLAG,
tday_Q,
yday_Q,
month_sale_qty_A,
remain_qty,
add_time,
last_update_time
FROM fe_dm.dm_op_shelf_product_trans_out_list 
  WHERE  id>=v_temp1
  AND    id<=v_temp2
ON DUPLICATE KEY UPDATE
id=VALUES(id), 
BUSINESS_AREA=VALUES(BUSINESS_AREA), 
SHELF_ID=VALUES(SHELF_ID), 
SHELF_CODE=VALUES(SHELF_CODE), 
SF_CODE=VALUES(SF_CODE), 
REAL_NAME=VALUES(REAL_NAME), 
warehouse_id=VALUES(warehouse_id), 
PRODUCT_ID=VALUES(PRODUCT_ID), 
PRODUCT_CODE2=VALUES(PRODUCT_CODE2), 
SALE_PRICE=VALUES(SALE_PRICE), 
PRODUCT_NAME=VALUES(PRODUCT_NAME), 
FILL_MODEL=VALUES(FILL_MODEL), 
SALES_FLAG=VALUES(SALES_FLAG), 
tday_Q=VALUES(tday_Q), 
yday_Q=VALUES(yday_Q), 
month_sale_qty_A=VALUES(month_sale_qty_A), 
remain_qty=VALUES(remain_qty), 
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