CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_machine_slot_type`()
BEGIN
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
	
	
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_machine_slot_type_TEST;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_machine_slot_type_TEST like fe_dwd.dwd_shelf_machine_slot_type;
INSERT INTO fe_dwd.dwd_shelf_machine_slot_type_TEST
(
`slot_id`
,`manufacturer_slot_code`
,`machine_id`
,`shelf_id`
,`slot_status`
,`product_id`
,`stock_num`
,slot_type_id
,machine_type_id
,slot_capacity_limit 
,slot_col 
)
select 
t.`slot_id`
,t.`manufacturer_slot_code`
,t.`machine_id`
,t.`shelf_id`
,t.`slot_status`
,t.`product_id`
,t.`stock_num`
,m.slot_type_id
,m.machine_type_id
,m.slot_capacity_limit 
,m.slot_col 
FROM
fe.sf_shelf_machine_slot t
LEFT JOIN fe.sf_shelf_machine_slot_type m
ON t.slot_type_id = m.slot_type_id
AND m.data_flag = 1
where t.data_flag =1;
  
-- 正式执行
truncate table fe_dwd.dwd_shelf_machine_slot_type;  
INSERT INTO fe_dwd.dwd_shelf_machine_slot_type
(
`slot_id`
,`manufacturer_slot_code`
,`machine_id`
,`shelf_id`
,`slot_status`
,`product_id`
,`stock_num`
,slot_type_id
,machine_type_id
,slot_capacity_limit 
,slot_col 
)
select 
t.`slot_id`
,t.`manufacturer_slot_code`
,t.`machine_id`
,t.`shelf_id`
,t.`slot_status`
,t.`product_id`
,t.`stock_num`
,m.slot_type_id
,m.machine_type_id
,m.slot_capacity_limit 
,m.slot_col 
FROM
fe.sf_shelf_machine_slot t
LEFT JOIN fe.sf_shelf_machine_slot_type m
ON t.slot_type_id = m.slot_type_id
AND m.data_flag = 1
where t.data_flag =1;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_machine_slot_type',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
COMMIT;
END