CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_machine_info`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_shelf_machine_info_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_machine_info_1  AS 
select 
a.shelf_id
,a.machine_name
,a.machine_type_id
,a.product_template_id
,a.machine_id
,a.imei
,a.mid
,a.pub_key
,a.pri_key
,a.accendant_phone
,a.online_status
,a.slot_sync_status
,a.standard_temperature
,a.app_key
,a.version_number
,a.ip
,a.machine_signal
,a.door
,b.type_name
,b.manufacturer_code
,b.manufacturer_name
,b.machine_type_code
,b.machine_type_name
,b.shelf_type
,b.second_type
,b.product_slots_num
,c.slot_template_id
,c.template_name
,c.machine_type
,c.template_no
from fe.sf_shelf_machine a
left join fe.`sf_shelf_machine_type` b
on a.machine_type_id = b.machine_type_id
and b.data_flag =1
left join fe.sf_shelf_machine_product_template c
on a.product_template_id = c.product_template_id
and c.data_flag =1
where a.data_flag =1;
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_machine_info_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_machine_info_test like fe_dwd.dwd_shelf_machine_info;
INSERT INTO fe_dwd.dwd_shelf_machine_info_test
(
shelf_id
,shelf_type
,product_slots_num
,second_type
,machine_name
,machine_type_code
,machine_type_id
,machine_type_name
,type_name
,product_template_id
,slot_template_id
,template_name
,machine_type
,template_no
,machine_id
,imei
,mid
,pub_key
,pri_key
,manufacturer_code
,manufacturer_name
,accendant_phone
,online_status
,slot_sync_status
,standard_temperature
,app_key
,version_number
,ip
,machine_signal
,door
)
select
a.shelf_id
,a.shelf_type
,a.product_slots_num
,a.second_type
,a.machine_name
,a.machine_type_code
,a.machine_type_id
,a.machine_type_name
,a.type_name
,a.product_template_id
,a.slot_template_id
,a.template_name
,a.machine_type
,a.template_no
,a.machine_id
,a.imei
,a.mid
,a.pub_key
,a.pri_key
,a.manufacturer_code
,a.manufacturer_name
,a.accendant_phone
,a.online_status
,a.slot_sync_status
,a.standard_temperature
,a.app_key
,a.version_number
,a.ip
,a.machine_signal
,a.door
from fe_dwd.dwd_shelf_machine_info_1 a;
truncate table fe_dwd.dwd_shelf_machine_info;
INSERT INTO fe_dwd.dwd_shelf_machine_info
(
shelf_id
,shelf_type
,product_slots_num
,second_type
,machine_name
,machine_type_code
,machine_type_id
,machine_type_name
,type_name
,product_template_id
,slot_template_id
,template_name
,machine_type
,template_no
,machine_id
,imei
,mid
,pub_key
,pri_key
,manufacturer_code
,manufacturer_name
,accendant_phone
,online_status
,slot_sync_status
,standard_temperature
,app_key
,version_number
,ip
,machine_signal
,door
)
select
a.shelf_id
,a.shelf_type
,a.product_slots_num
,a.second_type
,a.machine_name
,a.machine_type_code
,a.machine_type_id
,a.machine_type_name
,a.type_name
,a.product_template_id
,a.slot_template_id
,a.template_name
,a.machine_type
,a.template_no
,a.machine_id
,a.imei
,a.mid
,a.pub_key
,a.pri_key
,a.manufacturer_code
,a.manufacturer_name
,a.accendant_phone
,a.online_status
,a.slot_sync_status
,a.standard_temperature
,a.app_key
,a.version_number
,a.ip
,a.machine_signal
,a.door
from fe_dwd.dwd_shelf_machine_info_1 a;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_machine_info',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
COMMIT;
    END