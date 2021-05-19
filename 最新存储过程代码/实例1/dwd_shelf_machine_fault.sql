CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_machine_fault`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
 -- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_machine_fault_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_machine_fault_test like fe_dwd.dwd_shelf_machine_fault; 
  
INSERT INTO fe_dwd.dwd_shelf_machine_fault_test
(
fault_id
,shelf_id
,mid
,slot_code
,fault_type
,fault_name
,report_time
,solve_time
,fault_status
)
select
a.fault_id
,a.shelf_id
,a.mid
,a.slot_code
,a.fault_type
,a.fault_name
,a.report_time
,a.solve_time
,a.fault_status
from  fe.sf_shelf_machine_fault a
  WHERE a.data_flag = 1
    AND !ISNULL(a.shelf_id);
truncate table fe_dwd.dwd_shelf_machine_fault;
INSERT INTO fe_dwd.dwd_shelf_machine_fault
(
fault_id
,shelf_id
,mid
,slot_code
,fault_type
,fault_name
,report_time
,solve_time
,fault_status
)
select
a.fault_id
,a.shelf_id
,a.mid
,a.slot_code
,a.fault_type
,a.fault_name
,a.report_time
,a.solve_time
,a.fault_status
from  fe.sf_shelf_machine_fault a
  WHERE a.data_flag = 1
    AND !ISNULL(a.shelf_id);
	
	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_machine_fault',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
COMMIT;
END