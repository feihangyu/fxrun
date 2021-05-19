CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_machine_second_info`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
 -- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_machine_second_info_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_machine_second_info_test like fe_dwd.dwd_shelf_machine_second_info; 
  
INSERT INTO fe_dwd.dwd_shelf_machine_second_info_test
(
shelf_id
,machine_id
,product_id
,stock_num
,machine_second_name
,second_type
)
select
a.shelf_id
,a.machine_id
,b.product_id
,b.stock_num
,a.machine_second_name
,a.second_type
from fe.sf_shelf_machine_second a
left join fe.sf_shelf_machine_second_detail b 
ON a.machine_second_id = b.machine_second_id
AND b.data_flag = 1
AND b.stock_num != 0
WHERE a.data_flag = 1
AND ! ISNULL(a.shelf_id)
AND ! ISNULL(b.product_id);
truncate table fe_dwd.dwd_shelf_machine_second_info;
INSERT INTO fe_dwd.dwd_shelf_machine_second_info
(
shelf_id
,machine_id
,product_id
,stock_num
,machine_second_name
,second_type
)
select
a.shelf_id
,a.machine_id
,b.product_id
,b.stock_num
,a.machine_second_name
,a.second_type
from fe.sf_shelf_machine_second a
left join fe.sf_shelf_machine_second_detail b 
ON a.machine_second_id = b.machine_second_id
AND b.data_flag = 1
AND b.stock_num != 0
WHERE a.data_flag = 1
AND ! ISNULL(a.shelf_id)
AND ! ISNULL(b.product_id);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_machine_second_info',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
COMMIT;
END