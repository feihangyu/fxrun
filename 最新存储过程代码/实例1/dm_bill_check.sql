CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_bill_check`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @rankk = 0;
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SET @end_date = CURDATE(); 
SET @start_date = SUBDATE(@end_date,INTERVAL 4 DAY);
SET @month_id := DATE_FORMAT(SUBDATE(@end_date,INTERVAL 1 MONTH),'%Y-%m');
SET @month_id_cur := DATE_FORMAT(SUBDATE(@end_date,INTERVAL 0 MONTH),'%Y-%m');
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_bill_check_test;
CREATE TEMPORARY TABLE fe_dm.dm_bill_check_test LIKE fe_dm.dm_bill_check;
INSERT INTO fe_dm.dm_bill_check_test
(
trade_date,
third_merchant_id,
bill_type,
trade_amount
)
SELECT DATE(a.trade_time) trade_date,b.third_merchant_id ,
a.bill_type,
SUM(a.trade_amount) FROM fe_bill.bill_record  a
 JOIN fe_bill.bill_merchant b
 ON a.merchant_id = b.merchant_id 
WHERE a.trade_time >=@start_date
AND a.trade_time < @end_date
AND a.`data_flag` = 1
AND b.`data_flag` =1
GROUP BY DATE(a.trade_time) ,a.bill_type,b.third_merchant_id
;
DELETE FROM fe_dm.dm_bill_check WHERE trade_date >= DATE_SUB(CURDATE(),INTERVAL 4 DAY);
INSERT INTO fe_dm.dm_bill_check
(
trade_date,
third_merchant_id,
bill_type,
trade_amount
)
SELECT DATE(a.trade_time) trade_date,b.third_merchant_id ,
a.bill_type,
SUM(a.trade_amount) FROM fe_bill.bill_record  a
 JOIN fe_bill.bill_merchant b
 ON a.merchant_id = b.merchant_id
WHERE a.trade_time >=@start_date
AND a.trade_time < @end_date
AND a.`data_flag` = 1
AND b.`data_flag` =1
GROUP BY DATE(a.trade_time) ,a.bill_type,b.third_merchant_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_bill_check',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('李世龙@', @user, @timestamp));
 
  COMMIT;
END