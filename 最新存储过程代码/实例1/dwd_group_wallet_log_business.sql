CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_group_wallet_log_business`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
-- 防止异常报错
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_wallet_log_business_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_wallet_log_business_test LIKE fe_dwd.dwd_group_wallet_log_business;
insert into  fe_dwd.dwd_group_wallet_log_business_test
(
trade_business_id
,trade_id
,business_type
,business_id
,three_transaction_id
,user_name
,job_number
,dept
,professional_level
,remark
,member_id
,add_user_id
,add_user_type
,last_update_user_id
,grant_plan_id
,rev1
,rev2
,wallet_id
,amounts
,before_change
,after_change
,trade_type
,relate_trade_id
,trade_status
,trade_sign
)
SELECT 
a.`trade_business_id`
,a.`trade_id`
,a.`business_type`
,a.`business_id`
,a.`three_transaction_id`
,a.`user_name`
,a.`job_number`
,a.`dept`
,a.`professional_level`
,a.`remark`
,a.`member_id`
,a.`add_user_id`
,a.`add_user_type`
,a.`last_update_user_id`
,a.`grant_plan_id`
,a.`rev1`
,a.`rev2`
,b.`wallet_id`
,b.`amounts`
,b.`before_change`
,b.`after_change`
,b.`trade_type`
,b.`relate_trade_id`
,b.`trade_status`
,b.`trade_sign`
FROM fe_group.sf_group_wallet_log_business a 
LEFT JOIN
fe_group.sf_group_wallet_log b 
ON a.trade_id = b.trade_id
WHERE a.data_flag =1 
AND b.data_flag=1;
truncate table fe_dwd.dwd_group_wallet_log_business;
insert into  fe_dwd.dwd_group_wallet_log_business
(
trade_business_id
,trade_id
,business_type
,business_id
,three_transaction_id
,user_name
,job_number
,dept
,professional_level
,remark
,member_id
,add_user_id
,add_user_type
,last_update_user_id
,grant_plan_id
,rev1
,rev2
,wallet_id
,amounts
,before_change
,after_change
,trade_type
,relate_trade_id
,trade_status
,trade_sign
)
SELECT 
a.`trade_business_id`
,a.`trade_id`
,a.`business_type`
,a.`business_id`
,a.`three_transaction_id`
,a.`user_name`
,a.`job_number`
,a.`dept`
,a.`professional_level`
,a.`remark`
,a.`member_id`
,a.`add_user_id`
,a.`add_user_type`
,a.`last_update_user_id`
,a.`grant_plan_id`
,a.`rev1`
,a.`rev2`
,b.`wallet_id`
,b.`amounts`
,b.`before_change`
,b.`after_change`
,b.`trade_type`
,b.`relate_trade_id`
,b.`trade_status`
,b.`trade_sign`
FROM fe_group.sf_group_wallet_log_business a 
LEFT JOIN
fe_group.sf_group_wallet_log b 
ON a.trade_id = b.trade_id
WHERE a.data_flag =1 
AND b.data_flag=1;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_group_wallet_log_business',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END