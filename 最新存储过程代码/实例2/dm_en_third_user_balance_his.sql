CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_en_third_user_balance_his`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  DELETE
  FROM
    fe_dm.dm_en_third_user_balance_his
  WHERE sdate = CURDATE();
INSERT INTO fe_dm.dm_en_third_user_balance_his(
sdate,
open_id,
balance,
channel
)
SELECT 
CURDATE() sdate,
open_id,
balance,
channel
FROM fe_dwd.dwd_sf_third_user_balance sb
WHERE sb.data_flag =1
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_en_third_user_balance_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('黎尼和@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_third_user_balance_his','dm_en_third_user_balance_his','黎尼和');
 
END