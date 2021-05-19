CREATE DEFINER=`feprocess`@`%` PROCEDURE `prcd_en_third_user_balance_his`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.d_en_third_user_balance_his
  WHERE sdate = CURDATE();
INSERT INTO feods.d_en_third_user_balance_his(
sdate,
open_id,
balance,
channel
)
SELECT 
curdate() sdate,
open_id,
balance,
channel
from fe_goods.sf_third_user_balance  sb
where sb.data_flag =1
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prcd_en_third_user_balance_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
 COMMIT;
END