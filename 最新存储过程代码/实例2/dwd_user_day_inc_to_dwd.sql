CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_user_day_inc_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 用户宽表数据从fe_temp 到 fe_dwd 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_user_day_inc_tmp1;
CREATE TEMPORARY TABLE fe_dwd.dwd_user_day_inc_tmp1 AS
SELECT a.user_id    -- 只需取订单号即可 
FROM 
fe_temp.dwd_user_day_inc b -- 小表   小表要放在前面作为驱动表，加快速度
 JOIN fe_dwd.dwd_user_day_inc a   -- 大表 
    ON a.user_id = b.user_id 
    AND b.load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;   
CREATE INDEX idx_user_id
ON fe_dwd.dwd_user_day_inc_tmp1(user_id);
DELETE a.* FROM fe_dwd.dwd_user_day_inc a   -- 先删除共同的部分  按照订单号删除即可
JOIN  fe_dwd.dwd_user_day_inc_tmp1  b
    ON a.user_id = b.user_id ;
INSERT INTO fe_dwd.dwd_user_day_inc 
SELECT *
FROM fe_temp.dwd_user_day_inc 
WHERE load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY)  ;  
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_user_day_inc_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_user_day_inc','dwd_user_day_inc_to_dwd','李世龙');
 
  COMMIT;	
END