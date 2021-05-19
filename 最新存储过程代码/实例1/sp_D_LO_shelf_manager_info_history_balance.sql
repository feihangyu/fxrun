CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_shelf_manager_info_history_balance`()
begin
-- =============================================
-- Author:	物流店主组
-- Create date: 2019/10/16
-- Modify date: 
-- Description:	
-- 	货架店主信息截存（每天的0时跑）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
 -- 将货架主表的昨日截存更新进货架店主信息截存表
set @a:= 0 ;
if date_format(now(),'%H:%i') = '00:00' then
DELETE FROM feods.D_LO_shelf_manager_info_history_balance WHERE STAT_DATE = DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY);
set @a:= 1;
else
DELETE FROM feods.D_LO_shelf_manager_info_history_balance WHERE STAT_DATE = CURRENT_DATE;    -- 为了让该存储过程可手动调用而不破坏零点结存的结果而设置
end if;
INSERT INTO feods.D_LO_shelf_manager_info_history_balance(
 STAT_DATE           
,SHELF_ID         
,MANAGER_ID)
SELECT DISTINCT
 if(@a=1,DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),CURRENT_DATE) AS STAT_DATE,
 t.`SHELF_ID`,
 t.`MANAGER_ID`
FROM
 fe.`sf_shelf` t
 WHERE t.`DATA_FLAG` = 1
 AND t.`REVOKE_TIME` IS NULL;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_D_LO_shelf_manager_info_history_balance',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
COMMIT;
end