CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_dm_lo_shelf_manager_line_day`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 
-- Modify date: 
-- Description:	
-- 	全职店主线路推荐表更新
-- 
-- =============================================
  -- 建临时表，未分线路激活货架到已有线路参考距离
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
 
SET @time_1 := CURRENT_TIMESTAMP(); 
DROP TEMPORARY TABLE IF EXISTS feods.dm_lo_shelf_manager_line_day_mid;
CREATE TEMPORARY TABLE feods.dm_lo_shelf_manager_line_day_mid AS
SELECT t0.`MANAGER_ID`,t0.`shelf_id`,t0.`lng` lng_1,t0.`lat` lat_1,t1.`line_type`,t1.`lng`,t1.`lat`,POWER(t0.`lng`-t1.`lng`,2)+POWER(t0.`lat`-t1.`lat`,2) AS 'len'
FROM
(SELECT a.`MANAGER_ID`,a.`shelf_id`,a.`lng`,a.`lat`
FROM fe_dwd.`dwd_shelf_base_day_all` a
LEFT JOIN fe.`sf_shelf_line_relation` b
ON a.`shelf_id` = b.`shelf_id` AND b.`data_flag` = 1
WHERE a.`DATA_FLAG` = 1 AND a.`manager_type` = '全职店主' AND a.`SHELF_STATUS` = 2 AND b.`relation_id` IS NULL AND a.`lng` IS NOT NULL AND a.shelf_type IN (1,2,3,5,6,7)) t0,
(SELECT b.`MANAGER_ID`,a.`line_type`,AVG(b.`lng`) lng,AVG(b.`lat`) lat
FROM fe.`sf_shelf_line_relation` a
JOIN fe_dwd.`dwd_shelf_base_day_all` b
ON a.`shelf_id` = b.`shelf_id` AND a.`data_flag` = 1 AND b.`DATA_FLAG` = 1 AND b.`SHELF_STATUS` = 2
GROUP BY 1,2) t1
WHERE t0.`MANAGER_ID` = t1.`MANAGER_ID`
ORDER BY t0.`MANAGER_ID`,t0.`shelf_id`
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_dm_lo_shelf_manager_line_day","@time_1--@time_2",@time_1,@time_2);
-- 获取未分线路货架最近线路
DROP TEMPORARY TABLE IF EXISTS feods.dm_lo_shelf_manager_line_day_mid_group;
CREATE TEMPORARY TABLE feods.dm_lo_shelf_manager_line_day_mid_group AS 
SELECT a.`shelf_id`,MIN(a.`len`) len
FROM feods.dm_lo_shelf_manager_line_day_mid a
GROUP BY 1;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_dm_lo_shelf_manager_line_day","@time_2--@time_3",@time_2,@time_3);
INSERT INTO fe_dm.dm_lo_shelf_manager_line_day(
 manager_id
,shelf_id 
,line_type                                                           
)
SELECT t0.`MANAGER_ID`,t0.`shelf_id`,t0.`line_type` FROM feods.dm_lo_shelf_manager_line_day_mid t0
INNER JOIN
feods.dm_lo_shelf_manager_line_day_mid_group t1
ON t0.`shelf_id` = t1.`shelf_id` AND t0.`len` = t1.`len`
WHERE (t0.`MANAGER_ID`,t0.`shelf_id`) NOT IN (SELECT DISTINCT d.manager_id,d.shelf_id FROM fe_dm.dm_lo_shelf_manager_line_day d WHERE d.data_flag =1 AND ISNULL(d.manager_id)=0 AND ISNULL(d.shelf_id)=0)
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_dm_lo_shelf_manager_line_day","@time_3--@time_4",@time_3,@time_4);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_dm_lo_shelf_manager_line_day',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
  
  COMMIT;	
END