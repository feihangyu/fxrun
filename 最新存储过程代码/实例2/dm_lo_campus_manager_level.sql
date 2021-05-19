CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_lo_campus_manager_level`()
BEGIN
-- =============================================
-- Author:	物流校园组
-- Create date: 2019/10/14
-- Modify date: 
-- Description:	
-- 	校园货架店主等级标签（每天的2时13分跑）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
IF DAY(CURRENT_DATE) != 2 THEN 
DELETE FROM fe_dm.dm_lo_campus_manager_level WHERE STAT_DATE = DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY);
END IF;
DELETE FROM fe_dm.dm_lo_campus_manager_level WHERE STAT_DATE = CURRENT_DATE;
INSERT INTO fe_dm.dm_lo_campus_manager_level(
 STAT_DATE                                                                                                                 
,MANAGER_ID
,manager_name                                                                                                            
,pay_amount                                                                                                              
,manager_gmv
)
 SELECT
  CURRENT_DATE AS '统计日期',
  f.manager_id,
  f.real_name,
  SUM(e.PAY_TOTAL_AMOUNT) AS pay_amount,
  SUM(e.PAY_TOTAL_AMOUNT + e.COUPON_TOTAL_AMOUNT) AS manager_gmv
 FROM
   fe_dwd.`dwd_shelf_base_day_all` f
 JOIN fe_dwd.dwd_statistics_shelf_sale e
 ON f.shelf_id = e.shelf_id
 WHERE e.CREATE_DATE >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
 AND e.CREATE_DATE < DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),INTERVAL 1 DAY)
 AND f.shelf_type = 8
 GROUP BY f.manager_id;
 
 UPDATE fe_dm.dm_lo_campus_manager_level t
 SET t.manager_level = IF(t.manager_gmv < 2000,1,IF(t.manager_gmv >=2000 AND t.manager_gmv < 3000,2,IF(t.manager_gmv>=3000
 AND t.manager_gmv < 4000,3,IF(t.manager_gmv>=4000,4,NULL)))), t.last_level_interval = IF(t.manager_level=1,2000-t.manager_gmv,IF(t.manager_level=2,3000-t.manager_gmv,IF(t.manager_level=3,4000-t.manager_gmv,IF(t.manager_level=4,0,NULL))))
 WHERE t.STAT_DATE = CURRENT_DATE;
 
  SET @n:= 0;
  UPDATE fe_dm.dm_lo_campus_manager_level t
  JOIN
  (SELECT
  t.manager_id,
  t.`manager_gmv`,
  @n := @n+1 AS top
 FROM
   fe_dm.dm_lo_campus_manager_level t
 WHERE t.STAT_DATE = CURRENT_DATE
--  and t.manager_level = 4
--  and (SELECT COUNT(e.manager_id) FROM fe_dm.dm_lo_campus_manager_level e WHERE e.manager_level=4 AND e.STAT_DATE = DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),INTERVAL 1 DAY))>10
 ORDER BY t.manager_gmv DESC) g
 ON t.manager_id = g.manager_id
 SET t.manager_level = IF(g.top <= 3,6,5)
 WHERE t.STAT_DATE = CURRENT_DATE
 AND g.top <= 10
;
UPDATE fe_dm.dm_lo_campus_manager_level m
LEFT JOIN
(SELECT
  t.manager_level,
  MIN(t.manager_gmv) AS manager_gmv
 FROM
   fe_dm.dm_lo_campus_manager_level t
 WHERE t.STAT_DATE = CURRENT_DATE
 AND t.manager_level >= 4
 GROUP BY t.manager_level) r
ON m.manager_level = r.manager_level - 1
SET m.last_level_interval = IF(m.manager_level=6,0,r.manager_gmv - m.manager_gmv)
WHERE m.STAT_DATE = CURRENT_DATE
AND m.manager_level >= 4 ;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_lo_campus_manager_level',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_lo_campus_manager_level','dm_lo_campus_manager_level','蔡松林');
END