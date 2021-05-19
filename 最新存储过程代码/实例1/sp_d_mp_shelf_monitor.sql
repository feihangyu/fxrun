CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_shelf_monitor`(in_date DATE)
BEGIN
    DECLARE l_test VARCHAR(1);
    DECLARE l_row_cnt INT;
    DECLARE CODE CHAR(5) DEFAULT '00000';
    DECLARE done INT;
    
	DECLARE l_table_owner   VARCHAR(64);
	DECLARE l_city          VARCHAR(64);
    DECLARE l_task_name     VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1
			CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
			CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
		END; 
		
    SET l_task_name = 'sp_d_mp_shelf_monitor'; 
    
 #刘小兰多口径的货架数据
SET @sdate1 = in_date;
SET @sdate2 = DATE_FORMAT(DATE_SUB(@sdate1,INTERVAL DAY(@sdate1) DAY),"%Y-%m-01"); #上月第一天
SET @sdate3 =  DATE_FORMAT(@sdate1,'%Y-%m-01'); #本月第一天
SET @sdate4 = DATE_ADD(@sdate1,INTERVAL 1 DAY);
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM feods.d_mp_shelf_monitor WHERE sdate = @sdate1 ;
INSERT INTO feods.d_mp_shelf_monitor
(
`sdate` ,
  `region_area`,
  `business_area` ,
  `active_shelf_fridge` ,
  `active_shelf_fridge_yest`,
  `active_shelf_fridge_curmonth`,
  `all_shelf`,
  `remain_shelf_curmonth`,
  `active_shelf_curmonth`,
  `remain_shelf_beforemonth`,
  `revoke_shelf_active_curmonth` ,
  `revoke_shelf_active_beforemonth`,
  `revoke_shelf_active_lastmonth`,
  `revoke_shelf_curmonth`,
  `revoke_shelf_curyear`,
  `all_fridge`,
  `remain_fridge_curmonth`,
  `active_fridge_curmonth`,
  `remain_fridge_beforemonth`,
  `revoke_fridge_active_curmonth`,
  `revoke_fridge_active_beforemonth`,
  `revoke_fridge_active_lastmonth` ,
  `revoke_fridge_curmonth`,
  `revoke_fridge_curyear`,
  `unactive_shelf_chance` ,
  `valid_shelf_chance` ,
  `unactive_school_chance`,
  `valid_school_shelf` 
)
SELECT 
@sdate1 AS sdate
,b.REGION_AREA
,b.BUSINESS_AREA
,COUNT(DISTINCT IF(SHELF_TYPE IN (1,2,3,5) AND a.SHELF_STATUS IN (2,5)  AND a.ACTIVATE_TIME< @sdate4 ,a.shelf_id,NULL)) AS '冰箱+货架' # 激活状态的冰箱和货架
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>=@sdate1 AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (1,2,3,5) AND a.SHELF_STATUS IN (2,5),a.SHELF_ID, NULL)) AS '昨日激活（冰箱+货架）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>= @sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (1,2,3,5) AND a.SHELF_STATUS IN(2,5),a.SHELF_ID, NULL)) AS '本月激活（冰箱+货架）'
,COUNT(DISTINCT IF(SHELF_TYPE IN (1,3) AND a.SHELF_STATUS IN (2,5)  AND a.ACTIVATE_TIME< @sdate4 ,a.shelf_id,NULL)) AS '全网货架'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>= @sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (1,3) AND a.SHELF_STATUS IN(2,5),a.SHELF_ID, NULL)) AS '本月激活结存（货架）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>= @sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (1,3),a.SHELF_ID, NULL)) AS '本月激活（货架）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME < @sdate3 AND a.SHELF_TYPE IN (1,3) AND a.SHELF_STATUS IN(2,5),a.SHELF_ID, NULL)) AS '本月前激活结存（货架）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>= @sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (1,3) AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '本月激活本月撤架（货架）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME < @sdate2 AND a.SHELF_TYPE IN (1,3) AND a.revoke_time >= @sdate3 AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '上月前激活本月撤架（货架）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>= @sdate2 AND a.ACTIVATE_TIME< @sdate3 AND a.SHELF_TYPE IN (1,3) AND a.revoke_time >= @sdate3 AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '上月激活本月撤架（货架）'
,COUNT(DISTINCT IF(a.revoke_time>= @sdate3 AND a.ACTIVATE_TIME< @sdate4  AND a.SHELF_TYPE IN (1,3) AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '本月累计撤架货架' # 只包含货架
,COUNT(DISTINCT IF(a.revoke_time>= DATE_FORMAT(@sdate1,'%Y-01-01') AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (1,3) AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '本年累计撤架货架'
,COUNT(DISTINCT IF(SHELF_TYPE IN (2,5) AND a.SHELF_STATUS IN (2,5)  AND a.ACTIVATE_TIME< @sdate4 ,a.shelf_id,NULL)) AS '全网冰箱'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>=@sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (2,5) AND a.SHELF_STATUS IN(2,5),a.SHELF_ID, NULL)) AS '本月激活结存（冰箱）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>=@sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (2,5),a.SHELF_ID, NULL)) AS '本月激活（冰箱）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME <@sdate3 AND a.SHELF_TYPE IN (2,5) AND a.SHELF_STATUS IN(2,5),a.SHELF_ID, NULL)) AS '本月前激活结存（冰箱）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>=@sdate3  AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (2,5) AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '本月激活本月撤架（冰箱）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME < @sdate2 AND a.SHELF_TYPE IN (2,5) AND a.revoke_time >= @sdate3 AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '上月前激活本月撤架（冰箱）'
,COUNT(DISTINCT IF(a.ACTIVATE_TIME>= @sdate2 AND a.ACTIVATE_TIME< @sdate3 AND a.SHELF_TYPE IN (2,5) AND a.revoke_time >= @sdate3 AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '上月激活本月撤架（冰箱）'
,COUNT(DISTINCT IF(a.revoke_time>= @sdate3 AND a.ACTIVATE_TIME< @sdate4  AND a.SHELF_TYPE IN (2,5) AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '本月累计撤架冰箱' # 只包含冰箱
,COUNT(DISTINCT IF(a.revoke_time>= DATE_FORMAT(@sdate1,'%Y-01-01') AND a.ACTIVATE_TIME< @sdate4 AND a.SHELF_TYPE IN (2,5) AND a.SHELF_STATUS = 3,a.SHELF_ID, NULL)) AS '本年累计撤架冰箱'
,COUNT(IF(a.SHELF_TYPE IN (1,3) AND a.SHELF_STATUS = 1,a.SHELF_ID,NULL)) AS '待激活货架商机' # 只包含货架
,COUNT(IF(a.SHELF_TYPE IN (1,3) AND a.VALID_CHANCE = 1,a.SHELF_ID,NULL)) AS '有效货架商机量'
,COUNT(IF(a.SHELF_TYPE = 8 AND a.VALID_CHANCE = 1,a.SHELF_ID,NULL)) AS '有效校园货架商机量'
,COUNT(DISTINCT IF(SHELF_TYPE = 8 AND a.SHELF_STATUS IN (2,5)  AND a.ACTIVATE_TIME< @sdate4 ,a.shelf_id,NULL)) AS '有效校园货架数'
FROM fe.sf_shelf a
JOIN feods.zs_city_business b
ON SUBSTRING_INDEX(SUBSTRING_INDEX(a.AREA_ADDRESS, ',', 2),',',-1) = b.CITY_NAME
WHERE a.DATA_FLAG=1 
AND a.SHELF_STATUS IN (1,2,3,5)  # 1为待激活，2已激活，3已撤架
AND a.MANAGER_NAME NOT LIKE '%作废%' 
AND a.SHELF_TYPE IN (1,2,3,5,8)
-- AND a.ACTIVATE_TIME< CURDATE()
GROUP BY b.BUSINESS_AREA;  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_mp_shelf_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END