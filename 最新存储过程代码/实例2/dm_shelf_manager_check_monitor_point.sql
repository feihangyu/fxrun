CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_shelf_manager_check_monitor_point`()
BEGIN
-- =============================================
-- Author:	物流店主
-- Create date: 2019/03/14
-- Modify date: 
-- Description:	
-- 	清洗处理盘点埋点数据，创建疑似虚假盘点标签
-- 
-- =============================================
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  SET @date_top := DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY);
  SET @date_end := CURRENT_DATE;
  -- 增量清除昨日要更新的数据
  DELETE FROM fe_dm.dm_shelf_manager_check_monitor_point
  WHERE operate_time >= @date_top
  AND operate_time < @date_end;
  
  
SET @time_1 := CURRENT_TIMESTAMP();
  -- 建临时表解析mongoDB同步过来的盘点记录的埋点数据取盘点操作时长等特征（从2019年6月29号开始埋点数据格式更改了）（日增量更新）
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_check_buried_point_result;
  CREATE TEMPORARY TABLE fe_dm.shelf_check_buried_point_result AS
  SELECT
    t.check_id,
    MAX(t.logtimedate) AS operate_time,
    SUM(t.duration) AS operate_period,
    SUM(t.longitude) AS longitude,
    SUM(t.latitude) AS latitude
  FROM
  (SELECT
  IF(
    LOCATE(
      'checkId',
      a.`behaviorResultInfo`
    ) <> 0,
    SUBSTRING(
      REPLACE(a.`behaviorResultInfo`, '}', ','),
      LOCATE(
        'checkId=',
        a.`behaviorResultInfo`
      ) + CHAR_LENGTH('checkId='),
      LOCATE(
        ',',
        REPLACE(a.`behaviorResultInfo`, '}', ','),
        LOCATE(
          'checkId',
          a.`behaviorResultInfo`
        )
      ) - (
        LOCATE(
          'checkId=',
          a.`behaviorResultInfo`
        ) + CHAR_LENGTH('checkId=')
      )
    ),
    NULL
  ) AS check_id,
  IF(
    LOCATE(
      'duration',
      a.`behaviorResultInfo`
    ) <> 0,
    SUBSTRING(
      REPLACE(a.`behaviorResultInfo`, '}', ','),
      LOCATE(
        'duration=',
        a.`behaviorResultInfo`
      ) + CHAR_LENGTH('duration='),
      LOCATE(
        ',',
        REPLACE(a.`behaviorResultInfo`, '}', ','),
        LOCATE(
          'duration',
          a.`behaviorResultInfo`
        )
      ) - (
        LOCATE(
          'duration=',
          a.`behaviorResultInfo`
        ) + CHAR_LENGTH('duration=')
      )
    ),
    0
  ) AS duration,
  IF(
    LOCATE(
      'longitude',
      a.`behaviorResultInfo`
    ) <> 0,
    SUBSTRING(
      REPLACE(a.`behaviorResultInfo`, '}', ','),
      LOCATE(
        'longitude=',
        a.`behaviorResultInfo`
      ) + CHAR_LENGTH('longitude='),
      LOCATE(
        ',',
        REPLACE(a.`behaviorResultInfo`, '}', ','),
        LOCATE(
          'longitude',
          a.`behaviorResultInfo`
        )
      ) - (
        LOCATE(
          'longitude=',
          a.`behaviorResultInfo`
        ) + CHAR_LENGTH('longitude=')
      )
    ),
    0
  ) AS longitude,
  IF(
    LOCATE(
      'latitude',
      a.`behaviorResultInfo`
    ) <> 0,
    SUBSTRING(
      REPLACE(a.`behaviorResultInfo`, '}', ','),
      LOCATE(
        'latitude=',
        a.`behaviorResultInfo`
      ) + CHAR_LENGTH('latitude='),
      LOCATE(
        ',',
        REPLACE(a.`behaviorResultInfo`, '}', ','),
        LOCATE(
          'latitude',
          a.`behaviorResultInfo`
        )
      ) - (
        LOCATE(
          'latitude=',
          a.`behaviorResultInfo`
        ) + CHAR_LENGTH('latitude=')
      )
    ),
    0
  ) AS latitude,
  a.`behaviorCode`,
  a.`behaviorName`,
  a.`behaviorResult`,
  a.`behaviorResultInfo`,
  a.`userId`,
  a.`logTimeDate`
FROM
  fe_dwd.dwd_mongo_shelf_manager_behavior_log a
WHERE a.`behaviorName` IN (
    '盘点页面停留时长',
    '盘点页面(跳转)盘点异常页面',
    '盘点异常确认页面停留时长',
    ''
  )
  AND SUBSTRING_INDEX(a.behaviorresultinfo, '_', - 1) NOT LIKE '%停留时间%'
  AND a.`logTimeDate` >= DATE('20190629')
  AND a.`logTimeDate` >= @date_top
  AND a.`logTimeDate` < @date_end
  AND a.behaviorResultInfo REGEXP '^{'
  AND a.behaviorResultInfo REGEXP '}$'
ORDER BY a.`userId`,
  a.`logTimeDate`) t
WHERE t.check_id IS NOT NULL
GROUP BY t.check_id;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_shelf_manager_check_monitor_point","@time_1--@time_2",@time_1,@time_2);
  -- 增量插入疑似虚假盘点标签记录(日增量)
  INSERT INTO fe_dm.dm_shelf_manager_check_monitor_point(
    check_id,
    shelf_id,
    business_area,
    operate_time,
    operate_period,
    rule_1,                      
    rule_2,                          
    operate_stock,
    operate_sku,
    operate_error_num,
    shelf_type,
    operator_name,
    operator_ID
  )
  SELECT
    tb_other.check_id,
    tb_other.shelf_id,
    e.business_name AS business_area,
    tab.operate_time,
    tab.operate_period,
    IF(tab.operate_period < 60000,1,0) AS rule_1,
    IF(DATE_FORMAT(tab.operate_time,'%H:%i')>'22:00' OR DATE_FORMAT(tab.operate_time,'%H:%i')<'07:00',1,0) AS rule_2,
    tb_other.operated_stock,
    tb_other.sku_num,
    tb_other.operated_diff_num,
    e.shelf_type,
    tb_other.real_name,
    tb_other.operator_id
  FROM
  fe_dm.shelf_check_buried_point_result tab,
  fe_dwd.dwd_shelf_base_day_all e,
(SELECT
  k.`SHELF_ID`,
  k.check_id,
  SUM(IFNULL(k.`CHECK_NUM`,0)) AS operated_stock,
  COUNT(k.`PRODUCT_ID`) AS sku_num,
  SUM(ABS(IFNULL(k.`ERROR_NUM`,0))) AS operated_diff_num,
  k.`OPERATOR_ID`,
  e.real_name,
  k.operate_time
FROM
  fe_dwd.dwd_check_base_day_inc k,
  fe_dwd.dwd_shelf_base_day_all s,
  fe_dwd.dwd_pub_shelf_manager e
WHERE k.shelf_id = s.shelf_id
AND k.operator_id = e.manager_id
AND k.operate_time >= @date_top
AND k.operate_time < @date_end
AND k.check_type IN (1,3)
AND s.shelf_type IN (1,2,3,5,6,7)
GROUP BY k.`CHECK_ID`) tb_other
WHERE tb_other.check_id = tab.check_id
AND tb_other.shelf_id = e.shelf_id
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_shelf_manager_check_monitor_point","@time_2--@time_3",@time_2,@time_3);
-- 连续两次或以上盘点差异数量为0即定为疑似虚假盘点,以此更新虚盘结果表（上月与本月累计计算）
SET @shelf_num:=0;
SET @error:=0;
UPDATE fe_dm.dm_shelf_manager_check_monitor_point a,
(SELECT
  CASE WHEN @shelf_num != t.shelf_id
       THEN @error:=0
       END AS action_zero,
  CASE WHEN t.operate_error_num=0
       THEN @error:=@error+1
       ELSE @error:=0
       END AS action_one,
  @shelf_num:=t.`shelf_id` AS shelf_id,
  t.`check_id`,
  t.operate_time,
  t.`operate_error_num`,
  t.`suspect_fake_operate`,
  CASE WHEN @error>=2
       THEN 1
       END AS new_result
FROM
  fe_dm.dm_shelf_manager_check_monitor_point t
WHERE t.operate_time >= DATE('20190629')
AND t.operate_time >= DATE_SUB(DATE_ADD(@date_top,INTERVAL -DAY(@date_top)+1 DAY),INTERVAL 1 MONTH)
AND t.operate_time < DATE_ADD(LAST_DAY(@date_top),INTERVAL 1 DAY)
ORDER BY t.`shelf_id`,t.check_id) b
SET a.rule_3= 0,a.rule_3= b.new_result
WHERE a.check_id = b.check_id
AND a.operate_time >= DATE('20190629')
AND a.operate_time >= DATE_SUB(DATE_ADD(@date_top,INTERVAL -DAY(@date_top)+1 DAY),INTERVAL 1 MONTH)
AND a.operate_time < DATE_ADD(LAST_DAY(@date_top),INTERVAL 1 DAY)
AND b.new_result IS NOT NULL;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_shelf_manager_check_monitor_point","@time_3--@time_4",@time_3,@time_4);
-- 同个店主5分钟内，盘点2个公司及以上的货架的盘点记录定为疑似虚假盘点，以此更新虚盘结果表(本月累计计算)
SET @manager:= 0; SET @period:=0;  SET @n:=1;
SET @time:= NULL;
DROP TEMPORARY TABLE IF EXISTS fe_dm.check_log_mid_temp;
CREATE TEMPORARY TABLE fe_dm.check_log_mid_temp AS
SELECT
 a.check_id,
 a.shelf_id,
 a.company_id,
 @period:= IF(@manager != a.`operator_ID` OR @period > 5, 0 ,@period+IF(@manager = a.`operator_ID`,IFNULL(TIMESTAMPDIFF(MINUTE,@time,a.operate_time),0),0)) AS accum_num,
 @n:= IF(@manager = a.`operator_ID`,IF(@period>0 AND @period <=5,@n,@n+1),1) AS label,
 @manager :=a.`operator_ID` AS operator_ID,
 @time :=a.operate_time AS operate_time
FROM
(SELECT
 a.*,
 f.company_id
FROM
 fe_dm.dm_shelf_manager_check_monitor_point a
JOIN
 fe_dwd.dwd_shelf_base_day_all f
ON a.`shelf_id`= f.`SHELF_ID`
WHERE a.operate_time >= DATE_ADD(@date_top,INTERVAL -DAY(@date_top)+1 DAY)
AND a.operate_time < DATE_ADD(LAST_DAY(@date_top),INTERVAL 1 DAY)
ORDER BY a.`operator_ID`,a.`operate_time`) a;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_shelf_manager_check_monitor_point","@time_4--@time_5",@time_4,@time_5);
-- 更新虚盘结果表
UPDATE fe_dm.dm_shelf_manager_check_monitor_point c
JOIN 
(SELECT
 a.operator_ID,
 SUBSTRING_INDEX(SUBSTRING_INDEX(a.check_list,',',r.`number`+1),',',-1) AS check_id
FROM
(SELECT
 t.operator_ID,
 t.label,
 COUNT(DISTINCT t.company_id) AS company_num,
 GROUP_CONCAT(t.check_id SEPARATOR ",") AS check_list
FROM
 fe_dm.check_log_mid_temp t
GROUP BY t.operator_ID ,t.label
HAVING COUNT(DISTINCT t.company_id)>1) a
JOIN fe_dwd.dwd_pub_number r
ON (CHAR_LENGTH(a.check_list)-CHAR_LENGTH(REPLACE(a.check_list,',','')))+1 >= r.`number`+1) r
ON c.`check_id` = r.check_id
AND c.operate_time >= DATE_ADD(@date_top,INTERVAL -DAY(@date_top)+1 DAY)
AND c.operate_time < DATE_ADD(LAST_DAY(@date_top),INTERVAL 1 DAY)
SET c.rule_4 = 0,c.rule_4 = 1;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_shelf_manager_check_monitor_point","@time_5--@time_6",@time_5,@time_6);
-- 根据各种复现规则判断并更新疑似虚假标签
UPDATE fe_dm.dm_shelf_manager_check_monitor_point c
SET c.`suspect_fake_operate` = '疑似虚假盘点'
WHERE (c.`rule_1`+c.`rule_2`+c.`rule_3`+c.`rule_4`)>0
AND c.`suspect_fake_operate` IS NULL;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_shelf_manager_check_monitor_point","@time_6--@time_7",@time_6,@time_7);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_shelf_manager_check_monitor_point',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_manager_check_monitor_point','dm_shelf_manager_check_monitor_point','蔡松林');
END