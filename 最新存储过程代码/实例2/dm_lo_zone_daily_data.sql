CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_lo_zone_daily_data`()
BEGIN
  -- =============================================
-- Author:	运作门店
-- Create date: 2020/06/23
-- Modify date: 
-- Description:	
--    增量插入每天门店统计结果表（每天的）
-- 
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
 
DROP TEMPORARY TABLE IF EXISTS fe_dm.lo_zone_shelf_distance;
CREATE TEMPORARY TABLE fe_dm.lo_zone_shelf_distance (KEY idx_zone_code (zone_code)) AS
SELECT
  a.`business_name`,
  a.`zone_code`,
  a.`zone_name`,
  AVG(a.distance) distance
FROM
  (SELECT
    t0.`business_name`,
    t0.`zone_code`,
    t0.`zone_name`,
    t0.`shelf_id`,
    t0.`lat`,
    t0.`lng`,
    t1.`zone_code` zone_code_1,
    t1.`zone_name` zone_name_1,
    t1.`shelf_id` shelf_id_1,
    t1.`lat` lat_1,
    t1.`lng` lng_1,
    ROUND(
      6378.138 * 2 * ASIN(
        SQRT(
          POW(
            SIN(
              (
                t0.`lat` * PI() / 180 - t1.`lat` * PI() / 180
              ) / 2
            ),
            2
          ) + COS(t0.`lat` * PI() / 180) * COS(t1.`lat` * PI() / 180) * POW(
            SIN(
              (
                t0.`lng` * PI() / 180 - t1.`lng` * PI() / 180
              ) / 2
            ),
            2
          )
        )
      ) * 1000
    ) AS distance
  FROM
    fe_dwd.`dwd_shelf_base_day_all` t0
    LEFT JOIN fe_dwd.`dwd_shelf_base_day_all` t1
      ON t0.`zone_code` = t1.`zone_code`
      AND t1.`DATA_FLAG` = 1
      AND t1.`shelf_type` < 9
      AND t1.`SHELF_STATUS` IN (1, 2, 5)
      AND t1.lat > 0
  WHERE t0.`DATA_FLAG` = 1
    AND t0.`shelf_type` < 9
    AND t0.`SHELF_STATUS` IN (1, 2, 5)
    AND t0.`zone_code` > 0
    AND t0.lat > 0
    AND t0.`zone_code` NOT IN (
      '620',
      '547',
      '214',
      '613',
      '24',
      '612',
      '425',
      '184',
      '206',
      '394',
      '453',
      '621',
      '532',
      '559',
      '322',
      '516',
      '169',
      '459',
      '551',
      '425',
      '463',
      '642',
      '526',
      '546',
      '417',
      '503',
      '433',
      '502',
      '534'
    )) a
WHERE a.distance < 30000
GROUP BY a.`zone_code`
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.lo_zone_prewarehouse_distance;
CREATE TEMPORARY TABLE fe_dm.lo_zone_prewarehouse_distance (KEY idx_zone_code (zone_code)) AS
SELECT
  a.`business_name`,
  a.`zone_code`,
  a.`zone_name`,
  AVG(a.distance) distance
FROM
  (SELECT
    t0.`business_name`,
    t0.`zone_code`,
    t0.`zone_name`,
    t0.`shelf_id`,
    t0.`lat`,
    t0.`lng`,
    t1.`shelf_id` shelf_id_1,
    t1.`lat` lat_1,
    t1.`lng` lng_1,
    ROUND(
      6378.138 * 2 * ASIN(
        SQRT(
          POW(
            SIN(
              (
                t0.`lat` * PI() / 180 - t1.`lat` * PI() / 180
              ) / 2
            ),
            2
          ) + COS(t0.`lat` * PI() / 180) * COS(t1.`lat` * PI() / 180) * POW(
            SIN(
              (
                t0.`lng` * PI() / 180 - t1.`lng` * PI() / 180
              ) / 2
            ),
            2
          )
        )
      ) * 1000
    ) AS distance
  FROM
    fe_dwd.`dwd_shelf_base_day_all` t0
    JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` t2
      ON t0.`shelf_id` = t2.`shelf_id`
    JOIN fe_dwd.dwd_lo_prewarehouse_longitude_latitude_insert t1
      ON t2.`prewarehouse_id` = t1.`shelf_id`
      AND t1.data_flag = 1
      AND t1.lat > 0
  WHERE t0.`DATA_FLAG` = 1
    AND t0.`shelf_type` < 9
    AND t0.`SHELF_STATUS` IN (1, 2, 5)
    AND t0.`zone_code` > 0
    AND t0.lat > 0
    AND t0.`zone_code` NOT IN (
      '620',
      '547',
      '214',
      '613',
      '24',
      '612',
      '425',
      '184',
      '206',
      '394',
      '453',
      '621',
      '532',
      '559',
      '322',
      '516',
      '169',
      '459',
      '551',
      '425',
      '463',
      '642',
      '526',
      '546',
      '417',
      '503',
      '433',
      '502',
      '534'
    )) a
WHERE a.distance < 30000
GROUP BY a.`zone_code`
;
DELETE
FROM
  fe_dm.`dm_lo_zone_daily_data`
WHERE sdate = DATE_FORMAT(CURDATE(), '%Y-%m-%d')
;
INSERT INTO fe_dm.`dm_lo_zone_daily_data` (
  `sdate`,
  `business_area`,
  `zone_code`,
  `zone_name`,
  `last_month_gmv`,
  `shelfs`,
  `district`,
  `distance_avg`,
  `shelf_pre_distance_avg`,
  `area`
)
SELECT
  DATE_FORMAT(CURDATE(), '%Y-%m-%d') sdate,
  t0.`business_name` business_area,
  t0.`zone_code`,
  t0.`zone_name`,
  SUM(t1.gmv) last_month_gmv,
  COUNT(DISTINCT t0.`shelf_id`) shelfs,
  CASE
    WHEN t0.`zone_code` NOT IN (
      '620',
      '547',
      '214',
      '613',
      '24',
      '612',
      '425',
      '184',
      '206',
      '394',
      '453',
      '621',
      '532',
      '559',
      '322',
      '516',
      '169',
      '459',
      '551',
      '425',
      '463',
      '642',
      '526',
      '546',
      '417',
      '503',
      '433',
      '502',
      '534'
    )
    THEN GROUP_CONCAT(
      DISTINCT SUBSTRING_INDEX(t0.AREA_ADDRESS, ',', - 1)
    )
  END district,
  ROUND(t2.distance) distance_avg,
  ROUND(t3.distance) shelf_pre_distance_avg,
  ROUND(PI() * POWER((t2.distance / 2000), 2)) 'area'
FROM
  fe_dwd.`dwd_shelf_base_day_all` t0
  LEFT JOIN
    (SELECT
      `shelf_id`,
      SUM(
        IFNULL(`gmv`, 0) + IFNULL(`AFTER_PAYMENT_MONEY`, 0)
      ) gmv
    FROM
      fe_dwd.`dwd_shelf_day_his`
    WHERE `sdate` >= DATE_ADD(
        CURDATE() - DAY(CURDATE()) + 1,
        INTERVAL - 1 MONTH
      )
      AND `sdate` < DATE_ADD(
        CURDATE(),
        INTERVAL - DAY(CURDATE()) + 1 DAY
      )
    GROUP BY 1) t1
    ON t0.shelf_id = t1.shelf_id
  LEFT JOIN fe_dm.lo_zone_shelf_distance t2
    ON t0.`zone_code` = t2.`zone_code`
  LEFT JOIN fe_dm.lo_zone_prewarehouse_distance t3
    ON t0.`zone_code` = t3.`zone_code`
WHERE t0.`DATA_FLAG` = 1
  AND t0.`shelf_type` < 9
  AND t0.`SHELF_STATUS` IN (1, 2, 5)
  AND t0.`zone_code` > 0
GROUP BY t0.`business_name`,
  t0.`zone_code`
;
    
#记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_lo_zone_daily_data',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('汤云峰@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_lo_zone_daily_data','dm_lo_zone_daily_data','汤云峰');
 
END