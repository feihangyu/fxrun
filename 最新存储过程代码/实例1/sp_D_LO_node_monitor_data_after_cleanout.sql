CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_node_monitor_data_after_cleanout`()
begin
-- =============================================
-- Author:	物流店主
-- Create date: 2019/09/20
-- Modify date: 
-- Description:	
--    更新维护清洗后的埋点明细中间表（每天的2时43分跑）
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.D_LO_node_monitor_data_after_cleanout
  WHERE update_time = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY);
  INSERT INTO feods.D_LO_node_monitor_data_after_cleanout (
    update_time,
    action_type,
    action_id,
    action_time,
    duration,
    lng,
    lat
  )
  SELECT DISTINCT
    DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS update_time,
    1 AS action_type,
    CAST(t.check_id AS SIGNED) action_id,
    t.`logTimeDate` action_time,
    t.duration,
    t.longitude longitude,
    t.latitude latitude
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
      a.`logTimeDate`
    FROM
      feods.mongo_shelf_manager_behavior_log a
    WHERE a.`logTimeDate` >= DATE('20190701')
      AND a.`logTimeDate` >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
      AND a.`logTimeDate` < CURRENT_DATE
      AND a.`behaviorCode` = 'B01'
      AND a.pageCode = 'P01-2'
      AND a.`behaviorResultInfo` REGEXP '^{'
      AND a.`behaviorResultInfo` REGEXP '}$') t
  WHERE t.check_id IS NOT NULL
    AND (t.longitude, t.latitude) NOT IN
    (SELECT
      0 a,
      0 b)
    UNION
    SELECT DISTINCT
      DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS update_time,
      1 AS action_type,
      CAST(t.check_id AS SIGNED) action_id,
      t.`logTimeDate` action_time,
      t.duration,
      t.longitude longitude,
      t.latitude latitude
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
        a.`logTimeDate`
      FROM
        feods.mongo_shelf_manager_behavior_log a
      WHERE a.`logTimeDate` >= DATE('20190701')
        AND a.`logTimeDate` >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
        AND a.`logTimeDate` < CURRENT_DATE
        AND a.`behaviorCode` = 'B04'
        AND a.pageCode = 'P01-2'
        AND a.`behaviorResultInfo` REGEXP '^{'
        AND a.`behaviorResultInfo` REGEXP '}$') t
    WHERE t.check_id IS NOT NULL
      AND (t.longitude, t.latitude) NOT IN
      (SELECT
        0 a,
        0 b)
      UNION
      SELECT DISTINCT
        DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS update_time,
        2 AS action_type,
        CAST(t.order_id AS SIGNED) action_id,
        t.`logTimeDate` action_time,
        null as duration,
        t.longitude,
        t.latitude
      FROM
        (SELECT
          IF(
            LOCATE(
              'orderId',
              a.`behaviorResultInfo`
            ) <> 0,
            SUBSTRING(
              REPLACE(a.`behaviorResultInfo`, '}', ','),
              LOCATE(
                'orderId=',
                a.`behaviorResultInfo`
              ) + CHAR_LENGTH('orderId='),
              LOCATE(
                ',',
                REPLACE(a.`behaviorResultInfo`, '}', ','),
                LOCATE(
                  'orderId',
                  a.`behaviorResultInfo`
                )
              ) - (
                LOCATE(
                  'orderId=',
                  a.`behaviorResultInfo`
                ) + CHAR_LENGTH('orderId=')
              )
            ),
            NULL
          ) AS order_id,
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
          a.`logTimeDate`
        FROM
          feods.mongo_shelf_manager_behavior_log a
        WHERE a.`logTimeDate` >= DATE('20190701')
          AND a.`logTimeDate` >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
          AND a.`logTimeDate` < CURRENT_DATE
          AND a.pageCode = 'P06'
          AND a.`behaviorResultInfo` REGEXP '^{'
          AND a.`behaviorResultInfo` REGEXP '}$') t
      WHERE t.order_id IS NOT NULL
        AND t.longitude <> 0.0
        AND t.latitude <> 0.0;
  CALL sh_process.`sp_sf_dw_task_log`(
  'sp_D_LO_node_monitor_data_after_cleanout',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);
  commit;
end