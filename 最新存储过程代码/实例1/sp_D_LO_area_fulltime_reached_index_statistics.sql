CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_area_fulltime_reached_index_statistics`()
BEGIN
  -- =============================================
-- Author:	物流店主
-- Create date: 2019/09/20
-- Modify date: 
-- Description:	
--    更新区域维度全职达成指标统计结果表（每天的1时41分）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  
  SET @date_top:= DATE_ADD(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
            );
  SET @date_end:= DATE_ADD(
              LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),
              INTERVAL 1 DAY
            );
            
  DELETE
  FROM
    feods.`D_LO_area_fulltime_reached_index_statistics`
  WHERE STAT_DATE = DATE_FORMAT(
      DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),
      '%Y%m'
    );
  SET @sql_statement := CONCAT(
    "insert into feods.`D_LO_area_fulltime_reached_index_statistics` (
      STAT_DATE,
      REGION_area,
      business_area,
      shelf_type,
      manager_num,
      shelf_qty,
      area_shelf_qty,
      fulltime_GMV,
      area_GMV,
      revoked_shelf_qty,
      area_revoked_qty,
      low_stock_shelf_qty,
      area_low_stock_qty,
      stockout_rate,
      two_days_fill_rate
    )
    SELECT
      date_format(
        date_sub(current_date, interval 1 day),
        '%Y%m'
      ) as '统计月份',
      k.`REGION_NAME` AS '大区',
      k.business_name AS '地区',
      a.shelf_type AS '货架类型',
      COUNT(
        DISTINCT IF(
          b.second_user_type = 1,
          b.manager_id,
          NULL
        )
      ) AS '全职店主人数',
      COUNT(
        DISTINCT IF(
          b.second_user_type = 1,
          a.shelf_id,
          NULL
        )
      ) AS '全职店主货架数',
      COUNT(DISTINCT a.shelf_id) AS '地区货架数',
      SUM(
        IF(b.second_user_type = 1, c.GMV, 0)
      ) AS '全职GMV',
      SUM(c.GMV) AS '地区GMV',
      COUNT(
        DISTINCT IF(
          b.second_user_type = 1
          AND a.`SHELF_STATUS` = 3,
          a.shelf_id,
          NULL
        )
      ) AS '全职撤架数',
      COUNT(
        DISTINCT IF(
          a.`SHELF_STATUS` = 3,
          a.shelf_id,
          NULL
        )
      ) AS '地区撤架数',
      COUNT(
        DISTINCT IF(
          b.second_user_type = 1,
            CASE
            WHEN j.PACKAGE_MODEL = 3
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
            AND i.shelf_stock < 290
            THEN a.shelf_id
            WHEN j.PACKAGE_MODEL = 4
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
            AND i.shelf_stock < 360
            THEN a.shelf_id
            WHEN j.PACKAGE_MODEL = 5
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
            AND i.shelf_stock < 470
            THEN a.shelf_id
            WHEN a.shelf_type IN (1, 3)
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
            AND i.shelf_stock < 180
            THEN a.shelf_id
            WHEN a.shelf_type IN (2, 5)
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
            AND i.shelf_stock < 110
            THEN a.shelf_id
            WHEN j.PACKAGE_MODEL = 3
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
            AND i.shelf_stock < 200
            THEN a.shelf_id
            WHEN j.PACKAGE_MODEL = 4
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
            AND i.shelf_stock < 220
            THEN a.shelf_id
            WHEN j.PACKAGE_MODEL = 5
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
            AND i.shelf_stock < 310
            THEN a.shelf_id
            WHEN a.shelf_type IN (1, 3)
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
            AND i.shelf_stock < 110
            THEN a.shelf_id
            WHEN a.shelf_type IN (2, 5)
            AND a.shelf_status = 2
            AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
            AND i.shelf_stock < 90
            THEN a.shelf_id
          END,
          NULL
        )
      ) AS '全职低库存货架数',
      COUNT(
        DISTINCT
        CASE
          WHEN j.PACKAGE_MODEL = 3
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
          AND i.shelf_stock < 290
          THEN a.shelf_id
          WHEN j.PACKAGE_MODEL = 4
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
          AND i.shelf_stock < 360
          THEN a.shelf_id
          WHEN j.PACKAGE_MODEL = 5
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
          AND i.shelf_stock < 470
          THEN a.shelf_id
          WHEN a.shelf_type IN (1, 3)
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
          AND i.shelf_stock < 180
          THEN a.shelf_id
          WHEN a.shelf_type IN (2, 5)
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('甲级', '乙级')
          AND i.shelf_stock < 110
          THEN a.shelf_id
          WHEN j.PACKAGE_MODEL = 3
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
          AND i.shelf_stock < 200
          THEN a.shelf_id
          WHEN j.PACKAGE_MODEL = 4
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
          AND i.shelf_stock < 220
          THEN a.shelf_id
          WHEN j.PACKAGE_MODEL = 5
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
          AND i.shelf_stock < 310
          THEN a.shelf_id
          WHEN a.shelf_type IN (1, 3)
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
          AND i.shelf_stock < 110
          THEN a.shelf_id
          WHEN a.shelf_type IN (2, 5)
          AND a.shelf_status = 2
          AND LEFT(sf.shelf_level, 2) IN ('丙级', '丁级')
          AND i.shelf_stock < 90
          THEN a.shelf_id
        END
      ) AS '地区低库存货架数',
      SUM(st.ifsto_num)/SUM(st.ct) stockout_rate,
      SUM(fm.in_num)/SUM(fm.total_num) two_days_fill_rate
    FROM
      fe.sf_shelf a
      JOIN fe.pub_shelf_manager b
        ON a.manager_id = b.manager_id
        AND b.data_flag = 1
      JOIN feods.`fjr_city_business` k
        ON a.city = k.city
      LEFT JOIN feods.`pj_shelf_level_ab` sf
        ON a.shelf_id = sf.shelf_id
        AND STR_TO_DATE(CONCAT(sf.smonth, '01'), '%Y%m%d') = DATE_SUB(
          DATE_ADD(
            DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),
            INTERVAL - DAY(
              DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
            ) + 1 DAY
          ),
          INTERVAL 1 MONTH
        )
      LEFT JOIN
        (SELECT
          s.shelf_id,
          SUM(s.GMV) AS GMV
        FROM
          (SELECT
            f.shelf_id,
            f.`ORDER_ID`,
            f.`PRODUCT_TOTAL_AMOUNT` AS AMOUNT,
            SUM(e.QUANTITY * e.SALE_PRICE) AS GMV
          FROM
            fe.sf_order_item AS e,
            fe.sf_order AS f  FORCE INDEX(idx_order_orderdate)
          WHERE e.order_id = f.ORDER_ID
            AND f.ORDER_STATUS = 2
            AND f.order_date >= @date_top
            AND f.order_date < @date_end
            AND e.data_flag = 1
            AND f.data_flag = 1
          GROUP BY f.SHELF_ID,
            f.`ORDER_ID`) s
        GROUP BY s.shelf_id) c                       -- 销售数据
         ON a.shelf_id = c.shelf_id
      LEFT JOIN
(SELECT
 DATE_FORMAT(t.`sdate`,'%Y%m') smonth,
 t.`shelf_id`,
 SUM(IF(t.`ifsto`=0,t.`ct`,0)) ifsto_num,
 SUM(t.`ct`) ct
FROM
 feods.`d_op_s_offstock` t FORCE INDEX(sdate)
WHERE t.`sdate` >= @date_top
AND t.`sdate` < @date_end
GROUP BY t.`shelf_id`) st                            -- 缺货率指标
ON a.shelf_id = st.shelf_id
LEFT JOIN
(SELECT
 DATE_FORMAT(t.apply_time,'%Y%m') smonth,
 t.`SHELF_ID`,
 COUNT(DISTINCT IF(t.`two_days_fill_label`='及时',t.`ORDER_ID`,NULL)) in_num,
 COUNT(DISTINCT t.order_id) total_num
FROM feods.`D_LO_shelf_fill_timeliness_detail` t
WHERE t.`APPLY_TIME` >= @date_top
AND t.`APPLY_TIME` < @date_end
GROUP BY t.`SHELF_ID`) fm                            -- 补货次日上架率指标
on a.shelf_id = fm.shelf_id
      LEFT JOIN
        (SELECT
          s.shelf_id,
          SUM(s.day",
    DAY(CURRENT_DATE),
    "_quantity) AS shelf_stock
        FROM
          fe.`sf_shelf_product_stock_detail` s
        WHERE s.stat_date = DATE_FORMAT(CURRENT_DATE, '%Y-%m')
        GROUP BY s.shelf_id) i                       -- 当月截止到昨日的库存数据
         ON a.shelf_id = i.shelf_id
      LEFT JOIN
        (SELECT
          a.MAIN_SHELF_ID,
          MAX(a.PACKAGE_MODEL) AS PACKAGE_MODEL
        FROM
          fe.sf_shelf_relation_record a
        WHERE a.DATA_FLAG = 1
          AND a.SHELF_HANDLE_STATUS = 9
        GROUP BY a.MAIN_SHELF_ID) j -- 关联货架数据
         ON a.shelf_id = j.MAIN_SHELF_ID
    WHERE a.data_flag = 1
      AND (
        a.shelf_status IN (2, 5)
        OR (
          a.revoke_time >= @date_top
          AND a.revoke_time < @date_end
        )
      )
    GROUP BY k.business_name,a.shelf_type"
  );
  PREPARE stml FROM @sql_statement;
  EXECUTE stml;
  DEALLOCATE PREPARE stml;
  UPDATE
    feods.`D_LO_area_fulltime_reached_index_statistics` t
  SET
    t.shelf_coverage_rate = ROUND(t.shelf_qty / t.area_shelf_qty, 2)
  WHERE t.shelf_qty IS NOT NULL
    AND t.area_shelf_qty IS NOT NULL
    AND t.area_shelf_qty != 0;
  UPDATE
    feods.`D_LO_area_fulltime_reached_index_statistics` t
  SET
    t.gmv_coverage_rate = ROUND(t.fulltime_GMV / t.area_GMV, 2)
  WHERE t.fulltime_GMV IS NOT NULL
    AND t.area_GMV IS NOT NULL
    AND t.area_GMV != 0;
  -- 执行日志
  CALL sh_process.`sp_sf_dw_task_log` (
    'sp_D_LO_area_fulltime_reached_index_statistics',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
  COMMIT;
END