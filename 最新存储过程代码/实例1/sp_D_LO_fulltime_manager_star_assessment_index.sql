CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_fulltime_manager_star_assessment_index`()
BEGIN
-- =============================================
-- Author:	物流店主组
-- Create date: 2019/07/18
-- Modify date: 
-- Description:	
-- 	全职店主星级考评得分结果表（每天的1时38分更新）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
SET @start_date:= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY);
SET @end_date:= CURRENT_DATE;
SET @start_month_str:= DATE_FORMAT(@start_date,'%Y-%m');
SET @end_month_str:= DATE_FORMAT(@end_date,'%Y-%m');
-- 动态获取货架维度当月的初始库存、期末库存
DROP TEMPORARY TABLE IF EXISTS feods.shelf_day_stock_star_temp;
SET @time_8 := CURRENT_TIMESTAMP();
SET @sql_statement:= CONCAT(
"CREATE TEMPORARY TABLE feods.shelf_day_stock_star_temp(KEY idx_shelf_id(shelf_id),KEY idx_product_id(product_id)) AS
select
  t.shelf_id,
  t.product_id,
  sum(t.start_quantity) start_quantity,
  sum(t.end_quantity) end_quantity
from
(SELECT
  k.shelf_id,
  k.PRODUCT_ID,
  k.DAY",DAY(@start_date),"_QUANTITY AS start_quantity,
  0 AS end_quantity
FROM
  fe.sf_shelf_product_stock_detail k
WHERE k.STAT_DATE = '",@start_month_str,
"' union
SELECT
  k.shelf_id,
  k.PRODUCT_ID,
  0 AS start_quantity,
  k.DAY",DAY(@end_date),"_QUANTITY AS end_quantity
FROM
  fe.sf_shelf_product_stock_detail k
WHERE k.STAT_DATE = '",@end_month_str,"') t
group by t.shelf_id,t.product_id"
);
PREPARE stml FROM @sql_statement;
EXECUTE stml;
DEALLOCATE PREPARE stml;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_8--@time_10",@time_8,@time_10);
	
-- 货架维度当月截止到更新的前一天的货损数量、金额等
DROP TEMPORARY TABLE IF EXISTS feods.shelf_risk_control_index;
SET @time_16 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE feods.shelf_risk_control_index (KEY idx_shelf_id (shelf_id)) AS
SELECT
  k.shelf_id,
  SUM(k.start_quantity) + IFNULL(SUM(e.ACTUAL_fill_NUM), 0) - IFNULL(SUM(a.quantity), 0) - SUM(k.end_quantity) AS huosun_qty,
  SUM(
    IFNULL(
      k.start_quantity * t2.SALE_PRICE,
      0
    )
  ) + SUM(
    IFNULL(
      e.ACTUAL_fill_NUM * t2.SALE_PRICE,
      0
    )
  ) - SUM(
    IFNULL(a.quantity * t2.SALE_PRICE, 0)
  ) - SUM(
    IFNULL(k.end_quantity * t2.SALE_PRICE, 0)
  ) AS huosun_amount
FROM
  feods.shelf_day_stock_star_temp k
  LEFT JOIN
    (SELECT
      r.`SHELF_ID`,
      m.`PRODUCT_ID`,
      SUM(
        IF(
          r.`ORDER_STATUS` = 2,
          m.`QUANTITY`,
          m.`quantity_shipped`
        )
      ) AS quantity
    FROM
      fe.`sf_order` r,
      fe.`sf_order_item` m
    WHERE r.`ORDER_ID` = m.`ORDER_ID`
      AND r.`ORDER_DATE` >= DATE_ADD(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
      )
      AND r.`ORDER_DATE` < DATE_ADD(
        LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),
        INTERVAL 1 DAY
      )
      AND r.ORDER_STATUS IN (2, 6, 7)
      AND r.`DATA_FLAG` = 1
      AND m.`DATA_FLAG` = 1
    GROUP BY r.`SHELF_ID`,
      m.`PRODUCT_ID`) a
    ON k.shelf_id = a.shelf_id
    AND k.product_id = a.product_id
  LEFT JOIN fe.sf_product_fill_order_item e
    ON e.shelf_id = k.shelf_id
    AND e.product_id = k.product_id
    AND e.data_flag = 1
  LEFT JOIN fe.sf_product_fill_order f
    ON e.order_id = f.order_id
    AND f.data_flag = 1
  LEFT JOIN fe.sf_shelf_product_detail t2
    ON k.shelf_id = t2.SHELF_ID
    AND k.product_id = t2.PRODUCT_ID
    AND t2.data_flag = 1
WHERE f.FILL_TIME >= DATE_ADD(
    DATE_SUB(CURDATE(), INTERVAL 1 DAY),
    INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
  )
  AND f.FILL_TIME < DATE_ADD(
    LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),
    INTERVAL 1 DAY
  )
  AND f.order_status IN (3, 4)
GROUP BY k.shelf_id;
SET @time_18 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_16--@time_18",@time_16,@time_18);
-- 货架维度当月每日总库存、货架等级、运营天数、激活时间、撤架时间、低库存目标值
DROP TEMPORARY TABLE IF EXISTS feods.shelf_stock_statistic_temp;
SET @time_21 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE feods.shelf_stock_statistic_temp(KEY idx_shelf_id(shelf_id)) AS
SELECT
  t3.SHELF_ID,
  CASE
    WHEN t5.REVOKE_date = LAST_DAY(t5.REVOKE_date)
    THEN t5.day_long
    ELSE t5.day_long + 1
  END AS month_operation_days,
  SUM(
    (
      CASE
        WHEN t3.DAY1_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY2_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY3_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY4_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY5_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY6_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY7_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY8_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY9_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY10_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY11_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY12_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY13_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY14_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY15_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY16_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY17_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY18_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY19_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY20_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY21_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY22_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY23_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY24_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY25_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY26_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY27_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY28_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY29_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY30_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    ) + (
      CASE
        WHEN t3.DAY31_QUANTITY > t3.target_value
        THEN 1
        ELSE 0
      END
    )
  ) AS month_on_target_days
FROM
   (SELECT
    t1.SHELF_ID,
    t2.shelf_level,
    t2.shelf_type,
    t2.ACTIVATE_date,
    t2.ACTIVATE_date_old,
    t2.REVOKE_date,
    t2.REVOKE_date_old,
    t2.day_long,
    CASE
      WHEN (
        t2.new_shelf = 1
        OR t2.shelf_level IN ('甲级', '乙级')
      )
      AND t2.shelf_type IN (1, 3)
      THEN 180
      WHEN (
        t2.new_shelf = 1
        OR t2.shelf_level IN ('甲级', '乙级')
      )
      AND t2.shelf_type IN (2, 5)
      THEN 110
      WHEN t2.shelf_level IN ('丙级', '丁级')
      AND t2.shelf_type IN (1, 3)
      THEN 110
      WHEN t2.shelf_level IN ('丙级', '丁级')
      AND t2.shelf_type IN (2, 5)
      THEN 90
    END AS 'target_value',
    SUM(t1.DAY1_QUANTITY) AS DAY1_QUANTITY,
    SUM(t1.DAY2_QUANTITY) AS DAY2_QUANTITY,
    SUM(t1.DAY3_QUANTITY) AS DAY3_QUANTITY,
    SUM(t1.DAY4_QUANTITY) AS DAY4_QUANTITY,
    SUM(t1.DAY5_QUANTITY) AS DAY5_QUANTITY,
    SUM(t1.DAY6_QUANTITY) AS DAY6_QUANTITY,
    SUM(t1.DAY7_QUANTITY) AS DAY7_QUANTITY,
    SUM(t1.DAY8_QUANTITY) AS DAY8_QUANTITY,
    SUM(t1.DAY9_QUANTITY) AS DAY9_QUANTITY,
    SUM(t1.DAY10_QUANTITY) AS DAY10_QUANTITY,
    SUM(t1.DAY11_QUANTITY) AS DAY11_QUANTITY,
    SUM(t1.DAY12_QUANTITY) AS DAY12_QUANTITY,
    SUM(t1.DAY13_QUANTITY) AS DAY13_QUANTITY,
    SUM(t1.DAY14_QUANTITY) AS DAY14_QUANTITY,
    SUM(t1.DAY15_QUANTITY) AS DAY15_QUANTITY,
    SUM(t1.DAY16_QUANTITY) AS DAY16_QUANTITY,
    SUM(t1.DAY17_QUANTITY) AS DAY17_QUANTITY,
    SUM(t1.DAY18_QUANTITY) AS DAY18_QUANTITY,
    SUM(t1.DAY19_QUANTITY) AS DAY19_QUANTITY,
    SUM(t1.DAY20_QUANTITY) AS DAY20_QUANTITY,
    SUM(t1.DAY21_QUANTITY) AS DAY21_QUANTITY,
    SUM(t1.DAY22_QUANTITY) AS DAY22_QUANTITY,
    SUM(t1.DAY23_QUANTITY) AS DAY23_QUANTITY,
    SUM(t1.DAY24_QUANTITY) AS DAY24_QUANTITY,
    SUM(t1.DAY25_QUANTITY) AS DAY25_QUANTITY,
    SUM(t1.DAY26_QUANTITY) AS DAY26_QUANTITY,
    SUM(t1.DAY27_QUANTITY) AS DAY27_QUANTITY,
    SUM(t1.DAY28_QUANTITY) AS DAY28_QUANTITY,
    SUM(t1.DAY29_QUANTITY) AS DAY29_QUANTITY,
    SUM(t1.DAY30_QUANTITY) AS DAY30_QUANTITY,
    SUM(t1.DAY31_QUANTITY) AS DAY31_QUANTITY
  FROM
    fe.sf_shelf_product_stock_detail t1
    INNER JOIN
      (SELECT
        s.shelf_id,
        CASE
          WHEN shelf_level = '甲级2'
          THEN '甲级'
          WHEN shelf_level = '乙级2'
          THEN '乙级'
          WHEN shelf_level = '丙级2'
          THEN '丙级'
          WHEN shelf_level = '丁级2'
          THEN '丁级'
          ELSE shelf_level
        END AS shelf_level,
        s.shelf_type,
        s.ACTIVATE_date,
        s.ACTIVATE_date_old,
        CASE
          WHEN ACTIVATE_date = ACTIVATE_date_old
          THEN 1
          ELSE 0
        END AS 'new_shelf',
        s.REVOKE_date,
        s.REVOKE_date_old,
        s.day_long,
        s.gmv
      FROM
        feods.pj_shelf_level_ab s
      WHERE s.smonth = DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m')
        AND s.shelf_type IN (1, 2, 3, 5)
      GROUP BY 1) t2
      ON t1.SHELF_ID = t2.shelf_id
  WHERE t1.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y-%m')
  GROUP BY 1) t3
  LEFT JOIN fe.sf_shelf t4
    ON t3.SHELF_ID = t4.SHELF_ID
  LEFT JOIN (SELECT
      ta.shelf_id,
      ta.REVOKE_date_old,
      ta.ACTIVATE_date_old,
      ta.ACTIVATE_date,
      ta.REVOKE_date,
      TIMESTAMPDIFF(DAY, ta.ACTIVATE_date, ta.REVOKE_date) + 1  AS day_long
    FROM
      (SELECT
        t1.shelf_id,
        t1.SHELF_STATUS,
        t1.ACTIVATE_date AS ACTIVATE_date_old,
        t1.REVOKE_date AS REVOKE_date_old,
        CASE
          WHEN t1.ACTIVATE_date < t1.date01
          THEN t1.date01
          ELSE t1.ACTIVATE_date
        END AS ACTIVATE_date,
        CASE
          WHEN t1.REVOKE_date < t1.date30
          THEN t1.REVOKE_date
          ELSE t1.date30
        END AS REVOKE_date
      FROM
        (SELECT
          h.shelf_id,
          h.SHELF_STATUS,
          STR_TO_DATE(
            DATE_FORMAT(h.ACTIVATE_TIME, '%Y%m%d'),
            '%Y%m%d'
          ) AS ACTIVATE_date,
          STR_TO_DATE(
            DATE_FORMAT(h.REVOKE_TIME, '%Y%m%d'),
            '%Y%m%d'
          ) AS REVOKE_date,
          STR_TO_DATE(
            DATE_FORMAT(
              DATE_SUB(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                INTERVAL DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%d%'
                ) - 1 DAY
              ),
              '%Y%m%d'
            ),
            '%Y%m%d'
          ) AS date01,
          STR_TO_DATE(
            DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            ),
            '%Y%m%d'
          ) AS date30
        FROM
          fe.sf_shelf h
        WHERE h.SHELF_STATUS IN (2, 3, 5)
          AND h.shelf_type IN (1, 2, 3, 5, 8)) t1
      WHERE DATE_FORMAT(t1.ACTIVATE_date, '%Y%m%d') <= DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m%d'
        )
        AND (
          t1.REVOKE_date IS NULL
          OR t1.REVOKE_date > DATE_FORMAT(
            DATE_SUB(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              INTERVAL DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%d%'
              ) - 1 DAY
            ),
            '%Y%m%d'
          )
        )) ta
    GROUP BY ta.shelf_id) t5
   ON t3.SHELF_ID = t5.SHELF_ID
GROUP BY t3.SHELF_ID;
SET @time_23 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_21--@time_23",@time_21,@time_23);
  -- 货架效能宽表，与网易有数店主看板底层模型相同
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_statistic_mid;
SET @time_26 := CURRENT_TIMESTAMP();
  
  CREATE TEMPORARY TABLE feods.shelf_statistic_mid(KEY idx_shelf_id(shelf_id)) AS
  SELECT
    ac.business_area,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(a.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city,
    b.BRANCH_CODE,
    b.sf_code,
    b.real_name,
    a.manager_id,
    a.shelf_id,
    a.shelf_code,
    a.activate_time,
    a.shelf_status,
    a.REVOKE_STATUS,
    a.shelf_type,
    LEFT(sf.shelf_level,2) AS shelf_level,
    c.GMV,  -- GMV
    c.AMOUNT, -- 实收
    c.order_qty, -- 订单数
    c.user_qty, -- 用户数
    c.sales_qty, -- 销量
    e.qty AS operate_qty, -- 盘点次数
    e.yuedi_qty AS last_operate_qty, -- 月底盘点次数
    e.max_OPERATE_TIME AS last_operate_time, -- 最近一次盘点时间
    f.fill_qty, -- 补货次数
    f.youx_buh AS valid_fill_qty, -- 有效补货次数
    g.transfer_qty, -- 调货次数
    g.youx_diaoh AS valid_transfer_qty, -- 有效调货次数
    w.ACTUAL_SEND_NUM AS fill_onWay_num, -- 往前七天补货在途数量
    m.PAYMENT_MONEY -- 补款金额
  FROM
    fe.sf_shelf a
    JOIN fe.`zs_city_business` ac
    ON SUBSTRING_INDEX(SUBSTRING_INDEX(a.area_address,',',2),',',-1)= ac.city_name
    JOIN fe.pub_shelf_manager b
      ON a.manager_id = b.manager_id
    LEFT JOIN feods.`pj_shelf_level_ab` sf
      ON a.shelf_id = sf.shelf_id
      AND STR_TO_DATE(CONCAT(sf.smonth,'01'),'%Y%m%d')=DATE_SUB(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),INTERVAL 1 MONTH)
    LEFT JOIN
      (SELECT
        s.shelf_id,
        SUM(s.AMOUNT) AS AMOUNT,
        SUM(s.GMV) AS GMV,
        COUNT(DISTINCT s.order_id) AS order_qty,
        COUNT(DISTINCT s.user_id) AS user_qty,
        SUM(s.sales_qty) AS sales_qty
      FROM
        (SELECT
          f.shelf_id,
          f.`ORDER_ID`,
          f.user_id,
          f.`PRODUCT_TOTAL_AMOUNT` AS AMOUNT,
          SUM(IF(f.order_status = 2,
            e.quantity,e.quantity_shipped
          )
          ) AS sales_qty,
          SUM(e.QUANTITY * e.SALE_PRICE) AS GMV
        FROM
          fe.sf_order_item AS e,
          fe.sf_order AS f
        WHERE e.order_id = f.ORDER_ID
          AND f.ORDER_STATUS IN (2,6,7)
          AND e.data_flag = 1
          AND f.data_flag = 1
          AND f.order_date >= DATE_ADD(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
          )
          AND f.order_date < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        GROUP BY f.SHELF_ID,
          f.`ORDER_ID`) s
      GROUP BY s.shelf_id) c
      ON a.shelf_id = c.shelf_id
    LEFT JOIN
      (SELECT
        b.SHELF_ID,
        COUNT(
          DISTINCT DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d')
        ) AS qty,
        COUNT(
          DISTINCT
          CASE
            WHEN DAY(b.OPERATE_TIME) >= 25
            THEN DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d')
          END
        ) AS yuedi_qty,
        MAX(b.OPERATE_TIME) AS max_OPERATE_TIME
      FROM
        fe.sf_shelf_check b
      WHERE b.operate_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND b.operate_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        AND b.DATA_FLAG = 1
      GROUP BY b.SHELF_ID) e
      ON a.shelf_id = e.shelf_id
    LEFT JOIN
      (SELECT
        f.shelf_id,
        COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10
            THEN DATE_FORMAT(f.fill_time,'%Y%m%d')
          END
        ) AS youx_buh,
        COUNT(DISTINCT f.order_id) AS fill_qty
      FROM
        fe.sf_product_fill_order f
      WHERE f.order_status IN (3, 4)
--         AND f.fill_type IN (1, 2, 8, 9, 4, 7)
        AND f.shelf_id IN (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type <> 9)
        AND f.supplier_type <> 1
        AND f.data_flag = 1
        AND f.fill_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND f.fill_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      GROUP BY f.shelf_id) f
      ON a.shelf_id = f.shelf_id
    LEFT JOIN
      (SELECT
        g.shelf_id,
        COUNT(DISTINCT g.order_id) AS transfer_qty,
        COUNT(
          DISTINCT
          CASE
            WHEN g.PRODUCT_NUM > 10
            THEN DATE_FORMAT(g.fill_time,'%Y%m%d')
          END
        ) AS youx_diaoh
      FROM
        fe.sf_product_fill_order g
      WHERE g.order_status IN (3, 4)
--         AND g.fill_type IN (6, 11)
        AND g.shelf_id IN (SELECT DISTINCT s.shelf_id FROM fe.`sf_shelf` s WHERE s.data_flag =1 AND s.shelf_type <> 9)
        AND g.supplier_type = 1
        AND g.data_flag = 1
        AND g.fill_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND g.fill_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      GROUP BY g.shelf_id) g
      ON a.shelf_id = g.shelf_id
    LEFT JOIN
      (SELECT
        spfi.SHELF_ID,
        SUM(spfi.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM
      FROM
        fe.sf_product_fill_order_item spfi,
        fe.sf_product_fill_order AS spfo
      WHERE spfo.ORDER_ID = spfi.ORDER_ID
        AND spfo.DATA_FLAG = 1
        AND spfi.data_flag = 1
        AND spfo.ORDER_STATUS IN (1, 2)
        AND spfo.apply_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        AND spfo.apply_time < CURDATE()
      GROUP BY spfi.SHELF_ID) w
      ON a.shelf_id = w.shelf_id
    LEFT JOIN
      (SELECT
        p.shelf_id,
        SUM(p.PAYMENT_MONEY) AS PAYMENT_MONEY
      FROM
        fe.sf_after_payment p
      WHERE p.payment_date >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND p.payment_date < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        AND p.PAYMENT_STATUS = 2
      GROUP BY p.shelf_id) m
      ON a.shelf_id = m.shelf_id
  WHERE (
      a.shelf_status IN (2, 5)
      OR (
        a.revoke_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND a.revoke_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      )
    )
    AND b.`second_user_type` = 1
    AND a.data_flag =1 
    AND b.data_flag =1 
  GROUP BY a.shelf_id;
SET @time_28 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_26--@time_28",@time_26,@time_28);
 
  
DELETE
FROM
  feods.`D_LO_fulltime_manager_star_assessment_score_detail`
WHERE statis_time >= DATE_ADD(
    DATE_SUB(CURDATE(), INTERVAL 1 DAY),
    INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
  )
  AND statis_time < DATE_ADD(
    LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),
    INTERVAL 1 DAY
  );
SET @time_31 := CURRENT_TIMESTAMP();
-- 插入500GMV货架数达成率
INSERT INTO feods.`D_LO_fulltime_manager_star_assessment_score_detail`(
statis_time     
,manager_id      
,statis_type     
,statis_type_name
,task_finish_rate
)
SELECT
  DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS '截存日期',  -- 更新统计结果数据的日期（数据含该日期的）
  t1.manager_id,
  14 AS statis_type,
  '500GMV货架数达成率' AS statis_type_name,
  ROUND(COUNT(DISTINCT IF(t1.GMV>=500,t1.shelf_id,NULL))/COUNT(DISTINCT t1.shelf_id),2) AS task_finish_rate
FROM
  feods.shelf_statistic_mid t1
LEFT JOIN feods.shelf_risk_control_index t2
ON t1.shelf_id = t2.shelf_id
LEFT JOIN feods.shelf_stock_statistic_temp t3
ON t1.shelf_id = t3.shelf_id
GROUP BY t1.manager_id;
SET @time_33 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_31--@time_33",@time_31,@time_33);
SET @time_35 := CURRENT_TIMESTAMP();
-- 更新500GMV货架数达成率的权重和分数
UPDATE feods.`D_LO_fulltime_manager_star_assessment_score_detail` t
SET t.weight= 0.2,t.`score`= IF(t.`task_finish_rate`>= 0.8,20,IF(t.`task_finish_rate`>= 0.7,15,IF(t.`task_finish_rate`>= 0.6,10,IF(t.`task_finish_rate`>= 0.5,5,0))))
WHERE t.statis_type = 14;
SET @time_37 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_35--@time_37",@time_35,@time_37);
SET @time_39 := CURRENT_TIMESTAMP();
-- 插入补付款渗透率
INSERT INTO feods.`D_LO_fulltime_manager_star_assessment_score_detail`(
statis_time     
,manager_id      
,statis_type     
,statis_type_name
,task_finish_rate
)
SELECT
  DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS '截存日期',  -- 更新统计结果数据的日期（数据含该日期的）
  t1.manager_id,
  5 AS statis_type,
  '补付款渗透率' AS statis_type_name,
  ROUND(SUM(t1.PAYMENT_MONEY)/SUM(t2.huosun_amount),2) AS task_finish_rate
FROM
  feods.shelf_statistic_mid t1
LEFT JOIN feods.shelf_risk_control_index t2
ON t1.shelf_id = t2.shelf_id
LEFT JOIN feods.shelf_stock_statistic_temp t3
ON t1.shelf_id = t3.shelf_id
GROUP BY t1.manager_id;
SET @time_41 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_39--@time_41",@time_39,@time_41);
SET @time_43 := CURRENT_TIMESTAMP();
-- 更新补付款渗透率的权重和分数
UPDATE feods.`D_LO_fulltime_manager_star_assessment_score_detail` t
SET t.weight= 0.2,t.`score`= IF(t.`task_finish_rate`>= 0.5,20,IF(t.`task_finish_rate`>= 0.4,15,IF(t.`task_finish_rate`>= 0.3,10,IF(t.`task_finish_rate`>= 0.1,5,0))))
WHERE t.statis_type = 5;
SET @time_45 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_43--@time_45",@time_43,@time_45);
SET @time_47 := CURRENT_TIMESTAMP();
-- 插入撤架率
INSERT INTO feods.`D_LO_fulltime_manager_star_assessment_score_detail`(
statis_time     
,manager_id      
,statis_type     
,statis_type_name
,task_finish_rate
)
SELECT
  DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS '截存日期',  -- 更新统计结果数据的日期（数据含该日期的）
  t1.manager_id,
  11 AS statis_type,
  '撤架率' AS statis_type_name,
  ROUND(COUNT(IF(t1.shelf_status IN (3),t1.shelf_id,NULL))/COUNT(DISTINCT t1.shelf_id)) AS task_finish_rate
FROM
  feods.shelf_statistic_mid t1
LEFT JOIN feods.shelf_risk_control_index t2
ON t1.shelf_id = t2.shelf_id
LEFT JOIN feods.shelf_stock_statistic_temp t3
ON t1.shelf_id = t3.shelf_id
GROUP BY t1.manager_id;
SET @time_49 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_47--@time_49",@time_47,@time_49);
SET @time_51 := CURRENT_TIMESTAMP();
-- 更新撤架率的权重和分数
UPDATE feods.`D_LO_fulltime_manager_star_assessment_score_detail` t
SET t.weight= 0.1,t.`score`= IF(t.`task_finish_rate`<= 0.04,10,IF(t.`task_finish_rate`<= 0.08,5,0))
WHERE t.statis_type = 11;
SET @time_53 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_51--@time_53",@time_51,@time_53);
SET @time_55 := CURRENT_TIMESTAMP();
-- 插入增收任务完成率
INSERT INTO feods.`D_LO_fulltime_manager_star_assessment_score_detail`(
statis_time     
,manager_id      
,statis_type     
,statis_type_name
,task_finish_rate
)
SELECT
  DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS '截存日期',  -- 更新统计结果数据的日期（数据含该日期的）
  t1.manager_id,
  15 AS statis_type,
  '增收任务完成率' AS statis_type_name,
  (IF(COUNT(DISTINCT IF(t1.activate_time>= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        ) AND t1.activate_time< DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY) AND t1.shelf_status IN (2) AND t1.GMV>0,t1.shelf_id,NULL))>2,1,0) -- 货架开发任务
  +IF(COUNT(DISTINCT IF(t1.operate_qty >0,t1.shelf_id,NULL))=COUNT(DISTINCT t1.shelf_id),1,0)  -- 月底盘点任务
  +IF(SUM(t3.month_on_target_days)/SUM(t3.month_operation_days) >= 0.8,1,0)  -- 库存达标率
  +IF((SUM(t1.valid_fill_qty)+SUM(t1.operate_qty))/(SELECT SUM(w.if_work_day) FROM feods.`fjr_work_days` w WHERE w.sdate>=DATE_ADD(   -- 工作日均效能
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        ) AND w.sdate<DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)) >= 16,1,0))/4 AS task_finish_rate
FROM
  feods.shelf_statistic_mid t1
LEFT JOIN feods.shelf_risk_control_index t2
ON t1.shelf_id = t2.shelf_id
LEFT JOIN feods.shelf_stock_statistic_temp t3
ON t1.shelf_id = t3.shelf_id
GROUP BY t1.manager_id;
SET @time_57 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_55--@time_57",@time_55,@time_57);
SET @time_59 := CURRENT_TIMESTAMP();
-- 更新增收任务完成率的权重和分数
UPDATE feods.`D_LO_fulltime_manager_star_assessment_score_detail` t
SET t.weight= 0.5,t.`score`= IF(t.`task_finish_rate`= 1,50,IF(t.`task_finish_rate`>= 0.8,40,IF(t.`task_finish_rate`>= 0.5,30,IF(t.`task_finish_rate`>= 0.3,20,0))))
WHERE t.statis_type = 15;
SET @time_61 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_fulltime_manager_star_assessment_index","@time_59--@time_61",@time_59,@time_61);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log`(
    'sp_D_LO_fulltime_manager_star_assessment_index',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
COMMIT;
END