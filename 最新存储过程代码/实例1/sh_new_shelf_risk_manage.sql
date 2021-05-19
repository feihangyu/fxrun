CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_new_shelf_risk_manage`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.pj_new_shelf_risk_manage
  WHERE stat_date = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    );
  INSERT INTO feods.pj_new_shelf_risk_manage (
    stat_date,
    exploit_type,
    data_type,
    first_week_num,
    second_week_num,
    third_week_num,
    fourth_week_num,
    first_mon_num,
    second_mon_num,
    accum_num
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ) AS "数据日期",
    b.exploit_type AS 开发类型,
    1 AS data_type,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
        c.USER_ID,
        NULL
      )
    ) AS 引入首周下单用户数,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
        c.USER_ID,
        NULL
      )
    ) AS 引入次周下单用户数,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
        c.USER_ID,
        NULL
      )
    ) AS 引入第三周下单用户数,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
        c.USER_ID,
        NULL
      )
    ) AS 引入第四周下单用户数,
    COUNT(
      DISTINCT IF(
        DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
          '%Y%m'
        ),
        c.USER_ID,
        NULL
      )
    ) AS 引入首月下单用户数,
    COUNT(
      DISTINCT IF(
        DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
        c.USER_ID,
        NULL
      )
    ) AS 引入次月下单用户数,
    COUNT(DISTINCT c.USER_ID) AS 累计下单用户数
  FROM
    fe.sf_shelf b
    JOIN fe.sf_order c
      ON b.shelf_id = c.shelf_id
  WHERE DATE_FORMAT(b.activate_time, '%Y%m%d') BETWEEN DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 60 DAY),
      '%Y%m%d'
    )
    AND DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    )
  GROUP BY DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ),
    b.exploit_type;
  INSERT INTO feods.pj_new_shelf_risk_manage (
    stat_date,
    exploit_type,
    data_type,
    first_week_num,
    second_week_num,
    third_week_num,
    fourth_week_num,
    first_mon_num,
    second_mon_num,
    accum_num
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ) AS "数据日期",
    b.exploit_type AS 开发类型,
    2 AS data_type,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
        c.order_id,
        NULL
      )
    ) / (
      7 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入首周日架均订单量,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
        c.order_id,
        NULL
      )
    ) / (
      7 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入次周日架均订单量,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
        c.order_id,
        NULL
      )
    ) / (
      7 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入第三周日架均订单量,
    COUNT(
      DISTINCT IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
        c.order_id,
        NULL
      )
    ) / (
      10 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入第四周日架均订单量,
    COUNT(
      DISTINCT IF(
        DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
          '%Y%m'
        ),
        c.order_id,
        NULL
      )
    ) / (
      30 * COUNT(
        DISTINCT IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
            '%Y%m'
          ),
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入首月日架均订单量,
    COUNT(
      DISTINCT IF(
        DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
        c.order_id,
        NULL
      )
    ) / (
      CAST(
        (
          DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%d'
          )
        ) AS UNSIGNED
      ) * COUNT(
        DISTINCT IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入次月日架均订单量,
    COUNT(DISTINCT c.order_id) / (60 * COUNT(DISTINCT b.shelf_id)) AS 累计日架均订单量
  FROM
    fe.sf_shelf b
    JOIN fe.sf_order c
      ON b.shelf_id = c.shelf_id
  WHERE DATE_FORMAT(b.activate_time, '%Y%m%d') BETWEEN DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 60 DAY),
      '%Y%m%d'
    )
    AND DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    )
  GROUP BY DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ),
    b.exploit_type;
  INSERT INTO feods.pj_new_shelf_risk_manage (
    stat_date,
    exploit_type,
    data_type,
    first_week_num,
    second_week_num,
    third_week_num,
    fourth_week_num,
    first_mon_num,
    second_mon_num,
    accum_num
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ) AS "数据日期",
    b.exploit_type AS 开发类型,
    3 AS data_type,
    SUM(
      IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
        d.QUANTITY * d.SALE_PRICE,
        NULL
      )
    ) / (
      7 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入首周日架均GMV,
    SUM(
      IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
        d.QUANTITY * d.SALE_PRICE,
        NULL
      )
    ) / (
      7 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入次周日架均GMV,
    SUM(
      IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
        d.QUANTITY * d.SALE_PRICE,
        NULL
      )
    ) / (
      7 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入第三周日架均GMV,
    SUM(
      IF(
        DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
        AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
        d.QUANTITY * d.SALE_PRICE,
        NULL
      )
    ) / (
      10 * COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入第四周日架均GMV,
    SUM(
      IF(
        DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
          '%Y%m'
        ),
        d.QUANTITY * d.SALE_PRICE,
        NULL
      )
    ) / (
      30 * COUNT(
        DISTINCT IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
            '%Y%m'
          ),
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入首月日架均GMV,
    SUM(
      IF(
        DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
        d.QUANTITY * d.SALE_PRICE,
        NULL
      )
    ) / (
      CAST(
        (
          DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%d'
          )
        ) AS UNSIGNED
      ) * COUNT(
        DISTINCT IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
          b.shelf_id,
          NULL
        )
      )
    ) AS 引入次月日架均GMV,
    SUM(d.QUANTITY * d.SALE_PRICE) / (60 * COUNT(DISTINCT b.shelf_id)) AS 累计日架均GMV
  FROM
    fe.sf_shelf b
    JOIN fe.sf_order c
      ON b.shelf_id = c.shelf_id
    JOIN fe.sf_order_item d
      ON c.order_id = d.ORDER_ID
  WHERE DATE_FORMAT(b.activate_time, '%Y%m%d') BETWEEN DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 60 DAY),
      '%Y%m%d'
    )
    AND DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    )
    AND c.ORDER_STATUS = 2
  GROUP BY DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ),
    b.exploit_type;
  INSERT INTO feods.pj_new_shelf_risk_manage (
    stat_date,
    exploit_type,
    data_type,
    first_week_num,
    second_week_num,
    third_week_num,
    fourth_week_num,
    first_mon_num,
    second_mon_num,
    accum_num
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    ) AS "数据日期",
    ord_tab.exploit_type AS 开发类型,
    4 AS data_type,
    gmv_tab.firstnum / ord_tab.firstnum AS 引入首周订单单价,
    gmv_tab.secondnum / ord_tab.secondnum AS 引入次周订单单价,
    gmv_tab.thirdnum / ord_tab.thirdnum AS 引入第三周订单单价,
    gmv_tab.forthnum / ord_tab.forthnum AS 引入第四周订单单价,
    gmv_tab.lastmonnum / ord_tab.lastmonnum AS 引入首月订单单价,
    gmv_tab.thismonnum / ord_tab.thismonnum AS 引入次月订单单价,
    gmv_tab.addnum / ord_tab.addnum AS 累计订单单价
  FROM
    (SELECT
      b.exploit_type,
      COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
          c.order_id,
          NULL
        )
      ) AS firstnum,
      COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
          c.order_id,
          NULL
        )
      ) AS secondnum,
      COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
          c.order_id,
          NULL
        )
      ) AS thirdnum,
      COUNT(
        DISTINCT IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
          c.order_id,
          NULL
        )
      ) AS forthnum,
      COUNT(
        DISTINCT IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
            '%Y%m'
          ),
          c.order_id,
          NULL
        )
      ) AS lastmonnum,
      COUNT(
        DISTINCT IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
          c.order_id,
          NULL
        )
      ) AS thismonnum,
      COUNT(DISTINCT c.order_id) AS addnum
    FROM
      fe.sf_shelf b
      JOIN fe.sf_order c
        ON b.shelf_id = c.shelf_id
    WHERE DATE_FORMAT(b.activate_time, '%Y%m%d') BETWEEN DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 60 DAY),
        '%Y%m%d'
      )
      AND DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        '%Y%m%d'
      )
    GROUP BY b.exploit_type) ord_tab,
    (SELECT
      b.exploit_type,
      SUM(
        IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 7
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 1,
          d.QUANTITY * d.SALE_PRICE,
          NULL
        )
      ) AS firstnum,
      SUM(
        IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 14
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 8,
          d.QUANTITY * d.SALE_PRICE,
          NULL
        )
      ) AS secondnum,
      SUM(
        IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 21
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 15,
          d.QUANTITY * d.SALE_PRICE,
          NULL
        )
      ) AS thirdnum,
      SUM(
        IF(
          DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) <= 31
          AND DATEDIFF(c.ORDER_DATE, b.ACTIVATE_TIME) >= 22,
          d.QUANTITY * d.SALE_PRICE,
          NULL
        )
      ) AS forthnum,
      SUM(
        IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 MONTH),
            '%Y%m'
          ),
          d.QUANTITY * d.SALE_PRICE,
          NULL
        )
      ) AS lastmonnum,
      SUM(
        IF(
          DATE_FORMAT(c.ORDER_DATE, '%Y%m') = DATE_FORMAT(CURDATE(), '%Y%m'),
          d.QUANTITY * d.SALE_PRICE,
          NULL
        )
      ) AS thismonnum,
      SUM(d.QUANTITY * d.SALE_PRICE) AS addnum
    FROM
      fe.sf_shelf b
      JOIN fe.sf_order c
        ON b.shelf_id = c.shelf_id
      JOIN fe.sf_order_item d
        ON c.order_id = d.ORDER_ID
    WHERE DATE_FORMAT(b.activate_time, '%Y%m%d') BETWEEN DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 60 DAY),
        '%Y%m%d'
      )
      AND DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        '%Y%m%d'
      )
      AND c.ORDER_STATUS = 2
    GROUP BY b.exploit_type) gmv_tab
  WHERE ord_tab.exploit_type = gmv_tab.exploit_type;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_new_shelf_risk_manage',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));  
  COMMIT;
END