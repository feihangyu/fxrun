CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_boss_data_vending_machine`(in_sdate DATE)
    SQL SECURITY INVOKER
BEGIN
  #不可跑历史数据，否则会因为货架状态的变化而导致数据不准
  #自动售卖机结果
  SET @sdate := in_sdate,
  @run_date := CURRENT_DATE(),
  @user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    feods.d_mp_day_sale_vending_machine
  WHERE sdate = @sdate;
  INSERT INTO feods.d_mp_day_sale_vending_machine(
    sdate,
    gmv,
    buy_qty,
    amount,
    user_qty,
    order_qty,
    discount,
    activate_shelf_qty,
    revoke_shelf_qty,
    shelfs_status2,
--     gmv_jg,
--     shelfs_jg,
    add_user
  )
  SELECT
    @sdate sdate,
    SUM(o.gmv) gmv,
    0 buy_qty,
    0 amount,
    COUNT(DISTINCT o.user_id) users,
    COUNT(DISTINCT o.order_id) orders,
    SUM(o.gmv - o.product_total_amount) discount,
    (SELECT
      COUNT(*)
    FROM
      fe.sf_shelf s
    WHERE s.data_flag = 1
      AND s.shelf_type = 7 #自动售卖机
      AND s.activate_time >= @sdate
      AND s.activate_time < ADDDATE(@sdate, 1)) newshelfs,
    (SELECT
      COUNT(*)
    FROM
      fe.sf_shelf s
    WHERE s.data_flag = 1
      AND s.shelf_type = 7 #自动售卖机
      AND s.revoke_time >= @sdate
      AND s.revoke_time < ADDDATE(@sdate, 1)) revokeshelfs,
    (SELECT
      COUNT(*)
    FROM
      fe.sf_shelf s
    WHERE s.data_flag = 1
      AND s.shelf_type = 7 #自动售卖机
      AND s.shelf_status = 2) shelfs_status2,
--     (SELECT
--       SUM(oi.quantity * oi.sale_price)
--     FROM
--       fe.sf_order t
--       JOIN fe.sf_order_item oi
--         ON t.order_id = oi.order_id
--       JOIN fe.sf_shelf s
--         ON t.shelf_id = s.shelf_id
--         AND s.data_flag = 1
--         AND s.shelf_type IN (1, 2, 3, 5)
--         AND s.shelf_status IN (2, 4, 5)
--     WHERE t.order_status = 2
--       AND t.order_date >= @sdate
--       AND t.order_date < ADDDATE(@sdate, 1)) gmv_jg,
--     (SELECT
--       COUNT(*)
--     FROM
--       fe.sf_shelf s
--     WHERE s.data_flag = 1
--       AND s.shelf_type IN (1, 2, 3, 5)
--       AND s.shelf_status IN (2, 4, 5)) shelfs_jg,
    @user
  FROM
    (SELECT
      o.order_id,
      o.user_id,
      o.product_total_amount,
      SUM(CASE WHEN o.ORDER_STATUS = 2 THEN oi.quantity*oi.SALE_PRICE ELSE oi.quantity_shipped*oi.SALE_PRICE END) AS gmv,
      SUM(CASE WHEN o.ORDER_STATUS = 2 THEN oi.quantity ELSE oi.quantity_shipped END) AS quantity  #新口径
           --  SUM(oi.quantity) quantity,
     --       SUM(oi.quantity * oi.sale_price) gmv
    FROM
      fe.`sf_shelf` s
      JOIN fe.sf_order o
      ON s.shelf_id = o.shelf_id
      AND s.data_flag =1
      AND s.shelf_type = 7 # 自动售卖机
      JOIN fe.sf_order_item oi
      ON o.order_id = oi.order_id
      AND o.order_status IN (2,6,7)
      AND o.order_date >= @sdate
      AND o.order_date < ADDDATE(@sdate, 1)
    GROUP BY o.order_id) o;
  DELETE
  FROM
    feods.d_mp_user_vending_machine;
  INSERT INTO feods.d_mp_user_vending_machine (
    curweek,
    lastweek,
    lastweek_all,
    curmonth,
    lastmonth,
    curyear,
    lastyear,
    add_user
  )
  SELECT
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
      fe.`sf_shelf` s
    JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7) 
      AND o.order_date >= SUBDATE(@sdate, WEEKDAY(@sdate))
      AND o.order_date < ADDDATE(@sdate, 1)) curweek,
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
     fe.`sf_shelf` s
     JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7)
      AND o.order_date >= SUBDATE(@sdate, WEEKDAY(@sdate) + 7)
      AND o.order_date < SUBDATE(@sdate, 6)) lastweek,
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
     fe.`sf_shelf` s
    JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7)
      AND o.order_date >= SUBDATE(@sdate, WEEKDAY(@sdate) + 7)
      AND o.order_date < SUBDATE(@sdate, WEEKDAY(@sdate))) lastweek_all,
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
     fe.`sf_shelf` s
    JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7)
      AND o.order_date >= CONCAT(
        DATE_FORMAT(@sdate, '%Y-%m'),
        '-01'
      )
      AND o.order_date < ADDDATE(@sdate, 1)) curmonth,
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
    fe.`sf_shelf` s
    JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7)
      AND o.order_date >= CONCAT(
        DATE_FORMAT(
          SUBDATE(@sdate, INTERVAL 1 MONTH),
          '%Y-%m'
        ),
        '-01'
      )
      AND o.order_date < SUBDATE(ADDDATE(@sdate, 1), INTERVAL 1 MONTH)) lastmonth,
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
     fe.`sf_shelf` s
     JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7)
      AND o.order_date >= CONCAT(YEAR(@sdate), '-01-01')
      AND o.order_date < ADDDATE(@sdate, 1)) curyear,
    (SELECT
      COUNT(DISTINCT o.user_id) users
    FROM
     fe.`sf_shelf` s
     JOIN fe.sf_order o
    ON s.shelf_id = o.shelf_id
    AND s.data_flag = 1
    AND s.shelf_type = 7 # 自动售卖机
    WHERE o.order_status IN (2,6, 7)
      AND o.order_date >= CONCAT(YEAR(@sdate) - 1, '-01-01')
      AND o.order_date < SUBDATE(ADDDATE(@sdate, 1), INTERVAL 1 YEAR)) lastyear,
    @user;
  DELETE
  FROM
    feods.d_mp_boss_data_vending_machine
  WHERE report_day = @sdate;
  INSERT INTO feods.d_mp_boss_data_vending_machine(
    report_day,
    row_num,
    stime,
    sdate,
    gmv,
    activate_shelf_qty,
    revoke_shelf_qty,
    shelfs_status2,
    shelf_qty,
    user_qty,
    order_qty,
    discount,
 --    gmv_jg,
--     shelfs_jg,
    add_user
  )
  SELECT
    t.report_day,
    t.row_num,
    t.stime,
    t.sdate,
    t.gmv,
    t.activate_shelf_qty,
    t.revoke_shelf_qty,
    t.shelfs_status2,
    t.shelf_qty,
    t.user_qty,
    t.order_qty,
    t.discount,
   --  t.gmv_jg,
--     t.shelfs_jg,
    @user
  FROM
    (SELECT
      @sdate report_day,
      WEEKDAY(a.sdate) + 1 row_num,
      CASE
        WEEKDAY(a.sdate)
        WHEN 0
        THEN '上周一'
        WHEN 1
        THEN '上周二'
        WHEN 2
        THEN '上周三'
        WHEN 3
        THEN '上周四'
        WHEN 4
        THEN '上周五'
        WHEN 5
        THEN '上周六'
        WHEN 6
        THEN '上周日'
      END AS stime,
      a.sdate,
      a.gmv,
      a.activate_shelf_qty,
      a.revoke_shelf_qty,
      a.shelfs_status2,
      a.activate_shelf_qty - revoke_shelf_qty AS shelf_qty,
      a.user_qty,
      a.order_qty,
      a.discount
     --  ,
--       a.gmv_jg,
--       a.shelfs_jg
    FROM
      feods.d_mp_day_sale_vending_machine a
    WHERE a.sdate >= SUBDATE(@sdate, WEEKDAY(@sdate) + 7)
      AND a.sdate < SUBDATE(@sdate, WEEKDAY(@sdate))
      UNION
      ALL
      SELECT
        @sdate,
        8 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.lastweek_all AS user_qty,
        order_qty,
        discount
--         ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '上周累计' AS stime,
          MAX(a.sdate) sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
     --      ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate >= SUBDATE(@sdate, WEEKDAY(@sdate) + 7)
          AND a.sdate < SUBDATE(@sdate, WEEKDAY(@sdate))
        GROUP BY '上周累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1
      UNION
      ALL
      SELECT
        @sdate,
        WEEKDAY(t1.sdate) + 9 row_num,
        t1.stime,
        t1.sdate,
        t2.gmv,
        t2.activate_shelf_qty,
        t2.revoke_shelf_qty,
        t2.shelfs_status2,
        t2.shelf_qty,
        t2.user_qty,
        t2.order_qty,
        t2.discount
  --       ,
--         t2.gmv_jg,
--         t2.shelfs_jg
      FROM
        (SELECT
          CASE
            WEEKDAY(a.sdate)
            WHEN 0
            THEN '周一'
            WHEN 1
            THEN '周二'
            WHEN 2
            THEN '周三'
            WHEN 3
            THEN '周四'
            WHEN 4
            THEN '周五'
            WHEN 5
            THEN '周六'
            WHEN 6
            THEN '周日'
          END AS stime,
          ADDDATE(a.sdate, 7) sdate
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate >= SUBDATE(@sdate, WEEKDAY(@sdate) + 7)
          AND a.sdate < SUBDATE(@sdate, WEEKDAY(@sdate))) t1
        LEFT JOIN
          (SELECT
            CASE
              WEEKDAY(a.sdate)
              WHEN 0
              THEN '周一'
              WHEN 1
              THEN '周二'
              WHEN 2
              THEN '周三'
              WHEN 3
              THEN '周四'
              WHEN 4
              THEN '周五'
              WHEN 5
              THEN '周六'
              WHEN 6
              THEN '周日'
            END AS stime,
            a.gmv,
            a.activate_shelf_qty,
            a.revoke_shelf_qty,
            a.shelfs_status2,
            a.activate_shelf_qty - revoke_shelf_qty AS shelf_qty,
            a.user_qty,
            a.order_qty,
            a.discount
         --    ,
--             a.gmv_jg,
--             a.shelfs_jg
          FROM
            feods.d_mp_day_sale_vending_machine a
          WHERE a.sdate >= SUBDATE(@sdate, WEEKDAY(@sdate))
            AND a.sdate < ADDDATE(@sdate, 1)) t2
          ON t1.stime = t2.stime
      UNION
      ALL
      SELECT
        @sdate,
        16 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.curweek AS user_qty,
        order_qty,
        discount
      --   ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '本周累计' AS stime,
          @sdate sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
--           ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate >= SUBDATE(@sdate, WEEKDAY(@sdate))
          AND a.sdate < ADDDATE(@sdate, 1)
        GROUP BY '本周累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1
      UNION
      ALL
      SELECT
        @sdate,
        17 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.lastweek AS user_qty,
        order_qty,
        discount
--         ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '上周同期累计' AS stime,
          SUBDATE(@sdate, 7) sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
  --         ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate >= SUBDATE(@sdate, WEEKDAY(@sdate) + 7)
          AND a.sdate < SUBDATE(@sdate, 6)
        GROUP BY '上周同期累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1
      UNION
      ALL
      SELECT
        @sdate,
        18 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.curmonth AS user_qty,
        order_qty,
        discount
--         ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '本月累计' AS stime,
          @sdate sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
--           ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate < ADDDATE(@sdate, 1)
          AND a.sdate >= CONCAT(
            DATE_FORMAT(@sdate, '%Y-%m'),
            '-01'
          )
        GROUP BY '本月累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1
      UNION
      ALL
      SELECT
        @sdate,
        19 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.lastmonth AS user_qty,
        order_qty,
        discount
--         ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '上月同期累计' AS stime,
          SUBDATE(@sdate, INTERVAL 1 MONTH) sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
--           ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate < SUBDATE(ADDDATE(@sdate, 1), INTERVAL 1 MONTH)
          AND a.sdate >= CONCAT(
            DATE_FORMAT(
              SUBDATE(@sdate, INTERVAL 1 MONTH),
              '%Y-%m'
            ),
            '-01'
          )
        GROUP BY '上月同期累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1
      UNION
      ALL
      SELECT
        @sdate,
        20 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.curyear AS user_qty,
        order_qty,
        discount
--         ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '本年累计' AS stime,
          @sdate sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
--           ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate < ADDDATE(@sdate, 1)
          AND a.sdate >= CONCAT(YEAR(@sdate), '-01-01')
        GROUP BY '本年累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1
      UNION
      ALL
      SELECT
        @sdate,
        21 row_num,
        stime,
        sdate,
        gmv,
        activate_shelf_qty,
        revoke_shelf_qty,
        shelfs_status2,
        shelf_qty,
        t2.lastyear AS user_qty,
        order_qty,
        discount
--         ,
--         gmv_jg,
--         shelfs_jg
      FROM
        (SELECT
          '去年累计' AS stime,
          SUBDATE(@sdate, INTERVAL 1 YEAR) sdate,
          SUM(a.gmv) AS gmv,
          SUM(a.activate_shelf_qty) AS activate_shelf_qty,
          SUM(a.revoke_shelf_qty) AS revoke_shelf_qty,
          SUM(a.shelfs_status2) AS shelfs_status2,
          SUM(
            a.activate_shelf_qty - revoke_shelf_qty
          ) AS shelf_qty,
          SUM(a.user_qty) AS user_qty,
          SUM(a.order_qty) AS order_qty,
          SUM(a.discount) AS discount
--           ,
--           SUM(a.gmv_jg) gmv_jg,
--           SUM(a.shelfs_jg) shelfs_jg
        FROM
          feods.d_mp_day_sale_vending_machine a
        WHERE a.sdate < SUBDATE(ADDDATE(@sdate, 1), INTERVAL 1 YEAR)
          AND a.sdate >= CONCAT(YEAR(@sdate) - 1, '-01-01')
        GROUP BY '去年累计') t1
        LEFT JOIN feods.d_mp_user_vending_machine t2
          ON 1) t;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_mp_boss_data_vending_machine',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));         
  COMMIT;
END