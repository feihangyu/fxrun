CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_qzc_queh_shelf_lv`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.zs_qzc_queh_shelf_lv
  WHERE sdate = CURDATE();
  INSERT INTO feods.zs_qzc_queh_shelf_lv (
    sdate,
    SHELF_CODE,
    SHELF_NAME,
    queh_shelf_qty,
    shelf_qty,
    queh_lv
  )
  SELECT
    CURDATE() AS sdate,
    ta.SHELF_CODE,
    ta.SHELF_NAME,
    COUNT(
      DISTINCT
      CASE
        WHEN 状态 = '严重缺货'
        THEN tx.shelf_id
      END
    ) AS queh_shelf_qty,
    COUNT(DISTINCT tx.shelf_id) AS shelf_qty,
    COUNT(
      DISTINCT
      CASE
        WHEN 状态 = '严重缺货'
        THEN tx.shelf_id
      END
    ) / COUNT(DISTINCT tx.shelf_id) AS queh_lv
  FROM
    (SELECT
      t1.*,
      CASE
        WHEN fill_qty >= 20
        THEN '正常'
        WHEN zaitu_qty >= 20
        THEN '正常'
        WHEN if_band = 2
        AND stock_quantity < 200
        THEN '严重缺货'
        WHEN if_band = 3
        AND shelf_type IN (1, 3)
        AND stock_quantity < 110
        THEN '严重缺货'
        WHEN if_band = 3
        AND shelf_type IN (2, 5)
        AND stock_quantity < 90
        THEN '严重缺货'
        WHEN if_band = 3
        AND shelf_type IN (6)
        AND stock_quantity < 90
        THEN '严重缺货'
        WHEN if_band = 3
        AND shelf_type IN (7)
        AND stock_quantity < 120
        THEN '严重缺货'
        ELSE '正常'
      END AS 状态
    FROM
      (SELECT
        d.warehouse_id,
        a.shelf_id,
        CASE
          WHEN b.SECONDARY_SHELF_ID IS NOT NULL
          THEN 1 #'次货架' 
           WHEN c.MAIN_SHELF_ID IS NOT NULL
          THEN 2 #'主货架'
           ELSE 3 #'不绑定' 
         END AS if_band,
        f.fill_qty,
        g.zaitu_qty,
        e.shelf_status,
        e.shelf_type,
        e.REVOKE_status,
        e.WHETHER_CLOSE,
        SUM(a.stock_quantity) AS stock_quantity
      FROM
        fe.sf_shelf_product_detail a
        LEFT JOIN fe.sf_prewarehouse_shelf_detail d
          ON (
            a.SHELF_ID = d.shelf_id
            AND d.data_flag = 1
          )
        LEFT JOIN
          (SELECT DISTINCT
            SECONDARY_SHELF_ID
          FROM
            fe.sf_shelf_relation_record
          WHERE DATA_FLAG = 1
            AND SHELF_HANDLE_STATUS = 9) b
          ON a.SHELF_ID = b.SECONDARY_SHELF_ID
        LEFT JOIN
          (SELECT DISTINCT
            MAIN_SHELF_ID
          FROM
            fe.sf_shelf_relation_record
          WHERE DATA_FLAG = 1
            AND SHELF_HANDLE_STATUS = 9) c
          ON a.SHELF_ID = c.MAIN_SHELF_ID
        LEFT JOIN fe.sf_shelf e
          ON a.shelf_id = e.shelf_id
        LEFT JOIN
          (SELECT
            b.shelf_id,
            SUM(a.ACTUAL_SIGN_NUM) AS fill_qty
          FROM
            fe.sf_product_fill_order_item a
            LEFT JOIN fe.sf_product_fill_order b
              ON a.order_id = b.order_id
          WHERE FILL_TIME >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            AND order_status IN (3, 4)
          GROUP BY b.shelf_id) f
          ON a.shelf_id = f.shelf_id
        LEFT JOIN
          (SELECT
            b.shelf_id,
            SUM(a.ACTUAL_APPLY_NUM) AS zaitu_qty
          FROM
            fe.sf_product_fill_order_item a
            LEFT JOIN fe.sf_product_fill_order b
              ON a.order_id = b.order_id
          WHERE apply_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            AND order_status IN (2)
          GROUP BY b.shelf_id) g
          ON a.shelf_id = g.shelf_id
      WHERE d.shelf_id IS NOT NULL
      GROUP BY a.shelf_id) t1
    WHERE t1.REVOKE_status = 1
      AND t1.shelf_status = 2
      AND t1.if_band IN (2, 3)
      AND t1.WHETHER_CLOSE = 2
      AND t1.SHELF_TYPE IN (1, 2, 3, 5, 6, 7)) tx
    LEFT JOIN fe.sf_shelf ta
      ON tx.warehouse_id = ta.shelf_id
  GROUP BY ta.SHELF_CODE,
    ta.SHELF_NAME;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_qzc_queh_shelf_lv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));    
  COMMIT;
END