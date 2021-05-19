CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_shelf_machine_slot`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.zs_shelf_machine_slot_history
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 7 DAY);
  DELETE
  FROM
    feods.zs_shelf_machine_slot_history
  WHERE sdate = CURDATE();
  INSERT INTO feods.zs_shelf_machine_slot_history (
    sdate,
    shelf_id,
    product_id,
    manufacturer_slot_code,
    slot_status,
    stock_num,
    slot_capacity_limit
  )
  SELECT
    CURDATE() AS sdate,
    shelf_id,
    product_id,
    manufacturer_slot_code,
    slot_status,
    stock_num,
    slot_capacity_limit
  FROM
    fe.sf_shelf_machine_slot;
  DELETE
  FROM
    feods.zs_shelf_machine_sale_total
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
  INSERT INTO feods.zs_shelf_machine_sale_total (
    sdate,
    total_shelf_qty,
    sale_yday_shelf_qty,
    total_gmv,
    total_user_qty,
    total_order_qty,
    yday_gmv,
    yday_user_qty,
    yday_order_qty,
    w_tb_gmv,
    w_tb_user_qty,
    w_tb_order_qty,
    this_month_gmv,
    this_month_user_qty,
    this_month_order_qty,
    this_week_gmv,
    this_week_user_qty,
    this_week_order_qty
  )
  SELECT
    DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS sdate,
    COUNT(DISTINCT b.shelf_id) AS total_shelf_qty,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        AND a.quantity_shipped > 0
        THEN b.shelf_id
      END
    ) AS sale_yday_shelf_qty,
    SUM(
      a.quantity_shipped * a.sale_price
    ) AS total_gmv,
    COUNT(DISTINCT b.user_id) AS total_user_qty,
    COUNT(DISTINCT a.order_id) AS total_order_qty,
    SUM(
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        THEN a.quantity_shipped * a.sale_price
      END
    ) AS yday_gmv,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        THEN b.user_id
      END
    ) AS yday_user_qty,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        THEN a.order_id
      END
    ) AS yday_order_qty,
    SUM(
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 8 DAY)
        AND order_date < DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        THEN a.quantity_shipped * a.sale_price
      END
    ) AS w_tb_gmv,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 8 DAY)
        AND order_date < DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        THEN b.user_id
      END
    ) AS w_tb_user_qty,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(CURDATE(), INTERVAL 8 DAY)
        AND order_date < DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        THEN a.order_id
      END
    ) AS w_tb_order_qty,
    SUM(
      CASE
        WHEN order_date >= DATE_SUB(
          CURDATE(),
          INTERVAL DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) DAY
        )
        THEN a.quantity_shipped * a.sale_price
      END
    ) AS this_month_gmv,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(
          CURDATE(),
          INTERVAL DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) DAY
        )
        THEN b.user_id
      END
    ) AS this_month_user_qty,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(
          CURDATE(),
          INTERVAL DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) DAY
        )
        THEN a.order_id
      END
    ) AS this_month_order_qty,
    SUM(
      CASE
        WHEN order_date >= DATE_SUB(
          CURDATE(),
          INTERVAL (
            CASE
              WHEN DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              ) = 0
              THEN 7
              ELSE DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              )
            END
          ) DAY
        )
        THEN a.quantity_shipped * a.sale_price
      END
    ) AS this_week_gmv,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(
          CURDATE(),
          INTERVAL (
            CASE
              WHEN DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              ) = 0
              THEN 7
              ELSE DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              )
            END
          ) DAY
        )
        THEN b.user_id
      END
    ) AS this_week_user_qty,
    COUNT(
      DISTINCT
      CASE
        WHEN order_date >= DATE_SUB(
          CURDATE(),
          INTERVAL (
            CASE
              WHEN DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              ) = 0
              THEN 7
              ELSE DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%w'
              )
            END
          ) DAY
        )
        THEN b.order_id
      END
    ) AS this_week_order_qty
  FROM
    fe.sf_order_item AS a
    LEFT JOIN fe.sf_order AS b
      ON a.ORDER_ID = b.ORDER_ID
    LEFT JOIN fe.sf_shelf AS c
      ON b.SHELF_ID = c.SHELF_ID
  WHERE b.ORDER_STATUS IN (6, 7)
    AND c.shelf_type IN (7)
    AND order_date < CURDATE();
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_shelf_machine_slot',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
  COMMIT;
END