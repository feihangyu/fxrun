CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_vending_machine_history`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.pj_vending_machine_history
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
  INSERT INTO feods.pj_vending_machine_history (
    sdate,
    shelf_id,
    shelf_code,
    ACTIVATE_TIME,
    gmv
  )
  SELECT
    DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS sdate,
    a.shelf_id,
    a.SHELF_CODE,
    a.ACTIVATE_TIME,
    b.gmv
  FROM
    fe.sf_shelf a
    LEFT JOIN
      (SELECT
        b.shelf_id,
        SUM(a.quantity * a.sale_price) AS gmv
      FROM
        fe.sf_order_item AS a
        LEFT JOIN fe.sf_order AS b
          ON a.ORDER_ID = b.ORDER_ID
        LEFT JOIN fe.sf_shelf AS c
          ON b.SHELF_ID = c.SHELF_ID
      WHERE b.ORDER_STATUS IN (6, 7)
        AND c.shelf_type IN (7)
        AND order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        AND order_date < CURDATE()
      GROUP BY b.shelf_id) b
      ON a.shelf_id = b.shelf_id
  WHERE a.shelf_type IN (7)
    AND a.shelf_status = 2
    AND a.DATA_FLAG = 1
    AND a.ACTIVATE_TIME < CURDATE();
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_vending_machine_history',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
  COMMIT;
END