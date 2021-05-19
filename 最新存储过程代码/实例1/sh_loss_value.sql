CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_loss_value`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.pj_loss_value
  WHERE sdate = CURDATE();
  INSERT INTO feods.pj_loss_value (
    sdate,
    cityname,
    TYPE,
    PRODUCT_NAME,
    loss
  )
  SELECT
    CURDATE() AS sdate,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(e.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS cityname,
    d.TYPE_ID AS TYPE,
    d.PRODUCT_NAME,
    SUM(
      (
        (
          CASE
            WHEN (
              TO_DAYS(NOW()) - TO_DAYS(
                DATE_FORMAT(e.ACTIVATE_TIME, '%Y%m%d')
              )
            ) < 30
            THEN (
              TO_DAYS(NOW()) - TO_DAYS(
                DATE_FORMAT(e.ACTIVATE_TIME, '%Y%m%d')
              )
            )
            ELSE 30
          END
        ) - STOCK_DAY_NUM
      ) * (
        CASE
          WHEN STOCK_DAY_NUM = 0
          THEN 0
          ELSE DAY_AVG_SALE_NUM
        END
      ) * a.SALE_PRICE
    ) AS loss
  FROM
    fe.sf_shelf_product_detail a
    LEFT JOIN fe.sf_statistics_pre_fourteen_sale_product b
      ON a.product_id = b.product_id
      AND a.shelf_id = b.shelf_id
    LEFT JOIN fe.sf_shelf_product_detail_flag c
      ON a.product_id = c.product_id
      AND a.shelf_id = c.shelf_id
    LEFT JOIN fe.sf_product d
      ON a.PRODUCT_ID = d.PRODUCT_ID
    LEFT JOIN fe.sf_shelf e
      ON a.shelf_id = e.shelf_id
  WHERE c.SALES_FLAG IN (1, 2, 3)
    AND a.data_flag = 1
    AND a.SHELF_FILL_FLAG = 1
  GROUP BY cityname,
    TYPE,
    d.PRODUCT_NAME
  HAVING loss > 0;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_loss_value',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
  COMMIT;
END