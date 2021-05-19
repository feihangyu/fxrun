CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_fill_requirement_history`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.zs_fill_requirement_history
  WHERE sdate = CURDATE();
  DELETE
  FROM
    feods.zs_fill_requirement_history
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 7 DAY);
  INSERT INTO feods.zs_fill_requirement_history (
    sdate,
    SHELF_ID,
    PRODUCT_ID,
    SUPPLIER_ID,
    depot_code,
    SUPPLIER_TYPE,
    SUPPLIER_NAME,
    WEEK_SALE_NUM,
    STOCK_NUM,
    ONWAY_NUM,
    SUGGEST_FILL_NUM,
    ACTUAL_APPLY_NUM,
    NEW_FLAG,
    SALES_FLAG,
    cank_stock_qty,
    total_price
  )
  SELECT
    CURDATE() AS sdate,
    a.SHELF_ID,
    a.PRODUCT_ID,
    a.SUPPLIER_ID,
    b.depot_code,
    a.SUPPLIER_TYPE,
    a.SUPPLIER_NAME,
    a.WEEK_SALE_NUM,
    a.STOCK_NUM,
    a.ONWAY_NUM,
    a.SUGGEST_FILL_NUM,
    a.ACTUAL_APPLY_NUM,
    a.NEW_FLAG,
    a.SALES_FLAG,
    CASE
      WHEN a.SUPPLIER_TYPE = 2
      THEN d.fbaseqty
      WHEN a.SUPPLIER_TYPE = 9
      THEN e.available_stock
    END AS cank_stock_qty,
    f.total_price
  FROM
    fe.sf_product_fill_requirement a
    LEFT JOIN fe.sf_supplier b
      ON a.SUPPLIER_ID = b.SUPPLIER_ID
    LEFT JOIN fe.sf_product c
      ON a.PRODUCT_ID = c.PRODUCT_ID
    LEFT JOIN
      (SELECT
        f.BUSINESS_AREA,
        f.BIG_AREA,
        e.fnumber AS depot_code,
        a.FMATERIALID,
        d.fnumber AS product_code2,
        c.fname,
        SUM(a.fbaseqty) AS fbaseqty,
        SUM(a.fbaselockqty) AS fbaselockqty
      FROM
        sserp.T_STK_INVENTORY a
        LEFT JOIN sserp.T_BD_STOCK_L b
          ON a.FSTOCKID = b.FSTOCKID
        LEFT JOIN sserp.T_BD_MATERIAL_L c
          ON a.FMATERIALID = c.FMATERIALID
        LEFT JOIN sserp.T_BD_MATERIAL d
          ON a.FMATERIALID = d.FMATERIALID
        LEFT JOIN sserp.T_BD_STOCK e
          ON a.FSTOCKID = e.FSTOCKID
        LEFT JOIN sserp.ZS_DC_BUSINESS_AREA f
          ON e.fnumber = f.DC_CODE
      GROUP BY f.BUSINESS_AREA,
        f.BIG_AREA,
        e.fnumber,
        a.FMATERIALID,
        d.fnumber,
        c.fname) d
      ON (
        b.depot_code = d.depot_code
        AND c.product_code2 = d.product_code2
        AND a.SUPPLIER_TYPE = 2
      )
    LEFT JOIN fe.sf_prewarehouse_stock_detail e
      ON a.SUPPLIER_ID = e.warehouse_id
      AND a.PRODUCT_ID = e.product_id
    LEFT JOIN
      (SELECT
        shelf_id,
        SUM(
          SUGGEST_FILL_NUM * PURCHASE_PRICE
        ) AS total_price
      FROM
        fe.sf_product_fill_requirement
      GROUP BY shelf_id) f
      ON a.SHELF_ID = f.shelf_id;
      
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_fill_requirement_history',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
  COMMIT;
END