CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_area_product_stock_rate`(in_sdate DATE)
BEGIN
  SET @sdate := in_sdate, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m'), @d := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    b.business_name, s.shelf_id
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1
    AND ! ISNULL(s.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.stock_tmp;
  SET @str := CONCAT(
    " CREATE TEMPORARY TABLE feods.stock_tmp AS ", " SELECT ", "   s.business_name, ", "   t.shelf_id, ", "   t.product_id, ", "   t.day", @d, "_quantity qty_stock ", " FROM ", "   fe.sf_shelf_product_stock_detail t ", "   JOIN feods.shelf_tmp s ", "     ON t.shelf_id = s.shelf_id ", " WHERE t.stat_date = @y_m ", "   AND t.day", @d, "_quantity > 0; "
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_business_name_product_id
  ON feods.stock_tmp (business_name, product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.stock_shelf_tmp;
  CREATE TEMPORARY TABLE feods.stock_shelf_tmp AS
  SELECT
    t.business_name, COUNT(DISTINCT t.shelf_id) shelfs_area
  FROM
    feods.stock_tmp t
  GROUP BY t.business_name;
  DELETE
  FROM
    feods.fjr_area_product_stock_rate
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_area_product_stock_rate (
    sdate, business_name, product_id, shelfs_stock, shelfs_area, qty_stock, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.product_id, t.shelfs_stock, s.shelfs_area, t.qty_stock, @add_user add_user
  FROM
    (SELECT
      t.business_name, t.product_id, COUNT(*) shelfs_stock, SUM(t.qty_stock) qty_stock
    FROM
      feods.stock_tmp t
    GROUP BY t.business_name, t.product_id) t
    JOIN feods.stock_shelf_tmp s
      ON t.business_name = s.business_name;
  CALL feods.sp_task_log (
    'sp_area_product_stock_rate', @sdate, CONCAT(
      'fjr_d_878452c132d7e368fcacbbf8116dfbb2', @timestamp, @add_user
    )
  );
  COMMIT;
END