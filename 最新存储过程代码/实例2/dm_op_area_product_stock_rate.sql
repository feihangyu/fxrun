CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_stock_rate`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := subdate(current_date,interval 1 day), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m'), @d := DAY(@sdate);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    b.business_name,b.shelf_id
  FROM
   fe_dwd.dwd_shelf_base_day_all b;
	
	
 -- DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_tmp;
 -- SET @str := CONCAT(
 --   " CREATE TEMPORARY TABLE fe_dm.stock_tmp AS ", " SELECT ", "   s.business_name, ", "   t.shelf_id, ", "   t.product_id, ", "   t.day", @d, "_quantity qty_stock ", 
--	" FROM ", "   fe.sf_shelf_product_stock_detail t ", "   JOIN fe_dm.shelf_tmp s ", "    
--	ON t.shelf_id = s.shelf_id ", " WHERE t.stat_date = @y_m ", "   AND t.day", @d, "_quantity > 0; "
 -- );
 -- PREPARE str_exe FROM @str;
 -- EXECUTE str_exe;
  
  -- 如果报错，不能次日重跑。否则数据有问题
   DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_tmp; 
  CREATE TEMPORARY TABLE fe_dm.stock_tmp AS
SELECT
  s.business_name,
  t.shelf_id,
  t.product_id,
  t.stock_quantity qty_stock
FROM
  fe_dwd.`dwd_shelf_product_day_all` t
  JOIN fe_dm.shelf_tmp s
    ON t.shelf_id = s.shelf_id
WHERE  t.stock_quantity > 0;
  
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.stock_tmp (business_name, product_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.stock_shelf_tmp AS
  SELECT
    t.business_name, COUNT(DISTINCT t.shelf_id) shelfs_area
  FROM
    fe_dm.stock_tmp t
  GROUP BY t.business_name;
  
  
  DELETE
  FROM
    fe_dm.dm_op_area_product_stock_rate  
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_area_product_stock_rate (
    sdate, business_name, product_id, shelfs_stock, shelfs_area, qty_stock, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.product_id, t.shelfs_stock, s.shelfs_area, t.qty_stock, @add_user add_user
  FROM
    (SELECT
      t.business_name, t.product_id, COUNT(*) shelfs_stock, SUM(t.qty_stock) qty_stock
    FROM
      fe_dm.stock_tmp t
    GROUP BY t.business_name, t.product_id) t
    JOIN fe_dm.stock_shelf_tmp s
      ON t.business_name = s.business_name;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_stock_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_stock_rate','dm_op_area_product_stock_rate','李世龙');
COMMIT;
    END