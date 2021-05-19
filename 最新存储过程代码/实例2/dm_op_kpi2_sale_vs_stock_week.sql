CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_sale_vs_stock_week`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+1),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  
  SET @add_day := ADDDATE(@week_end, 1),
  @week_start := SUBDATE(@week_end, 6),
  @week_d2 := SUBDATE(@week_end, 5),
  @week_d3 := SUBDATE(@week_end, 4),
  @week_d4 := SUBDATE(@week_end, 3),
  @week_d5 := SUBDATE(@week_end, 2),
  @week_d6 := SUBDATE(@week_end, 1);
  SET @y_m1 := DATE_FORMAT(@week_start, '%Y-%m'),
  @y_m2 := DATE_FORMAT(@week_d2, '%Y-%m'),
  @y_m3 := DATE_FORMAT(@week_d3, '%Y-%m'),
  @y_m4 := DATE_FORMAT(@week_d4, '%Y-%m'),
  @y_m5 := DATE_FORMAT(@week_d5, '%Y-%m'),
  @y_m6 := DATE_FORMAT(@week_d6, '%Y-%m'),
  @y_m7 := DATE_FORMAT(@week_end, '%Y-%m');
  SET @d1 := DAY(@week_start),
  @d2 := DAY(@week_d2),
  @d3 := DAY(@week_d3),
  @d4 := DAY(@week_d4),
  @d5 := DAY(@week_d5),
  @d6 := DAY(@week_d6),
  @d7 := DAY(@week_end);
  
  SET @time_1 := CURRENT_TIMESTAMP();
   DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp AS
  SELECT
    b.shelf_id,
    b.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all b
  WHERE b.data_flag = 1;
  
  CREATE INDEX idx_shelf_id
  ON fe_dm.shelf_tmp (shelf_id);
  
  SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_1--@time_2",@time_1,@time_2);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sal_tmp;
  CREATE TEMPORARY TABLE fe_dm.sal_tmp AS
  SELECT
    s.business_name,
    t.product_id,
    COUNT(DISTINCT t.shelf_id) shelfs_sal
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE  t.pay_date >= @week_start
    AND t.pay_date < @add_day
  GROUP BY s.business_name,
    t.product_id;
	
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_3--@time_4",@time_3,@time_4);
-- 上周有库存的货架数
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_stat_tmp;
  CREATE TEMPORARY TABLE fe_dm.sto_stat_tmp AS
SELECT 
t.business_name,
 t.product_id ,
COUNT(DISTINCT t.shelf_id) shelfs_sto
from
fe_dwd.dwd_shelf_product_sto_sal_30_days  t 
WHERE t.sdate >=@week_start
AND t.sdate < @add_day
AND t.stock_quantity >0
GROUP BY t.business_name,
 t.product_id ;
	
	
	
	
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_4--@time_5",@time_4,@time_5);	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi2_sale_vs_stock_week  
  WHERE week_end = @week_end;
  
  INSERT INTO fe_dm.dm_op_kpi2_sale_vs_stock_week (
    week_end,
    business_name,
    product_id,
    shelfs_sal,
    shelfs_sto,
    add_user
  )
  SELECT
    @week_end week_end,
    a.business_name,
    a.product_id,
    SUM(a.shelfs_sal) shelfs_sal,
    SUM(a.shelfs_sto) shelfs_sto,
    @add_user add_user
  FROM
    (SELECT
      t.business_name,
      t.product_id,
      t.shelfs_sal,
      0 shelfs_sto
    FROM
      fe_dm.sal_tmp t
    UNION
    ALL
    SELECT
      t.business_name,
      t.product_id,
      0 shelfs_sal,
      t.shelfs_sto
    FROM
      fe_dm.sto_stat_tmp t) a
  GROUP BY a.business_name,
    a.product_id;
    
    
	
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_5--@time_6",@time_5,@time_6);	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 103;
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @week_end sdate,
    'w' indicate_type,
    103 indicate_id,
    'dm_op_kpi2_sale_vs_stock_week' indicate_name,
    ROUND(
      SUM(t.shelfs_sal) / SUM(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_sale_vs_stock_week t
  WHERE t.week_end = @week_end;
  
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_kpi2_sale_vs_stock_week","@time_6--@time_7",@time_6,@time_7);  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 103;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate,
    business_name,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @week_end sdate,
    t.business_name,
    'w' indicate_type,
    103 indicate_id,
    'dm_op_kpi2_sale_vs_stock_week' indicate_name,
    ROUND(
      SUM(t.shelfs_sal) / SUM(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_sale_vs_stock_week t
  WHERE t.week_end = @week_end
  GROUP BY business_name;
  
  
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_sale_vs_stock_week',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_sale_vs_stock_week','dm_op_kpi2_sale_vs_stock_week','李世龙');
COMMIT;
    END