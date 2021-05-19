CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_sto_val_rate`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @d := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, t.business_name, t.region_name
  FROM
    fe_dwd.dwd_shelf_base_day_all t
  WHERE  t.shelf_status = 2
    AND t.shelf_type IN (1, 2, 3, 4, 5);
	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi_sto_val_rate  
  WHERE sdate = @sdate;
  
  
  INSERT INTO fe_dm.dm_op_kpi_sto_val_rate (
  sdate,
  region,
  business_area,
  product_id,
  sto_val,
  sto_val_flag5,
  sto_val_out,
  add_user
) 
SELECT 
  @sdate sdate,
  s.region_name region,
  s.business_name business_area,
  t.product_id,
  SUM(t.sale_price * t.stock_quantity) sto_val,
  SUM(
    IF(
      t.sales_flag = 5 && (ISNULL(t.new_flag) || t.new_flag = 2),
      t.sale_price * t.stock_quantity,
      0
    )
  ) sto_val_flag5,
  SUM(
    IF(
      ISNULL(pd.product_id),
      0,
      t.sale_price * t.stock_quantity
    )
  ) sto_val_out,
  @add_user add_user 
FROM
  fe_dwd.dwd_shelf_product_day_all t 
  JOIN fe_dm.shelf_tmp s 
    ON t.shelf_id = s.shelf_id 
  LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp pd 
    ON t.product_id = pd.product_id 
    AND pd.business_area = s.business_name 
    AND pd.product_type = '淘汰（替补）' 
GROUP BY s.business_name,
  t.product_id 
HAVING sto_val != 0 ;
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi_sto_val_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_sto_val_rate','dm_op_kpi_sto_val_rate','李世龙');
COMMIT;
    END