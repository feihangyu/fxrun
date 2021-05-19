CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_type_flag`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET  @sdate := SUBDATE(CURRENT_DATE,1);
DELETE FROM fe_dm.dm_op_shelf_type_flag WHERE sdate = @sdate;	   
INSERT INTO fe_dm.dm_op_shelf_type_flag  
(
 sdate        
,business_name
,shelf_type 
,danger_flag  
,sales_flag     
,stock_qty    
,stock_m      
,load_time    
) 
SELECT 
      @sdate AS sdate,
      b.business_name,
      s.shelf_type,
	  t.danger_flag,
      t.sales_flag,
      SUM(t.stock_quantity) AS stock_qty,
      SUM(IFNULL(t.stock_quantity,0)*IFNULL(t.sale_price,0)) AS stock_m,      
	  CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.dwd_shelf_product_day_all t 
JOIN fe_dwd.dwd_shelf_base_day_all s
    ON s.shelf_id=t.shelf_id  AND s.data_flag = 1
JOIN fe_dwd.dwd_city_business b
    ON s.city=b.city
WHERE ( t.stock_quantity > 0 OR (t.stock_quantity = 0 AND t.shelf_fill_flag = 1))
  AND s.SHELF_STATUS=2
GROUP BY  b.business_name,
          s.shelf_type,
          t.danger_flag,
          t.sales_flag;
          
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_type_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（李吹防）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_type_flag','dm_op_shelf_type_flag','李吹防');
  COMMIT;	
END