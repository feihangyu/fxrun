CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_op_shelf_type_flag`()
BEGIN
    SET  @sdate := SUBDATE(CURRENT_DATE,1);
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM feods.d_op_shelf_type_flag WHERE sdate = @sdate;	   
INSERT INTO feods.d_op_shelf_type_flag  
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
	  f.danger_flag,
      f.sales_flag,
      SUM(t.stock_quantity) as stock_qty,
      SUM(IFNULL(t.stock_quantity,0)*IFNULL(t.sale_price,0)) as stock_m,      
	  current_timestamp as load_time
FROM fe.sf_shelf_product_detail t            -- 货架商品详情
JOIN fe.sf_shelf_product_detail_flag f   -- 货架商品详情标识信息
    ON( t.shelf_id = f.shelf_id
    AND t.product_id = f.product_id
    AND f.data_flag = 1)
JOIN fe.sf_shelf s
    ON s.shelf_id=t.shelf_id  AND s.data_flag = 1
JOIN feods.fjr_city_business b
    ON s.city=b.city
WHERE t.data_flag = 1 and t.ADD_TIME <CURRENT_DATE    -- and t.ADD_TIME >= @sdate and t.ADD_TIME <CURRENT_DATE
  AND ( t.stock_quantity > 0 OR (t.stock_quantity = 0 AND t.shelf_fill_flag = 1))
  AND s.SHELF_STATUS=2
GROUP BY  b.business_name,
          s.shelf_type,
          f.danger_flag,
          f.sales_flag;
          
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'd_op_shelf_type_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('唐进@', @user, @timestamp));
 
COMMIT;
END