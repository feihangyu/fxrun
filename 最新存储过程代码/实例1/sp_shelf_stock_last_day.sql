CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_stock_last_day`()
BEGIN
   
   SET @dt = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
   SET @colname = CONCAT("DAY",CAST(DATE_FORMAT(@dt,'%d') AS UNSIGNED),"_QUANTITY" );
   set @stat_date = DATE_FORMAT(@dt,'%Y-%m');
   SET @run_date:= CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
--    TRUNCATE TABLE feods.zs_shelf_stock_last_day;
   
--    set @select_result = concat('INSERT INTO feods.zs_shelf_stock_last_day (stat_date,shelf_id,product_id,last_day_stock)
--    select stat_date,',@colname,',shelf_id,product_id from fe.sf_shelf_product_stock_detail where stat_date = "',@stat_date,'"');
   
   SET @select_result = CONCAT('select stat_date,',@colname,',shelf_id,product_id from fe.sf_shelf_product_stock_detail where stat_date = "',@stat_date,'" limit 10');
   prepare pr1 from @select_result;
   execute pr1;
   deallocate prepare pr1; 
    
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'sp_shelf_stock_last_day',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('未知@',@user,@timestamp)
);
COMMIT;
    END