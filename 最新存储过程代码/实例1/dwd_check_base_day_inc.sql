CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_check_base_day_inc`()
BEGIN 
   SET @end_date = CURDATE();
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
	
   DELETE FROM fe_dwd.dwd_check_base_day_inc WHERE OPERATE_TIME >= @start_date;
   insert INTO  fe_dwd.dwd_check_base_day_inc 
   (
        DETAIL_ID              
    ,CHECK_ID               
    ,SHELF_ID               
    ,SHELF_CODE             
    ,PRODUCT_ID             
    ,STOCK_NUM              
    ,CHECK_NUM              
    ,total_error_num        
    ,ERROR_NUM              
    ,SALE_PRICE             
    ,ERROR_REASON           
    ,ERROR_PHOTO            
    ,production_date        
    ,production_date_photo  
    ,AUDIT_ERROR_NUM        
    ,AUDIT_STATUS           
    ,AUDIT_USER_ID          
    ,AUDIT_USER_NAME        
    ,AUDIT_TIME             
    ,AUDIT_TYPE             
    ,ATTRIBUTE1             
    ,ATTRIBUTE2             
    ,AUDIT_REMARK           
    ,REMARK                 
    ,ADD_TIME               
    ,ADD_USER_ID            
    ,LAST_UPDATE_USER_ID    
    ,LAST_UPDATE_TIME       
    ,DATA_FLAG              
    ,danger_flag            
    ,risk_source            
    ,auto_check_flag        
    ,date_empty_flag  
    ,OPERATE_TIME           
    ,OPERATOR_ID            
    ,OPERATOR_NAME          
    ,check_type             
    ,CHECK_STATUS           
    ,SHELF_PHOTO            
    ,PHOTO_AUDIT_STATUS     
    ,PHOTO_NOPASS_REASON    
    ,PHOTO_AUDIT_USER_ID    
    ,PHOTO_AUDIT_TIME  
    ,load_time
    )
    SELECT 
     a.DETAIL_ID              
    ,a.CHECK_ID               
    ,a.SHELF_ID               
    ,a.SHELF_CODE             
    ,a.PRODUCT_ID             
    ,a.STOCK_NUM              
    ,a.CHECK_NUM              
    ,a.total_error_num        
    ,a.ERROR_NUM              
    ,a.SALE_PRICE             
    ,a.ERROR_REASON           
    ,a.ERROR_PHOTO            
    ,a.production_date        
    ,a.production_date_photo  
    ,a.AUDIT_ERROR_NUM        
    ,a.AUDIT_STATUS           
    ,a.AUDIT_USER_ID          
    ,a.AUDIT_USER_NAME        
    ,a.AUDIT_TIME             
    ,a.AUDIT_TYPE             
    ,a.ATTRIBUTE1             
    ,a.ATTRIBUTE2             
    ,a.AUDIT_REMARK           
    ,a.REMARK                 
    ,a.ADD_TIME               
    ,a.ADD_USER_ID            
    ,a.LAST_UPDATE_USER_ID    
    ,a.LAST_UPDATE_TIME       
    ,a.DATA_FLAG              
    ,a.danger_flag            
    ,a.risk_source            
    ,a.auto_check_flag        
    ,a.date_empty_flag  
    ,b.OPERATE_TIME           
    ,b.OPERATOR_ID            
    ,b.OPERATOR_NAME          
    ,b.check_type             
    ,b.CHECK_STATUS           
    ,b.SHELF_PHOTO            
    ,b.PHOTO_AUDIT_STATUS     
    ,b.PHOTO_NOPASS_REASON    
    ,b.PHOTO_AUDIT_USER_ID    
    ,b.PHOTO_AUDIT_TIME  
    ,CURRENT_TIMESTAMP AS load_time
    FROM fe.sf_shelf_check_detail a 
    LEFT JOIN fe.sf_shelf_check b    -- 不发散 
      ON  a.check_id = b.check_id 
	  AND a.data_flag=1 
	  AND b.data_flag=1
    WHERE b.OPERATE_TIME >= @start_date  
	AND b.OPERATE_TIME <@end_date;
    
	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_check_base_day_inc',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
  COMMIT;
END