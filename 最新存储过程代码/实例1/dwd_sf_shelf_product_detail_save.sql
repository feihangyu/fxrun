CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_sf_shelf_product_detail_save`()
BEGIN
IF CURTIME()<='01:00'  THEN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
#INSERT INTO fe_dwd.dwd_sf_shelf_product_detail_save(
#ITEM_ID,
#PRODUCT_ID,
#SHELF_ID,
#STOCK_QUANTITY,
#ADD_TIME,
#LAST_UPDATE_TIME,
#DATA_FLAG)
#SELECT 
#ITEM_ID,
#PRODUCT_ID,
#SHELF_ID,
#STOCK_QUANTITY,
#ADD_TIME,
#LAST_UPDATE_TIME,
#DATA_FLAG
#FROM fe.sf_shelf_product_detail WHERE last_update_time>CURRENT_DATE;
INSERT INTO fe_dwd.dwd_sf_shelf_product_detail_save(
        SHELF_ID,
        PRODUCT_ID,
        detail_stock,
        dwd_stock
)
SELECT
        a.`SHELF_ID`,
        a.`PRODUCT_ID`,
        a.STOCK_QUANTITY AS detail_stock,
        b.STOCK_QUANTITY AS dwd_stock
FROM
        fe.`sf_shelf_product_detail` a       
        JOIN fe_dwd.`dwd_shelf_product_day_all` b       -- 00:06:23
                ON a.`DETAIL_ID` = b.`DETAIL_ID`
WHERE a.STOCK_QUANTITY != b.STOCK_QUANTITY
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_sf_shelf_product_detail_save',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('唐进@', @user, @timestamp)
  );
  
END IF;
COMMIT;	
END