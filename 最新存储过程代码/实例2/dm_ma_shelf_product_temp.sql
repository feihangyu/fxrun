CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_product_temp`()
BEGIN
-- =============================================
-- Author:	市场  业务方(罗辉)
-- Create date: 2020-4-17
-- Modify date:
-- Description:
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate=CURRENT_DATE;
#删除数据
TRUNCATE fe_dm.dm_ma_shelf_product_temp;
#临时数据
#插入数据
INSERT INTO fe_dm.dm_ma_shelf_product_temp
    (SHELF_ID, PRODUCT_ID, city_name, business_name, if_valid, if_sale, if_out, STOCK_QUANTITY, SALES_FLAG)
SELECT a1.SHELF_ID,a1.PRODUCT_ID
    ,a3.CITY_NAME city_name,a3.business_name
    ,IF(a5.pid IS NOT NULL,1,0) if_valid
    ,IF(a2.shelf_id IS NOT NULL ,1,0) if_sale
    ,IF(a7.ext8=5,1,0) if_out
    ,a1.STOCK_QUANTITY
    ,a1.SALES_FLAG
FROM fe_dwd.dwd_shelf_product_day_all a1
JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a1.SHELF_ID AND IFNULL(a3.REVOKE_TIME,ADDDATE(CURDATE(),2))>=SUBDATE(CURDATE(),30)
JOIN fe_dm.dm_shelf_product_flag a7 ON a7.shelf_id=a1.SHELF_ID AND a7.product_id=a1.PRODUCT_ID
LEFT JOIN fe_dwd.dwd_shelf_product_sto_sal_30_days a2 ON a2.sdate=SUBDATE(@sdate,1)   AND a1.shelf_id=a2.SHELF_ID AND a1.PRODUCT_ID=a2.PRODUCT_ID AND a2.sal_qty>0  #近三十天有销量货架商品
LEFT JOIN fe_dm.dm_shelf_flag a5 ON a1.SHELF_ID=a5.shelf_id AND (a5.ext4 IN (1,2) OR a5.ext7=1) #终端生命周期维度
;
#记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_product_temp',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_product_temp','dm_ma_shelf_product_temp','纪伟铨');
 
END