CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_product_temp`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();

TRUNCATE feods.d_ma_shelf_product_temp;
SET @time_2 := CURRENT_TIMESTAMP();
#插入数据
INSERT INTO feods.d_ma_shelf_product_temp
    (SHELF_ID, PRODUCT_ID, city_name, if_valid, if_sale, STOCK_QUANTITY, SALES_FLAG,if_out)
SELECT a1.SHELF_ID,a1.PRODUCT_ID
    ,a3.CITY_NAME city_name
    ,IF(a5.pid IS NOT NULL,1,0) if_valid
    ,IF(a2.shelf_id IS NOT NULL ,1,0) if_sale
    ,a1.STOCK_QUANTITY
    ,a1.SALES_FLAG
    ,if(a7.ext8=5,1,0) if_out
FROM fe_dwd.dwd_shelf_product_day_all a1
JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a1.SHELF_ID AND IFNULL(a3.REVOKE_TIME,ADDDATE(CURDATE(),2))>=SUBDATE(CURDATE(),30)
LEFT JOIN feods.d_op_sp_avgsal30 a2 ON a1.shelf_id=a2.SHELF_ID AND a1.PRODUCT_ID=a2.PRODUCT_ID AND a2.qty_sal30>0  #近三十天有销量货架商品
LEFT JOIN feods.zs_shelf_flag a5 ON a1.SHELF_ID=a5.shelf_id AND (a5.ext4 IN (1,2) OR a5.ext7=1) #终端生命周期维度
LEFT JOIN feods.zs_shelf_product_flag a7 ON a7.shelf_id=a1.SHELF_ID AND a7.product_id=a1.PRODUCT_ID
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_d_ma_shelf_product_temp","@time_2--@time_4",@time_2,@time_4);
#执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_shelf_product_temp',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('纪伟铨@', @user, @timestamp));
END