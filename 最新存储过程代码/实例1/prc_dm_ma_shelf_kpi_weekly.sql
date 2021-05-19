CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_shelf_kpi_weekly`(IN p_date DATE)
BEGIN
SET @run_date:= CURRENT_DATE(),  @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_date;
SET @weekmonday := ADDDATE(@sdate, -IF(DAYOFWEEK(@sdate)=1,8,DAYOFWEEK(@sdate))+2 );
DELETE FROM feods.dm_ma_shelf_kpi_weekly WHERE sdate=@sdate OR sdate<SUBDATE(@weekmonday,7*10) ;

#插入数据
DROP TEMPORARY TABLE IF EXISTS test.temp_shelf_stcok;
CREATE TEMPORARY TABLE test.temp_shelf_stcok (INDEX(shelf_id)) AS
    SELECT a1.SHELF_ID
         ,SUM(STOCK_QUANTITY) STOCK_QUANTITY,SUM(STOCK_QUANTITY*SALE_PRICE) STOCK_value
         ,SUM(1) SKU,SUM(IF(a1.SALES_FLAG IN (1,2),1,0)) SKU_boom
    FROM fe_dwd.dwd_shelf_product_day_all a1
    WHERE a1.STOCK_QUANTITY>0
    GROUP BY a1.SHELF_ID;

INSERT INTO feods.dm_ma_shelf_kpi_weekly
    (sdate, SHELF_ID, shelf_type, activate_time, revoke_time, city_name,zone_name
    , gmv, gmv_avg, user_num, user_num_reorder, user_num_lw, user_num_reorder_lw
    , stock_quantity, stock_value, sku, sku_boom)
SELECT @sdate sdate,a2.SHELF_ID,a21.ITEM_NAME SHELF_TYPE,a2.ACTIVATE_TIME,a2.REVOKE_TIME,a2.CITY_NAME,a2.zone_name
     ,a1.GMV,ROUND(a1.GMV/a3.work_days,2) gmv_avg
     ,a1.user_num,a1.user_num_reorder,a4.user_num user_num_lw,a4.user_num_reorder user_num_reorder_lw
    ,a5.STOCK_QUANTITY,a5.STOCK_value,a5.SKU,a5.SKU_boom
FROM fe_dwd.dwd_shelf_base_day_all a2
JOIN fe.pub_dictionary_item a21 ON a21.ITEM_VALUE=a2.SHELF_TYPE AND a21.DICTIONARY_ID=8
JOIN (SELECT COUNT(1) work_days FROM feods.fjr_work_days WHERE sdate>= @weekmonday AND sdate<=@sdate) a3
LEFT JOIN feods.d_ma_shelf_sale_weekly a1 ON a1.sweek=@weekmonday AND a1.SHELF_ID=a2.SHELF_ID
LEFT JOIN feods.d_ma_shelf_sale_weekly a4 ON a4.sweek=SUBDATE(a1.sweek,7) AND a4.SHELF_ID=a2.SHELF_ID
LEFT JOIN test.temp_shelf_stcok a5 ON a5.SHELF_ID=a1.shelf_id
WHERE a2.SHELF_TYPE BETWEEN 1 AND 8 AND a2.SHELF_STATUS IN (2,5)
;
#记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_shelf_kpi_weekly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));

END