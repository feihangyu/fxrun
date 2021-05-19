CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_unsale_io_ratio`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := CURDATE();
SET @y_date := SUBDATE(@stat_date,1);
SET @pre_day2 := SUBDATE(@stat_date,2);
-- 昨天的gmv,昨天凌晨更新的销售标识5  7s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`gmv_5_tmp`;  
CREATE TEMPORARY TABLE fe_dwd.gmv_5_tmp (                          
        KEY idx_business_name_product_id(business_name,product_id) 
) AS                                                              
SELECT                                                           
       b.business_name,                                         
       a.product_id,                                             
       SUM(a.gmv) AS gmv,                               
       SUM(IF(a.sales_flag = 5,a.gmv,0)) AS gmv_5    
FROM
        fe_dwd.`dwd_shelf_product_day_all_recent_32` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.`shelf_id` = b.`shelf_id`
                AND a.sdate = @y_date
WHERE gmv > 0
GROUP BY b.business_name,a.product_id
;
-- 前天的库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_day2_gmv_5_tmp`;  
CREATE TEMPORARY TABLE fe_dwd.pre_day2_gmv_5_tmp (                          
        KEY idx_business_name_product_id(business_name,product_id) 
) AS                                                              
SELECT                                                           
       b.business_name,                                         
       a.product_id,                                             
       SUM(a.stock_quantity * a.sale_price) AS stock_val,                               
       SUM(IF(a.sales_flag = 5,a.stock_quantity * a.sale_price,0)) AS stock_val_5    
FROM
        fe_dwd.`dwd_shelf_product_day_all_recent_32` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.`shelf_id` = b.`shelf_id`
                AND a.sdate = @pre_day2 
GROUP BY b.business_name,a.product_id

;
DELETE FROM fe_dm.dm_op_area_product_unsale_io_ratio WHERE y_date = @y_date OR y_date < SUBDATE(CURDATE(),INTERVAL 1 YEAR);
INSERT INTO fe_dm.dm_op_area_product_unsale_io_ratio
(
        y_date,
        business_name,
        product_id,
        product_code2,
        product_name,
        PRODUCT_TYPE,
        stock_val,
        stock_val_5,
        stock_val_5_ratio,
        gmv,
        gmv_5,
        gmv_5_ratio,
        stock_gmv_5_ratio_coef   
)
SELECT
        @y_date AS y_date,
        a.business_name,
        a.product_id,
        d.product_code2,
        d.product_name,
        b.PRODUCT_TYPE,
        a.stock_val,
        a.stock_val_5,
        a.stock_val_5 / a.stock_val AS stock_val_5_ratio,
        c.gmv,
        c.gmv_5,
        c.gmv_5 / c.gmv AS gmv_5_ratio,
        (c.gmv_5 / c.gmv) / (a.stock_val_5 / a.stock_val) AS stock_gmv_5_ratio_coef
FROM 
        fe_dwd.pre_day2_gmv_5_tmp a
        LEFT JOIN fe_dwd.`dwd_pub_product_dim_sserp` b
                ON a.`product_id` = b.`PRODUCT_ID`
                AND a.business_name = b.business_area
        JOIN fe_dwd.gmv_5_tmp c
                ON a.`product_id` = c.`PRODUCT_ID`
                AND a.business_name = c.business_name
        JOIN fe_dwd.`dwd_product_base_day_all` d
                ON a.product_id = d.product_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_unsale_io_ratio',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_unsale_io_ratio','dm_op_area_product_unsale_io_ratio','宋英南');
 
END