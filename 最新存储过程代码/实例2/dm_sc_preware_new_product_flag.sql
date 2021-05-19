CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_preware_new_product_flag`()
BEGIN
 SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
TRUNCATE TABLE fe_dm.dm_sc_preware_new_product_flag; 
INSERT INTO fe_dm.dm_sc_preware_new_product_flag
(`region_area`
    , `business_area`
    , `warehouse_id`
    , `shelf_code`
    , `shelf_name`
    , first_add_time   
    , `product_id`
    , `product_code2`
    , `product_name`
    , `version_id`    
    , `product_type`
    , np_flag
)
SELECT r.`region_area`
    , s.`business_name`
    , t.`warehouse_id`
    , s.`SHELF_CODE`
    , s.`shelf_name`
    , t.`ADD_time` AS first_add_time   
    , t.`product_id`
    , p.`PRODUCT_FE`
    , p.`product_name`
    , p.`version`    
    , p.`PRODUCT_TYPE`
    , IF(DATEDIFF(CURDATE(),t.`add_time`)<= 30,1,0) preware_np
FROM
    fe_dwd.`dwd_sf_prewarehouse_stock_detail` t
    JOIN fe_dwd.`dwd_shelf_base_day_all` s
    ON t.`warehouse_id` = s.`shelf_id`
    JOIN fe_dwd.`dwd_sc_business_region` r
    ON s.`business_name` = r.`business_area`
    JOIN fe_dwd.`dwd_pub_product_dim_sserp` p
    ON  p.business_area = s.business_name
    AND p.product_id = t.product_id 
    WHERE p.`PRODUCT_TYPE` = '新增（试运行）'
    ;
   
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_preware_new_product_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_preware_new_product_flag','dm_sc_preware_new_product_flag','吴婷');
COMMIT;
    END