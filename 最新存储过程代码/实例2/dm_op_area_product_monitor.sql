CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_monitor`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/03/27
-- Modify date: 
-- Description:	
-- 	商品监控（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`machine_tmp`;
CREATE TEMPORARY TABLE fe_dwd.machine_tmp(
        KEY idx_shelf_id_product_id(shelf_id,PRODUCT_ID)
) AS 
SELECT
        shelf_id,
        product_id
FROM
        fe_dwd.`dwd_shelf_machine_slot_type`
GROUP BY shelf_id,product_id
HAVING SUM(stock_num) > 0
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_monitor","@time_2--@time_3",@time_2,@time_3);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`area_shelf_tmp`;
CREATE TEMPORARY TABLE fe_dwd.area_shelf_tmp(
        KEY idx_area(business_name)
) AS 
SELECT 
        business_name,
        SUM(IF(a.`shelf_type` IN (1,2,3,5,6,7) AND a.`SHELF_STATUS` = 2,1,0)) AS activate_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5,6,7) AND a.`SHELF_STATUS` = 2 AND a.`WHETHER_CLOSE` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS normal_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5) AND a.`SHELF_STATUS` = 2 AND a.`WHETHER_CLOSE` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS normal_self_service_shelf_qty,
        SUM(IF(a.`shelf_type` = 6 AND a.`SHELF_STATUS` = 2,1,0)) AS activate_smart_shelf_qty,
        SUM(IF(a.`shelf_type` = 7 AND a.`SHELF_STATUS` = 2,1,0)) AS activate_machine_shelf_qty
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
GROUP BY business_name
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`area_product_tmp`;
CREATE TEMPORARY TABLE fe_dwd.area_product_tmp(
        KEY idx_area_product(business_name,PRODUCT_ID)
) AS 
SELECT 
        a.region_name,
        a.business_name,
        b.PRODUCT_ID,
        c.`PRODUCT_CODE2`,
        CONCAT(a.business_name,c.`PRODUCT_CODE2`) AS area_product_flag,
        c.`PRODUCT_NAME`,
        c.second_type_name,
        c.sub_type_name,
        c.`FILL_MODEL`,
        c.CATEGORY_NAME,
        d.PRODUCT_TYPE,
        d.PUB_TIME,
        f.activate_shelf_qty,
        f.normal_shelf_qty,
        f.normal_self_service_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5,6,7) AND b.STOCK_QUANTITY > 0,1,0)) AS stock_shelf_qty,
        f.activate_smart_shelf_qty,
        SUM(IF(a.`shelf_type` = 6 AND b.STOCK_QUANTITY > 0,1,0)) AS stock_smart_shelf_qty,
        f.activate_machine_shelf_qty,
        SUM(IF(e.shelf_id IS NOT NULL,1,0)) AS stock_machine_shelf_slot_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5) AND b.`SHELF_FILL_FLAG` = 1 AND a.`SHELF_STATUS` = 2 AND a.`WHETHER_CLOSE` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS allow_fill_normal_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5) AND b.`SHELF_FILL_FLAG` = 2 AND a.`SHELF_STATUS` = 2 AND a.`WHETHER_CLOSE` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS stop_fill_normal_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5) AND b.`SHELF_FILL_FLAG` = 1 AND a.`SHELF_STATUS` = 2 AND (a.`WHETHER_CLOSE` = 1 OR a.`REVOKE_STATUS` != 1),1,0)) AS allow_fill_abnormal_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5) AND b.`SHELF_FILL_FLAG` = 2 AND a.`SHELF_STATUS` = 2 AND (a.`WHETHER_CLOSE` = 1 OR a.`REVOKE_STATUS` != 1),1,0)) AS stop_fill_abnormal_shelf_qty,
        SUM(IF(a.`shelf_type` = 6 AND b.`SHELF_FILL_FLAG` = 1 AND a.`SHELF_STATUS` = 2 ,1,0)) AS smart_shelf_qty,
        SUM(IF(a.`shelf_type` = 7 AND b.`SHELF_FILL_FLAG` = 1 AND a.`SHELF_STATUS` = 2 ,1,0)) AS machine_shelf_qty,
        SUM(IF(a.`shelf_type` IN (1,2,3,5) AND b.`SHELF_FILL_FLAG` = 1 AND  a.`SHELF_STATUS` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) 
                        / SUM(IF(a.`shelf_type` IN (1,2,3,5) AND  a.`SHELF_STATUS` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS normal_shelf_package_range,
        SUM(IF(a.`shelf_type` = 6 AND b.`SHELF_FILL_FLAG` = 1 AND  a.`SHELF_STATUS` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) 
                        / SUM(IF(a.`shelf_type` = 6 AND  a.`SHELF_STATUS` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS smart_shelf_package_range,
         SUM(IF(a.`shelf_type` = 7 AND b.`SHELF_FILL_FLAG` = 1 AND  a.`SHELF_STATUS` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) 
                        / SUM(IF(a.`shelf_type` = 7 AND  a.`SHELF_STATUS` = 2 AND a.`REVOKE_STATUS`= 1,1,0)) AS machine_shelf_package_range
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b
                ON a.`shelf_id` = b.`SHELF_ID`
        JOIN fe_dwd.`dwd_product_base_day_all` c
                ON b.`PRODUCT_ID` = c.`PRODUCT_ID`
        LEFT JOIN fe_dwd.`dwd_pub_product_dim_sserp` d
                ON a.`business_name` = d.`business_area`
                AND c.`PRODUCT_ID` = d.`PRODUCT_ID`
        LEFT JOIN fe_dwd.machine_tmp e
                ON b.shelf_id = e.shelf_id
                AND b.product_id = e.product_id
        JOIN fe_dwd.area_shelf_tmp f
                ON a.business_name = f.business_name
GROUP BY a.`business_name`,b.`PRODUCT_ID`
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_monitor","@time_3--@time_4",@time_3,@time_4);
TRUNCATE TABLE fe_dm.dm_op_area_product_monitor;
INSERT INTO fe_dm.dm_op_area_product_monitor
(
        region_name,
        business_name,
        PRODUCT_ID,
        PRODUCT_CODE2,
        area_product_flag,
        PRODUCT_NAME,
        second_type_name,
        sub_type_name,
        FILL_MODEL,
        CATEGORY_NAME,
        PRODUCT_TYPE,
        PUB_TIME,
        activate_shelf_qty,
        normal_shelf_qty,
        normal_self_service_shelf_qty,
        stock_shelf_qty,
        activate_smart_shelf_qty,
        stock_smart_shelf_qty,
        activate_machine_shelf_qty,
        stock_machine_shelf_slot_qty,
        allow_fill_normal_shelf_qty,
        stop_fill_normal_shelf_qty,
        allow_fill_abnormal_shelf_qty,
        stop_fill_abnormal_shelf_qty,
        smart_shelf_qty,
        machine_shelf_qty,
        normal_shelf_package_range,
        smart_shelf_package_range,
        machine_shelf_package_range
)
SELECT
        region_name,
        business_name,
        PRODUCT_ID,
        PRODUCT_CODE2,
        area_product_flag,
        PRODUCT_NAME,
        second_type_name,
        sub_type_name,
        FILL_MODEL,
        CATEGORY_NAME,
        PRODUCT_TYPE,
        PUB_TIME,
        activate_shelf_qty,
        normal_shelf_qty,
        normal_self_service_shelf_qty,
        stock_shelf_qty,
        activate_smart_shelf_qty,
        stock_smart_shelf_qty,
        activate_machine_shelf_qty,
        stock_machine_shelf_slot_qty,
        allow_fill_normal_shelf_qty,
        stop_fill_normal_shelf_qty,
        allow_fill_abnormal_shelf_qty,
        stop_fill_abnormal_shelf_qty,
        smart_shelf_qty,
        machine_shelf_qty,
        CASE
                WHEN normal_shelf_package_range < 0.1 THEN 'A.<10%'
                WHEN normal_shelf_package_range < 0.3 THEN 'B.[10%,30%)'
                WHEN normal_shelf_package_range < 0.5 THEN 'C.[30%,50%)'
                WHEN normal_shelf_package_range < 0.6 THEN 'D.[50%,60%)'
                WHEN normal_shelf_package_range < 0.7 THEN 'E.[60%,70%)'
                WHEN normal_shelf_package_range < 0.8 THEN 'F.[70%,80%)'
                WHEN normal_shelf_package_range <= 1 THEN 'G.超80%'
        END AS normal_shelf_package_range,
        CASE
                WHEN smart_shelf_package_range < 0.1 THEN 'A.<10%'
                WHEN smart_shelf_package_range < 0.3 THEN 'B.[10%,30%)'
                WHEN smart_shelf_package_range < 0.5 THEN 'C.[30%,50%)'
                WHEN smart_shelf_package_range < 0.6 THEN 'D.[50%,60%)'
                WHEN smart_shelf_package_range < 0.7 THEN 'E.[60%,70%)'
                WHEN smart_shelf_package_range < 0.8 THEN 'F.[70%,80%)'
                WHEN smart_shelf_package_range <= 1 THEN 'G.超80%'
        END AS smart_shelf_package_range,
        CASE
                WHEN machine_shelf_package_range < 0.1 THEN 'A.<10%'
                WHEN machine_shelf_package_range < 0.3 THEN 'B.[10%,30%)'
                WHEN machine_shelf_package_range < 0.5 THEN 'C.[30%,50%)'
                WHEN machine_shelf_package_range < 0.6 THEN 'D.[50%,60%)'
                WHEN machine_shelf_package_range < 0.7 THEN 'E.[60%,70%)'
                WHEN machine_shelf_package_range < 0.8 THEN 'F.[70%,80%)'
                WHEN machine_shelf_package_range <= 1 THEN 'G.超80%'
        END AS machine_shelf_package_range
FROM
        fe_dwd.area_product_tmp
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_monitor","@time_4--@time_5",@time_4,@time_5);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_monitor','dm_op_area_product_monitor','宋英南');
 
END