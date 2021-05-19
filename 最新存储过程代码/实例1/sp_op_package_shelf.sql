CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_package_shelf`()
    SQL SECURITY INVOKER
BEGIN
        DECLARE l_test VARCHAR(1);
        DECLARE l_row_cnt INT;
        DECLARE CODE CHAR(5) DEFAULT '00000';
        DECLARE done INT;
        DECLARE l_table_owner   VARCHAR(64);
        DECLARE l_city          VARCHAR(64);
        DECLARE l_task_name     VARCHAR(64);
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
                DECLARE EXIT HANDLER FOR SQLEXCEPTION
                BEGIN
                        GET DIAGNOSTICS CONDITION 1
                        CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
                        CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
                END; 
        SET l_task_name = 'sp_op_package_shelf';
        
SET @smonth = DATE_FORMAT(SUBDATE(CURDATE(),INTERVAL 1 MONTH),'%Y-%m');
SET @ydate := SUBDATE(CURDATE(),1);
SET @stat_date := CURDATE();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 商品包 1s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_package_tmp`;
CREATE TEMPORARY TABLE feods.shelf_package_tmp (
KEY idx_shelf_id_package_id (shelf_id,PACKAGE_ID)
) AS
 SELECT
        a.`SHELF_ID`,
        a.`PACKAGE_ID`,
        b.PACKAGE_NAME,
        c.PACKAGE_TYPE_NAME
FROM
        fe.`sf_shelf_package_detail` a
        JOIN fe.`sf_package` b
                ON a.PACKAGE_ID = b.PACKAGE_ID
                AND a.DATA_FLAG = 1
                AND b.DATA_FLAG = 1
                AND b.STATU_FLAG = 1
        JOIN fe.`sf_package_type` c
                ON b.PACKAGE_TYPE_ID = c.PACKAGE_TYPE_ID
                AND c.`DATA_FLAG` = 1
                AND c.STATU_FLAG = 1 
;
# 前置仓覆盖货架库存 37s
DROP TEMPORARY TABLE IF EXISTS feods.`prewarehouse_stock_tmp`;
CREATE TEMPORARY TABLE feods.prewarehouse_stock_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        b.product_id,
        b.available_stock
FROM 
        fe.`sf_prewarehouse_shelf_detail` a
        JOIN fe.`sf_prewarehouse_stock_detail` b
                ON a.warehouse_id = b.warehouse_id
                AND b.data_flag = 1 
                AND a.data_flag = 1
;
# 大仓库存 1s
DROP TEMPORARY TABLE IF EXISTS feods.`warehouse_stock_tmp`;
CREATE TEMPORARY TABLE feods.warehouse_stock_tmp(
        KEY idx_business_area_product_bar(BUSINESS_AREA,PRODUCT_BAR)
) AS 
SELECT
        a.BUSINESS_AREA,
        a.PRODUCT_BAR,
        a.QUALITYQTY
FROM 
        feods.`PJ_OUTSTOCK2_DAY` a
        JOIN feods.`wt_warehouse_business_area` b
                ON a.WAREHOUSE_NUMBER = b.WAREHOUSE_NUMBER
                AND FPRODUCEDATE = @ydate
                AND b.data_flag = 1
;
-- =================================================================================================
# 智能柜商品清单 1s
DROP TEMPORARY TABLE IF EXISTS feods.`smart_shelf_tmp`;
CREATE TEMPORARY TABLE feods.smart_shelf_tmp (
KEY idx_shelf_id(shelf_id)
) AS
SELECT
        e.business_name,
        a.shelf_id,
        a.template_id,
        b.template_name,
        CASE
                WHEN i.machine_type LIKE '%静态柜' AND SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) >= 15 
                        AND SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) <= 22 
                        THEN '正常'
                WHEN i.machine_type LIKE '%动态柜' AND SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) >= 15 
                        AND SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) <= 30 
                        THEN '正常'
                WHEN (SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) < 15)
                        THEN '过低'
                WHEN (i.machine_type LIKE '%静态柜' AND SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) > 22 )
                        OR (i.machine_type LIKE '%动态柜' AND SUM((f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND h.SHELF_FILL_FLAG = 1)) > 30)
                        THEN '超量'
        END AS is_sku_full,
        SUM((h.SHELF_FILL_FLAG = 1)) AS allow_fill_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 2 AND h.STOCK_QUANTITY > 0 ,c.product_id,NULL)) AS normal_stock_stop_fill_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 1 AND f.PRODUCT_TYPE IN ('新增（试运行）','原有')  ,c.product_id,NULL)) AS normal_product_allow_fill_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 1 AND f.PRODUCT_TYPE NOT IN ('新增（试运行）','原有')  ,c.product_id,NULL)) AS eliminate_product_allow_fill_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 2 AND f.PRODUCT_TYPE IN ('新增（试运行）','原有')  ,c.product_id,NULL)) AS normal_product_stop_fill_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 1 AND g.SECOND_TYPE_ID IN (1,2),c.product_id,NULL)) AS allow_fill_drink_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 1 AND g.SECOND_TYPE_ID IN (4,7,44,6,5),c.product_id,NULL)) AS allow_fill_food_sku_qty,
        COUNT(IF(h.SHELF_FILL_FLAG = 1 AND g.SECOND_TYPE_ID NOT IN (1,2,4,7,44,6,5),c.product_id,NULL)) AS allow_fill_other_sku_qty,
        SUM(h.SHELF_FILL_FLAG = 1 AND h.STOCK_QUANTITY IN (1,2)) AS allow_fill_low_stock_sku,
        SUM(h.SHELF_FILL_FLAG = 2 AND h.STOCK_QUANTITY IN (1,2)) AS stop_fill_low_stock_sku,
        SUM(h.STOCK_QUANTITY > 0) AS have_stock_sku,
        SUM((e.is_prewarehouse_cover = 0 AND h.SHELF_FILL_FLAG = 1 AND f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND IFNULL(k.QUALITYQTY,0) <= 0) OR
                (e.is_prewarehouse_cover = 1 AND f.PRODUCT_TYPE IN ('新增（试运行）','原有') AND IFNULL(j.available_stock,0) <= 0)
                ) AS normal_product_warehouse_offstock_sku,
        SUM((e.is_prewarehouse_cover = 0 AND h.SHELF_FILL_FLAG = 1 AND f.PRODUCT_TYPE NOT IN ('新增（试运行）','原有') AND IFNULL(k.QUALITYQTY,0) >= 200) OR
                (e.is_prewarehouse_cover = 1 AND f.PRODUCT_TYPE NOT IN ('新增（试运行）','原有') AND IFNULL(j.available_stock,0) >= 20)
                ) AS eliminate_product_warehouse_rich_sku
FROM
        fe.`sf_shelf_smart_template_relation` a
        JOIN fe.`sf_shelf_smart_product_template` b
                ON a.template_id = b.template_id
                AND a.data_flag = 1
                AND b.data_flag = 1
        JOIN fe.`sf_shelf_smart_product_template_item` c
                ON a.template_id = c.template_id
                AND c.data_flag = 1
        JOIN fe.`product_shelf_type` d
                ON c.product_id = d.product_id
                AND d.data_flag = 1
                AND d.shelf_type = 6
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` e
                ON a.shelf_id = e.shelf_id
        LEFT JOIN feods.`zs_product_dim_sserp` f
                ON f.business_area = e.business_name
                AND f.PRODUCT_ID = c.product_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` g
                ON c.product_id = g.product_id
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` h
                ON h.shelf_id = a.shelf_id
                AND h.product_id = c.product_id
        JOIN  feods.`d_op_shelf_info` i
                ON a.shelf_id = i.shelf_id
        LEFT JOIN feods.prewarehouse_stock_tmp j
                ON h.shelf_id = j.shelf_id
                AND h.product_id = j.product_id
        LEFT JOIN feods.warehouse_stock_tmp k
                ON k.BUSINESS_AREA = e.business_name
                AND k.PRODUCT_BAR = g.PRODUCT_CODE2
GROUP BY a.shelf_id
;
-- =============================================================================================
# 货架层级商品包sku 4min20s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_package_product`;
CREATE TEMPORARY TABLE feods.shelf_package_product (
KEY idx_shelf_id_package_id (shelf_id,package_id)
) AS
SELECT
        a.shelf_id,
        b.package_id,
        SUM((a.SHELF_FILL_FLAG = 1)) AS allow_fill_sku_qty,
        c.business_name AS business_area,
        g.skus,
        CASE 
                WHEN c.SHELF_TYPE IN (1,3) AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) >= 25 
                        AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) <= 55 
                        THEN '正常'
                WHEN c.SHELF_TYPE = 2 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) >= 10 
                        AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) <= 20 
                        THEN '正常'
                WHEN c.relation_flag = 1 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) >= 25 
                        THEN '正常'
                WHEN c.SHELF_TYPE = 7 AND g.skus >= 15 AND g.skus <= 30
                        THEN '正常' 
                WHEN c.SHELF_TYPE = 8 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) >= 15
                        THEN '正常' 
                WHEN (c.SHELF_TYPE IN (1,3) AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) < 25) 
                        OR (c.SHELF_TYPE = 2 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) < 10)
                        OR (c.SHELF_TYPE = 6 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) < 15)
                        OR (c.relation_flag = 1 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) < 25 )
                        OR (c.SHELF_TYPE = 7 AND g.skus < 15)
                        OR (c.SHELF_TYPE = 8 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) < 15)
                        THEN '过低'
                WHEN (c.SHELF_TYPE IN (1,3) AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) > 55) 
                        OR (c.SHELF_TYPE = 2 AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) > 20)
                        OR (c.SHELF_TYPE = 6 AND h.machine_type LIKE '%静态柜' AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) > 22 )
                        OR (c.SHELF_TYPE = 6 AND h.machine_type LIKE '%动态柜' AND SUM((e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SHELF_FILL_FLAG = 1)) > 30)
                        OR (c.SHELF_TYPE = 7 AND g.skus > 30)
                        THEN '超量'
        END AS is_sku_full,
        COUNT(IF(a.SHELF_FILL_FLAG = 2 AND a.STOCK_QUANTITY > 0 ,a.product_id,NULL)) AS normal_stock_stop_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND e.PRODUCT_TYPE IN ('新增（试运行）','原有')  ,a.product_id,NULL)) AS normal_product_allow_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND e.PRODUCT_TYPE NOT IN ('新增（试运行）','原有')  ,a.product_id,NULL)) AS eliminate_product_allow_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 2 AND e.PRODUCT_TYPE IN ('新增（试运行）','原有')  ,a.product_id,NULL)) AS normal_product_stop_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND f.SECOND_TYPE_ID IN (1,2),a.product_id,NULL)) AS allow_fill_drink_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND f.SECOND_TYPE_ID IN (4,7,44,6,5),a.product_id,NULL)) AS allow_fill_food_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND f.SECOND_TYPE_ID NOT IN (1,2,4,7,44,6,5),a.product_id,NULL)) AS allow_fill_other_sku_qty,
        SUM(a.SHELF_FILL_FLAG = 1 AND a.STOCK_QUANTITY IN (1,2)) AS allow_fill_low_stock_sku,
        SUM(a.SHELF_FILL_FLAG = 2 AND a.STOCK_QUANTITY IN (1,2)) AS stop_fill_low_stock_sku,
        SUM(a.STOCK_QUANTITY > 0) AS have_stock_sku,
        SUM((c.is_prewarehouse_cover = 0 AND a.SHELF_FILL_FLAG = 1 AND e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND IFNULL(k.QUALITYQTY,0) <= 0) OR
                (c.is_prewarehouse_cover = 1 AND e.PRODUCT_TYPE IN ('新增（试运行）','原有') AND IFNULL(j.available_stock,0) <= 0)
                ) AS normal_product_warehouse_offstock_sku,
        SUM((c.is_prewarehouse_cover = 0 AND a.SHELF_FILL_FLAG = 1 AND e.PRODUCT_TYPE NOT IN ('新增（试运行）','原有') AND IFNULL(k.QUALITYQTY,0) >= 200) OR
                (c.is_prewarehouse_cover = 1 AND e.PRODUCT_TYPE NOT IN ('新增（试运行）','原有') AND IFNULL(j.available_stock,0) >= 20)
                ) AS eliminate_product_warehouse_rich_sku
FROM 
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN fe.`sf_shelf_package_detail` b
                ON a.shelf_id = b.shelf_id
                AND b.DATA_FLAG = 1
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON a.shelf_id = c.shelf_id
                AND c.SHELF_TYPE IN (1,2,3,5,7,8)
                AND c.SHELF_STATUS = 2
        LEFT JOIN feods.`zs_product_dim_sserp` e
                ON e.business_area = c.business_name
                AND e.PRODUCT_ID = a.product_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` f
                ON a.product_id = f.product_id
        LEFT JOIN feods.d_op_offstock_s7 g
                ON a.shelf_id = g.shelf_id
                AND g.sdate = @ydate
        JOIN feods.`d_op_shelf_info` h
                ON a.shelf_id = h.shelf_id
        LEFT JOIN feods.prewarehouse_stock_tmp j
                ON a.shelf_id = j.shelf_id
                AND a.product_id = j.product_id
        LEFT JOIN feods.warehouse_stock_tmp k
                ON k.BUSINESS_AREA = c.business_name
                AND k.PRODUCT_BAR = f.PRODUCT_CODE2
GROUP BY a.shelf_id
;
# 货架信息 1s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_info`;
CREATE TEMPORARY TABLE feods.shelf_info (
KEY idx_shelf_id (shelf_id)
) AS
SELECT
        a.shelf_id,
        a.shelf_type,
        a.prewh_falg AS is_prewarehouse_shelf,
        a.fulltime_falg AS is_full_time_manager,
        CASE
                WHEN d.grade IN ('甲','乙','新装') THEN '甲乙新'
                WHEN d.grade IN ('丁','丙') THEN '丙丁级'
        END AS shelf_level,
        a.SHELF_STATUS,
        a.REVOKE_STATUS,
        a.WHETHER_CLOSE,
        e.zone_name,
        a.machine_type,
        e.if_bind if_band
FROM
        feods.`d_op_shelf_info` a
        JOIN feods.`d_op_shelf_grade` d
                ON a.shelf_id = d.shelf_id
                AND d.month_id = @smonth
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` e
                ON a.shelf_id = e.shelf_id
;
-- 商品包结果表(货架层级) 1s
DELETE FROM feods.d_op_package_shelf WHERE stat_date < SUBDATE(@stat_date,30) OR stat_date = CURDATE();
INSERT INTO feods.d_op_package_shelf
(
        stat_date,
        shelf_id,
        package_id,
        PACKAGE_NAME,
        PACKAGE_TYPE_NAME,
        allow_fill_sku_qty,
        business_area,
        shelf_level,
        is_prewarehouse_shelf,
        is_full_time_manager,
        if_band,
        shelf_type,
        SHELF_STATUS,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        is_sku_full,
        zone_name,
        normal_stock_stop_fill_sku_qty,
        machine_type,
        skus,
        normal_product_allow_fill_sku_qty,
        eliminate_product_allow_fill_sku_qty,
        normal_product_stop_fill_sku_qty,
        allow_fill_drink_sku_qty,
        allow_fill_food_sku_qty,
        allow_fill_other_sku_qty,
        allow_fill_low_stock_sku,
        stop_fill_low_stock_sku,
        have_stock_sku,
        normal_product_warehouse_offstock_sku,
        eliminate_product_warehouse_rich_sku
)
SELECT
        @stat_date,
        a.shelf_id,
        a.package_id,
        a.PACKAGE_NAME,
        a.PACKAGE_TYPE_NAME,
        a.allow_fill_sku_qty,
        a.business_area,
        c.shelf_level,
        c.is_prewarehouse_shelf,
        c.is_full_time_manager,
        c.if_band,
        c.shelf_type,
        c.SHELF_STATUS,
        c.REVOKE_STATUS,
        c.WHETHER_CLOSE,
        a.is_sku_full,
        c.zone_name,
        a.normal_stock_stop_fill_sku_qty,
        c.machine_type,
        a.skus,
        a.normal_product_allow_fill_sku_qty,
        a.eliminate_product_allow_fill_sku_qty,
        a.normal_product_stop_fill_sku_qty,
        a.allow_fill_drink_sku_qty,
        a.allow_fill_food_sku_qty,
        a.allow_fill_other_sku_qty,
        a.allow_fill_low_stock_sku,
        a.stop_fill_low_stock_sku,
        a.have_stock_sku,
        a.normal_product_warehouse_offstock_sku,
        a.eliminate_product_warehouse_rich_sku
FROM
(
        SELECT
                a.shelf_id,
                a.package_id,
                a.PACKAGE_NAME,
                a.PACKAGE_TYPE_NAME,
                b.allow_fill_sku_qty,
                b.business_area,
                b.is_sku_full,
                b.normal_stock_stop_fill_sku_qty,
                b.skus,
                b.normal_product_allow_fill_sku_qty,
                b.eliminate_product_allow_fill_sku_qty,
                b.normal_product_stop_fill_sku_qty,
                b.allow_fill_drink_sku_qty,
                b.allow_fill_food_sku_qty,
                b.allow_fill_other_sku_qty,
                b.allow_fill_low_stock_sku,
                b.stop_fill_low_stock_sku,
                b.have_stock_sku,
                b.normal_product_warehouse_offstock_sku,
                b.eliminate_product_warehouse_rich_sku
        FROM 
                feods.shelf_package_tmp a
                JOIN feods.shelf_package_product b
                        ON a.shelf_id = b.shelf_id
                        AND a.package_id = b.package_id
        UNION
        SELECT
                a.shelf_id,
                a.template_id AS package_id,
                a.template_name AS PACKAGE_NAME,
                NULL AS PACKAGE_TYPE_NAME,
                a.allow_fill_sku_qty,
                a.business_name AS business_area,
                a.is_sku_full,
                a.normal_stock_stop_fill_sku_qty,
                NULL AS skus,
                a.normal_product_allow_fill_sku_qty,
                a.eliminate_product_allow_fill_sku_qty,
                a.normal_product_stop_fill_sku_qty,
                a.allow_fill_drink_sku_qty,
                a.allow_fill_food_sku_qty,
                a.allow_fill_other_sku_qty,
                a.allow_fill_low_stock_sku,
                a.stop_fill_low_stock_sku,
                a.have_stock_sku,
                a.normal_product_warehouse_offstock_sku,
                a.eliminate_product_warehouse_rich_sku
        FROM 
                feods.`smart_shelf_tmp` a
) a
        JOIN feods.shelf_info c
                ON a.shelf_id = c.shelf_id
;
-- ========================================================================================
-- 商品包基础信息 1min45s
DROP TEMPORARY TABLE IF EXISTS feods.`package_product`;
CREATE TEMPORARY TABLE feods.package_product (
KEY idx_package_id_product_id (package_id,product_id)
) AS
SELECT
        b.package_id,
        a.product_id,
        c.business_name AS business_area,
        b.SHELF_FILL_FLAG
FROM
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN fe.`sf_package_item` b
                ON a.ITEM_ID = b.ITEM_ID
                AND b.DATA_FLAG = 1
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.shelf_id = c.shelf_id
                AND c.SHELF_TYPE IN (1,2,3,5,6,7,8)
                AND c.SHELF_STATUS = 2
GROUP BY b.package_id,a.product_id
;
DROP TEMPORARY TABLE IF EXISTS feods.`package_sku`;
CREATE TEMPORARY TABLE feods.package_sku (
KEY idx_package_id (package_id)
) AS
SELECT 
        a.package_id,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND e.PRODUCT_TYPE IN ('新增（试运行）','原有')  ,a.product_id,NULL)) AS normal_product_allow_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND e.PRODUCT_TYPE NOT IN ('新增（试运行）','原有')  ,a.product_id,NULL)) AS eliminate_product_allow_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 2 AND e.PRODUCT_TYPE IN ('新增（试运行）','原有')  ,a.product_id,NULL)) AS normal_product_stop_fill_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND f.SECOND_TYPE_ID IN (1,2),a.product_id,NULL)) AS allow_fill_drink_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND f.SECOND_TYPE_ID IN (4,7,44,6,5),a.product_id,NULL)) AS allow_fill_food_sku_qty,
        COUNT(IF(a.SHELF_FILL_FLAG = 1 AND f.SECOND_TYPE_ID NOT IN (1,2,4,7,44,6,5),a.product_id,NULL)) AS allow_fill_other_sku_qty
FROM 
        feods.package_product a
        JOIN feods.`zs_product_dim_sserp` e
                ON e.business_area = a.BUSINESS_AREA
                AND e.PRODUCT_ID = a.product_id      
        JOIN `fe_dwd`.`dwd_product_base_day_all` f
                ON a.product_id = f.product_id
GROUP BY a.package_id
;
-- 商品包基础信息结果表1s
TRUNCATE feods.d_op_package_config;
INSERT INTO feods.d_op_package_config
(
        stat_date,
        PACKAGE_ID,
        PACKAGE_NAME,
        BUSINESS_AREA,
        shelf_qty,
        MAX_STOCK,
        STOCK_RATE,
        normal_product_allow_fill_sku_qty,
        eliminate_product_allow_fill_sku_qty,
        normal_product_stop_fill_sku_qty,
        allow_fill_drink_sku_qty,
        allow_fill_food_sku_qty,
        allow_fill_other_sku_qty,
        PACKAGE_TYPE_NAME
)
SELECT 
        @stat_date,
        a.PACKAGE_ID,
        a.PACKAGE_NAME,
        c.business_name AS `BUSINESS_AREA`,
        COUNT(c.`SHELF_ID`) AS shelf_qty,
        e.MAX_STOCK,
        e.STOCK_RATE,
        f.normal_product_allow_fill_sku_qty,
        f.eliminate_product_allow_fill_sku_qty,
        f.normal_product_stop_fill_sku_qty,
        f.allow_fill_drink_sku_qty,
        f.allow_fill_food_sku_qty,
        f.allow_fill_other_sku_qty,
        e.PACKAGE_TYPE_NAME
FROM 
        fe.`sf_package` a
        JOIN fe.`sf_shelf_package_detail` b
                ON a.PACKAGE_ID = b.PACKAGE_ID
                AND a.DATA_FLAG = 1
                AND b.DATA_FLAG = 1
                AND a.STATU_FLAG = 1
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON b.`SHELF_ID` = c.`SHELF_ID`
                AND c.SHELF_TYPE IN (1,2,3,5,6,7,8)
                AND c.SHELF_STATUS = 2
        JOIN fe.`sf_package_type` e
                ON e.PACKAGE_TYPE_ID = a.PACKAGE_TYPE_ID
                AND e.DATA_FLAG = 1
        JOIN feods.package_sku f
                ON a.PACKAGE_ID = f.PACKAGE_ID
GROUP BY a.PACKAGE_ID
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_package_shelf',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
END