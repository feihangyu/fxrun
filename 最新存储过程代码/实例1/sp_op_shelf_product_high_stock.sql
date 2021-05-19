CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_product_high_stock`()
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
 
        SET l_task_name = 'sp_op_shelf_product_high_stock';
SET @stat_date := CURDATE();
SET @last_month := DATE_FORMAT(DATE_SUB(@stat_date, INTERVAL DAY(@stat_date) DAY),'%Y-%m');
SET @month_id := DATE_FORMAT(CURDATE(),'%Y-%m');
SET @pre_day30 := SUBDATE(@stat_date,INTERVAL 30 DAY);
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 货架口径临时表 2s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_tmp`;
SET @time_18 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE feods.shelf_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        b.business_name,
        b.`SHELF_ID`,
        b.SHELF_CODE,
        b.shelf_name,
        b.shelf_type,
        b.MANAGER_ID,
        b.REAL_NAME,
        b.manager_type,
        f.prewarehouse_code,
        f.prewarehouse_name,
        b.branch_code,
        b.branch_name,
        b.bind_cnt,
        b.grade
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` b
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` f
                ON b.shelf_id = f.shelf_id
WHERE b.revoke_status = 1
        AND b.shelf_status = 2
        AND b.shelf_type != 9
        AND b.business_name NOT IN ('内蒙古区','台州区','冀北区','惠州区','烟台市')
        AND b.shelf_id NOT IN (50914,50915,50916,62365,62366,63820,71059,81921,81922,81923,85789,97166)    -- 剔除部队货架
        AND b.loss_pro_flag = '否'
;
# 保留数量 4s
DROP TEMPORARY TABLE IF EXISTS feods.`remain_qty_tmp`;
CREATE TEMPORARY TABLE feods.remain_qty_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        shelf_id,
        product_id,
        day_sale_qty,
        CEILING(2 + h.day_sale_qty * (IFNULL(h.fill_cycle,0) + IFNULL(h.fill_days,0) + 1) + h.safe_stock_qty + h.suspect_false_stock_qty) AS remain_qty
FROM
        feods.`d_op_shelf_product_fill_update` h
UNION ALL
SELECT
        shelf_id,
        product_id,
        day_sale_qty,
        CEILING(2 + j.day_sale_qty * (IFNULL(j.fill_cycle,0) + IFNULL(j.fill_days,0) + 1) + j.safe_stock_qty + IFNULL(j.suspect_false_stock_qty,0)) AS remain_qty
FROM
        (    
                SELECT
                        DISTINCT
                        shelf_id,
                        product_id,
                        day_sale_qty,
                        fill_cycle,
                        fill_days,
                        safe_stock_qty,
                        suspect_false_stock_qty
                FROM feods.`d_op_machine_fill_update`
        ) j
UNION ALL
SELECT
        shelf_id,
        product_id,     
        day_sale_qty,
        CEILING(2 + k.day_sale_qty * (7 + 1) + k.safe_stock_qty) AS remain_qty
FROM
        feods.`d_op_smart_shelf_fill_update` k
;
SET @time_20 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_high_stock","@time_18--@time_20",@time_18,@time_20);
-- 货架商品库存明细 3min
SET @time_23 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        b.business_name, 
        a.`SHELF_ID`, 
        b.SHELF_CODE,
        b.shelf_name,
        b.shelf_type, 
        b.bind_cnt,
        b.MANAGER_ID,
        b.REAL_NAME,
        b.manager_type,
        b.prewarehouse_code,
        b.prewarehouse_name,
        b.branch_code,
        b.branch_name,
        a.`PRODUCT_ID`,
        c.PRODUCT_CODE2,
        c.PRODUCT_NAME,
        d.PRODUCT_TYPE,
        c.SECOND_TYPE_ID,
        c.FILL_MODEL,
        a.SALES_FLAG,
        IF(a.STOCK_QUANTITY < 0,0,a.STOCK_QUANTITY) AS STOCK_QUANTITY, 
        IF(a.STOCK_QUANTITY < 0,0,a.STOCK_QUANTITY) * a.SALE_PRICE AS STOCK_VALUE,
        i.day_sale_qty,
        ROUND(IF(a.STOCK_QUANTITY < 0,0,a.STOCK_QUANTITY) / i.day_sale_qty,2) AS turnover_days,   
        r.remain_qty,
        IF(a.STOCK_QUANTITY < 0,0,a.STOCK_QUANTITY) - r.remain_qty AS unnecessary_stock,
        r.remain_qty * a.SALE_PRICE AS remain_value,
        IF(a.STOCK_QUANTITY < 0,0,a.STOCK_QUANTITY)  * a.SALE_PRICE - r.remain_qty * a.SALE_PRICE AS unnecessary_value,
        a.FIRST_FILL_TIME,
        b.grade,
        a.SALE_PRICE,
        a.SHELF_FILL_FLAG
FROM
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN feods.shelf_tmp b
                ON a.`SHELF_ID` = b.`shelf_id`
        JOIN `fe_dwd`.`dwd_product_base_day_all` c
                ON a.`PRODUCT_ID` = c.`PRODUCT_ID`
        LEFT JOIN feods.`zs_product_dim_sserp` d
                ON b.`business_name` = d.business_area
                AND d.`PRODUCT_ID` = a.`PRODUCT_ID`
        LEFT JOIN feods.`d_op_fill_day_sale_qty` i
                ON a.shelf_id = i.shelf_id
                AND a.product_id = i.product_id
        LEFT JOIN feods.remain_qty_tmp r
                ON a.shelf_id = r.shelf_id
                AND a.product_id = r.product_id
;
SET @time_25 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_high_stock","@time_23--@time_25",@time_23,@time_25);
-- 货架商品级高库存明细
-- TRUNCATE fe_dm.dm_op_shelf_product_high_stock;
SET @time_33 := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.dm_op_shelf_product_high_stock WHERE @stat_date != DATE_FORMAT(CURDATE(),'%Y-%m-01') AND month_id = DATE_FORMAT(SUBDATE(CURDATE(),1),'%Y-%m'); -- 每月1号截存
INSERT INTO fe_dm.dm_op_shelf_product_high_stock
(
        month_id,
        business_name,
        SHELF_ID,
        SHELF_CODE,
        shelf_name,
        grade,
        shelf_type,
        MANAGER_ID,
        REAL_NAME,
        manager_type,
        prewarehouse_code,
        prewarehouse_name,
        branch_code,
        branch_name,
        PRODUCT_ID,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        PRODUCT_TYPE,
        SECOND_TYPE_ID,
        FILL_MODEL,
        SALES_FLAG,
        STOCK_QUANTITY,
        STOCK_VALUE,
        day_sale_qty,
        turnover_days,
        remain_qty,
        unnecessary_stock,
        remain_value,
        unnecessary_value
)
SELECT
        @month_id AS month_id,
        a.business_name,
        a.`SHELF_ID`,
        a.SHELF_CODE,
        a.shelf_name,
        a.grade,
        a.shelf_type,
        a.MANAGER_ID,
        a.REAL_NAME,
        a.manager_type,
        a.prewarehouse_code,
        a.prewarehouse_name,
        a.branch_code,
        a.branch_name,
        a.`PRODUCT_ID`,
        a.PRODUCT_CODE2,
        a.PRODUCT_NAME,
        a.PRODUCT_TYPE,
        a.SECOND_TYPE_ID,
        a.FILL_MODEL,
        a.SALES_FLAG,
        a.STOCK_QUANTITY,
        a.STOCK_VALUE,
        a.day_sale_qty,
        a.turnover_days,
        a.remain_qty,
        a.unnecessary_stock,
        a.remain_value,
        a.unnecessary_value
FROM 
         feods.shelf_product_tmp a   -- feods.shelf_product_tmp a
WHERE a.FIRST_FILL_TIME < @pre_day30 
        AND a.STOCK_QUANTITY / a.FILL_MODEL > 1.5
        AND ((a.grade IN ('甲','乙','新装') AND a.turnover_days > 15 AND a.STOCK_QUANTITY > 20) OR (a.grade IN ('丙','丁') AND a.turnover_days > 50 AND a.STOCK_QUANTITY > 10))
;
SET @time_35 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_high_stock","@time_33--@time_35",@time_33,@time_35);
-- 货架高库存占比 1min
-- 关联货架排面量 
DROP TEMPORARY TABLE IF EXISTS feods.`layout_standard`;
CREATE TEMPORARY TABLE feods.layout_standard(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        CASE 
            WHEN a.shelf_type = 1 THEN 330 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type = 2 THEN 250 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type = 3 THEN 330 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type = 4 THEN 300 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type = 5 THEN 220 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type = 6 AND a.type_name LIKE '%静态柜' THEN 220 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type = 6 AND a.type_name LIKE '%动态柜' THEN 250 + IFNULL(b.high_relation_standard,0)
            WHEN a.shelf_type IN (7,8) THEN 350 + IFNULL(b.high_relation_standard,0)
            else 250 + IFNULL(b.high_relation_standard,0)
        END AS high_layout_standard,         
        CASE 
            WHEN a.shelf_type IN (1,3,4) THEN 110 + IFNULL(b.low_relation_standard,0)
            WHEN a.shelf_type IN (2,5) THEN 90 + IFNULL(b.low_relation_standard,0)
            WHEN a.shelf_type = 6 AND a.type_name LIKE '%静态柜' THEN 70 + IFNULL(b.low_relation_standard,0)
            WHEN a.shelf_type = 6 AND a.type_name LIKE '%动态柜' THEN 90 + IFNULL(b.low_relation_standard,0)
            WHEN a.shelf_type = 7 THEN 150 + IFNULL(b.low_relation_standard,0)
            WHEN a.shelf_type = 8 THEN 90 + IFNULL(b.low_relation_standard,0)
            else 90 + IFNULL(b.low_relation_standard,0)
        END AS low_layout_standard
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        LEFT JOIN 
                (
                        SELECT 
                                a.`MAIN_SHELF_ID` AS shelf_id, 
                                SUM(CASE 
                                            WHEN a.shelf_type = 1 THEN 330
                                            WHEN a.shelf_type = 2 THEN 250
                                            WHEN a.shelf_type = 3 THEN 330
                                            WHEN a.shelf_type = 4 THEN 300
                                            WHEN a.shelf_type = 5 THEN 220
                                            WHEN a.shelf_type = 6 AND a.type_name LIKE '%静态柜' THEN 220
                                            WHEN a.shelf_type = 6 AND a.type_name LIKE '%动态柜' THEN 250
                                            WHEN a.shelf_type IN (7,8) THEN 350
                                            else 250
                                        END) AS high_relation_standard,
                                SUM(CASE 
                                            WHEN a.shelf_type IN (1,3,4) THEN 110
                                            WHEN a.shelf_type IN (2,5) THEN 90
                                            WHEN a.shelf_type = 6 AND a.type_name LIKE '%静态柜' THEN 70
                                            WHEN a.shelf_type = 6 AND a.type_name LIKE '%动态柜' THEN 90
                                            WHEN a.shelf_type = 7 THEN 150
                                            WHEN a.shelf_type = 8 THEN 90
                                            else 90
                                        END ) AS low_relation_standard  
                        FROM
                                fe_dwd.`dwd_shelf_base_day_all` a
                        WHERE a.SHELF_HANDLE_STATUS = 9
                        GROUP BY a.`MAIN_SHELF_ID`
                ) b
                ON a.SHELF_STATUS = 2
                AND a.REVOKE_STATUS = 1
                AND a.shelf_id = b.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS feods.`high_stock_shelf_tmp`;
CREATE TEMPORARY TABLE feods.high_stock_shelf_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        @month_id AS month_id,
        a.business_name,
        a.`SHELF_ID`,
        a.SHELF_CODE,
        a.shelf_name,
        a.grade,
        a.shelf_type,
        a.bind_cnt,
        d.high_layout_standard, 
        d.low_layout_standard, 
        a.MANAGER_ID,
        a.REAL_NAME,
        a.manager_type,
        a.prewarehouse_code,
        a.prewarehouse_name,
        a.branch_code,
        a.branch_name,
        SUM(IF(a.SHELF_FILL_FLAG = 1,1,0)) AS allow_fill_sku_qty,
        SUM(IF(a.STOCK_QUANTITY > 0,1,0)) AS stock_sku_qty,
        SUM(CASE 
            WHEN a.FILL_MODEL = 1 THEN IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0)
            WHEN a.FILL_MODEL > 0 THEN IF(CEILING(IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0) / a.FILL_MODEL * 3)>
                IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),
                CEILING(IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0) / a.FILL_MODEL * 3))     -- =MIN(ROUNDUP(库存数量/补货规格*3,0)，库存数量)（库存数量小于0时，取0）
        END) AS layout_stock_qty,
        SUM(a.STOCK_QUANTITY) AS total_shelf_stock_qty,
        SUM(a.STOCK_VALUE) AS total_shelf_stock_value,
        SUM(b.STOCK_QUANTITY) AS high_stock_qty,
        SUM(b.STOCK_VALUE) AS high_stock_value,
        ROUND(SUM(a.day_sale_qty * a.SALE_PRICE),2) AS day_sale_gmv,
        ROUND(SUM(a.STOCK_VALUE) / SUM(a.day_sale_qty * a.SALE_PRICE),2) AS turnover_days,
        ROUND(SUM(b.STOCK_VALUE) /  SUM(a.STOCK_VALUE),2) AS high_stock_ratio
FROM
         feods.shelf_product_tmp a  -- feods.shelf_product_tmp a
        LEFT JOIN fe_dm.dm_op_shelf_product_high_stock b   FORCE INDEX(idx_shelf_id_product_id) -- 25560
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
                AND b.month_id = @month_id
        JOIN feods.layout_standard d
                ON a.shelf_id = d.shelf_id
GROUP BY a.shelf_id
;
DELETE FROM fe_dm.dm_op_shelf_high_stock WHERE @stat_date != DATE_FORMAT(CURDATE(),'%Y-%m-01') AND month_id = DATE_FORMAT(SUBDATE(CURDATE(),1),'%Y-%m'); -- 每月1号截存
SET @time_43 := CURRENT_TIMESTAMP();
INSERT INTO fe_dm.dm_op_shelf_high_stock
(
        month_id,
        business_name,
        SHELF_ID,
        SHELF_CODE,
        shelf_name,
        grade,
        shelf_type,
        bind_cnt,
        high_layout_standard, 
        low_layout_standard, 
        MANAGER_ID,
        REAL_NAME,
        manager_type,
        prewarehouse_code,
        prewarehouse_name,
        branch_code,
        branch_name,
        allow_fill_sku_qty,
        stock_sku_qty,
        layout_stock_qty,
        total_shelf_stock_qty,
        total_shelf_stock_value,
        high_stock_qty,
        high_stock_value,
        day_sale_gmv,
        turnover_days,
        high_stock_ratio,
        layout_stock_result
)
SELECT
        month_id,
        business_name,
        SHELF_ID,
        SHELF_CODE,
        shelf_name,
        grade,
        shelf_type,
        bind_cnt,
        high_layout_standard, 
        low_layout_standard, 
        MANAGER_ID,
        REAL_NAME,
        manager_type,
        prewarehouse_code,
        prewarehouse_name,
        branch_code,
        branch_name,
        allow_fill_sku_qty,
        stock_sku_qty,
        layout_stock_qty,
        total_shelf_stock_qty,
        total_shelf_stock_value,
        high_stock_qty,
        high_stock_value,
        day_sale_gmv,
        turnover_days,
        high_stock_ratio,
        CASE
            WHEN total_shelf_stock_value  = 0
                THEN '排面库存为0'
            WHEN layout_stock_qty > high_layout_standard AND turnover_days > 26 
                THEN '排面库存偏高'
            WHEN layout_stock_qty < low_layout_standard AND  turnover_days < 26 
                THEN '排面库存偏低'
            ELSE '排面库存正常'
        END AS layout_stock_result
FROM
        feods.high_stock_shelf_tmp
;
SET @time_45 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_high_stock","@time_43--@time_45",@time_43,@time_45);
-- 地区高库存占比
DELETE FROM fe_dm.dm_op_area_high_stock WHERE @stat_date != DATE_FORMAT(CURDATE(),'%Y-%m-01') AND month_id = DATE_FORMAT(SUBDATE(CURDATE(),1),'%Y-%m'); -- 每月1号截存
SET @time_43 := CURRENT_TIMESTAMP();
INSERT INTO fe_dm.dm_op_area_high_stock
(
        month_id,
        business_name,
        shelf_qty,
        bind_cnt,
        avg_sku_qty,
        avg_stock_value,
        total_stock_value,
        total_high_stock_value,
        high_stock_value_ratio,
        layout_high_stock_shelf_qty,
        layout_low_stock_shelf_qty
)
SELECT 
        @month_id AS month_id,
        b.business_name,
        COUNT(b.shelf_id) AS shelf_qty,
        SUM(d.bind_cnt) AS bind_cnt,    
        ROUND(AVG(c.sku_qty),2) AS avg_sku_qty,
        SUM(c.stock_value) / COUNT(b.shelf_id) AS avg_stock_value,
        SUM(c.stock_value) AS total_stock_value,
        SUM(d.high_stock_value) AS total_high_stock_value,
        ROUND(SUM(d.high_stock_value) / SUM(c.stock_value),2) AS high_stock_value_ratio,
        SUM(IF(d.layout_stock_result = '排面库存偏高',1,0)) AS layout_high_stock_shelf_qty,
        SUM(IF(d.layout_stock_result = '排面库存偏低',1,0)) AS layout_low_stock_shelf_qty
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` b
        LEFT JOIN
        (
                SELECT
                        shelf_id,
                        COUNT(product_id) AS sku_qty,
                        SUM(STOCK_QUANTITY * SALE_PRICE) AS stock_value
                FROM 
                        `fe_dwd`.dwd_shelf_product_day_all
                WHERE STOCK_QUANTITY > 0
                GROUP BY shelf_id
        ) c
                ON b.shelf_id = c.shelf_id
        JOIN `fe_dm`.`dm_op_shelf_high_stock` d
                ON b.shelf_id = d.shelf_id     
                AND d.month_id = @month_id       
GROUP BY b.business_name
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_shelf_product_high_stock',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
END