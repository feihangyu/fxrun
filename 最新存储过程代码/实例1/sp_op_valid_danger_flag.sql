CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_valid_danger_flag`()
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
 
        SET l_task_name = 'sp_op_valid_danger_flag';
        SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
        
SET @pre_day30 := SUBDATE(CURDATE(),INTERVAL 30 DAY);
SET @pre_3month := SUBDATE(CURDATE(),INTERVAL 3 MONTH);
-- 货架商品口径 2min24s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_tmp (
KEY idx_shelf_id_product_id (shelf_id,product_id)
) AS
SELECT
--         b.business_name,
        a.shelf_id,
--         b.SHELF_CODE,
--         b.SF_CODE,
--         b.REAL_NAME,
        a.product_id,
--         c.PRODUCT_CODE2,
--         c.PRODUCT_NAME,
        a.STOCK_QUANTITY,
        a.SALES_FLAG,
        a.DANGER_FLAG,
        a.production_date
FROM
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
                AND b.SHELF_STATUS = 2
--         JOIN `fe_dwd`.`dwd_product_base_day_all` c
--                 ON a.product_id = c.product_id
;
DROP TEMPORARY TABLE IF EXISTS feods.`check_stock_tmp`;
CREATE TEMPORARY TABLE feods.check_stock_tmp (
        shelf_id INT(8),
        product_id INT(8),
        danger_type TINYINT(1),
        KEY `idx_shelf_id_product_id` (`shelf_id`,product_id)
        ) ;
-- ①近30天内货架进行临期盘点，且单个商品盘亏数量≥5 1min28s
INSERT INTO feods.check_stock_tmp
SELECT
        a.shelf_id,
        a.product_id,
        1 AS danger_type
FROM
        `fe_dwd`.`dwd_check_base_day_inc` a
WHERE a.OPERATE_TIME >= @pre_day30
        AND a.check_type = 2
GROUP BY a.shelf_id,a.product_id
HAVING SUM(ERROR_NUM) <= -5
;
-- ②近30天内对货架风险45商品进行普通盘点，且多个商品（≥3）同时盘亏 1min
DROP TEMPORARY TABLE IF EXISTS feods.`danger_flag_45_tmp`;
CREATE TEMPORARY TABLE feods.danger_flag_45_tmp (
KEY idx_shelf_id (shelf_id)
) AS
SELECT
        a.shelf_id
FROM
        `fe_dwd`.`dwd_check_base_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
                AND b.DANGER_FLAG IN (4,5)
                AND a.check_type = 1
                AND a.OPERATE_TIME >= @pre_day30
                AND a.ERROR_NUM < 0
GROUP BY a.shelf_id
HAVING COUNT(*) >= 3
;
INSERT INTO feods.check_stock_tmp
SELECT
        a.shelf_id,
        a.product_id,
        2 AS danger_type
FROM
        feods.shelf_product_tmp a
        JOIN  feods.danger_flag_45_tmp b
                ON a.shelf_id = b.shelf_id
;
-- ③近30天内有撤架转移商品
INSERT INTO feods.check_stock_tmp
SELECT
    DISTINCT 
    a.shelf_id,
    a.`PRODUCT_ID`,
    3 AS danger_type
FROM
    fe_dwd.`dwd_fill_day_inc` a
WHERE a.FILL_TIME >= @pre_day30
    AND a.FILL_TYPE = 4
;
-- ④近30天内补货次数＜1，且商品标识为严重滞销，风险标识为45
DROP TEMPORARY TABLE IF EXISTS feods.`fill_day30`;
CREATE TEMPORARY TABLE feods.fill_day30 (
KEY idx_shelf_id_product_id (shelf_id,product_id)
) AS
SELECT
        DISTINCT
        a.shelf_id,
        a.product_id
FROM
        fe_dwd.`dwd_fill_day_inc` a
WHERE a.FILL_TIME >= @pre_day30
;
INSERT INTO feods.check_stock_tmp
SELECT
        a.shelf_id,
        a.product_id,
        4 AS danger_type
FROM
        feods.`shelf_product_tmp` a
        LEFT JOIN feods.fill_day30 b
            ON a.shelf_id = b.shelf_id
            AND a.product_id = b.product_id
WHERE b.shelf_id IS NULL
        AND a.SALES_FLAG = 5
        AND a.DANGER_FLAG IN (4,5)
;
-- ⑤临期盘点，客服审核商品有标记【虚假盘点】标签
INSERT INTO feods.check_stock_tmp
SELECT
        shelf_id,
        product_id,
        5 AS danger_type
FROM
        fe_dwd.`dwd_check_base_day_inc`
WHERE OPERATE_TIME >= @pre_day30
        AND 10 = SUBSTRING(ATTRIBUTE2,1) 
GROUP BY shelf_id,product_id
;
-- 最后一次库存变动记录 24min
-- DROP TEMPORARY TABLE IF EXISTS feods.`stock_change_tmp`;
-- CREATE TEMPORARY TABLE feods.stock_change_tmp (
-- KEY idx_warehouse_id_product_id (warehouse_id,product_id)
-- ) AS
-- SELECT
--         warehouse_id,
--         product_id,
--         SUBSTRING_INDEX(GROUP_CONCAT(change_quantity ORDER BY add_time DESC SEPARATOR ','),',',1) AS change_quantity,
--         SUBSTRING_INDEX(GROUP_CONCAT(source_type ORDER BY add_time DESC SEPARATOR ','),',',1) AS source_type,
--         MAX(add_time) AS add_time
-- FROM
--         fe.`sf_prewarehouse_stock_record_7`
-- GROUP BY warehouse_id,product_id
-- ;
TRUNCATE TABLE fe_dm.`dm_op_valid_danger_flag`;
INSERT INTO fe_dm.`dm_op_valid_danger_flag`
(
--         business_name,
        shelf_id,
--         SHELF_CODE,
--         SF_CODE,
--         REAL_NAME,
        product_id,
--         PRODUCT_CODE2,
--         PRODUCT_NAME,
        STOCK_QUANTITY,
--         change_quantity,
--         source_type,
--         change_time,
        danger_type,
        product_date
)
SELECT 
--         a.business_name,
        a.shelf_id,
--         a.SHELF_CODE,
--         a.SF_CODE,
--         a.REAL_NAME,
        a.product_id,
--         a.PRODUCT_CODE2,
--         a.PRODUCT_NAME,
        a.STOCK_QUANTITY,
--         c.change_quantity,
--         c.source_type,
--         c.add_time AS change_time,
        b.danger_type,
        a.production_date AS product_date
FROM
        feods.`shelf_product_tmp` a
        JOIN feods.check_stock_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
--         LEFT JOIN feods.stock_change_tmp c
--                 ON a.shelf_id = c.warehouse_id
--                 AND a.product_id = c.product_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_valid_danger_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
END