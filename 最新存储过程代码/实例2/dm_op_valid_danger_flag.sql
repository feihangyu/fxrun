CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_valid_danger_flag`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @pre_day30 := SUBDATE(CURDATE(),INTERVAL 30 DAY);
SET @pre_3month := SUBDATE(CURDATE(),INTERVAL 3 MONTH);
SET @time_1 := CURRENT_TIMESTAMP();
-- 货架商品口径 2min24s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp (
KEY idx_shelf_id_product_id (shelf_id,product_id)
) AS
SELECT
        a.shelf_id,
        a.product_id,
        a.STOCK_QUANTITY,
        a.SALES_FLAG,
        a.DANGER_FLAG,
        a.production_date
FROM
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
                AND b.SHELF_STATUS = 2
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_1--@time_2",@time_1,@time_2);	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`check_stock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.check_stock_tmp (
        shelf_id INT(8),
        product_id INT(8),
        danger_type TINYINT(1),
        KEY `idx_shelf_id_product_id` (`shelf_id`,product_id)
        ) ;
-- ①近30天内货架进行临期盘点，且单个商品盘亏数量≥5 1min28s
INSERT INTO fe_dwd.check_stock_tmp
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
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_2--@time_3",@time_2,@time_3);	
-- ②近30天内对货架风险45商品进行普通盘点，且多个商品（≥3）同时盘亏 1min
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`danger_flag_45_tmp`;
CREATE TEMPORARY TABLE fe_dwd.danger_flag_45_tmp (
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
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_3--@time_4",@time_3,@time_4);	
INSERT INTO fe_dwd.check_stock_tmp
SELECT
        a.shelf_id,
        a.product_id,
        2 AS danger_type
FROM
        fe_dwd.shelf_product_tmp a
        JOIN  fe_dwd.danger_flag_45_tmp b
                ON a.shelf_id = b.shelf_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_4--@time_5",@time_4,@time_5);	
-- ③近30天内有撤架转移商品
INSERT INTO fe_dwd.check_stock_tmp
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
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_5--@time_6",@time_5,@time_6);	
-- ④近30天内补货次数＜1，且商品标识为严重滞销，风险标识为45
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_day30`;
CREATE TEMPORARY TABLE fe_dwd.fill_day30 (
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
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_6--@time_7",@time_6,@time_7);	
INSERT INTO fe_dwd.check_stock_tmp
SELECT
        a.shelf_id,
        a.product_id,
        4 AS danger_type
FROM
        fe_dwd.`shelf_product_tmp` a
        LEFT JOIN fe_dwd.fill_day30 b
            ON a.shelf_id = b.shelf_id
            AND a.product_id = b.product_id
WHERE b.shelf_id IS NULL
        AND a.SALES_FLAG = 5
        AND a.DANGER_FLAG IN (4,5)
;
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_7--@time_8",@time_7,@time_8);	
-- ⑤临期盘点，客服审核商品有标记【虚假盘点】标签
INSERT INTO fe_dwd.check_stock_tmp
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
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_8--@time_9",@time_8,@time_9);	
TRUNCATE TABLE fe_dm.`dm_op_valid_danger_flag`;
INSERT INTO fe_dm.`dm_op_valid_danger_flag`
(
        shelf_id,
        product_id,
        STOCK_QUANTITY,
        danger_type,
        product_date
)
SELECT 
        a.shelf_id,
        a.product_id,
        a.STOCK_QUANTITY,
        b.danger_type,
        a.production_date AS product_date
FROM
        fe_dwd.`shelf_product_tmp` a
        JOIN fe_dwd.check_stock_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_valid_danger_flag","@time_9--@time_10",@time_9,@time_10);	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_valid_danger_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_valid_danger_flag','dm_op_valid_danger_flag','宋英南');
 
COMMIT;
	END