CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_manager_product_trans_list`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := CURDATE();
SET @pre_day30 := SUBDATE(CURDATE(),INTERVAL 30 DAY);
-- 货架、店主、分部关系表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`manager_branch_tmp`;
CREATE TEMPORARY TABLE fe_dwd.manager_branch_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.SHELF_ID,
        a.MANAGER_ID,
        a.MANAGER_NAME,
        a.BRANCH_NAME,
        a.BRANCH_CODE
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` a
WHERE a.SHELF_STATUS = 2
        AND a.manager_type = '全职店主'
;
-- 调货店主_商品清单(严重滞销)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`manager_product_unsale_out`;
CREATE TEMPORARY TABLE fe_dwd.manager_product_unsale_out(
        KEY idx_manager_id_product_id(manager_id,product_id)
) AS 
SELECT 
        b.business_name AS BUSINESS_AREA,
        g.BRANCH_NAME,
        g.BRANCH_CODE,
        g.MANAGER_ID,
        g.MANAGER_NAME,
        a.PRODUCT_ID,
        c.PRODUCT_NAME,
        c.FILL_MODEL,
        SUM(a.STOCK_QUANTITY) AS stock_qty,
        SUM(a.STOCK_QUANTITY * a.SALE_PRICE) AS sale_value,
        COUNT(a.shelf_id) AS shelf_qty
FROM 
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.SHELF_ID
                AND b.SHELF_STATUS = 2
                AND b.shelf_type IN (1, 2, 3)
                AND a.STOCK_QUANTITY >= 20
                AND a.NEW_FLAG = 2 
                AND a.SALES_FLAG = 5    #判断货架商品严重滞销
        JOIN `fe_dwd`.`dwd_product_base_day_all` c
                ON a.PRODUCT_ID = c.PRODUCT_ID
                AND c.FILL_MODEL <= 10
        JOIN fe_dwd.manager_branch_tmp g
                ON a.shelf_id = g.shelf_id
GROUP BY 
        g.MANAGER_ID,
        a.PRODUCT_ID
;
-- 调货店主_商品清单(非严重滞销)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`manager_product_sale_out`;
CREATE TEMPORARY TABLE fe_dwd.manager_product_sale_out(
        KEY idx_manager_id_product_id(manager_id,product_id)
) AS 
SELECT 
        b.business_name AS BUSINESS_AREA,
        g.BRANCH_NAME,
        g.BRANCH_CODE,
        g.MANAGER_ID,
        g.MANAGER_NAME,
        a.PRODUCT_ID,
        c.PRODUCT_NAME,
        c.FILL_MODEL,
        SUM(a.STOCK_QUANTITY) AS stock_qty,
        SUM(a.STOCK_QUANTITY * a.SALE_PRICE) AS sale_value,
        COUNT(a.shelf_id) AS shelf_qty
FROM 
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.SHELF_ID
                AND b.SHELF_STATUS = 2
                AND b.shelf_type IN (1, 2, 3)
                AND a.STOCK_QUANTITY >= 50
                AND a.NEW_FLAG = 2 
                AND a.SALES_FLAG <> 5   #判断货架商品非严重滞销
        JOIN `fe_dwd`.`dwd_product_base_day_all` c
                ON a.PRODUCT_ID = c.PRODUCT_ID
                AND c.FILL_MODEL <= 10
        JOIN fe_dwd.manager_branch_tmp g
                ON a.shelf_id = g.shelf_id
GROUP BY 
        g.MANAGER_ID,
        a.PRODUCT_ID
;
# 近30天销量和GMV
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_day30`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_day30(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        shelf_id,
        product_id,
        SUM(QUANTITY) AS sale_qty30,
        SUM(QUANTITY * sale_price) AS sale_amount30
FROM 
        fe_dwd.`dwd_pub_order_item_recent_one_month` 
WHERE PAY_DATE >= @pre_day30
GROUP BY shelf_id,product_id
;
-- 调货货架_商品清单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_out`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_out (
        business_area VARCHAR(32),
        MANAGER_ID INT(20),
        MANAGER_name VARCHAR(50),
        shelf_id INT(8),
        shelf_code VARCHAR(20),
        SHELF_NAME VARCHAR(50),
        product_id INT(8),
        PRODUCT_CODE2 VARCHAR(20),
        PRODUCT_NAME VARCHAR(50),
        SALE_PRICE DECIMAL(8,2),
        SALES_FLAG TINYINT(2),
        fill_model TINYINT(2),
        STOCK_QUANTITY INT(3),
        stock_value DECIMAL(8,2),
        sale_qty30 INT(4),
        sale_amount30 DECIMAL(8,2),
        FIRST_FILL_TIME DATETIME,
        retain_qty INT(3),
        trans_out_qty INT(3),
        KEY `idx_shelf_id_product_id` (`shelf_id`,product_id)
        ) ;
INSERT INTO fe_dwd.shelf_product_out
SELECT 
        c.business_name AS BUSINESS_AREA,
        c.MANAGER_ID,
        c.MANAGER_name,
        b.shelf_id,
        c.shelf_code,
        c.SHELF_NAME,
        b.product_id,
        h.PRODUCT_CODE2,
        h.PRODUCT_NAME,
        b.SALE_PRICE,
        b.SALES_FLAG,
        h.fill_model,
        b.STOCK_QUANTITY,
        b.STOCK_QUANTITY * b.SALE_PRICE AS stock_value,
        IFNULL(d.sale_qty30,0) AS sale_qty30,
        IFNULL(d.sale_amount30,0) AS sale_amount30,
        b.FIRST_FILL_TIME,
        CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5 AS retain_qty,
        b.STOCK_QUANTITY - (CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5) AS trans_out_qty
FROM 
        fe_dwd.manager_product_unsale_out a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.product_id = b.product_id
                AND b.STOCK_QUANTITY >= 20
                AND b.NEW_FLAG = 2 
                AND b.SALES_FLAG = 5
                AND b.FIRST_FILL_TIME < SUBDATE(CURDATE(),INTERVAL 30 DAY)
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON b.shelf_id = c.SHELF_ID
                AND c.SHELF_STATUS = 2 
                AND c.shelf_type IN (1, 2, 3)
        LEFT JOIN fe_dwd.shelf_product_day30 d
                ON b.shelf_id = d.shelf_id
                AND b.product_id = d.product_id
        JOIN fe_dwd.manager_branch_tmp f
                ON b.shelf_id = f.shelf_id
                AND a.MANAGER_ID = f.MANAGER_ID
        JOIN `fe_dwd`.`dwd_product_base_day_all` h
                ON a.PRODUCT_ID = h.PRODUCT_ID
                AND h.FILL_MODEL <= 10
WHERE b.STOCK_QUANTITY - (CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5)  >= 5 
        AND IFNULL(d.sale_qty30,0) < 2
;
INSERT INTO fe_dwd.shelf_product_out
SELECT 
        c.business_name AS BUSINESS_AREA,
        c.MANAGER_ID,
        c.MANAGER_name,
        b.shelf_id,
        c.shelf_code,
        c.SHELF_NAME,
        b.product_id,
        h.PRODUCT_CODE2,
        h.PRODUCT_NAME,
        b.SALE_PRICE,
        b.SALES_FLAG,
        h.fill_model,
        b.STOCK_QUANTITY,
        b.STOCK_QUANTITY * b.SALE_PRICE AS stock_value,
        IFNULL(d.sale_qty30,0) AS sale_qty30,
        IFNULL(d.sale_amount30,0) AS sale_amount30,
        b.FIRST_FILL_TIME,
        IF(CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5 > 50,CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5,50) AS retain_qty,
        b.STOCK_QUANTITY - IF(CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5 > 50,CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5,50) AS trans_out_qty
FROM 
        fe_dwd.manager_product_sale_out a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.product_id = b.product_id
                AND b.STOCK_QUANTITY >= 50
                AND b.NEW_FLAG = 2 
                AND b.SALES_FLAG <> 5
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON b.shelf_id = c.SHELF_ID
                AND c.SHELF_STATUS = 2 
                AND c.shelf_type IN (1, 2, 3)
        LEFT JOIN fe_dwd.shelf_product_day30 d
                ON b.shelf_id = d.shelf_id
                AND b.product_id = d.product_id
        JOIN fe_dwd.manager_branch_tmp f
                ON b.shelf_id = f.shelf_id
                AND a.MANAGER_ID = f.MANAGER_ID
        JOIN `fe_dwd`.`dwd_product_base_day_all` h
                ON a.PRODUCT_ID = h.PRODUCT_ID
                AND h.FILL_MODEL <= 10
WHERE b.STOCK_QUANTITY - IF(CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5 > 50,CEILING((IFNULL(d.sale_qty30,0) / 30) * 28) + 5,50) >= 5
;
-- 接收的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_in`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_in(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        c.business_name AS BUSINESS_AREA,
        c.MANAGER_ID,
        c.MANAGER_name,
        b.shelf_id,
        c.shelf_code,
        c.SHELF_NAME,
        b.product_id,
        h.PRODUCT_CODE2,
        h.PRODUCT_NAME,
        b.SALE_PRICE,
        b.SALES_FLAG,
        h.fill_model,
        b.STOCK_QUANTITY,
        b.STOCK_QUANTITY * b.SALE_PRICE AS stock_value,
        d.sale_qty30,
        d.sale_amount30,
        b.FIRST_FILL_TIME,
        IF(d.sale_qty30 / 30 * 28 < 50,d.sale_qty30 / 30 * 28 , 50) - b.STOCK_QUANTITY AS allow_trans_in_qty
FROM 
        fe_dwd.manager_product_unsale_out a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.product_id = b.product_id
                AND b.NEW_FLAG = 2 
                AND b.SALES_FLAG IN (1,2,3)
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON b.shelf_id = c.SHELF_ID
                AND c.SHELF_STATUS = 2 
                AND c.shelf_type IN (1, 2, 3)
        JOIN fe_dwd.shelf_product_day30 d
                ON b.shelf_id = d.shelf_id
                AND b.product_id = d.product_id
        JOIN fe_dwd.manager_branch_tmp f
                ON b.shelf_id = f.shelf_id
                AND a.MANAGER_ID = f.MANAGER_ID
        JOIN `fe_dwd`.`dwd_product_base_day_all` h
                ON a.PRODUCT_ID = h.PRODUCT_ID
;
-- 前三个可调入货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`allow_shelf_product_in_top3`;
CREATE TEMPORARY TABLE fe_dwd.allow_shelf_product_in_top3(
        KEY idx_manager_id_product_id(manager_id,product_id)
) AS 
SELECT
        a.manager_id,
        a.product_id,
        SUBSTRING_INDEX(a.allow_trans_in_shelf,',',1) AS allow_trans_in_shelf1,
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(a.allow_trans_in_shelf,',',2),',',-1) = 
                SUBSTRING_INDEX(a.allow_trans_in_shelf,',',1) ,NULL,SUBSTRING_INDEX(SUBSTRING_INDEX(a.allow_trans_in_shelf,',',2),',',-1) )
                AS allow_trans_in_shelf2,
        IF(SUBSTRING_INDEX(SUBSTRING_INDEX(a.allow_trans_in_shelf,',',3),',',-1) =
                SUBSTRING_INDEX(SUBSTRING_INDEX(a.allow_trans_in_shelf,',',2),',',-1),NULL,SUBSTRING_INDEX(SUBSTRING_INDEX(a.allow_trans_in_shelf,',',3),',',-1)) 
                AS allow_trans_in_shelf3
FROM
(
        SELECT 
                manager_id,
                product_id,
                GROUP_CONCAT(shelf_code,'_',ROUND(allow_trans_in_qty) ORDER BY allow_trans_in_qty DESC ) AS allow_trans_in_shelf
        FROM 
                fe_dwd.shelf_product_in
        WHERE allow_trans_in_qty >= 5
        GROUP BY MANAGER_ID,product_id
) a
;
-- 保留两个月的数据
DELETE FROM fe_dm.dm_op_manager_product_trans_list WHERE stat_date < SUBDATE(@stat_date,INTERVAL 2 MONTH) OR stat_date = @stat_date;       
INSERT INTO fe_dm.dm_op_manager_product_trans_list
(
        stat_date,
        BUSINESS_AREA,
        MANAGER_ID,
        MANAGER_NAME,
        shelf_id,
        shelf_code,
        SHELF_NAME,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        SALE_PRICE,
        SALES_FLAG,
        fill_model,
        STOCK_QUANTITY,
        stock_value,
        sale_qty30,
        sale_amount30,
        FIRST_FILL_TIME,
        retain_qty,
        trans_out_qty,
        allow_trans_in_shelf1,
        allow_trans_in_shelf2,
        allow_trans_in_shelf3
)
SELECT 
        @stat_date AS stat_date,
        a.BUSINESS_AREA,
        a.MANAGER_ID,
        a.MANAGER_name,
        a.shelf_id,
        a.shelf_code,
        a.SHELF_NAME,
        a.product_id,
        a.PRODUCT_CODE2 AS PRODUCT_FE,
        a.PRODUCT_NAME,
        a.SALE_PRICE,
        a.SALES_FLAG,
        a.fill_model,
        a.STOCK_QUANTITY,
        a.stock_value,
        a.sale_qty30,
        a.sale_amount30,
        a.FIRST_FILL_TIME,
        a.retain_qty,
        a.trans_out_qty,
        b.allow_trans_in_shelf1,
        b.allow_trans_in_shelf2,
        b.allow_trans_in_shelf3
FROM
        fe_dwd.shelf_product_out a
        JOIN fe_dwd.allow_shelf_product_in_top3 b
                ON a.manager_id = b.manager_id
                AND a.product_id = b.product_id
ORDER BY a.BUSINESS_AREA,a.MANAGER_ID,a.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_manager_product_trans_list',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_manager_product_trans_list','dm_op_manager_product_trans_list','宋英南');
END