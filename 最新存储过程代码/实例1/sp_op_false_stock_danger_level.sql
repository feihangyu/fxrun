CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_false_stock_danger_level`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @cdate := CURDATE();
SET @pre_day90 := SUBDATE(@cdate,90);
SET @pre_day90_ym := DATE_FORMAT(@pre_day90,"%Y-%m");
SET @pre_day90_ym_num := DAY(@pre_day90);
SET @cdate_ym := DATE_FORMAT(@cdate,"%Y-%m");
SET @cdate_num := DAY(@cdate);
SET @pre_1month_ym := DATE_FORMAT(SUBDATE(@cdate,INTERVAL 1 MONTH),"%Y-%m");
SET @pre_2month_ym := DATE_FORMAT(SUBDATE(@cdate,INTERVAL 2 MONTH),"%Y-%m");
SET @pre_day30 := SUBDATE(@cdate,30);
SET @pre_day60 := SUBDATE(@cdate,60);
-- 口径 1min16s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_info`;   
CREATE TEMPORARY TABLE feods.shelf_product_info (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        b.`business_name`,
        a.`SHELF_ID`,
        b.`SHELF_CODE`,
        a.`PRODUCT_ID`,
        c.PRODUCT_CODE2,
        c.PRODUCT_NAME,
        a.STOCK_QUANTITY,
        a.STOCK_QUANTITY * a.SALE_PRICE AS stock_value,
        a.DANGER_FLAG,
        a.SALES_FLAG,
        a.SHELF_FILL_FLAG,
        b.SF_CODE
FROM
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
                AND b.SHELF_STATUS = 2 
                AND b.shelf_type IN (1,2,3,5)
        JOIN `fe_dwd`.`dwd_product_base_day_all` c
                ON a.product_id = c.product_id
WHERE a.STOCK_QUANTITY > 0
;
-- =================================================================================
-- 一、单品风险等级
-- ①连续90天库存无变化且库存数量≠0  1min30s
-- 开始日期和结束日期库存相等
DROP TEMPORARY TABLE IF EXISTS feods.`total_stock_qty`;   
SET @str :=CONCAT(
"CREATE TEMPORARY TABLE feods.total_stock_qty (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.`SHELF_ID`,
        a.`PRODUCT_ID`,
        b.DAY",@pre_day90_ym_num,"_QUANTITY AS stock_qty 
FROM
        feods.`shelf_product_info` a
        join fe.`sf_shelf_product_stock_detail` b
                on b.STAT_DATE = ","'",@pre_day90_ym,"'","
                and b.DAY",@pre_day90_ym_num,"_QUANTITY > 0
                and a.shelf_id = b.shelf_id 
                and a.product_id = b.product_id
        join fe.`sf_shelf_product_stock_detail` c
                on c.STAT_DATE = ","'",@cdate_ym,"'","
                and c.DAY",@cdate_num,"_QUANTITY  > 0
                and b.DAY",@pre_day90_ym_num,"_QUANTITY = c.DAY",@cdate_num,"_QUANTITY
                AND a.shelf_id = c.shelf_id 
                AND a.product_id = c.product_id
;");
PREPARE str_exe FROM @str;
EXECUTE str_exe;
-- 每个月1号库存是否有变化 30s
DROP TEMPORARY TABLE IF EXISTS feods.`stock_day90_tmp`;   
CREATE TEMPORARY TABLE feods.stock_day90_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`,
        a.stock_qty
FROM 
        feods.total_stock_qty a
        JOIN  fe.`sf_shelf_product_stock_detail` b
                ON a.shelf_id = b.`SHELF_ID`
                AND a.product_id = b.`PRODUCT_ID`
                AND b.stat_date = @pre_1month_ym 
                AND a.stock_qty = b.DAY1_QUANTITY
        JOIN  fe.`sf_shelf_product_stock_detail` c
                ON b.SHELF_DETAIL_ID = c.SHELF_DETAIL_ID
                AND c.stat_date = @pre_2month_ym 
                AND a.stock_qty = c.DAY1_QUANTITY 
;
-- ②今天最后一次上架时间大于保质期且当前库存数量≠0   5min
DROP TEMPORARY TABLE IF EXISTS feods.`fill_tmp1`;
CREATE TEMPORARY TABLE feods.fill_tmp1 (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.SHELF_ID,
        a.PRODUCT_ID,
        b.FILL_TIME
FROM 
        feods.shelf_product_info a
        JOIN fe_dm.`dm_op_shelf_product_fill_last_time` b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
                AND b.FILL_TYPE IN (1,2,3,4,7,8,9)
                AND b.FILL_TIME IS NOT NULL
;
DROP TEMPORARY TABLE IF EXISTS feods.`save_time_tmp`;   
CREATE TEMPORARY TABLE feods.save_time_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        feods.fill_tmp1 a
        JOIN feods.zs_area_stock_detail c
                ON a.shelf_id = c.`shelf_id`
                AND a.product_id = c.`product_id`
WHERE DATEDIFF(@cdate,a.FILL_TIME)  > c.save_time_days
;
-- ③今天-淘汰时间大于365且当前库存数量≠0 4s
DROP TEMPORARY TABLE IF EXISTS feods.`out_date_tmp`;   
CREATE TEMPORARY TABLE feods.out_date_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT 
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        feods.shelf_product_info a
        JOIN feods.`zs_product_dim_sserp` b
                ON a.`business_name` = b.business_area
                AND a.product_id = b.`PRODUCT_ID`
WHERE b.PRODUCT_TYPE IN ('退出','预淘汰','淘汰（替补）') 
        AND b.out_date IS NOT NULL
        AND DATEDIFF(@cdate,b.out_date) > 365
;
-- ①商品类型为淘汰品&停止补货&30天销量为0&架上库存≤3，
-- ④逻辑1计算后货架疑似虚库存个数大于10      1min
DROP TEMPORARY TABLE IF EXISTS feods.`sale30_tmp`;   
CREATE TEMPORARY TABLE feods.sale30_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.shelf_id,
        a.product_id,
        SUM(a.quantity) AS sal_qty
FROM
        `fe_dwd`.`dwd_order_item_refund_day` a
WHERE a.PAY_DATE >= @pre_day30 
GROUP BY a.shelf_id,a.product_id
HAVING SUM(a.quantity) > 0
;
DROP TEMPORARY TABLE IF EXISTS feods.`out_tmp`;   
CREATE TEMPORARY TABLE feods.out_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        feods.shelf_product_info a
        JOIN feods.`zs_product_dim_sserp` b
                ON a.`business_name` = b.business_area
                AND a.product_id = b.`PRODUCT_ID`
WHERE b.PRODUCT_TYPE IN ('退出','预淘汰','淘汰（替补）') 
        AND a.STOCK_QUANTITY <= 3
        AND a.SHELF_FILL_FLAG = 2
;
DROP TEMPORARY TABLE IF EXISTS feods.`sale30_out_tmp`;   
CREATE TEMPORARY TABLE feods.sale30_out_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        feods.out_tmp a
        LEFT JOIN feods.sale30_tmp d
                ON a.shelf_id = d.shelf_id
                AND a.product_id = d.product_id
WHERE d.shelf_id IS NULL
;
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_sale30_out_tmp`;   
CREATE TEMPORARY TABLE feods.shelf_sale30_out_tmp (
        KEY idx_shelf_id(shelf_id)
        ) AS
SELECT
        a.shelf_id
FROM 
        feods.sale30_out_tmp a
GROUP BY a.shelf_id
HAVING COUNT(*) > 10
;
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_sale30_out_tmp`;   
CREATE TEMPORARY TABLE feods.shelf_product_sale30_out_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM 
        feods.sale30_out_tmp a
        JOIN feods.shelf_sale30_out_tmp b
                ON a.shelf_id = b.shelf_id
;
-- 5.连续60天零销售且60天内有盘盈盘亏记录且当前库存数量≠0（得分1）
-- 近60天有销售 42s
DROP TEMPORARY TABLE IF EXISTS feods.`sale_day60_tmp`;   
CREATE TEMPORARY TABLE feods.sale_day60_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        DISTINCT 
        shelf_id,
        product_id
FROM
        fe_dwd.`dwd_pub_order_item_recent_two_month`
WHERE pay_date >= @pre_day60
;
-- 近60天有盘盈亏 1min41s
DROP TEMPORARY TABLE IF EXISTS feods.`check_day60_tmp`;   
CREATE TEMPORARY TABLE feods.check_day60_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT
        DISTINCT 
        shelf_id,
        product_id
FROM
        fe_dwd.`dwd_check_base_day_inc` 
WHERE OPERATE_TIME >= @pre_day60
        AND ERROR_NUM != 0
;
DROP TEMPORARY TABLE IF EXISTS feods.`sale_check_day60`;   
CREATE TEMPORARY TABLE feods.sale_check_day60 (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT 
        a.shelf_id,
        a.product_id
FROM
        feods.shelf_product_info a
        LEFT JOIN feods.sale_day60_tmp b
            ON a.shelf_id = b.shelf_id
            AND a.product_id = b.product_id
        JOIN feods.check_day60_tmp c
            ON a.shelf_id = c.shelf_id
            AND a.product_id = c.product_id
WHERE b.shelf_id IS NULL
;
-- ====================================================================================
-- 二、货架风险等级
-- 商品货架风险等级=2*近两个月虚假盘点次数+1*近两个月盘点失误次数+average(近两个月货架等级得分)
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_false_check_count_tmp`;   
CREATE TEMPORARY TABLE feods.shelf_false_check_count_tmp (
        KEY idx_shelf_id(shelf_id)
        ) AS
SELECT 
        b.shelf_id,
        SUM(IF(a.is_fake_check = 1,1,0)) AS false_check_qty,
        SUM(IF(a.is_error_check = 1,1,0)) AS check_error_qty
FROM
        fe.`sf_check_audit_record` a
        JOIN fe.sf_shelf_check b
                ON a.check_id = b.check_id
                AND a.audit_status = 1
                AND a.data_flag = 1
                AND b.data_flag = 1
                AND b.OPERATE_TIME >= DATE_FORMAT(SUBDATE(CURDATE(),INTERVAL 1 MONTH),'%Y-%m-01')
                AND b.OPERATE_TIME < DATE_FORMAT(CURDATE(),'%Y-%m-01')
WHERE a.is_fake_check = 1 OR a.is_error_check = 1
GROUP BY b.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_level_tmp`;   
CREATE TEMPORARY TABLE feods.shelf_level_tmp (
        KEY idx_shelf_id(shelf_id)
        ) AS
SELECT
    a.shelf_id,
    AVG(
        CASE 
            WHEN a.grade = '新装' THEN 0
            WHEN a.grade = '甲' THEN 2
            WHEN a.grade = '乙' THEN 4
            WHEN a.grade = '丙' THEN 6
            WHEN a.grade = '丁' THEN 8
            ELSE 0
        END) AS shelf_level_score
FROM
    feods.`d_op_shelf_grade` a
WHERE month_id IN (@pre_1month_ym,@pre_2month_ym)
GROUP BY a.shelf_id
;
-- ===============================================================================
-- 三、店主风险等级
DROP TEMPORARY TABLE IF EXISTS feods.`manager_level_tmp`;   
CREATE TEMPORARY TABLE feods.manager_level_tmp (
        KEY idx_sf_code(sf_code)
        ) AS
SELECT
        a.SF_CODE,
        CASE
            WHEN star_level = '一星店主' THEN 8
            WHEN star_level = '二星店主' THEN 6
            WHEN star_level = '三星店主' THEN 4
            WHEN star_level = '四星店主' THEN 2
            WHEN star_level = '五星店主' THEN 0
            WHEN star_level = '取消星级评定资格' THEN 10
        END AS manager_danger_level
FROM
        feods.`d_op_manager_star` a
;
-- ==============================================================================
-- 四、综合风险等级
-- 综合风险等级=0.15*min(单品风险等级,10)+0.35*min(货架风险等级,10)+0.5*min(店主风险等级,10)
DROP TEMPORARY TABLE IF EXISTS feods.`com_danger_level_tmp`;   
CREATE TEMPORARY TABLE feods.com_danger_level_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT 
        a.`business_name`,
        a.`SHELF_ID`,
        a.`SHELF_CODE`,
        a.`PRODUCT_ID`,
        a.PRODUCT_CODE2,
        a.PRODUCT_NAME,
        a.STOCK_QUANTITY,
        a.stock_value,
        a.DANGER_FLAG,
        a.SALES_FLAG,
        IF(b.shelf_id,4,0) + IF(c.shelf_id,3,0) + IF(d.shelf_id,3,0) + IF(e.shelf_id,3,0) + IF(i.shelf_id,2,0) AS shelf_product_danger_level,   -- 单品风险等级
        2 * IFNULL(f.false_check_qty,0) + IFNULL(f.check_error_qty,0) + g.shelf_level_score AS shelf_danger_level,  -- 货架风险等级
        IFNULL(h.manager_danger_level,0) AS manager_danger_level,    -- 店主风险等级
        0.3 * IF(IF(b.shelf_id,4,0) + IF(c.shelf_id,3,0) + IF(d.shelf_id,3,0) + IF(e.shelf_id,3,0) + IF(i.shelf_id,2,0) > 10,10,IF(b.shelf_id,4,0) + IF(c.shelf_id,3,0) + IF(d.shelf_id,3,0) + IF(e.shelf_id,3,0) + IF(i.shelf_id,2,0)) + 
            0.35 * IF(2 * IFNULL(f.false_check_qty,0) + IFNULL(f.check_error_qty,0) + g.shelf_level_score > 10,10,2 * IFNULL(f.false_check_qty,0) + IFNULL(f.check_error_qty,0) + g.shelf_level_score) + 
            0.35  * IF(IFNULL(h.manager_danger_level,0) > 10,10,IFNULL(h.manager_danger_level,0)) AS com_danger_level  -- 综合风险等级
FROM
        feods.shelf_product_info a
        LEFT JOIN feods.stock_day90_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
        LEFT JOIN feods.save_time_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.product_id = c.product_id
        LEFT JOIN feods.out_date_tmp d
                ON a.shelf_id = d.shelf_id
                AND a.product_id = d.product_id
        LEFT JOIN feods.shelf_product_sale30_out_tmp e
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id
        LEFT JOIN feods.shelf_false_check_count_tmp f
                ON a.shelf_id = f.shelf_id
        LEFT JOIN feods.shelf_level_tmp g
                ON a.shelf_id = g.shelf_id
        LEFT JOIN feods.manager_level_tmp h
                ON a.SF_CODE = h.SF_CODE
        LEFT JOIN feods.sale_check_day60  i
                ON a.shelf_id = i.shelf_id
                AND a.product_id = i.product_id
;
TRUNCATE TABLE `fe_dm`.dm_op_false_stock_danger_level;
INSERT INTO `fe_dm`.dm_op_false_stock_danger_level
(
        business_name,
        SHELF_ID,
        SHELF_CODE,
        PRODUCT_ID,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        STOCK_QUANTITY,
        stock_value,
        DANGER_FLAG,
        SALES_FLAG,
        shelf_product_danger_level,
        shelf_danger_level,
        manager_danger_level,
        com_danger_level,
        false_stock_danger_level
)
SELECT
        business_name,
        SHELF_ID,
        SHELF_CODE,
        PRODUCT_ID,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        STOCK_QUANTITY,
        stock_value,
        DANGER_FLAG,
        SALES_FLAG,
        shelf_product_danger_level,
        shelf_danger_level,
        manager_danger_level,
        com_danger_level,
        CASE
            WHEN com_danger_level > 8 THEN '高风险'        -- 严重风险
            WHEN com_danger_level > 6 THEN '高风险'
            WHEN com_danger_level > 4 THEN '中风险'
            WHEN com_danger_level > 2 THEN '低风险'
            ELSE '无风险'
        END AS false_stock_danger_level
FROM
        feods.com_danger_level_tmp
;
COMMIT; 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_false_stock_danger_level',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
END