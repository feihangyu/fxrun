CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_stock_reach_ratio`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/04/14
-- Modify date: 
-- Description:	
-- 	库存满足率（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @month_id := DATE_FORMAT(@stat_date,'%Y%m');
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
SET @time_1 := CURRENT_TIMESTAMP();
-- 当月货架有库存天数、当月货架正常运营天数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_stock`;
CREATE TEMPORARY TABLE fe_dwd.shelf_stock(
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM(stock_quantity > 0) AS month_stock_days,
        SUM(REVOKE_STATUS != 7) AS month_operate_days,
        SUM(
                CASE 
                        WHEN (a.grade IN ('甲','乙','新装') AND a.shelf_type IN (1,3) AND stock_quantity >= 180) OR (a.grade IN ('甲','乙','新装') AND a.shelf_type IN (2,5) AND stock_quantity >= 110) THEN 1
                        WHEN (a.grade IN ('丙','丁') AND a.shelf_type IN (1,3) AND stock_quantity >= 110) OR (a.grade IN ('丙','丁') AND a.shelf_type IN (2,5) AND stock_quantity >= 90) THEN 1
                        WHEN (a.grade IN ('甲','乙','新装') AND IFNULL(stock_quantity,0) + IFNULL(c.sec_stock_quantity,0) >= 300) OR (a.grade IN ('丙','丁') AND IFNULL(stock_quantity,0) + IFNULL(c.sec_stock_quantity,0) >= 200) THEN 1
                        ELSE 0
                END
        ) AS month_stock_reach_days
FROM
        fe_dwd.`dwd_shelf_day_his` a
        LEFT JOIN 
        (
                SELECT 
                        sdate,
                        main_shelf_id,
                        SUM(stock_quantity) AS sec_stock_quantity
                FROM
                        fe_dwd.`dwd_shelf_day_his` 
                WHERE sdate >= @cur_month_01
                        AND main_shelf_id IS NOT NULL
                GROUP BY sdate,main_shelf_id
        ) c
                ON a.sdate = c.sdate
                AND a.shelf_id = c.main_shelf_id
WHERE a.sdate >= @cur_month_01
        AND a.shelf_type IN (1,2,3,5)
GROUP BY a.shelf_id
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_stock_reach_ratio","@time_1--@time_2",@time_1,@time_2);	
# sku达标天数明细
DROP TEMPORARY TABLE IF EXISTS fe_dwd.month_sku_tmp;
CREATE TEMPORARY TABLE fe_dwd.month_sku_tmp(
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM((a.shelf_type IN (1,3) AND a.stock_skus >= 30) OR (a.shelf_type IN (2,5) AND a.stock_skus >= 10) OR (IFNULL(a.stock_skus,0) + IFNULL(c.sec_sku_qty,0) >= 30)) AS month_sku_reach_days
FROM
        fe_dwd.`dwd_shelf_day_his` a
        LEFT JOIN 
        (
                SELECT 
                        sdate,
                        main_shelf_id,
                        stock_skus AS sec_sku_qty
                FROM
                        fe_dwd.`dwd_shelf_day_his` 
                WHERE sdate >= @cur_month_01
                        AND main_shelf_id IS NOT NULL
                GROUP BY sdate,main_shelf_id
        ) c
                ON a.sdate = c.sdate
                AND a.shelf_id = c.main_shelf_id
WHERE a.sdate >= @cur_month_01
        AND a.shelf_type IN (1,2,3,5)
GROUP BY a.shelf_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_stock_reach_ratio","@time_4--@time_5",@time_4,@time_5);	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.stock_sku_tmp;
CREATE TEMPORARY TABLE fe_dwd.stock_sku_tmp(
        KEY idx_shelf_id(shelf_id)
) AS
SELECT 
        shelf_id,
        stock_quantity AS stock_quantity,
        stock_sum AS stock_value, 
        stock_skus AS sku_qty
FROM
        fe_dwd.`dwd_shelf_day_his`
WHERE sdate = @stat_date
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_stock_reach_ratio","@time_5--@time_6",@time_5,@time_6);	
DELETE FROM fe_dm.dm_op_stock_reach_ratio WHERE month_id = @month_id;
INSERT INTO fe_dm.dm_op_stock_reach_ratio
(
        month_id,
        business_name,
        zone_name,
        shelf_id,
        MANAGER_ID,
        SHELF_CODE,
        shelf_type,
        shelf_status,
        WHETHER_CLOSE,
        REVOKE_STATUS,
        grade,
        stock_quantity,
        stock_value, 
        sku_qty,
        month_stock_days,
        month_operate_days,
        month_stock_reach_days,
        month_sku_reach_days,
        stock_reach_ratio,
        sku_reach_ratio
)
SELECT
        @month_id AS month_id,
        a.business_name,
        a.zone_name,
        a.shelf_id,
        a.MANAGER_ID,
        a.SHELF_CODE,
        a.shelf_type,
        a.shelf_status,
        a.WHETHER_CLOSE,
        a.REVOKE_STATUS,
        a.grade,
        b.stock_quantity,
        b.stock_value, 
        b.sku_qty,
        c.month_stock_days,
        c.month_operate_days,
        c.month_stock_reach_days,
        d.month_sku_reach_days,
        ROUND(c.month_stock_reach_days / c.month_operate_days,2) AS stock_reach_ratio,
        ROUND(d.month_sku_reach_days / c.month_operate_days,2) AS sku_reach_ratio
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.stock_sku_tmp b
                ON a.`shelf_id` = b.`SHELF_ID`
        JOIN fe_dwd.shelf_stock c
                ON a.`shelf_id` = c.shelf_id
        JOIN fe_dwd.month_sku_tmp d
                ON a.`shelf_id` = d.shelf_id
GROUP BY a.`shelf_id`
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_stock_reach_ratio","@time_6--@time_7",@time_6,@time_7);	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_stock_reach_ratio',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_stock_reach_ratio','dm_op_stock_reach_ratio','宋英南');
END