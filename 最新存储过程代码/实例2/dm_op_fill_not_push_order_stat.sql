CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_fill_not_push_order_stat`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @month_id := DATE_FORMAT(@stat_date,'%Y-%m');
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
SET @pre_1year := DATE_FORMAT(SUBDATE(@stat_date,INTERVAL 1 YEAR),'%Y-%m'); -- 结存一年数据
-- 货架口径
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.business_name,
        a.zone_name,
        a.`shelf_id`,
        a.`SHELF_CODE`,
        a.`MANAGER_ID`,
        a.`REAL_NAME`,
        a.`shelf_type`,
        a.`SHELF_STATUS`,
        a.`REVOKE_STATUS`,
        a.`WHETHER_CLOSE`,
        a.`ACTIVATE_TIME`
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
WHERE a.shelf_type IN (1,2,3,5,6,7)
        AND a.`SHELF_STATUS` = 2
        AND a.`REVOKE_STATUS` = 1
        AND a.`WHETHER_CLOSE` = 2
;
-- 当月补货次数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_qty_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_qty_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        COUNT(DISTINCT order_id) AS fill_qty
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month`
WHERE FILL_TIME >= @cur_month_01
        AND fill_type IN (1,2,3,4,7,8,9)
GROUP BY shelf_id       
;
-- 最近一次补货时间
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`recent_fill_time_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.recent_fill_time_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        MAX(FILL_TIME) AS recent_fill_time
FROM
        fe_dm.`dm_op_shelf_product_fill_last_time` 
GROUP BY shelf_id
;
-- 无人货架当月建议补货次数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`push_order_qty_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.push_order_qty_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        SUM(whether_push_order = 1) AS push_order_qty
FROM
        fe_dm.`dm_op_auto_push_fill_date_his`
WHERE stat_date >= @cur_month_01
GROUP BY shelf_id
;
DELETE FROM fe_dm.dm_op_fill_not_push_order_stat WHERE month_id = @month_id OR month_id < @pre_1year;
-- TRUNCATE TABLE fe_dm.dm_op_fill_not_push_order_stat;
INSERT INTO fe_dm.dm_op_fill_not_push_order_stat
(
        month_id,
        business_name,
        zone_name,
        shelf_id,
        SHELF_CODE,
        MANAGER_ID,
        REAL_NAME,
        shelf_type,
        SHELF_STATUS,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        is_new_shelf,
        fill_qty,
        recent_fill_time,
        push_order_qty,
        is_not_push_order
)
SELECT
        @month_id AS month_id,
        a.business_name,
        a.zone_name,
        a.`shelf_id`,
        a.`SHELF_CODE`,
        a.`MANAGER_ID`,
        a.`REAL_NAME`,
        a.`shelf_type`,
        a.`SHELF_STATUS`,
        a.`REVOKE_STATUS`,
        a.`WHETHER_CLOSE`,
        IF(a.`ACTIVATE_TIME` >= @cur_month_01,'是','否') AS is_new_shelf,
        IFNULL(b.fill_qty,0) AS fill_qty,
        c.recent_fill_time,
        IFNULL(d.push_order_qty,0) AS push_order_qty,
        IF(IFNULL(b.fill_qty,0) = 0 AND IFNULL(d.push_order_qty,0) = 0,'是','否') AS is_not_push_order
FROM
        fe_dwd.`shelf_tmp` a
        LEFT JOIN fe_dwd.`fill_qty_tmp` b
                ON a.`shelf_id` = b.shelf_id
        LEFT JOIN fe_dwd.recent_fill_time_tmp c
                ON a.`shelf_id` = c.shelf_id
        LEFT JOIN fe_dwd.push_order_qty_tmp d
                ON a.`shelf_id` = d.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_fill_not_push_order_stat',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_not_push_order_stat','dm_op_fill_not_push_order_stat','宋英南');
END