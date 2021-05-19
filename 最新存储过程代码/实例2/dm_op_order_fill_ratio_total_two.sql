CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_order_fill_ratio_total_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @cur_date := CURDATE();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @pre_day2 := SUBDATE(CURDATE(),2);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        b.business_name,
        b.zone_name,
        b.shelf_id,
        b.manager_id,
        b.REAL_NAME,
        b.manager_type
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` b
        LEFT JOIN fe_dwd.`dwd_shelf_machine_info` c
                ON b.shelf_id = c.shelf_id
WHERE b.shelf_type IN (1,2,3,5) OR (b.shelf_type = 6 AND c.machine_type_name = '静态柜')
;
#昨天系统触发订单总量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        b.shelf_id,
        COUNT(DISTINCT a.shelf_id) AS total_order_qty
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN fe_dwd.shelf_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.FILL_TYPE = 2
                AND a.apply_time >= @stat_date
                AND a.apply_time < @cur_date
--         LEFT JOIN fe_dm.`dm_op_auto_push_fill_date_his` c
--                 ON a.shelf_id = c.shelf_id
--                 AND c.stat_date = @stat_date
--         LEFT JOIN fe_dm.`dm_op_smart_shelf_fill_update_his` d
--                 ON a.shelf_id = d.shelf_id
--                 AND a.product_id = d.product_id
--                 AND d.cdate = @stat_date
-- WHERE c.whether_push_order = 1 OR d.whether_push_order = 1
GROUP BY b.shelf_id
;
-- 无人货架+智能柜系统建议补货单数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`suggest_orders_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.suggest_orders_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT 
        shelf_id,
        1 AS suggest_fill_order_qty
FROM
        fe_dm.`dm_op_auto_push_fill_date_his`    -- 用昨天的数据
WHERE stat_date = @stat_date
        AND whether_push_order = 1
UNION ALL
SELECT 
        DISTINCT 
        shelf_id,
        1 AS suggest_fill_order_qty
FROM
        fe_dm.`dm_op_smart_shelf_fill_update_his` a
WHERE a.cdate = @stat_date
        AND a.whether_push_order = 1
;
DELETE FROM fe_dm.dm_op_order_fill_ratio WHERE stat_date = @stat_date OR stat_date < SUBDATE(@stat_date,INTERVAL 2 YEAR);
INSERT INTO fe_dm.dm_op_order_fill_ratio
(
        stat_date,
        business_name,
        zone_name,
        REAL_NAME,
        MANAGER_ID,
        manager_type,
        total_order_qty,
        suggest_fill_order_qty,
        fill_order_rate
)
SELECT
        DISTINCT
        @stat_date,
        b.business_name,
        b.zone_name,
        b.REAL_NAME,
        b.MANAGER_ID,
        b.manager_type,
        SUM(a.total_order_qty) AS total_order_qty,
        IFNULL(SUM(t1.suggest_fill_order_qty),0) AS suggest_fill_order_qty,
        ROUND(SUM(a.total_order_qty) / IFNULL(SUM(t1.suggest_fill_order_qty),0),2) AS fill_order_rate
FROM fe_dwd.suggest_orders_tmp t1
        LEFT JOIN fe_dwd.`fill_tmp` a
                ON t1.shelf_id = a.`shelf_id`
        JOIN `fe_dwd`.`shelf_tmp` b
                ON t1.shelf_id = b.`shelf_id`
GROUP BY b.zone_name,b.MANAGER_ID
;
#次日上架订单状态汇总
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_pre_day2_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_pre_day2_tmp (
        KEY idx_MANAGER_ID(MANAGER_ID)
) AS
SELECT
        @pre_day2 AS pre_day2,
        b.zone_name,
        b.MANAGER_ID,
        SUM((a.order_status = 4)) AS fill_4_qty,
        SUM((a.order_status = 3)) AS fill_3_qty,
        SUM((a.order_status = 2)) AS fill_2_qty,
        SUM((a.order_status = 1)) AS fill_1_qty,
        SUM((a.order_status = 9)) AS fill_9_qty,
        SUM((a.order_status IN (5,7,8,11))) AS fill_other_qty
FROM
        (
                SELECT
                        DISTINCT
                        shelf_id,
                        order_status
                FROM
                `fe_dwd`.`dwd_fill_day_inc` 
                WHERE FILL_TYPE = 2
                        AND apply_time >= @pre_day2
                        AND apply_time < @stat_date
        ) a
        JOIN fe_dwd.shelf_tmp b
                ON a.shelf_id = b.shelf_id
GROUP BY b.zone_name,b.MANAGER_ID
;
UPDATE
        fe_dm.dm_op_order_fill_ratio a
        JOIN fe_dwd.fill_pre_day2_tmp b
                ON a.MANAGER_ID = b.MANAGER_ID
                AND a.zone_name = b.zone_name
                AND a.stat_date = b.pre_day2
SET a.fill_4_qty = b.fill_4_qty,
        a.fill_3_qty = b.fill_3_qty,
        a.fill_2_qty = b.fill_2_qty,
        a.fill_1_qty = b.fill_1_qty,
        a.fill_9_qty = b.fill_9_qty,
        a.fill_other_qty = b.fill_other_qty,
        a.order_fill_ratio = ROUND(b.fill_4_qty / a.total_order_qty,2),
        a.order_cancel_ratio = ROUND(b.fill_9_qty / a.total_order_qty,2)
;
-- ===============================================================================================================
-- 按地区、兼职和全职店主汇总
DELETE FROM fe_dm.dm_op_order_fill_ratio_total WHERE stat_date = @stat_date OR stat_date < SUBDATE(@stat_date,INTERVAL 2 YEAR);
INSERT INTO fe_dm.dm_op_order_fill_ratio_total
(
        stat_date,
        business_name,
        zone_name,
        manager_type,
        total_suggest_fill_order_qty,
        total_order_qty,
        total_fill_order_rate
)
SELECT
        a.stat_date,
        a.business_name,
        a.zone_name,
        a.manager_type,
        SUM(a.suggest_fill_order_qty) AS total_suggest_fill_order_qty,
        SUM(a.total_order_qty) AS total_order_qty,
        ROUND(SUM(a.total_order_qty) / SUM(a.suggest_fill_order_qty),2) AS total_fill_order_rate
FROM
        fe_dm.dm_op_order_fill_ratio a
WHERE stat_date = @stat_date
GROUP BY zone_name,manager_type
;
#次日上架订单状态汇总
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_pre_day2_tmp_total`;   
CREATE TEMPORARY TABLE fe_dwd.fill_pre_day2_tmp_total (
        KEY idx_area_manager_type(zone_name,manager_type)
) AS
SELECT
        @pre_day2 AS pre_day2,
        zone_name,
        manager_type,
        SUM(fill_4_qty) AS fill_4_qty,
        SUM(fill_3_qty) AS fill_3_qty,
        SUM(fill_2_qty) AS fill_2_qty,
        SUM(fill_1_qty) AS fill_1_qty,
        SUM(fill_9_qty) AS fill_9_qty,
        SUM(fill_other_qty) AS fill_other_qty
FROM
        fe_dm.dm_op_order_fill_ratio
WHERE stat_date = @pre_day2
GROUP BY zone_name,manager_type
;
UPDATE
        fe_dm.dm_op_order_fill_ratio_total a
        JOIN fe_dwd.fill_pre_day2_tmp_total b
                ON a.stat_date = b.pre_day2
                AND a.zone_name = b.zone_name
                AND a.manager_type = b.manager_type
SET a.fill_4_qty = b.fill_4_qty,
        a.fill_3_qty = b.fill_3_qty,
        a.fill_2_qty = b.fill_2_qty,
        a.fill_1_qty = b.fill_1_qty,
        a.fill_9_qty = b.fill_9_qty,
        a.fill_other_qty = b.fill_other_qty,
        a.order_fill_ratio = ROUND(b.fill_4_qty / a.total_order_qty,2),
        a.order_cancel_ratio = ROUND(b.fill_9_qty / a.total_order_qty,2)
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_order_fill_ratio_total_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_order_fill_ratio','dm_op_order_fill_ratio_total_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_order_fill_ratio_total','dm_op_order_fill_ratio_total_two','宋英南');
END