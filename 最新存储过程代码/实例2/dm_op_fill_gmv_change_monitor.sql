CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_fill_gmv_change_monitor`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/05/18
-- Modify date: 
-- Description:	
-- 	补货前后GMV变化监控（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @cur_date := CURDATE();
SET @stat_date := SUBDATE(@cur_date,1);
SET @pre_2month := DATE_FORMAT(SUBDATE(@stat_date,INTERVAL 2 MONTH),'%Y-%m-01');
SET @pre_2week := SUBDATE(@stat_date,INTERVAL 2 WEEK);
SET @pre_1week := SUBDATE(@stat_date,INTERVAL 1 WEEK);  
SET @pre_3day := SUBDATE(@stat_date,3);  
-- 上架前两周的日均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_2week_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.pre_2week_tmp (
        PRIMARY KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        ROUND(SUM(gmv) / 14,2) AS pre_2week_gmv
FROM
        fe_dwd.`dwd_shelf_day_his`
WHERE sdate > @pre_2week
        AND gmv IS NOT NULL
GROUP BY shelf_id
;
-- 上架前一周的日均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_1week_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.pre_1week_tmp (
        PRIMARY KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        ROUND(SUM(gmv) / 7,2) AS pre_1week_gmv
FROM
        fe_dwd.`dwd_shelf_day_his`
WHERE sdate > @pre_1week
        AND gmv IS NOT NULL
GROUP BY shelf_id
;
-- 补货订单货架维度
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_tmp (
        KEY idx_order_id(order_id)
) AS
SELECT 
        order_id,
        shelf_id,
        FILL_TYPE,
        FILL_TIME,
        FILL_USER_ID,
        FILL_USER_NAME,
        FILL_AUDIT_USER_ID,
        FILL_AUDIT_USER_NAME,
        COUNT(*) AS fill_sku,
        SUM(ACTUAL_FILL_NUM * SALE_PRICE) AS fill_value,
        ROUND(SUM(SALES_FLAG IN (4,5)) / COUNT(*),2) AS fill_order_unsale_rate
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month`
WHERE FILL_TIME >= @stat_date
        AND FILL_TIME < @cur_date
GROUP BY order_id
;
-- 补完后货架滞销+严重滞销sku占比
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_unsale_rate_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.shelf_unsale_rate_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        ROUND(SUM(SALES_FLAG IN (4,5)) / COUNT(*),2) AS shelf_unsale_rate
FROM
        fe_dwd.`dwd_shelf_product_day_all`
GROUP BY shelf_id
;
DELETE FROM fe_dm.dm_op_fill_gmv_change_monitor WHERE stat_date = @stat_date OR stat_date < @pre_2month;
INSERT INTO fe_dm.dm_op_fill_gmv_change_monitor
(
        stat_date,
        business_name,
        shelf_id,
        SHELF_CODE,
        shelf_type,
        order_id,
        FILL_TIME,
        SF_CODE,
        REAL_NAME,
        manager_type,
        FILL_AUDIT_USER_ID,
        FILL_AUDIT_USER_NAME,
        audit_status,
        audit_result,
        remark,
        pre_2week_gmv,
        pre_1week_gmv,
        FILL_TYPE,
        fill_sku,
        fill_value,
        fill_order_unsale_rate,
        shelf_unsale_rate
)
SELECT
        @stat_date AS stat_date,
        a.`business_name`,
        a.`shelf_id`,
        a.`SHELF_CODE`,
        a.`shelf_type`,
        b.`order_id`,
        b.`FILL_TIME`,
        f.`SF_CODE`,
        b.FILL_USER_NAME AS REAL_NAME,
        CASE
                WHEN f.second_user_type=1
                THEN '全职店主'
                WHEN f.second_user_type=2
                THEN '兼职店主'
                ELSE '非兼非全'
        END AS manager_type,
--         a.`manager_type`,
        b.FILL_AUDIT_USER_ID,
        b.FILL_AUDIT_USER_NAME,
        c.audit_status,
        c.audit_result,
        c.remark,
        d.pre_2week_gmv,
        e.pre_1week_gmv,
        b.FILL_TYPE,
        b.fill_sku,
        b.fill_value,
        b.fill_order_unsale_rate,
        g.shelf_unsale_rate
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.fill_tmp b
                ON a.`shelf_id` = b.`SHELF_ID`
        LEFT JOIN fe_dm.`sf_sham_assign_record` c
                ON b.order_id = c.order_id
                AND c.data_flag = 1
        LEFT JOIN fe_dwd.`pre_2week_tmp` d
                ON a.`shelf_id` = d.shelf_id
        LEFT JOIN fe_dwd.pre_1week_tmp e
                ON a.`shelf_id` = e.shelf_id
        LEFT JOIN fe_dwd.`dwd_pub_shelf_manager` f
                ON b.FILL_USER_ID = f.manager_id
                AND f.data_flag = 1
        JOIN fe_dwd.shelf_unsale_rate_tmp g
                ON a.shelf_id = g.shelf_id
;
-- 上架后3日内日均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`after_3day_gmv_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.after_3day_gmv_tmp (
        PRIMARY KEY idx_shelf_id(shelf_id)
) AS
SELECT
        shelf_id,
        ROUND(SUM(gmv) / 3,2) AS after_3day_gmv
FROM
        fe_dwd.`dwd_shelf_day_his`
WHERE sdate > @pre_3day
        AND gmv IS NOT NULL
GROUP BY shelf_id
;
UPDATE 
        fe_dm.dm_op_fill_gmv_change_monitor a
        JOIN fe_dwd.after_3day_gmv_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.stat_date = @pre_3day
SET a.after_3day_gmv = b.after_3day_gmv
;
UPDATE 
        fe_dm.dm_op_fill_gmv_change_monitor a
        JOIN fe_dwd.pre_1week_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.stat_date = @pre_1week
SET a.after_1week_gmv = b.pre_1week_gmv
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_fill_gmv_change_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_gmv_change_monitor','dm_op_fill_gmv_change_monitor','宋英南');
END