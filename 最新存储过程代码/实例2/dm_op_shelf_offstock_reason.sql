CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_offstock_reason`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/04/29
-- Modify date: 
-- Description:	
-- 	货架缺货条数与缺货原因（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
-- 缺货原因记录数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_reason_classify_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_reason_classify_tmp(
        KEY idx_shelf_id_reason_classify(shelf_id,reason_classify)
) AS 
SELECT
        shelf_id,
        reason_classify,
        COUNT(*) AS reason_classify_num
FROM
        fe_dm.`dm_op_sp_offstock`
WHERE shelf_type IN (1,2,3,5,6)
        AND shelf_fill_flag = 1
GROUP BY shelf_id,reason_classify
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`main_shelf_reason_classify_tmp`;
CREATE TEMPORARY TABLE fe_dwd.main_shelf_reason_classify_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        SUBSTRING_INDEX(GROUP_CONCAT(reason_classify ORDER BY reason_classify_num DESC SEPARATOR ","),",",1)  AS main_reason_classify,
        MAX(reason_classify_num) AS max_reason_classify_num
FROM
        fe_dwd.shelf_reason_classify_tmp
GROUP BY shelf_id
;
-- 相同缺货原因记录数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`max_shelf_reason_classify_tmp`;
CREATE TEMPORARY TABLE fe_dwd.max_shelf_reason_classify_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id
FROM
        fe_dwd.shelf_reason_classify_tmp a
        JOIN fe_dwd.main_shelf_reason_classify_tmp b
                ON a.shelf_id = b.shelf_id
WHERE a.reason_classify_num = b.max_reason_classify_num
GROUP BY a.shelf_id
HAVING COUNT(*) >= 2
;
-- 无爆畅平货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_sales_flag_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_sales_flag_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        DISTINCT 
        a.shelf_id
FROM
        fe_dm.`dm_op_s_offstock` a
        LEFT JOIN 
                (
                        SELECT
                                DISTINCT shelf_id
                        FROM
                                fe_dm.`dm_op_s_offstock`
                        WHERE sdate = @stat_date
                                AND sales_flag IN (1,2,3)
                ) b
                ON a.shelf_id = b.shelf_id
WHERE b.shelf_id IS NULL
;
-- 缺货原因
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_offstock_reason_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_offstock_reason_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        CASE
                WHEN b.shelf_id IS NOT NULL AND c.shelf_id IS NOT NULL 
                        THEN '无畅销品'
                WHEN b.shelf_id IS NOT NULL 
                        THEN '无补货'
                ELSE a.main_reason_classify
        END offstock_reason
FROM
        fe_dwd.main_shelf_reason_classify_tmp a
        LEFT JOIN fe_dwd.max_shelf_reason_classify_tmp b
                ON a.shelf_id = b.shelf_id
        LEFT JOIN fe_dwd.shelf_sales_flag_tmp c
                ON a.shelf_id = c.shelf_id
;
-- 无人货架缺货记录数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_offstock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_offstock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        SUM(ct * (1 - ifsto)) AS offstock_ct
FROM
        fe_dm.dm_op_s_offstock
WHERE sdate > @cur_month_01      -- 本月1号到当前的汇总 
        AND sales_flag IN (1, 2, 3)
        AND shelf_fill_flag = 1
        AND shelf_type IN (1, 2, 3, 5, 6)
GROUP BY shelf_id
;
-- 自贩机缺货记录数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf7_offstock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf7_offstock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        SUM(slots) - SUM(slots_sto) AS offstock_ct
FROM
        fe_dm.dm_op_offstock_s7
WHERE @cur_month_01 < sdate     -- 本月1号到当前的汇总
GROUP BY shelf_id
;
TRUNCATE TABLE fe_dm.dm_op_shelf_offstock_reason;
INSERT INTO fe_dm.dm_op_shelf_offstock_reason
(
        business_name,
        shelf_id,
        shelf_name,
        SHELF_CODE,
        shelf_type,
        SHELF_STATUS,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        MANAGER_ID,
        REAL_NAME,
        is_prewarehouse_cover,
        inner_flag,
        stock_quantity,
        onway_num,
        offstock_ct,
        offstock_reason
)
SELECT
        a.`business_name`,
        a.`shelf_id`,
        a.`shelf_name`,
        a.`SHELF_CODE`,
        a.`shelf_type`,
        a.`SHELF_STATUS`,
        a.`REVOKE_STATUS`,
        a.`WHETHER_CLOSE`,
        a.`MANAGER_ID`,
        a.`REAL_NAME`,
        a.`is_prewarehouse_cover`,
        a.`inner_flag`,
        b.stock_quantity,
        b.onway_num,
        IFNULL(c.offstock_ct,d.offstock_ct) AS offstock_ct,
        IFNULL(e.offstock_reason,f.reason_classify) AS offstock_reason
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`dwd_shelf_day_his` b
                ON a.`shelf_id` = b.shelf_id
                AND b.`sdate` = @stat_date
        LEFT JOIN fe_dwd.shelf_offstock_tmp c
                ON a.`shelf_id` = c.shelf_id
        LEFT JOIN fe_dwd.shelf7_offstock_tmp d
                ON a.`shelf_id` = d.shelf_id
        LEFT JOIN fe_dwd.shelf_offstock_reason_tmp  e
                ON a.`shelf_id` = e.shelf_id
        LEFT JOIN (
                SELECT
                        DISTINCT
                        shelf_id,
                        reason_classify
                FROM
                        fe_dm.`dm_op_offstock_s7_key`
                WHERE sdate = @stat_date
                ) f
                ON a.`shelf_id` = f.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_offstock_reason',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_offstock_reason','dm_op_shelf_offstock_reason','宋英南');
END