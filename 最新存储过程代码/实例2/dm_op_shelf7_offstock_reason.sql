CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf7_offstock_reason`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/04/30
-- Modify date: 
-- Description:	
-- 	自贩机-缺货原因（月度汇总）（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
SET @month_id := DATE_FORMAT(@stat_date,'%Y-%m');
SET @pre_6month := DATE_FORMAT(SUBDATE(@stat_date,INTERVAL 6 MONTH),'%Y-%m');
-- 自贩机-缺货原因月度汇总
DELETE FROM fe_dm.dm_op_shelf7_offstock_reason WHERE month_id = @month_id OR month_id < @pre_6month;
INSERT INTO fe_dm.dm_op_shelf7_offstock_reason
(
        month_id,
        business_name,
        zone_code,
        zone_name,
        is_prewarehouse_cover,
        manager_type,
        reason_classify,
        slots,
        slots_sto,
        skus,
        skus_sto
)
SELECT
        DATE_FORMAT(a.sdate,'%Y-%m') AS month_id,
        a.business_name,
        b.`zone_code`,
        b.`zone_name`,
        b.is_prewarehouse_cover,
        b.manager_type,
        a.reason_classify,
        COUNT(*) AS slots,
        SUM(a.stock_num > 0) AS slots_sto,
        COUNT(DISTINCT sdate,a.`product_id`) AS skus,
        COUNT(DISTINCT sdate,IF(stock_num > 0,product_id,NULL)) AS skus_sto
FROM
        fe_dm.`dm_op_offstock_slot` a
        JOIN fe_dwd.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.`shelf_id`
                AND a.sdate >= @cur_month_01
GROUP BY month_id,a.business_name,b.`zone_code`,b.is_prewarehouse_cover,b.manager_type,a.reason_classify
;
-- 自贩机-商品缺货原因月度汇总
DELETE FROM fe_dm.dm_op_shelf7_product_offstock_reason WHERE month_id = @month_id OR month_id < @pre_6month;
INSERT INTO fe_dm.dm_op_shelf7_product_offstock_reason
(
        month_id,
        business_name,
        zone_code,
        zone_name,
        product_id,
        PRODUCT_CODE2,
        reason_classify,
        slots,
        offstock_slots
)
SELECT 
        DATE_FORMAT(a.sdate,'%Y-%m') AS month_id,
        a.business_name,
        c.`zone_code`,
        c.`zone_name`,
        a.product_id,
        b.`PRODUCT_CODE2`,
        a.reason_classify,
        COUNT(*) AS slots,
        SUM(a.stock_num <= 0) AS offstock_slots
FROM
        fe_dm.dm_op_offstock_slot a
        JOIN fe_dwd.`dwd_product_base_day_all` b
                ON a.`product_id` = b.`PRODUCT_ID`
                AND a.sdate >= @cur_month_01
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.`shelf_id` = c.`shelf_id`
GROUP BY month_id,a.business_name,c.`zone_code`,a.product_id,a.reason_classify
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf7_offstock_reason',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf7_offstock_reason','dm_op_shelf7_offstock_reason','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf7_product_offstock_reason','dm_op_shelf7_offstock_reason','宋英南');
END