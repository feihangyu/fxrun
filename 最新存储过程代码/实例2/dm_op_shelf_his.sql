CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_his`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := CURDATE();
SET @year_month := DATE_FORMAT(@stat_date,'%Y-%m');
DELETE FROM fe_dm.dm_op_shelf_his WHERE stat_date < DATE_FORMAT(SUBDATE(@stat_date,INTERVAL 1 YEAR),'%Y-%m');    -- 截存一年的数据
INSERT INTO fe_dm.dm_op_shelf_his
(
        stat_date,
        REGION_AREA,
        BUSINESS_AREA,
        shelf_type,
        shelf_qty
)
SELECT 
        @year_month AS stat_date,
        a.region_name AS REGION_AREA,
        a.business_name AS BUSINESS_AREA,
        a.shelf_type,
        COUNT(a.shelf_id) AS shelf_qty
FROM 
        `fe_dwd`.`dwd_shelf_base_day_all` a
WHERE a.SHELF_STATUS = 2          -- 已激活
GROUP BY a.business_name,a.shelf_type;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_his','dm_op_shelf_his','宋英南');
END