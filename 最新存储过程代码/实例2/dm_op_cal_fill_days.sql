CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_cal_fill_days`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @smonth := DATE_FORMAT(CURDATE(),'%Y%m'),
@stat_date := CURDATE(),         -- 每月最后一天调度
@syear := DATE_FORMAT(CURDATE(),'%Y-01-01');
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_fill_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_fill_tmp (
        KEY idx_shelf_id(shelf_id)
        ) AS
SELECT 
        DISTINCT
        order_id,
        shelf_id,
        apply_time,
        fill_time
FROM
        fe_dwd.`dwd_fill_day_inc`
WHERE APPLY_TIME >= @syear
;
TRUNCATE fe_dm.dm_op_cal_fill_days;
INSERT INTO fe_dm.dm_op_cal_fill_days
(
        smonth,
        REGION_AREA,
        BUSINESS_AREA,
        SHELF_TYPE,
        shelf_id,
        total_order_qty,
        fill_day0_ratio, 
        fill_day1_ratio,
        fill_day2_3_ratio, 
        fill_day3_ratio,  
        fill_days
)
SELECT 
        @smonth AS smonth,
        b.region_name AS REGION_AREA,
        b.business_name AS BUSINESS_AREA,
        b.SHELF_TYPE,
        b.shelf_id,
        COUNT(a.ORDER_ID) AS total_order_qty,
        SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 0,1,0)) / COUNT(a.ORDER_ID) AS  fill_day0_ratio,  -- 上架时间-订单下单时间的天数差为0的订单数
        SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 1,1,0)) / COUNT(a.ORDER_ID) AS  fill_day1_ratio,  -- 上架时间-订单下单时间的天数差为1的订单数
        SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 2 OR DATEDIFF(FILL_TIME,APPLY_TIME) = 3,1,0)) / COUNT(a.ORDER_ID) AS  fill_day2_3_ratio,   -- 上架时间-订单下单时间的天数差为2-3天的订单数
        SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) > 3,1,0)) / COUNT(a.ORDER_ID) AS  fill_day3_ratio,  -- 上架时间-订单下单时间的天数差为3天以上的订单数
        CASE
                WHEN SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 0,1,0)) / COUNT(a.ORDER_ID) >= 0.5
                        THEN 0
                WHEN SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 1,1,0)) / COUNT(a.ORDER_ID) >= 0.5
                        THEN 1
                WHEN SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 2 OR DATEDIFF(FILL_TIME,APPLY_TIME) = 3,1,0)) / COUNT(a.ORDER_ID) >=0.5
                        THEN 2
                WHEN (SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 0,1,0)) / COUNT(a.ORDER_ID)) + (SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 1,1,0)) / COUNT(a.ORDER_ID)) >= 0.5
                        THEN 1
                WHEN (SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 0,1,0)) / COUNT(a.ORDER_ID)) + (SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 1,1,0)) / COUNT(a.ORDER_ID)) +
                        (SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) = 2 OR DATEDIFF(FILL_TIME,APPLY_TIME) = 3,1,0)) / COUNT(a.ORDER_ID)) >= 0.5
                        THEN 2
                WHEN SUM(IF(DATEDIFF(FILL_TIME,APPLY_TIME) > 3,1,0)) / COUNT(a.ORDER_ID)  >= 0.5
                        THEN 3
        END AS fill_days
FROM 
        fe_dwd.shelf_fill_tmp a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON b.SHELF_TYPE IN (1,2,3,5,6,7,8)
                AND b.SHELF_STATUS = 2          -- 以前货架状态为准
                AND a.shelf_id = b.shelf_id
WHERE a.FILL_TIME IS NOT NULL   -- 筛选有上架时间的订单
GROUP BY b.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_cal_fill_days',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_cal_fill_days','dm_op_cal_fill_days','宋英南');
END