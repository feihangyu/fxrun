CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_cal_fill_days`()
    SQL SECURITY INVOKER
BEGIN
	DECLARE l_test VARCHAR(1);
        DECLARE l_row_cnt INT;
        DECLARE CODE CHAR(5) DEFAULT '00000';
        DECLARE done INT;
        DECLARE l_table_owner   VARCHAR(64);
        DECLARE l_city          VARCHAR(64);
        DECLARE l_task_name     VARCHAR(64);
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
                DECLARE EXIT HANDLER FOR SQLEXCEPTION
                BEGIN
                        GET DIAGNOSTICS CONDITION 1
                        CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
                        CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
                END; 
        SET l_task_name = 'sp_op_cal_fill_days';
        
SET @smonth := DATE_FORMAT(CURDATE(),'%Y%m'),
@stat_date := CURDATE(),         -- 每月最后一天调度
@syear := DATE_FORMAT(CURDATE(),'%Y-01-01');
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
TRUNCATE feods.d_op_cal_fill_days;
INSERT INTO feods.d_op_cal_fill_days
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
        fe.`sf_product_fill_order` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.APPLY_TIME >= @syear
                AND b.SHELF_TYPE IN (1,2,3,5,6,7,8)
                AND b.SHELF_STATUS = 2          -- 以前货架状态为准
                AND a.shelf_id = b.shelf_id
WHERE a.FILL_TIME IS NOT NULL   -- 筛选有上架时间的订单
GROUP BY b.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_cal_fill_days',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
	END