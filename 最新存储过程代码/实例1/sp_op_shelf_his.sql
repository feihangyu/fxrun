CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_his`()
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
        SET l_task_name = 'sp_op_shelf_his';
 
SET @stat_date := CURDATE();
SET @year_month := DATE_FORMAT(@stat_date,'%Y-%m');
SET @stat_year := YEAR(@stat_date);
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM feods.d_op_shelf_his WHERE @stat_year > YEAR(stat_date);    -- 截存一年的数据
INSERT INTO feods.d_op_shelf_his
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
'sp_op_shelf_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
	END