CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_preware_stock_weekly_monthly`()
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
		
    SET l_task_name = 'sh_preware_stock_weekly_monthly'; 
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();    
SET @sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
# 前置仓每周库存总计
DELETE FROM feods.pj_prewarehouse_stock_detail_weekly WHERE week_monday >= DATE_SUB(@sdate,INTERVAL WEEKDAY(@sdate) DAY); #本周的数据更新；
INSERT INTO feods.pj_prewarehouse_stock_detail_weekly
(week_monday,
check_week,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_id,
product_code2,
product_name,
purchase_price,
freeze_stock,
available_stock,
total_stock
)
SELECT DATE_SUB(check_date,INTERVAL WEEKDAY(check_date) DAY) AS week_monday,
CONCAT(DATE_FORMAT(check_date,'%Y-%u'),'周') AS check_week,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_id,
product_code2,
product_name,
purchase_price,
SUM(freeze_stock) AS freeze_stock,
SUM(available_stock) AS available_stock,
SUM(total_stock) AS total_stock
FROM feods.`pj_prewarehouse_stock_detail`
WHERE check_date >= DATE_SUB(@sdate,INTERVAL WEEKDAY(@sdate) DAY)
GROUP BY check_week,warehouse_id,product_id;
# 前置仓库存每月总计
DELETE FROM feods.pj_prewarehouse_stock_detail_monthly WHERE check_month = DATE_FORMAT(@sdate,'%Y-%m');
INSERT INTO feods.pj_prewarehouse_stock_detail_monthly
(check_month,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_id,
product_code2,
product_name,
purchase_price,
freeze_stock,
available_stock,
total_stock
)
SELECT DATE_FORMAT(check_date,'%Y-%m') AS check_month,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_code,
shelf_name,
product_id,
product_code2,
product_name,
purchase_price,
SUM(freeze_stock) AS freeze_stock,
SUM(available_stock) AS available_stock,
SUM(total_stock) AS total_stock
FROM feods.`pj_prewarehouse_stock_detail`
WHERE check_date >= DATE_FORMAT(@sdate,'%Y-%m-01')
GROUP BY check_month,warehouse_id,product_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_preware_stock_weekly_monthly',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END