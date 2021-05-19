CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_stock_real_time_monitor`()
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
        SET l_task_name = 'sp_op_shelf_stock_real_time_monitor';
        
SET @cdate := CURDATE(),
@last_month := DATE_FORMAT(DATE_SUB(@cdate, INTERVAL DAY(@cdate) DAY),'%Y-%m') ,
@last_month_01 := DATE_FORMAT(DATE_SUB(@cdate, INTERVAL DAY(@cdate) DAY),'%Y-%m-01') ;
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();

-- 当天在途库存数 2s
DROP TEMPORARY TABLE IF EXISTS feods.`fill_onload_tmp`;
CREATE TEMPORARY TABLE feods.fill_onload_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        SUM(b.ACTUAL_APPLY_NUM) AS ONWAY_NUM
FROM 
        fe.`sf_product_fill_order` a
        JOIN fe.`sf_product_fill_order_item` b
                ON a.order_id = b.ORDER_ID
                AND a.ORDER_STATUS IN (1,2)
                AND a.APPLY_TIME >= SUBDATE(CURDATE(),INTERVAL 1 YEAR)
                AND a.DATA_FLAG = 1
                AND b.DATA_FLAG = 1
GROUP BY a.shelf_id
;

DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        SUM(a.STOCK_QUANTITY) AS STOCK_QUANTITY
FROM fe.`sf_shelf_product_detail` a 
WHERE a.`DATA_FLAG` = 1
GROUP BY a.shelf_id
;

-- 过程表 1min
DROP TEMPORARY TABLE IF EXISTS feods.`stock_tmp`;
CREATE TEMPORARY TABLE feods.stock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        DISTINCT d.region_name AS REGION_AREA,
        d.business_name AS BUSINESS_AREA,
        d.SHELF_STATUS,
        d.REVOKE_STATUS,
        d.WHETHER_CLOSE,
        d.shelf_type,
        a.`SHELF_ID`,
        d.`SHELF_NAME`,
        d.`SHELF_CODE`,
        d.REAL_NAME,
        d.`MANAGER_ID`,
        IF(f.shelf_id IS NOT NULL,'前置仓','大仓') AS is_prewarehouse_shelf,
        f.prewarehouse_code,
        f.prewarehouse_name,
        d.manager_type AS is_full_time_manager,
        a.STOCK_QUANTITY,
        b.ONWAY_NUM,
        IF(d.MAIN_SHELF_ID IS NOT NULL,'是','否') AS is_relation_shelf,
        CASE
                WHEN j.grade IN ( '甲','乙','新装')
                        THEN '甲乙新'
                WHEN j.grade IN ('丙','丁')
                        THEN '丙丁级'
                ELSE '其他'
        END shelf_level
FROM 
        feods.shelf_product_tmp a
        LEFT JOIN feods.fill_onload_tmp b
                ON a.shelf_id = b.shelf_id
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` d
                ON a.`SHELF_ID` = d.`SHELF_ID`
                AND d.`SHELF_STATUS` = 2
                AND d.`SHELF_TYPE` IN (1,2,3,4,5,6,7,8)
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` f
                ON d.`SHELF_ID` = f.shelf_id
        LEFT JOIN feods.`d_op_shelf_grade` j
                ON d.shelf_id = j.shelf_id
                AND j.month_id = @last_month     -- 取上个月的货架等级
;

-- 结果表
TRUNCATE feods.d_op_shelf_stock_real_time_monitor;
INSERT INTO feods.d_op_shelf_stock_real_time_monitor
(
        REGION_AREA,
        BUSINESS_AREA,
        SHELF_STATUS,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        shelf_type,
        SHELF_ID,
        SHELF_NAME,
        SHELF_CODE,
        MANAGER_NAME,
        MANAGER_ID,
        is_prewarehouse_shelf,
        prewarehouse_code,
        prewarehouse_name,
        is_full_time_manager,
        STOCK_QUANTITY,
        ONWAY_NUM,
        is_relation_shelf,
        shelf_level,
        stock_meet_flag
)
SELECT
        REGION_AREA,
        BUSINESS_AREA,
        SHELF_STATUS,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        shelf_type,
        SHELF_ID,
        SHELF_NAME,
        SHELF_CODE,
        REAL_NAME AS MANAGER_NAME,
        MANAGER_ID,
        is_prewarehouse_shelf,
        prewarehouse_code,
        prewarehouse_name,
        is_full_time_manager,
        STOCK_QUANTITY,
        ONWAY_NUM,
        is_relation_shelf,
        shelf_level,
        CASE
                WHEN shelf_type IN (1,3) AND is_relation_shelf = '否' AND shelf_level = '甲乙新' AND STOCK_QUANTITY >= 180
                        THEN '满足'
                WHEN shelf_type IN (1,3) AND is_relation_shelf = '否' AND shelf_level = '丙丁级' AND STOCK_QUANTITY >= 110
                        THEN '满足'
                WHEN shelf_type IN (2,5) AND is_relation_shelf = '否' AND shelf_level = '甲乙新' AND STOCK_QUANTITY >= 110
                        THEN '满足'
                WHEN shelf_type IN (1,3) AND is_relation_shelf = '否' AND shelf_level = '丙丁级' AND STOCK_QUANTITY >= 90
                        THEN '满足'
                WHEN shelf_type IN (1,2,3,5) AND is_relation_shelf = '是' AND shelf_level = '甲乙新' AND STOCK_QUANTITY >= 300
                        THEN '满足'
                WHEN shelf_type IN (1,2,3,5) AND is_relation_shelf = '是' AND shelf_level = '丙丁级' AND STOCK_QUANTITY >= 200
                        THEN '满足'
                WHEN shelf_type = 6 AND STOCK_QUANTITY >= 110
                        THEN '满足'
                WHEN shelf_type = 8 AND STOCK_QUANTITY >= 150
                        THEN '满足'
                WHEN shelf_type = 4
                        THEN '不考虑'
                ELSE '不满足'
        END stock_meet_flag
FROM 
        feods.stock_tmp 
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_shelf_stock_real_time_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
	END