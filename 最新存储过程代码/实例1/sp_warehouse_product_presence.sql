CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_warehouse_product_presence`()
    SQL SECURITY INVOKER
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2019/04/10
-- Modify date: 
-- Description:	
-- 	监控大仓商品的上架率 - 采购报表
-- 
-- =============================================
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
		
    SET l_task_name = 'sp_warehouse_product_presence'; 
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
#(1)大仓商品上架率
DELETE FROM feods.pj_warehouse_product_presence WHERE FPRODUCEDATE = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
INSERT INTO feods.pj_warehouse_product_presence
(fproducedate, 
big_area,
business_area ,
warehouse_number,
warehouse_name,
product_bar,
product_name,
product_category,
product_type,
qualityqty,
active_shelf_cnt,
standar_shelf_cnt,
fridge_cnt,
sales_shelf_cnt,
stock_shelf_cnt,
normal_fill_cnt, 
presence_rate
)
SELECT p.`FPRODUCEDATE`
    , p.`BIG_AREA`
    , p.`BUSINESS_AREA`
    , p.`WAREHOUSE_NUMBER`
    , p.`WAREHOUSE_NAME` 
    , p.`PRODUCT_BAR`
    , p.`PRODUCT_NAME`
    , p.`PRODUCT_CATEGORY`
    , p.`PRODUCT_TYPE`
    , p.`QUALITYQTY`
    , t1.shelf_cnt_all
    , t1.shelf_cnt_only
    , t1.fridge_cnt
    , IFNULL(t2.sale_shelf_cnt,0) sale_shelf_cnt
    , IFNULL(t2.stock_shelf_cnt,0) stock_shelf_cnt
    , IFNULL(t2.fill_shelf_cnt,0) fill_shelf_cnt
    , IFNULL(t2.stock_shelf_cnt,0)/(t1.shelf_cnt_only+t1.fridge_cnt) AS presence_rate
FROM
    feods.`PJ_OUTSTOCK2_DAY` p 
    JOIN fe_dwd.`dwd_pub_warehouse_business_area` w
    ON p.`WAREHOUSE_NUMBER` = w.`WAREHOUSE_NUMBER`
    AND w.`data_flag`  = 1
    AND p.`FPRODUCEDATE` = SUBDATE(CURDATE(),1)
    AND p.`PRODUCT_BAR` NOT LIKE "WZ%"
    JOIN 
    (
    SELECT s.`business_name`,
    COUNT(s.`shelf_id`) shelf_cnt_all,
    COUNT(IF(s.`shelf_type` IN (1,3),s.shelf_id,NULL)) shelf_cnt_only,
    COUNT(IF(s.`shelf_type` IN (2,5),s.shelf_id,NULL)) fridge_cnt 
    FROM fe_dwd.`dwd_shelf_base_day_all` s
    WHERE s.`SHELF_STATUS` = 2
--     AND s.`shelf_type` IN (1,2,3,5)
--     and s.whether_close = 2
    GROUP BY s.`business_name`
    ) t1
    ON p.`BUSINESS_AREA` = t1.business_name
    LEFT JOIN 
    (SELECT s.`business_name`
     , pa.product_code2
     , pa.`product_id`
     , COUNT(DISTINCT(IF(st.qty_sal30 >0,pa.`shelf_id`,NULL))) sale_shelf_cnt 
     , COUNT(DISTINCT(IF(pa.`stock` >0,pa.`shelf_id`,NULL))) AS stock_shelf_cnt
     , COUNT(DISTINCT pa.`shelf_id` ) fill_shelf_cnt
     , COUNT(DISTINCT pa.`shelf_id` ) - COUNT(DISTINCT(IF(st.qty_sal30 >0,pa.`shelf_id`,NULL))) diff_cnt
     FROM feods.`d_sc_shelf_packages` pa
     LEFT JOIN feods.`d_op_sp_avgsal30` st
     ON pa.`shelf_id` = st.`shelf_id`
     AND pa.`PRODUCT_ID` = st.`product_id`
     JOIN fe_dwd.`dwd_shelf_base_day_all` s
     ON st.`shelf_id` = s.`shelf_id`
     WHERE s.`SHELF_STATUS` = 2
     AND s.`shelf_type` IN (1,2,3,5)
     GROUP BY s.`business_name`,pa.`product_id`
    ) t2
    ON p.`BUSINESS_AREA` = t2.business_name
    AND p.`PRODUCT_BAR` = t2.product_code2
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_warehouse_product_presence',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
 COMMIT;
    END