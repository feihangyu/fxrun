CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_daily_shelf_stock_stag`(in_sdate date)
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
		
    SET l_task_name = 'sp_d_mp_daily_shelf_stock_stag'; 
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();   
SET @sdate2 = in_sdate;
SET @sdate3 = ADDDATE(in_sdate,1);
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_shelf_onload_tmp;
CREATE TEMPORARY TABLE feods.d_sc_shelf_onload_tmp
(KEY idx_shelf(shelf_id))
AS
SELECT @sdate2,f.shelf_id,SUM(f.product_num) AS onload_num 
FROM 
fe.sf_product_fill_order f,
fe.`sf_shelf` s
WHERE f.order_status IN (1,2)
AND f.fill_type IN (1,2,8,9,11)
AND f.`DATA_FLAG` = 1
AND f.apply_time >= DATE_SUB(@sdate3,INTERVAL 7 DAY) 
AND f.apply_time < @sdate3
AND f.`SHELF_ID` = s.shelf_id
AND (s.shelf_status IN (2,5) OR (s.shelf_status = 3 AND s.`REVOKE_TIME` >= @sdate3))
AND s.shelf_type IN (1,2,3,4,5,6,8) 
AND s.data_flag =1
GROUP BY shelf_id
;
# 4、0库存货架数，滞销品占比超60%，库存不足,只存当前一天的
TRUNCATE TABLE feods.d_mp_daily_shelf_stock_stag_detail;
INSERT INTO feods.d_mp_daily_shelf_stock_stag_detail
(sdate,
 shelf_id ,
 grade,
 shelf_type,
 stock_qty,
 stag_rate
)
SELECT @sdate2
,t1.`SHELF_ID`
,t2.grade
,t1.shelf_type
,SUM(t3.stock_quantity) AS stock_qty
,IFNULL(SUM(IF(t4.`SALES_FLAG` IN (4,5),t3.stock_quantity,0))/SUM(t3.stock_quantity),0) AS stag_rate
FROM 
fe.`sf_shelf` t1
JOIN feods.`d_op_shelf_grade` t2
ON t1.shelf_id = t2.shelf_id
AND (t1.shelf_status IN (2,5) OR (t1.shelf_status = 3 AND t1.`REVOKE_TIME` >= @sdate3))
AND t1.shelf_type IN (1,2,3,4,5,6,8) 
AND t1.`DATA_FLAG` =1
AND t2.month_id = DATE_FORMAT(@sdate2,'%Y-%m')
JOIN fe.`sf_shelf_product_detail` t3
ON t1.shelf_id = t3.shelf_id
AND t3.`STOCK_QUANTITY` > 0 
AND t3.data_flag = 1
LEFT JOIN fe.`sf_shelf_product_detail_flag` t4
ON t3.shelf_id = t4.shelf_id
AND t3.product_id = t4.product_id
AND t4.data_flag = 1
GROUP BY t1.`SHELF_ID`;
# 当日留存货架数
SELECT COUNT(IF(a.ACTIVATE_TIME < @sdate3 AND (a.`SHELF_STATUS` IN (2,5) OR (a.`SHELF_STATUS` = 3 AND a.`REVOKE_TIME` >= @sdate3)) ,a.SHELF_ID,NULL)) AS daily_remain_shelf  #'本周留存货架'
INTO @daily_remain_shelf
FROM fe.sf_shelf a
WHERE a.DATA_FLAG=1 
AND a.MANAGER_NAME NOT LIKE '%作废%' 
AND a.SHELF_TYPE IN (1,2,3,4,5,6,8) # 非前置仓,非自贩机
AND a.SHELF_STATUS IN (2,3,5);
# 当日0库存，滞销占比超60%货架，库存不足货架数统计
SELECT COUNT(t1.shelf_id) AS stock_shelf_cnt
,SUM(CASE 
WHEN t1.grade IN ("甲","乙") AND t1.stock_qty + IFNULL(t2.onload_num,0) < 180 AND t1.shelf_type IN (1,3) THEN 1
WHEN t1.grade IN ("甲","乙") AND t1.stock_qty + IFNULL(t2.onload_num,0) < 110 AND t1.shelf_type IN (2,5) THEN 1
WHEN t1.stock_qty + IFNULL(t2.onload_num,0) < 110 AND t1.shelf_type IN (1,3) THEN 1
WHEN t1.stock_qty + IFNULL(t2.onload_num,0) < 90 AND t1.shelf_type IN (2,5) THEN 1
END) AS stock_lack_shelf
, SUM(IF( t1.stag_rate >= 0.6,1,0)) AS stag_shelf_cnt
INTO @stock_shelf_cnt,@stock_lack_shelf,@stag_shelf_cnt
FROM feods.d_mp_daily_shelf_stock_stag_detail t1
LEFT JOIN feods.d_sc_shelf_onload_tmp t2
ON t1.shelf_id = t2.shelf_id
;
# 写入结果表
DELETE FROM feods.d_mp_daily_shelf_stock_stag WHERE sdate= @sdate2;
INSERT INTO feods.d_mp_daily_shelf_stock_stag
(`sdate`
, daily_remain_shelf
, stock_shelf_cnt
, no_stock_shelf
, stock_lack_shelf
, stag_shelf_cnt
 )
SELECT
    @sdate2
    , @daily_remain_shelf
    , @stock_shelf_cnt
    , @daily_remain_shelf - @stock_shelf_cnt AS no_stock_shelf
    , @stock_lack_shelf
    , @stag_shelf_cnt
    ;
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sf_new_product_gmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END