CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_week_kpi_monitor`(in_sdate DATE)
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
		
    SET l_task_name = 'sp_d_mp_week_kpi_monitor'; 
 
SET @wd = IF( WEEKDAY(in_sdate) >= 3,WEEKDAY(in_sdate) -3 , WEEKDAY(in_sdate) +4 ); 
SET @sdate1 = SUBDATE(in_sdate,@wd + 6);
SET @sdate2 = SUBDATE(in_sdate,@wd);
SET @sdate3 = ADDDATE(@sdate2,1);
SET @smonth = DATE_FORMAT(@sdate2,"%Y-%m");
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 1、本周货架截存请款
SELECT COUNT(IF(a.ACTIVATE_TIME>= @sdate1 AND a.ACTIVATE_TIME< @sdate3 AND a.`SHELF_STATUS` IN (2,5),a.SHELF_ID,NULL)) AS week_new_shelf  #'本周新增货架'
,COUNT(IF(a.ACTIVATE_TIME>= @sdate1 AND a.ACTIVATE_TIME< @sdate3 AND (a.`SHELF_STATUS` IN (2,5) OR (a.`SHELF_STATUS` = 3 AND a.`REVOKE_TIME` >= @sdate3)),a.SHELF_ID,NULL)) AS week_new_shelf_remain  #'本周新增货架(不含本周撤架)'
,COUNT(IF(a.`REVOKE_TIME`>= @sdate1 AND a.`REVOKE_TIME`< @sdate3 AND a.`SHELF_STATUS` = 3,a.SHELF_ID,NULL)) AS week_revoke_shelf #'本周撤架货架'
,COUNT(IF((a.ACTIVATE_TIME< @sdate3 AND a.`SHELF_STATUS` IN (2,5) OR (a.`SHELF_STATUS` = 3 AND a.`REVOKE_TIME` >= @sdate3)) ,a.SHELF_ID,NULL)) AS week_remain_shelf  #'本周留存货架'
INTO  @week_new_shelf, @week_new_shelf_remain, @week_revoke_shelf, @week_remain_shelf
FROM fe.sf_shelf a
WHERE a.DATA_FLAG=1 
AND a.MANAGER_NAME NOT LIKE '%作废%' 
AND a.SHELF_TYPE IN (1,2,3,4,5,6,8) # 非前置仓,非自贩机
AND a.SHELF_STATUS IN (2,3,5);  # 2已激活，3已撤架 5为已注销
# 0库存货架新口径
-- SELECT  COUNT(DISTINCT t1.shelf_id) - COUNT(DISTINCT(IF(t2.d5 > 0, t2.`shelf_id`,NULL))) AS no_stock_shelf_new
-- INTO @no_stock_shelf_new
-- FROM
-- (SELECT a.`SHELF_ID`
-- FROM fe.sf_shelf a
-- WHERE a.DATA_FLAG=1 
-- AND a.MANAGER_NAME NOT LIKE '%作废%' 
-- AND a.SHELF_TYPE IN (1,2,3,5,6,8) # 非前置仓,非自贩机,非自贩机
-- AND a.SHELF_STATUS = 2
-- AND a.`REVOKE_STATUS` = 1
-- AND a.`WHETHER_CLOSE` = 2
-- AND CASE WHEN a.`SHELF_TYPE` IN (1,2,3,5) AND DATEDIFF(@sdate3,a.ACTIVATE_TIME) >1 THEN a.`SHELF_ID`
-- WHEN a.`SHELF_TYPE` = 6 AND DATEDIFF(@sdate3,a.ACTIVATE_TIME) > 5 THEN a.`SHELF_ID`
-- WHEN a.`SHELF_TYPE` = 8 AND DATEDIFF(@sdate3,a.ACTIVATE_TIME) > 4 THEN a.`SHELF_ID`
-- END) t1
-- LEFT JOIN feods.`d_op_sp_stock_detail` t2
-- ON t1.shelf_id = t2.shelf_id
-- AND t2.`month_id` = '2019-12';
SET @sql_str = 
CONCAT
(
'SELECT  COUNT(DISTINCT t1.shelf_id) - COUNT(DISTINCT(IF(t2.d',
DAY(@sdate2),
' > 0, t2.`shelf_id`,NULL))) AS no_stock_shelf_new
INTO @no_stock_shelf_new
FROM
(SELECT a.`SHELF_ID`
FROM fe.sf_shelf a
WHERE a.DATA_FLAG=1 
AND a.MANAGER_NAME NOT LIKE "%作废%" 
AND a.SHELF_TYPE IN (1,2,3,5,6,8) # 非前置仓,非自贩机,非自贩机
AND a.SHELF_STATUS = 2
AND a.`REVOKE_STATUS` = 1
AND a.`WHETHER_CLOSE` = 2
AND CASE WHEN a.`SHELF_TYPE` IN (1,2,3,5) AND DATEDIFF(@sdate3,a.ACTIVATE_TIME) >1 THEN a.`SHELF_ID`
WHEN a.`SHELF_TYPE` = 6 AND DATEDIFF(@sdate3,a.ACTIVATE_TIME) > 5 THEN a.`SHELF_ID`
WHEN a.`SHELF_TYPE` = 8 AND DATEDIFF(@sdate3,a.ACTIVATE_TIME) > 4 THEN a.`SHELF_ID`
END) t1
LEFT JOIN feods.`d_op_sp_stock_detail` t2
ON t1.shelf_id = t2.shelf_id
AND t2.`month_id` = @smonth ;'
)
;
PREPARE stm_sql FROM @sql_str;
EXECUTE stm_sql;
-- select @sdate,@week_new_shelf, @week_new_shelf_remain, @week_revoke_shelf, @week_remain_shelf;
# 2、本周库存和销量情况
SELECT SUM(IF(o.`PAY_DATE` >= SUBDATE(@sdate3,7) AND o.`PAY_DATE` < @sdate3,o.`sale_price`*o.`quantity_act`,0)) AS week_gmv
,SUM(IF(o.`PAY_DATE` >= SUBDATE(@sdate3,14) AND o.`PAY_DATE` < SUBDATE(@sdate3,7),o.`sale_price`*o.`quantity_act`,0)) AS last_week_gmv
,SUM(IF(o.`PAY_DATE` >= DATE_FORMAT(@sdate2,"%Y-%m-01") AND o.`PAY_DATE` < @sdate3,o.`sale_price`*o.`quantity_act`,0)) AS month_gmv
,SUM(IF(o.`PAY_DATE` >= DATE_FORMAT(SUBDATE(@sdate2,INTERVAL 1 MONTH),"%Y-%m-01")  AND o.`PAY_DATE` < DATE_FORMAT(@sdate2,"%Y-%m-01"),o.`sale_price`*o.`quantity_act`,0)) AS last_month_gmv
,SUM(IF(o.`PAY_DATE` >= DATE_FORMAT(@sdate2,"%Y-%m-01") AND o.`PAY_DATE` < @sdate3,o.`sale_price`*o.`quantity_act`,0)) 
/ DAY(@sdate2) *30 AS predict_month_gmv
,COUNT(DISTINCT o.shelf_id) AS sale_shelf_cnt
INTO @week_gmv,@last_week_gmv, @month_gmv, @last_month_gmv, @predict_month_gmv,@sale_shelf_cnt
FROM fe_dwd.`dwd_order_item_refund_day` o
WHERE o.`ORDER_STATUS` = 2 # 不包括自贩机
AND o.`PAY_DATE` >= DATE_FORMAT(SUBDATE(@sdate2,INTERVAL 1 MONTH),"%Y-%m-01") 
AND o.`PAY_DATE` < @sdate3
;
-- select @week_gmv,@last_week_gmv, @month_gmv, @last_month_gmv, @predict_month_gmv;
 
#3、周日均0库存货架数，滞销货架数，库存不足货架数
# 滞销，缺货，0库存
SELECT 
SUM(IF(t.sdate = @sdate2,stock_shelf_cnt,0)) AS stock_shelf_cnt
, SUM(IF(t.sdate = @sdate2 ,no_stock_shelf,0)) AS no_stock_shelf
, ROUND(AVG(stock_lack_shelf)) AS stock_lack_shelf
, ROUND(AVG(stag_shelf_cnt)) AS stag_shelf_cnt
, AVG(stock_lack_shelf/daily_remain_shelf) AS lack_shelf_rate
, AVG(stag_shelf_cnt/daily_remain_shelf) AS stag_shelf_rate
INTO @stock_shelf_cnt,@no_stock_shelf,@stock_lack_shelf,@stag_shelf_cnt,@lack_shelf_rate,@stag_shelf_rate
FROM feods.`d_mp_daily_shelf_stock_stag` t
WHERE t.`sdate` >= @sdate1 AND t.`sdate` <= @sdate2
;
DELETE FROM feods.d_mp_week_kpi_monitor WHERE sdate = @sdate2;
INSERT INTO feods.d_mp_week_kpi_monitor
(`sdate` ,
  week_new_shelf ,
  week_new_shelf_remain ,
  week_revoke_shelf ,
  week_remain_shelf ,
  sale_shelf_cnt,
  no_sale_shelf,
  stock_shelf_cnt ,
  no_stock_shelf ,
  stock_lack_shelf ,
  lack_shelf_rate,
  stag_shelf_cnt,
  stag_shelf_rate,
  week_gmv ,
  last_week_gmv ,
  month_gmv ,
  last_month_gmv ,
  predict_month_gmv
)
SELECT @sdate2
, @week_new_shelf
, @week_new_shelf_remain
, @week_revoke_shelf
, @week_remain_shelf
, @sale_shelf_cnt
, @week_remain_shelf - @sale_shelf_cnt AS no_sale_shelf
, @stock_shelf_cnt
, @no_stock_shelf_new
, @stock_lack_shelf
, @lack_shelf_rate
, @stag_shelf_cnt 
, @stag_shelf_rate
, @week_gmv
, @last_week_gmv
, @month_gmv
, @last_month_gmv
, @predict_month_gmv
;
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sf_new_product_gmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END