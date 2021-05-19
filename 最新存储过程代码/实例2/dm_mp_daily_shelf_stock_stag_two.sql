CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_mp_daily_shelf_stock_stag_two`(in_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate2 = in_sdate;
SET @sdate3 = ADDDATE(in_sdate,1);
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_shelf_onload_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_shelf_onload_tmp
(KEY idx_shelf(shelf_id))
AS
SELECT @sdate2,f.shelf_id,SUM(f.`actual_apply_num`) AS onload_num 
FROM 
fe_dwd.`dwd_fill_day_inc` f,
fe_dwd.`dwd_shelf_base_day_all` s
WHERE f.order_status IN (1,2)
AND f.fill_type IN (1,2,8,9,11)
AND f.apply_time >= DATE_SUB(@sdate3,INTERVAL 7 DAY) 
AND f.apply_time < @sdate3
AND f.`SHELF_ID` = s.shelf_id
AND (s.shelf_status IN (2,5) OR (s.shelf_status = 3 AND s.`REVOKE_TIME` >= @sdate3))
AND s.shelf_type IN (1,2,3,4,5,6,8) 
GROUP BY shelf_id
;
# 4、0库存货架数，滞销品占比超60%，库存不足,只存当前一天的
TRUNCATE TABLE fe_dm.dm_mp_daily_shelf_stock_stag_detail;
INSERT INTO fe_dm.dm_mp_daily_shelf_stock_stag_detail
(sdate,
 shelf_id ,
 grade,
 shelf_type,
 stock_qty,
 stag_rate
)
SELECT @sdate2
,t1.`SHELF_ID`
,t1.`grade_cur_month`
,t1.shelf_type
,SUM(t3.stock_quantity) AS stock_qty
,IFNULL(SUM(IF(t3.`SALES_FLAG` IN (4,5),t3.stock_quantity,0))/SUM(t3.stock_quantity),0) AS stag_rate
FROM 
fe_dwd.`dwd_shelf_base_day_all` t1
JOIN fe_dwd.`dwd_shelf_product_day_all` t3
ON t1.shelf_id = t3.shelf_id
AND (t1.shelf_status IN (2,5) OR (t1.shelf_status = 3 AND t1.`REVOKE_TIME` >= @sdate3))
AND t1.shelf_type IN (1,2,3,4,5,6,8) 
AND t3.`STOCK_QUANTITY` > 0 
GROUP BY t1.`SHELF_ID`;
# 当日留存货架数
SELECT COUNT(IF(a.ACTIVATE_TIME < @sdate3 AND (a.`SHELF_STATUS` IN (2,5) OR (a.`SHELF_STATUS` = 3 AND a.`REVOKE_TIME` >= @sdate3)) ,a.SHELF_ID,NULL)) AS daily_remain_shelf  #'本周留存货架'
INTO @daily_remain_shelf
FROM fe_dwd.`dwd_shelf_base_day_all` a
WHERE a.MANAGER_NAME NOT LIKE '%作废%' 
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
FROM fe_dm.dm_mp_daily_shelf_stock_stag_detail t1
LEFT JOIN fe_dm.dm_sc_shelf_onload_tmp t2
ON t1.shelf_id = t2.shelf_id
;
# 写入结果表
DELETE FROM fe_dm.dm_mp_daily_shelf_stock_stag WHERE sdate= @sdate2;
INSERT INTO fe_dm.dm_mp_daily_shelf_stock_stag
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
'dm_mp_daily_shelf_stock_stag_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_mp_daily_shelf_stock_stag_detail','dm_mp_daily_shelf_stock_stag_two','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_mp_daily_shelf_stock_stag','dm_mp_daily_shelf_stock_stag_two','吴婷');
COMMIT;
    END