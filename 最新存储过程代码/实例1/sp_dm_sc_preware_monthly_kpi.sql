CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_dm_sc_preware_monthly_kpi`(in_sdate DATETIME)
BEGIN   
# 月度KPI继续
# 月度KPI
SET @sdate = in_sdate;
SET @sdate1 = IF(LAST_DAY(@sdate) < SUBDATE(CURDATE(),1),LAST_DAY(@sdate),SUBDATE(CURDATE(),1));
SET @sdate_last = LAST_DAY(@sdate);
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_monthly_kpi_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_monthly_kpi_tmp
(INDEX idx_business_warehouse(business_area,warehouse_id))
AS
SELECT 
    DATE_FORMAT(t.`sdate`, "%Y-%m") AS smonth
    , t.region_area
    , t.`business_area`
    , t.`warehouse_id` 
    , t.`shelf_code`
    , t.`shelf_name`
--     , SUM(IF(t.sdate = @sdate1,t.`cover_shelf_cnt`,0)) cover_shelf_cnt
    -- 需要覆盖货架的甲乙级，丙丁级数量
    , SUM(t.`GMV`) gmv_month
    , SUM(t.`quantity`) quantity_month
    , SUM(t.available_stock) available_stock  # 增加的月度库存5指标-1
    , SUM(t.available_stock * p.`purchase_price`) available_amount # 增加的月度库存金额5指标-2
    , SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT t.whether_close ORDER BY t.sdate DESC SEPARATOR ","),",",1) whether_close
    , SUM(t.`available_stock` * p.`purchase_price`)/SUM(t.actual_send_num * p.`purchase_price` ) AS turn_over_day
    , SUM(IF(t.sdate = @sdate1,t.seriously_stagnant_amount,0)) seriously_stagnant_amount
    , SUM(IF(t.sdate = @sdate1,t.`available_stock` * p.`purchase_price`,0)) seriously_stagnant_all
    , SUM(IF(t.sdate = @sdate1,t.seriously_lack_amount,0)) seriously_lack_amount
    , SUM(IF(t.sdate = @sdate1,t.`available_stock` * p.`purchase_price`,0)) seriously_lack_all 
    , SUM(IF(t.satisfy = "满足" AND t.product_type IN ("原有","新增（试运行）") AND t.sales_level IN ("爆款","畅销","平销"),1,0)) satisfy_cnt    
    , SUM(IF(t.product_type IN ("原有","新增（试运行）") AND t.sales_level IN ("爆款","畅销","平销"),1,0)) satisfy_all   
    ,SUM(IF(t.sdate = @sdate1 AND t.product_type IN ("个性化商品","淘汰（替补）","退出"),t.`available_stock` * p.`purchase_price`,0)) obsolete_amount
    , SUM(IF(t.sdate = @sdate1,t.`available_stock` * p.`purchase_price`,0))  obsolete_rate_all  
   --  , SUM(IF(e.warehouse_id AND e.product_type = "原有" AND t.sales_level IN ("爆款","畅销","平销") AND e.`forteen_bef_out` > 0  
--       AND t.`available_stock`/(e.`forteen_bef_out`/14) >= 2  AND t.`available_stock`/(e.`forteen_bef_out`/14) <= 15 ,1,0)) fill_intime
--     , SUM(IF(e.warehouse_id AND e.product_type = "原有" AND t.sales_level IN ("爆款","畅销","平销") AND e.`forteen_bef_out` >0 ,1,0)) fill_intime_all
FROM feods.`d_sc_preware_daily_report` t 
-- JOIN fe_dm.`dm_sc_current_dynamic_purchase_price` p
-- ON t.`sdate` >= DATE_FORMAT(@sdate,"%Y-%m-01") AND t.`sdate` <= @sdate_last
-- and t.`business_area` = p.`business_area`
-- AND t.`product_id` = p.`product_id`
JOIN feods.`wt_monthly_manual_purchase_price` p 
ON t.`sdate` >= DATE_FORMAT(@sdate,"%Y-%m-01") AND t.`sdate` <= @sdate_last
AND p.stat_month = @sdate_last       
AND t.`business_area` = p.business_area 
AND t.`PRODUCT_code2` = p.product_code2 
-- LEFT JOIN feods.pj_fill_order_efficiency e
-- ON t.sdate = e.apply_time
-- AND t.warehouse_id = e.warehouse_id
-- AND t.product_id = t.product_id
GROUP BY smonth,t.`business_area`,t.`warehouse_id`
-- HAVING whether_close = 2
;
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_monthly_fillintime_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_monthly_fillintime_tmp
(INDEX idx_business_warehouse(business_area,warehouse_id))
AS
SELECT 
    DATE_FORMAT(t.`sdate`, "%Y-%m") AS smonth
    , t.region_area
    , t.`business_area`
    , t.`warehouse_id` 
    , t.`shelf_code`
    , t.`shelf_name`
    , SUM(IF(e.warehouse_id AND e.product_type = "原有" AND t.sales_level IN ("爆款","畅销","平销") AND e.`forteen_bef_out` > 0  
      AND t.`available_stock`/(e.`forteen_bef_out`/14) >= 2  AND t.`available_stock`/(e.`forteen_bef_out`/14) <= 15 ,1,0)) fill_intime
    , SUM(IF(e.warehouse_id AND e.product_type = "原有" AND t.sales_level IN ("爆款","畅销","平销") AND e.`forteen_bef_out` >0 ,1,0)) fill_intime_all
FROM feods.`d_sc_preware_daily_report` t 
LEFT JOIN feods.pj_fill_order_efficiency e
ON t.sdate = e.apply_time
AND t.warehouse_id = e.warehouse_id
AND t.product_id = t.product_id
where t.`sdate` >= DATE_FORMAT(@sdate,"%Y-%m-01") AND t.`sdate` <= @sdate_last
GROUP BY smonth,t.`business_area`,t.`warehouse_id`
;
# 增加的月度出库（剔除非正常补货的量及前置仓调前置仓的量）5指标-3、4
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_monthly_out_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_monthly_out_tmp
(INDEX idx_business_warehouse(business_area,warehouse_id))
AS
SELECT 
    po.`out_month`
    , po.`business_area`
    , po.`warehouse_id`
    , SUM(po.`send_nogroup`) send_nogroup
    , SUM(po.`send_nogroup` * p.purchase_price) send_nogroup_amount 
FROM
    feods.`preware_outbound_monthly` po 
    JOIN feods.`wt_monthly_manual_purchase_price` p 
        ON po.`business_area` = p.business_area 
        AND po.`product_id` = p.product_id 
        AND p.stat_month = @sdate_last 
WHERE po.`out_month` = DATE_FORMAT(@sdate, "%Y-%m") 
GROUP BY po.`out_month`
    , po.`business_area`
    , po.`warehouse_id` ;   
# 增加的月度入库（剔除非撤架及货架商品调回的数量）5指标-5
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_monthly_fill_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_monthly_fill_tmp
(INDEX idx_business_warehouse(business_area,warehouse_id))
AS
SELECT 
    f.region_area
    , f.business_area
    , f.warehouse_id
    , SUM(f.ACTUAL_FILL_NUM) AS fill_noback
    , SUM(f.`ACTUAL_FILL_NUM` * p.purchase_price) AS fill_noback_amount
FROM
    feods.`preware_fill_daily` f 
    JOIN feods.`wt_monthly_manual_purchase_price` p 
        ON f.`business_area` = p.business_area 
        AND f.`product_id` = p.product_id 
        AND p.stat_month = @sdate_last 
WHERE f.fill_date >= DATE_FORMAT(@sdate, "%Y-%m-01") 
    AND f.fill_date < @sdate_last 
    AND fill_type IN (1, 2, 4, 8, 10) 
GROUP BY f.business_area
    , f.warehouse_id ;
# 2、当月最后一天的覆盖货架数等
-- 覆盖货架数（总数）
-- 覆盖货架占比   上面两个直接用日报的结果
-- 覆盖甲乙级货架数
-- 覆盖丙丁级货架数
# 后两个
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_cover_shelf_grade_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_cover_shelf_grade_tmp
(INDEX idx_warehouse(warehouse_id))
AS
-- SELECT 
--     g.`prewarehouse_id` AS warehouse_id
--     , COUNT(IF(g.`grade` IN ("甲", "乙"),g.`shelf_id`,NULL)) shelf_cnt_a
--     , COUNT(IF(g.`grade` IN ("丙", "丁"),g.`shelf_id`,NULL)) shelf_cnt_b
--     , COUNT(g.shelf_id) cover_shelf_cnt
-- FROM fe_dwd.`dwd_shelf_day_his` g
-- WHERE g.`sdate`	= @sdate1
-- AND g.`shelf_status` = 2
-- and g.whether_close = 2 
-- AND g.`prewarehouse_id` IS NOT NULL
-- GROUP BY g.`prewarehouse_id`
-- ;
select t.`prewarehouse_id` as warehouse_id
    , COUNT(IF(g.`grade` IN ("甲", "乙"),g.`shelf_id`,NULL)) shelf_cnt_a
    , COUNT(IF(g.`grade` IN ("丙", "丁"),g.`shelf_id`,NULL)) shelf_cnt_b
from fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` t
join feods.`d_op_shelf_grade` g
on t.`shelf_id` = g.`shelf_id`
and g.`shelf_status` = '已激活'
and g.`month_id` = date_format(@sdate,"%Y-%m")
group by t.`prewarehouse_id`
;
# 合理性
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_fill_ration_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_fill_ration_tmp
(INDEX idx_business_warehouse(business_area,warehouse_id))
AS
SELECT e.business_area,e.warehouse_id
,SUM(CASE 
  WHEN e.actual_apply_num = sug.suggest_fill_qty THEN 1
  WHEN (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) >= 2 
  AND (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) <= 15 
  THEN 1
  WHEN (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) > 15 
  AND e.actual_apply_num <= s.F_BGJ_FBOXEDSTANDARDS THEN 1
  WHEN (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) > 15 
  AND e.`available_stock` <= 24 THEN 1
  END) AS fill_rational
  , COUNT(*) AS fill_rational_all
 ,SUM(CASE 
  WHEN e.actual_apply_num = sug.suggest_fill_qty THEN 1
  WHEN (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) >= 2 
  AND (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) <= 15 
  THEN 1
  WHEN (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) > 15 
  AND e.actual_apply_num <= s.F_BGJ_FBOXEDSTANDARDS THEN 1
  WHEN (e.`available_stock` + e.actual_apply_num)/(e.`forteen_bef_out`/14) > 15 
  AND e.`available_stock` <= 24 THEN 1
  END)/COUNT(*) AS fill_rational_rate
FROM feods.pj_fill_order_efficiency e
JOIN fe_dwd.`dwd_product_base_day_all` s
ON e.product_id = s.product_id
LEFT JOIN 
(
SELECT DATE_ADD(t.`sdate`,INTERVAL 1 DAY ) AS sdate
, t.`warehouse_id`
, t.`product_id`
, t.`suggest_fill_qty`
FROM feods.`d_sc_preware_daily_report` t
WHERE t.`suggest_fill_qty` > 0
AND t.sdate >= DATE_FORMAT(@sdate,"%Y-%m-01")
AND t.sdate <= LAST_DAY(@sdate)
) sug
ON e.`apply_time` = sug.sdate
AND e.`warehouse_id` = sug.warehouse_id
AND e.`PRODUCT_ID` = sug.product_id
WHERE e.apply_time >= DATE_FORMAT(@sdate,"%Y-%m-01")
AND e.apply_time <= @sdate_last
AND e.product_type ='原有'
AND e.`forteen_bef_out` > 0 
GROUP BY e.business_area,e.warehouse_id
;
# 盘点结果
-- 库存准确率	取前置站盘点界面数据，计算公式：库存准确率=（库存金额-ABS（货损金额））/库存金额											
-- 报损金额	取前置站盘点界面数据											
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_monthly_check_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_monthly_check_tmp
(INDEX idx_warehouse(warehouse_id))
AS 
SELECT 
      c.`SHELF_ID` AS warehouse_id
--     , c.`PRODUCT_ID`
    , SUM(c.STOCK_NUM) AS STOCK_NUM
    , SUM(c.`CHECK_NUM`) AS CHECK_NUM 
    , SUM(IFNULL(c.AUDIT_ERROR_NUM,0)) AS AUDIT_ERROR_NUM
--     , w.purchase_price
    , SUM(c.STOCK_NUM * w.purchase_price) AS stock_amount
    , SUM(IFNULL(c.AUDIT_ERROR_NUM,0)* w.purchase_price) AS audit_amount
FROM
    fe_dwd.`dwd_check_base_day_inc` c 
    JOIN fe_dwd.`dwd_shelf_base_day_all` s 
        ON c.`SHELF_ID` = s.`shelf_id` 
        AND s.`shelf_type` = 9 # 前置仓盘点
    JOIN feods.`wt_monthly_manual_purchase_price` w 
        ON s.`business_name` = w.business_area 
        AND c.`PRODUCT_ID` = w.product_id 
        AND w.stat_month = @sdate_last 
WHERE c.`DATA_FLAG` = 1 
    AND c.`OPERATE_TIME` >= DATE_FORMAT(@sdate, "%Y-%m-01") 
    AND c.`OPERATE_TIME` <= LAST_DAY(@sdate)
    AND c.`CHECK_STATUS` = 1
    AND c.`AUDIT_STATUS` = 2
GROUP BY c.`SHELF_ID`
;
# 最终结果
DELETE FROM fe_dm.`dm_sc_preware_monthly_kpi` WHERE smonth = DATE_FORMAT(@sdate,"%Y-%m") ;
INSERT INTO fe_dm.`dm_sc_preware_monthly_kpi`
(
smonth
,region_area
, business_area
, warehouse_id
, shelf_code
, shelf_name
, all_shelf
, all_qzc_shelf
, `SHELF_NUMBER`
, `QZC_SHELF_NUMBER`
,  QZC_NUMBER
, `COVERAGE_RATE`
, shelf_cnt_a
, shelf_cnt_b 
, available_stock
, available_amount
, send_nogroup
, send_nogroup_amount
, fill_noback 
, fill_noback_amount
, gmv_month
, quantity_month 
, turn_over_day
, seriously_stagnant_amount
, seriously_stagnant_all
, seriously_lack_amount
, seriously_lack_all
, satisfy_cnt
, satisfy_all
, obsolete_amount
, obsolete_rate_all
, fill_intime
, fill_intime_all
, fill_rational_all
, fill_rational
, STOCK_NUM
, CHECK_NUM
, AUDIT_ERROR_NUM
, stock_amount
, audit_amount 
)
SELECT t1.smonth
, t1.region_area
, t1.business_area
, t1.warehouse_id
, t1.shelf_code
, t1.shelf_name
, c.all_shelf
, c.all_qzc_shelf
, c.`SHELF_NUMBER`
, c.`QZC_SHELF_NUMBER`
, c.QZC_NUMBER
, c.`COVERAGE_RATE`
, IFNULL(t4.shelf_cnt_a,0) shelf_cnt_a
, IFNULL(t4.shelf_cnt_b,0) shelf_cnt_b 
, t1.available_stock
, t1.available_amount
, IFNULL(t2.send_nogroup,0) send_nogroup
, IFNULL(t2.send_nogroup_amount,0) send_nogroup_amount
, IFNULL(t3.fill_noback,0) fill_noback 
, IFNULL(t3.fill_noback_amount,0) fill_noback_amount
, t1.gmv_month
, t1.quantity_month 
, t1.turn_over_day
, t1.seriously_stagnant_amount
, t1.seriously_stagnant_all
, t1.seriously_lack_amount
, t1.seriously_lack_all
, t1.satisfy_cnt
, t1.satisfy_all
, t1.obsolete_amount
, t1.obsolete_rate_all
, t7.fill_intime
, t7.fill_intime_all
, IFNULL(t5.fill_rational_all,0) fill_rational_all
, IFNULL(t5.fill_rational,0) fill_rational
, IFNULL(t6.STOCK_NUM,0) STOCK_NUM
, IFNULL(t6.CHECK_NUM,0) CHECK_NUM
, IFNULL(t6.AUDIT_ERROR_NUM,0) AUDIT_ERROR_NUM
, IFNULL(t6.stock_amount,0) stock_amount
, IFNULL(t6.audit_amount,0) audit_amount 
-- , t1.gmv_month
-- , t1.quantity_month
-- , t1.available_stock
-- , t1.available_amount
-- , t1.whether_close
-- , t1.turn_over_day
-- , t1.seriously_stagnant_amount
-- , t1.seriously_stagnant_all
-- , t1.seriously_lack_amount
-- , t1.seriously_lack_all
-- , t1.satisfy_cnt
-- , t1.satisfy_all
-- , t1.obsolete_amount
-- , t1.obsolete_rate_all
-- , t1.fill_intime
-- , t1.fill_intime_all
-- , ifnull(t2.send_nogroup,0) send_nogroup
-- , ifnull(t2.send_nogroup_amount,0) send_nogroup_amount
-- , ifnull(t3.fill_noback,0) fill_noback 
-- , ifnull(t3.fill_noback_amount,0) fill_noback_amount
-- , ifnull(t4.cover_shelf_cnt,0) cover_shelf_cnt
-- , ifnull(t4.shelf_cnt_a,0) shelf_cnt_a
-- , ifnull(t4.shelf_cnt_b,0) shelf_cnt_b
-- , ifnull(t5.fill_rational_all,0) fill_rational_all
-- , ifnull(t5.fill_rational,0) fill_rational
-- , ifnull(t5.fill_rational_rate,0) fill_rational_rate
-- , ifnull(t6.STOCK_NUM,0) STOCK_NUM
-- , ifnull(t6.CHECK_NUM,0) CHECK_NUM
-- , ifnull(t6.AUDIT_ERROR_NUM,0) AUDIT_ERROR_NUM
-- , ifnull(t6.stock_amount,0) stock_amount
-- , ifnull(t6.audit_amount,0) audit_amount
FROM feods.d_sc_preware_monthly_kpi_tmp t1
LEFT JOIN feods.d_sc_preware_monthly_out_tmp t2
ON t1.warehouse_id = t2.warehouse_id
LEFT JOIN feods.d_sc_preware_monthly_fill_tmp t3
ON t1.warehouse_id = t3.warehouse_id
LEFT JOIN feods.d_sc_preware_cover_shelf_grade_tmp t4
ON t1.warehouse_id = t4.warehouse_id
LEFT JOIN feods.d_sc_preware_fill_ration_tmp t5
ON t1.warehouse_id = t5.warehouse_id
LEFT JOIN feods.d_sc_preware_monthly_check_tmp t6
ON t1.warehouse_id = t6.warehouse_id
JOIN feods.`pj_prewarehouse_coverage_rate` c
ON t1.business_area = c.`BUSINESS_AREA`
AND c.`CHECK_DATE` = @sdate1
AND c.`SHELF_NUMBER` >0 
left join feods.d_sc_preware_monthly_fillintime_tmp t7
on t1.business_area =t7.business_area
and t1.warehouse_id = t7.warehouse_id
;
 
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_dm_sc_preware_monthly_kpi',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('wuting@', @user, @timestamp)
  );
COMMIT;
    END