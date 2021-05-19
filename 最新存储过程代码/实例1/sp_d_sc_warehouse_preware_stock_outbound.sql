CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_warehouse_preware_stock_outbound`(in_sdate DATE)
    SQL SECURITY INVOKER
BEGIN
-- =============================================
-- Author:	wuting
-- Create date: 2019/07/26
-- Modify date: 
-- Description:	
-- 	监控大仓商品、前置仓库存、出库量、周转等 - 采购报表
-- 
-- =============================================
#（1）oms库存等
SET @sdate = in_sdate;
set @wdate = IF(WEEKDAY(@sdate) = 6,ADDDATE(@sdate,1),SUBDATE(@sdate,WEEKDAY(@sdate))); # 周日用下周一，其余用本周一
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_warehouse_daily_tmp;
CREATE TEMPORARY TABLE feods.d_sc_warehouse_daily_tmp
AS 
SELECT 
    t1.`FPRODUCEDATE`
    , t1.big_area AS region_area
    , t1.business_area
    , t1.warehouse_name
    , p.product_id
    , t1.product_bar AS product_code2
    , t1.product_name
    , t1.product_category
    , IFNULL(s.storage_amount,0) AS oms_qualityqty # oms_stock_quantity
    , IFNULL(t1.`QUALITYQTY`,0) + IFNULL(t1.`INFERQUAQTY`,0) AS erp_stock
    , t1.`product_type`
    , IFNULL(t2.out_day_sp,0) AS out_days #'货架+前置仓有出库天数'
    , IFNULL(t2.out_qty_sp,0) AS out_qty #'货架+前置仓近14天出库量'
    , IFNULL(t2.out_qty_sp/ t2.out_day_sp,0) AS avg_out_qty # '近14天日均出库量' 
    , IFNULL(t2.out_qty_shelf,0) AS dest_shelf_qty #近14天发往货架的出库量
    , IFNULL(t2.out_qty_shelf,0) * t3.`purchase_price` AS dest_shelf_amount #近14天发往货架的金额
    , IFNULL(t2.out_day_shelf,0) AS dest_shelf_days #货架出库天数
    , IFNULL(t2.out_qty_shelf/ t2.out_day_shelf,0) AS avg_shelf_qty #近14天发往货架的日均出库量
    , t3.`purchase_price` #'采购加权价',
    , IFNULL(s.storage_amount,0) * t3.`purchase_price` AS oms_stock_amount #库存金额
    ,IFNULL(t2.out_qty_sp * t3.`purchase_price`,0) AS forteen_out_amount #14出库金额
    ,IFNULL(t2.out_qty_sp/ t2.out_day_sp,0) * t3.`purchase_price` AS avg_out_amount #日均出库金额
    ,IF(ISNULL(s.storage_amount),0,s.storage_amount/IFNULL(t2.out_qty_sp/t2.out_day_sp ,0)) AS turnover_day #周转天数
    ,IFNULL(t4.stock_quantity,0)/DAY(@sdate)  AS avg_stock_month  #当月日均库存量
    ,IFNULL(t5.out_qty_sp,0)/DAY(@sdate) AS avg_out_month #当月日均出库量
    ,IFNULL(t4.stock_quantity,0)/IFNULL(t5.out_qty_sp,0) AS turnover_day_month #月度周转天
    -- , #满足
    ,IFNULL(t6.dest_shelf_curr,0) AS dest_shelf_qty_curr # 当日大仓出库量到货架
    ,IFNULL(t6.dest_shelf_curr,0) * t3.purchase_price  AS dest_shelf_amount_curr
    ,IFNULL(t6.fqty_curr ,0) AS all_qty_curr # 当日大仓出库量
    ,IFNULL(t6.fqty_curr ,0) * t3.purchase_price AS all_amount_curr
    
    ,IFNULL(t10.available_stock_curr,0) AS available_stock  # 当日前置仓累计库存
    ,IFNULL(t10.available_stock_curr,0) * t3.purchase_price AS 当日available_amount
    ,IFNULL(t10.actual_send_forteen,0) AS actual_send_forteen # 前置仓近14天出库
    ,IFNULL(t11.send_noholiday,0) AS send_nopre # 前置仓近14出库到货架（2020-04-10改和日报日均保持一致）
    ,p.F_BGJ_FBOXEDSTANDARDS
    ,sa.sale_level # 区域销售等级
    ,IFNULL(t10.actual_send_curr,0) AS preware_send_qty_curr # 前置仓当日出库
    ,IFNULL(t10.actual_send_curr,0) * t3.purchase_price AS preware_send_amount_curr
    ,IFNULL(t8.preware_avg_stock_month,0) AS preware_avg_stock_month
    ,IFNULL(t9.preware_avg_send_month,0) AS preware_avg_send_month
    ,IF(IFNULL(t8.available_stock,0) = 0 AND IFNULL(t9.actual_send_num,0) = 0,0,
     IFNULL(t8.preware_avg_stock_month,0) / IFNULL(t9.preware_avg_send_month,0))  AS preware_turnover_month
    ,IFNULL(t7.onload_qty,0) AS onload_qty # 大仓在途
    ,CASE 
    WHEN t1.product_type IN("原有","新增（试运行）") AND sa.sale_level IN ("热卖","非常好卖","好卖") THEN "快周转品"
    WHEN t1.product_type = "原有"  AND  sa.sale_level IN ("一般","局部好卖") THEN "正常周转品"
    WHEN t1.product_type = "新增（试运行）"  AND  sa.sale_level IN ("一般","局部好卖") THEN "观察周转品"
    WHEN t1.product_type IN("原有","新增（试运行）") AND (sa.sale_level IN ("非常不好卖","难卖") OR ISNULL(sa.sale_level))THEN "难周转品"
    ELSE "非正常品"    
    END AS stock_flag
    
    # 目标周转天原为下面的逻辑，2020-04-03日改回本逻辑
    ,CASE 
    WHEN t1.product_type IN("原有","新增（试运行）") AND sa.sale_level IN ("热卖","非常好卖","好卖") THEN 15
    WHEN t1.product_type = "原有"  AND  sa.sale_level IN ("一般","局部好卖") THEN 10
    WHEN t1.product_type = "新增（试运行）"  AND  sa.sale_level IN ("一般","局部好卖") THEN 8
    WHEN t1.product_type IN("原有","新增（试运行）") AND (sa.sale_level IN ("非常不好卖","难卖") OR ISNULL(sa.sale_level))THEN 3   
    END AS aim_turnover_day
--    ,CASE 
--     WHEN w.region_area IN ("华北","中西") THEN 12
--     WHEN w.region_area IN ("华南","华东") THEN 10  
--     END AS aim_turnover_day
   ,wp.presence_rate 
FROM fe_dwd.`dwd_pub_warehouse_business_area` w
# ERP库存主表
JOIN feods.`PJ_OUTSTOCK2_DAY` t1
ON w.warehouse_number = t1.warehouse_number
AND w.warehouse_type = 1
AND t1.FPRODUCEDATE = @sdate
AND t1.product_bar NOT LIKE "WZ%"
# BDP真实库存
LEFT JOIN fe_dwd.`dwd_sc_bdp_warehouse_stock_daily` s
ON t1.FPRODUCEDATE = s.`sdate`
AND t1.warehouse_number = s.warehouse
AND t1.`PRODUCT_BAR` = s.`sku_no`
# t2 近14天出库 情况
LEFT JOIN feods.`d_sc_warehouse_outbound_forteen_total` t2 
ON t1.`FPRODUCEDATE` = t2.sdate
AND t1.business_Area = t2.business_area
AND t1.`product_bar` = t2.`product_code2`
# t3 当日动态加权采购价
LEFT JOIN fe_dm.`dm_sc_current_dynamic_purchase_price` t3
ON t1.`business_area` = t3.`business_area`
AND t1.`product_bar` = t3.`product_code2`
# t4 本月累计库存
LEFT JOIN fe_dm.`dm_sc_warehouse_stock_monthly` t4
ON t1.business_area = t4.business_area
AND t1.product_bar = t4.product_code2
AND t4.smonth = DATE_FORMAT(@sdate,"%Y-%m")
# t5 本月累计出库
LEFT JOIN fe_dm.`dm_sc_warehouse_outbound_monthly_total` t5 
ON t1.business_area = t5.business_area
AND t1.product_bar = t5.product_code2
AND t5.smonth = DATE_FORMAT(@sdate,"%Y-%m")
# p 箱规及商品信息
JOIN fe_dwd.`dwd_product_base_day_all` p
ON t1.product_bar = p.product_code2
# t6 大仓当日出库
LEFT JOIN
( 
SELECT t.business_area
, t.product_code2
, SUM(FQTY) AS fqty_curr 
, SUM(IF( destination = '货架', FQTY,0)) AS dest_shelf_curr
FROM feods.`d_sc_warehouse_outbound_daily` t
WHERE t.sdate = @sdate
GROUP BY t.business_area,t.product_code2
) t6
ON t1.business_area = t6.business_area
AND t1.product_bar = t6.product_code2
# t7 当日大仓在途
LEFT JOIN feods.d_sc_warehouse_onload t7
ON  t1.business_Area = t7.business_area
AND t1.product_bar = t7.product_code2
# t8 当月前置仓累计库存
LEFT JOIN
(
SELECT t.business_area
, t.product_code2
, SUM(t.available_stock) available_stock 
, SUM(t.available_stock) / DAY(@sdate) preware_avg_stock_month
FROM feods.pj_prewarehouse_stock_detail_monthly t
WHERE t.check_month = DATE_FORMAT(@sdate,"%Y-%m") 
GROUP BY t.business_area,t.product_code2
) t8
ON t1.business_area = t8.business_area
AND t1.product_bar = t8.product_code2
# t9 当月前置仓出库
LEFT JOIN 
(SELECT t.business_area
, t.product_code2
, SUM(send_nopre) ACTUAL_SEND_NUM 
, SUM(send_nopre) / DAY(@sdate) preware_avg_send_month
FROM feods.`preware_outbound_monthly` t
WHERE t.out_month = DATE_FORMAT(@sdate,"%Y-%m") 
GROUP BY t.business_area,t.product_code2
) t9
ON t1.business_area = t9.business_area
AND t1.product_bar = t9.product_code2
# t10 前置仓近14天出库汇总
LEFT JOIN 
(
SELECT t.business_area
, t.product_code2
, SUM(actual_send_forteen) actual_send_forteen
, SUM(actual_send_num) actual_send_curr
, SUM(available_stock) available_stock_curr
FROM feods.`d_sc_preware_balance` t
WHERE t.sdate = @sdate
GROUP BY t.business_area,t.product_code2
) t10
ON t1.business_area = t10.business_area
AND t1.product_bar = t10.product_code2
# t11 前置仓到货架近14天
LEFT JOIN 
(
SELECT t.business_area
, t.product_code2
, SUM(ACTUAL_SEND_NUM) actual_send_num
, SUM(send_noholiday) send_noholiday 
FROM feods.`preware_outbound_forteen_day` t
WHERE t.sdate = @sdate
GROUP BY t.business_area,t.product_code2
) t11
ON t1.business_area = t11.business_area
AND t1.product_bar = t11.product_code2
# sa(sale_area_level)商品清单
LEFT JOIN feods.zs_area_product_sale_flag sa
ON  t1.business_Area = sa.business_area
AND p.product_id = sa.product_id
AND sa.sdate = @wdate # 为了和商品清单保持一致，周日用下周一，其余用本周一数据
JOIN feods.pj_warehouse_product_presence wp # 区域上架率
ON t1.FPRODUCEDATE = wp.FPRODUCEDATE
AND t1.business_area = wp.business_Area
AND t1.product_bar = wp.product_bar
WHERE t1.FPRODUCEDATE = @sdate
;
# 周转等级
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_warehouse_daily_tmp2;
CREATE TEMPORARY TABLE feods.d_sc_warehouse_daily_tmp2
AS 
SELECT 
t1.*,
# 原为下面逻辑，在2020-04-03改回本逻辑
CASE 
WHEN t1.stock_flag = '非正常品' THEN "无等级"
WHEN t1.stock_flag  IN ("快周转品","正常周转品") AND t1.oms_qualityqty < 300 AND t1.turnover_day < 3 THEN "严重缺货"
WHEN t1.stock_flag  = "观察周转品"  AND t1.oms_qualityqty < 300 AND t1.turnover_day < 2 THEN "严重缺货"
WHEN t1.stock_flag  IN ("快周转品","正常周转品")  AND t1.turnover_day >= 3 AND t1.turnover_day < 8 THEN "缺货"
WHEN t1.stock_flag  = "观察周转品"  AND t1.turnover_day >= 2 AND t1.turnover_day < 7 THEN "缺货"
when t1.stock_flag  IN ("快周转品","正常周转品")  AND t1.turnover_day < 3 and oms_qualityqty >= 300 then "缺货"
WHEN t1.stock_flag  IN ("快周转品","正常周转品")  AND t1.turnover_day >= 3 AND oms_qualityqty < 300 THEN "缺货"
WHEN t1.stock_flag  = "观察周转品" AND t1.turnover_day < 2 AND oms_qualityqty >= 300 THEN "缺货"
WHEN t1.stock_flag  = "观察周转品" AND t1.turnover_day >= 2 AND oms_qualityqty < 300 THEN "缺货"
WHEN t1.stock_flag  = "快周转品" AND t1.turnover_day >= 8 AND t1.turnover_day < 18 THEN "正常"
WHEN t1.stock_flag  = "正常周转品" AND t1.turnover_day >= 8 AND t1.turnover_day < 15 THEN "正常"
WHEN t1.stock_flag  = "观察周转品" AND t1.turnover_day >= 7 AND t1.turnover_day < 12 THEN "正常"
WHEN t1.stock_flag  = "难周转品" AND t1.turnover_day < 15 THEN "正常"
WHEN t1.stock_flag  = "快周转品" AND t1.turnover_day >= 18 AND t1.turnover_day < 25 THEN "滞压"
WHEN t1.stock_flag  = "正常周转品" AND t1.turnover_day >= 15 AND t1.turnover_day < 20 THEN "滞压"
WHEN t1.stock_flag  = "观察周转品" AND t1.turnover_day >= 12 AND t1.turnover_day < 18 THEN "滞压"
WHEN t1.stock_flag  = "快周转品" AND t1.turnover_day >= 25 THEN "严重滞压"
WHEN t1.stock_flag  = "正常周转品" AND t1.turnover_day >= 20 THEN "严重滞压"
WHEN t1.stock_flag  = "观察周转品" AND t1.turnover_day >= 18 THEN "严重滞压"
WHEN t1.stock_flag  = "难周转品" AND t1.turnover_day >= 15 THEN "严重滞压"
WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.out_qty = 0 THEN "无出库"
END AS turnover_level
-- CASE 
-- WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.turnover_day <= 3 THEN "严重缺货"
-- WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.turnover_day > 3 AND t1.turnover_day <= 8 THEN "缺货"
-- WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.turnover_day > 8 AND t1.turnover_day < 18 THEN "正常"
-- WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.turnover_day >= 18 AND t1.turnover_day < 23 THEN "滞压"
-- WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.turnover_day >= 23 THEN "严重滞压"
-- WHEN t1.product_type IN ("原有","新增（试运行）") AND t1.out_qty = 0 THEN "无出库"
-- ELSE "无等级"
-- END AS turnover_level
FROM feods.d_sc_warehouse_daily_tmp t1
;
DELETE FROM feods.d_sc_warehouse_preware_stock_outbound WHERE sdate = @sdate;
INSERT INTO feods.d_sc_warehouse_preware_stock_outbound
(sdate,
  region_area,
  business_area,
  warehouse_name, 
  PRODUCT_ID,
  product_code2,
  product_name,
  product_category,
  oms_qualityqty,
  erp_stock,
  product_type,
  out_days,
  out_qty,
  avg_out_qty,
  dest_shelf_qty,
  dest_shelf_amount,
  dest_shelf_days, # muyou
  avg_shelf_qty,
  purchase_price,
  oms_stock_amount,
  forteen_out_amount,
  avg_out_amount,
  turnover_day,
  avg_stock_month,
  avg_out_month,
  turnover_day_month,
  dest_shelf_qty_curr,  #
  dest_shelf_amount_curr, #
  shelf_qty_curr,
  shelf_amount_curr,
  available_stock,
  available_amount,
  actual_send_num,
  preware_shelf_qty,
  F_BGJ_FBOXEDSTANDARDS,
  area_sale_flag,
  preware_send_qty_curr,
  preware_send_amount_curr,
  preware_avg_stock_month,
  preware_avg_send_month,
  preware_turnover_month,
  onload_qty,
  stock_flag,
  aim_turnover_day,
  presence_rate,
  turnover_level,
  satisfy_status
)
SELECT t2.*,
CASE 
# 原为下面逻辑，在2020-04-03改回本逻辑
when t2.product_type not IN ("原有","新增（试运行）") or isnull(t2.product_type) then "不考核"
WHEN t2.product_type IN ("原有","新增（试运行）") AND t2.stock_flag != "难周转品" 
AND (t2.oms_qualityqty >= 300 OR t2.turnover_level != "严重缺货" ) THEN "是"
WHEN t2.product_type IN ("原有","新增（试运行）") AND t2.stock_flag != "难周转品" 
AND t2.oms_qualityqty < 300 AND t2.turnover_level = "严重缺货"  THEN "否"
WHEN t2.product_type NOT IN ("原有","新增（试运行）") OR t2.stock_flag = "难周转品" THEN "不考核"
-- else "不考核"
-- WHEN t2.product_type NOT IN ("原有","新增（试运行）") OR ISNULL(t2.product_type) THEN "不考核"
-- WHEN t2.product_type IN ("原有","新增（试运行）") AND t2.oms_qualityqty >= 200 THEN "是"
-- WHEN t2.product_type IN ("原有","新增（试运行）") AND t2.turnover_day >= 3 THEN "是"
-- WHEN t2.product_type IN ("原有","新增（试运行）") AND t2.turnover_day < 3 AND t2.oms_qualityqty < 200 THEN "否"
END AS satisfy
FROM feods.d_sc_warehouse_daily_tmp2 t2
;
#(2) 加覆盖货架数量运营商品数量等
DELETE FROM feods.d_sc_warehouse_sku_shelf_cnt WHERE sdate = @sdate;
INSERT INTO feods.d_sc_warehouse_sku_shelf_cnt
(sdate,
 region_area,
  business_area,
  qzc_cnt,
  shelf_cnt, 
  qzc_shelf_cnt,
  coverage_rate,
  op_sku_cnt,
  op_new_sku_cnt,
  pub_time 
)
SELECT @sdate,t1.*,t2.op_sku_cnt,t2.op_new_sku_cnt,t2.`pub_time`
FROM
(SELECT t3.region_name,t3.business_name,
COUNT(DISTINCT t2.warehouse_id) AS 'qzc_cnt',
COUNT(IF(t1.shelf_code NOT LIKE "QZC%",t1.shelf_id,NULL)) AS 'shelf_cnt',
COUNT(IF(t2.warehouse_id IS NOT NULL,t1.shelf_id,NULL)) AS 'qzc_shelf_cnt',
COUNT(IF(t2.warehouse_id IS NOT NULL,t1.shelf_id,NULL)) / COUNT(IF(t1.shelf_code NOT LIKE "QZC%",t1.shelf_id,NULL))  AS 'coverage_rate'
FROM fe.`sf_shelf` t1
LEFT JOIN fe.`sf_prewarehouse_shelf_detail` t2
ON t1.shelf_id = t2.shelf_id
AND t2.data_flag = 1
JOIN feods.fjr_city_business t3
ON t1.city = t3.city
WHERE t1.`DATA_FLAG` = 1
AND t1.`SHELF_STATUS` = 2
AND t1.`ACTIVATE_TIME` < DATE_ADD(@sdate,INTERVAL 1 DAY)
GROUP BY t3.business_name) t1
JOIN 
(SELECT t.`business_area`,COUNT(*) AS op_sku_cnt,COUNT(CASE WHEN t.`PRODUCT_TYPE` = "新增（试运行）" THEN 1 ELSE NULL END) AS op_new_sku_cnt,t.`PUB_TIME`
FROM feods.`zs_product_dim_sserp` t
WHERE t.`PRODUCT_TYPE` IN ("原有","新增（试运行）")
GROUP BY t.`business_area`) t2
ON t1.business_name = t2.business_area;
# 结余表结果
call sh_process.`sp_d_sc_warehouse_balance`(@sdate);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_sc_warehouse_preware_stock_outbound',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
 
 COMMIT;
    END