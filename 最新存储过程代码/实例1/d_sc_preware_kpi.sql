CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_sc_preware_kpi`()
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
		
    SET l_task_name = 'd_sc_preware_kpi'; 
	
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
 
 #KPI按照周和月度来统计，分别在每周或者每月汇报
# 整体前置仓周转天  仓内周转天 = 统计周期内日均库存金额 / 近14天日均出库金额 
# DATE_SUB('20190101',INTERVAL WEEKDAY('20190101')+7 DAY)  上周第一天
SET @sdate1 = CURDATE();
SET @sdate2 = DATE_SUB(@sdate1,INTERVAL WEEKDAY(@sdate1)+7 DAY); #上周第一天
SET @sdate3 = DATE_SUB(@sdate1,INTERVAL WEEKDAY(@sdate1)+1 DAY); #上周最后一天
# SELECT @sdate1,@sdate2,@sdate3;
#(1)近14天日均出库量
#整体前置仓周转天
SET @avg_forteen_out_amount = 
(SELECT (SUM(t1.F_BGJ_POPRICE * t1.`ACTUAL_SEND_NUM`)/14) AS avg_forteen_out_amount
FROM feods.preware_outbound_forteen_day t1
WHERE t1.`sdate` = @sdate3); # 近14天日均出库金额
SET @avg_forteen_out_qty = 
(SELECT (SUM(t1.`ACTUAL_SEND_NUM`)/14) AS avg_forteen_out_qty
FROM feods.preware_outbound_forteen_day t1
WHERE t1.`sdate` = @sdate3);
SET @avg_week_stock_amount = 
(SELECT SUM(t1.`purchase_price` * t1.`available_stock`)/7 AS avg_week_stock_amount
FROM feods.`pj_prewarehouse_stock_detail_weekly` t1
WHERE t1.`week_monday` = @sdate2); # 本周日均库存金额
-- SELECT ROUND(@avg_week_stock_amount / @avg_forteen_out_amount,2) AS '整体前置仓周转天';
# 前置仓严重滞压金额 前置仓继续运营商品（原有及新增）库存金额  - 正常需求金额
SET @stag_amount =
(SELECT ROUND(SUM(m1.available_amount - IFNULL(m3.normal_demand,0)),2) AS stag_amount
-- m1.business_area,m1.warehouse_id,m1.product_code2,m1.available_amount,m2.product_type,ifnull(m3.normal_demand,0) as normal_demand
FROM 
(SELECT t1.business_area,t1.`warehouse_id`,t1.product_code2,t1.available_stock,t1.`purchase_price` * t1.`available_stock` AS available_amount
FROM feods.`pj_prewarehouse_stock_detail` t1
WHERE t1.check_date = @sdate3
) m1
JOIN 
(SELECT a.business_area,a.product_fe,a.product_type
FROM feods.zs_product_dim_sserp_his a
WHERE a.pub_time > @sdate2 AND a.pub_time < @sdate3
AND a.product_type ='原有') m2 #以前为'新增（试运行）','原有'，7月29日改
ON m1.business_area = m2.business_area
AND m1.product_code2 = m2.product_fe
LEFT JOIN 
(SELECT t1.`business_area`,t1.warehouse_id,t1.`product_code2`, ((t1.`ACTUAL_SEND_NUM` * t1.`F_BGJ_POPRICE`) / 14) * 8 AS normal_demand,
((t1.`ACTUAL_SEND_NUM` * t1.`F_BGJ_POPRICE`) / 14)  AS avg_out_amount
FROM feods.preware_outbound_forteen_day t1
WHERE t1.`sdate` = @sdate3
) m3 #正常需求金额
ON m1.warehouse_id = m3.warehouse_id
AND m1.product_code2 = m3.product_code2
JOIN 
(SELECT DISTINCT fnumber,`F_BGJ_FBOXEDSTANDARDS`
FROM sserp.`T_BD_MATERIAL` 
) m4
ON m1.product_code2 = m4.fnumber
WHERE m1.available_amount > IFNULL(m3.normal_demand,0)
AND m1.available_amount / avg_out_amount > 15
AND m1.available_stock > m4.F_BGJ_FBOXEDSTANDARDS
);
# 原有品可用库存
SET @available_amount = 
(SELECT SUM(m1.available_amount) AS stag_amount
-- m1.business_area,m1.warehouse_id,m1.product_code2,m1.available_amount,m2.product_type,ifnull(m3.normal_demand,0) as normal_demand
FROM 
(SELECT t1.business_area,t1.`warehouse_id`,t1.product_code2,t1.`purchase_price` * t1.`available_stock` AS available_amount
FROM feods.`pj_prewarehouse_stock_detail` t1
WHERE t1.check_date = @sdate3
) m1
JOIN 
(SELECT a.business_area,a.product_fe,a.product_type
FROM feods.zs_product_dim_sserp_his a
WHERE a.pub_time > @sdate2 AND a.pub_time < @sdate3
-- AND a.product_type ='原有'  #8月5日改，为全部商品库存
) m2 #以前为'新增（试运行）','原有'，7月29日改
ON m1.business_area = m2.business_area
AND m1.product_code2 = m2.product_fe
);
#严重滞压金额占比
SET @stag_amount_rate = @stag_amount/ @available_amount;
# 严重缺货库存金额
SET @lack_amount = 
(SELECT ROUND(SUM(m3.normal_demand - m1.available_amount),2) AS lack_amount
-- m1.business_area,m1.warehouse_id,m1.product_code2,m1.available_amount,m2.product_type,ifnull(m3.normal_demand,0) as normal_demand
FROM 
(SELECT t1.business_area,t1.`warehouse_id`,t1.product_code2,t1.`purchase_price` * t1.`available_stock` AS available_amount
FROM feods.`pj_prewarehouse_stock_detail` t1
WHERE t1.check_date = @sdate3
) m1
JOIN 
(SELECT a.business_area,a.product_fe,a.product_type
FROM feods.zs_product_dim_sserp_his a
WHERE a.pub_time < @sdate3 AND a.pub_time > @sdate2
AND a.product_type ='原有') m2
ON m1.business_area = m2.business_area
AND m1.product_code2 = m2.product_fe
LEFT JOIN 
(SELECT t1.`business_area`,t1.warehouse_id,t1.`product_code2`, ((t1.`ACTUAL_SEND_NUM` * t1.`F_BGJ_POPRICE`) / 14) * 8 AS normal_demand,((t1.`ACTUAL_SEND_NUM` * t1.`F_BGJ_POPRICE`) / 14)  AS avg_out_amount
FROM feods.preware_outbound_forteen_day t1
WHERE t1.`sdate` = @sdate3
) m3 #正常需求金额
ON m1.warehouse_id = m3.warehouse_id
AND m1.product_code2 = m3.product_code2
WHERE m1.available_amount < m3.normal_demand
AND m1.available_amount / avg_out_amount <= 2); 
#严重缺货金额占比
SET  @lack_amount_rate =  @lack_amount/ @available_amount;
# 淘汰品库存金额占比
SET @oust_amount_rate = 
(SELECT ROUND(SUM(IF(m2.product_type IN ('停补','停补（替补）','淘汰','淘汰（替补）','退出'),available_amount,0))/ SUM(m1.available_amount),4) AS oust_amount_rate
-- m1.business_area,m1.warehouse_id,m1.product_code2,m1.available_amount,m2.product_type,ifnull(m3.normal_demand,0) as normal_demand
FROM 
(SELECT t1.business_area,t1.`warehouse_id`,t1.product_code2,t1.`purchase_price` * t1.`available_stock` AS available_amount
FROM feods.`pj_prewarehouse_stock_detail` t1
WHERE t1.check_date = @sdate3
) m1
JOIN 
(SELECT a.business_area,a.product_fe,a.product_type
FROM feods.zs_product_dim_sserp_his a
WHERE a.pub_time < @sdate3 AND a.pub_time > @sdate2
-- AND a.product_type IN ('停补','停补（替补）','淘汰','淘汰（替补）','退出')
)m2
ON m1.business_area = m2.business_area
AND m1.product_code2 = m2.product_fe);
-- SELECT  @stag_amount, @lack_amount,@oust_amount_rate;
#淘汰品库存金额
SET @oust_amount = 
(SELECT ROUND(SUM(IF(m2.product_type IN ('停补','停补（替补）','淘汰','淘汰（替补）','退出'),available_amount,0)),4) AS oust_amount
-- m1.business_area,m1.warehouse_id,m1.product_code2,m1.available_amount,m2.product_type,ifnull(m3.normal_demand,0) as normal_demand
FROM 
(SELECT t1.business_area,t1.`warehouse_id`,t1.product_code2,t1.`purchase_price` * t1.`available_stock` AS available_amount
FROM feods.`pj_prewarehouse_stock_detail` t1
WHERE t1.check_date = @sdate3
) m1
JOIN 
(SELECT a.business_area,a.product_fe,a.product_type
FROM feods.zs_product_dim_sserp_his a
WHERE a.pub_time < @sdate3 AND a.pub_time > @sdate2
-- AND a.product_type IN ('停补','停补（替补）','淘汰','淘汰（替补）','退出')
)m2
ON m1.business_area = m2.business_area
AND m1.product_code2 = m2.product_fe);
 
#前置仓爆款SKU满足率
SET @sku_satisfy = 
(
SELECT AVG(t.sku_satisfy)
FROM
(SELECT 
t.`sdate`
-- ,SUM(CASE 
-- WHEN t.preware_sale_flag IN ("爆款","畅销","平销") AND t.`preware_sale_flag` IN ("爆款","畅销","平销") AND m.turnover_range IN ("有销售无库存","0-2天（不含2天）","2-3天（不含3天）") THEN 0
-- WHEN t.preware_sale_flag IN ("爆款","畅销","平销") AND t.`preware_sale_flag` IN ("爆款","畅销","平销")  AND m.turnover_range IN ("3天及以上","有库存无销售","无数据") THEN 1
-- ELSE IF(t.`satisfy`= "满足",1,0)
-- END)/ COUNT(*) AS sku_satisfy
,SUM(IF(t.`satisfy`= "满足",1,0))/ COUNT(*) AS sku_satisfy
FROM feods.`d_sc_preware_sku_satisfy` t
-- LEFT JOIN feods.`d_op_warehouse_monitor` m
--   ON t.`sdate` = m.`stat_date`
--   AND t.`shelf_code` = m.warehouse_code
--   AND t.`product_id` = m.`product_id`
WHERE t.sdate >= @sdate2 AND t.sdate <= @sdate3
-- AND t.preware_sale_flag IN ("爆款","畅销","平销")
AND t.preware_sale_flag IN ("爆款","畅销","平销")
GROUP BY t.`sdate`) t
 );
# 增加了5类满足率
-- SELECT CURDATE(),4 AS seq,'运营类' AS kpi_type,AVG(satisfy_rate) AS satisfy_rate ,satisfy_type
--  FROM 
-- (
-- SELECT sdate,COUNT(IF(satisfy = '满足',1,NULL))/COUNT(*) AS satisfy_rate,CONCAT(product_type,"-",sale_flag,"满足率") AS satisfy_type
-- FROM feods.d_sc_preware_sku_satisfy s
-- WHERE s.sdate >= '2019-10-14'
-- AND product_type IN ('原有','新增（试运行）') 
-- AND sale_flag IN ('爆款','畅销','平销')
-- GROUP BY product_type,sale_flag,sdate
-- ) t
-- GROUP BY satisfy_type ;
 
# 备货订单合理率
SET @fill_rational_rate =  
(SELECT
 SUM(CASE 
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
JOIN
(SELECT DISTINCT FNUMBER,F_BGJ_FBOXEDSTANDARDS
FROM sserp.`T_BD_MATERIAL`) s
ON e.`product_code2` = s.fnumber
LEFT JOIN 
(SELECT DATE_ADD(t.`sdate`,INTERVAL 1 DAY ) AS sdate
, t.`warehouse_id`
, t.`product_id`
, t.`suggest_fill_qty`
FROM feods.`d_sc_preware_daily_report` t
WHERE t.`suggest_fill_qty` > 0
AND t.sdate >= DATE_SUB(@sdate2,INTERVAL 1 DAY) AND t.sdate <= DATE_SUB(@sdate3,INTERVAL 1 DAY)
) sug
ON e.`apply_time` = sug.sdate
AND e.`warehouse_id` = sug.warehouse_id
AND e.`PRODUCT_ID` = sug.product_id
WHERE e.apply_time >= @sdate2 AND e.apply_time <= @sdate3
AND e.product_type ='原有'
AND e.`forteen_bef_out` > 0 );
# 下单即时率
SET @fill_intime_rate =  
(SELECT
 SUM(CASE 
  -- WHEN e.actual_apply_num = sug.suggest_fill_qty THEN 1
  WHEN e.`available_stock`/(e.`forteen_bef_out`/14) >= 2 
  AND e.`available_stock`/(e.`forteen_bef_out`/14) <= 15 
  THEN 1
  END)/COUNT(*) AS fill_rational_rate
  FROM feods.pj_fill_order_efficiency e
  -- JOIN feods.`pj_preware_shelf_sales_thirty` s
  JOIN feods.`d_sc_preware_daily_report` s #为了和过年期间调整后的日期和畅销等级一致，改用d_sc_preware_daily_report
  ON e.apply_time = s.sdate
  AND e.warehouse_id = s.warehouse_id
  AND e.product_id = s.product_id 
WHERE e.apply_time >= @sdate2 AND e.apply_time <= @sdate3
-- AND e.product_type IN ('原有','新增（正式运行）','新增（试运行）') 
AND e.product_type = '原有'
AND e.`forteen_bef_out` > 0 
AND s.sales_level IN ("爆款","畅销","平销") 
);
#达标前置仓占比（前置站覆盖货架月分拣量>= 2000PCS为达标量,月度）
SELECT IFNULL(t1.out_month,t2.fill_month) AS smonth,SUM(IF(IFNULL(t2.ACTUAL_FILL_NUM,0) + IFNULL(t1.ACTUAL_SEND_NUM,0) >= 2000,1,0)) / COUNT(*) AS goal_achieve_preware_rate
-- a.shelf_code,ifnull(t2.ACTUAL_FILL_NUM,0) + ifnull(t1.ACTUAL_SEND_NUM,0) as 'psc'
FROM 
(SELECT shelf_id,shelf_code,shelf_name
 FROM fe.sf_shelf
 WHERE shelf_code LIKE 'QZC%' AND data_flag = 1 ) a
LEFT JOIN 
(SELECT out_month,shelf_code,shelf_name,warehouse_id,SUM(ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM
FROM feods.preware_outbound_monthly
GROUP BY out_month,shelf_code ) t1
ON a.shelf_id = t1.`warehouse_id`
LEFT JOIN
(SELECT DATE_FORMAT(f.`fill_date`,"%Y-%m") AS fill_month,shelf_code,shelf_name,warehouse_id,SUM(f.`ACTUAL_FILL_NUM`) AS ACTUAL_FILL_NUM
FROM feods.`preware_fill_daily` f
WHERE f.supplier_type <> 2
GROUP BY shelf_code,shelf_name,DATE_FORMAT(f.`fill_date`,"%Y-%m")) t2
ON a.shelf_id = t2.warehouse_id
AND t1.out_month = t2.fill_month
WHERE t1.out_month IS NOT NULL OR t2.fill_month IS NOT NULL
GROUP BY smonth;
# 系统触发前置仓订单占比
SET @system_fill_rate =
(SELECT
ROUND(COUNT(DISTINCT IF(b.fill_type = 2,a.order_id,NULL))/ COUNT(DISTINCT a.order_id),4) AS system_fill_rate
FROM fe.sf_product_fill_order_item a
JOIN fe.sf_product_fill_order b
ON a.order_id = b.order_id
WHERE b.supplier_type = 9
AND b.fill_type IN (2,8,9)
AND b.order_status IN (3,4)
AND a.data_flag =1
AND b.data_flag =1
AND 
CASE WHEN b.fill_type IN (9) THEN DATE(b.apply_time)
WHEN b.fill_type IN (2,8) AND b.send_time IS NOT NULL THEN DATE(b.send_time)
WHEN b.fill_type IN (2,8) AND b.send_time IS NULL THEN DATE(b.apply_time)
END >= @sdate2 
AND 
CASE WHEN b.fill_type IN (9) THEN DATE(b.apply_time)
WHEN b.fill_type IN (2,8) AND b.send_time IS NOT NULL THEN DATE(b.send_time)
WHEN b.fill_type IN (2,8) AND b.send_time IS NULL THEN DATE(b.apply_time)
END <= @sdate3);
# 货架商品撤入量占比
SET @shelf_fill_preware =
(SELECT ROUND(SUM(IF(f.fill_type IN (4,12),f.`F_BGJ_POPRICE`*f.`ACTUAL_FILL_NUM`,0))/SUM(f.`F_BGJ_POPRICE`*f.`ACTUAL_FILL_NUM`),4) AS shelf_fill_preware
FROM feods.preware_fill_daily f
WHERE fill_date >= @sdate2  AND fill_date <= @sdate3);
# 低库存货架占比
# 为了提高反应效率，feods.d_sc_shelf_stock_daily 的存储过程放在了sh_prewarehouse_coverage_rate
SET @low_stock_rate = 
(SELECT ROUND(AVG(t.low_stock_rate),4) AS low_stock_rate
FROM 
(SELECT t3.sdate,
SUM(CASE WHEN t2.grade IN ('甲','乙') AND t1.shelf_type IN (1,3) AND t3.stock < 180 THEN 1
WHEN t2.grade IN ('甲','乙') AND t1.shelf_type IN (2,5) AND t3.stock < 100 THEN 1
WHEN t1.ACTIVATE_TIME >= DATE_SUB(@sdate3,INTERVAL 60 DAY) AND t1.shelf_type IN (1,3) AND t3.stock < 180 THEN 1
WHEN t1.ACTIVATE_TIME >= DATE_SUB(@sdate3,INTERVAL 60 DAY) AND t1.shelf_type IN (2,5) AND t3.stock < 100 THEN 1
WHEN t2.grade IN ('丙','丁') AND t1.shelf_type IN (1,3) AND t3.stock < 110 THEN 1
WHEN t2.grade IN ('丙','丁') AND t1.shelf_type IN (2,5) AND t3.stock < 90 THEN 1
WHEN t2.grade IN ('甲','乙','丙','丁') AND t3.stock < 300 THEN 1
END)/COUNT(*) AS low_stock_rate
FROM 
(SELECT a.`shelf_id`,b.`ACTIVATE_TIME`,b.`SHELF_TYPE` 
FROM fe.sf_prewarehouse_shelf_detail a
JOIN fe.`sf_shelf` b
ON a.shelf_id = b.shelf_id
AND a.data_flag =1
AND b.data_flag =1
AND b.`shelf_type` IN (1,2,3,5)
AND b.`WHETHER_CLOSE` = 2 #未关闭
AND b.`REVOKE_STATUS` = 1 #没撤架申请
AND b.`SHELF_STATUS` = 2 #激活状态
) t1
JOIN
(SELECT a.`shelf_id`,a.`grade`
FROM feods.`d_op_shelf_grade` a
WHERE a.month_id = DATE_FORMAT(@sdate3,"%Y-%m")
) t2
ON t1.`shelf_id` = t2.shelf_id
JOIN feods.d_sc_shelf_stock_daily t3
ON t1.`shelf_id` = t3.shelf_id
AND t3.sdate >= @sdate2  AND t3.sdate <= @sdate3
GROUP BY t3.sdate) t
);
DELETE FROM feods.d_sc_preware_kpi WHERE sdate = @sdate3;
INSERT INTO feods.d_sc_preware_kpi
(sdate,
 seq,
 kpi_type,
 kpi,
 kpi_name)
SELECT @sdate3,1 AS seq,'运营类' AS kpi_type,ROUND(@avg_week_stock_amount / @avg_forteen_out_amount,2) kpi, '整体前置仓周转天' AS kpi_name
UNION 
SELECT @sdate3,2 AS seq,'运营类' AS kpi_type,@stag_amount kpi, '严重滞压库存金额' AS kpi_name
UNION 
SELECT @sdate3,2 AS seq,'运营类' AS kpi_type,@stag_amount_rate kpi, '严重滞压库存金额占比' AS kpi_name
UNION
SELECT @sdate3,3 AS seq,'运营类' AS kpi_type,@lack_amount kpi, '严重缺货库存金额' AS kpi_name
UNION
SELECT @sdate3,3 AS seq,'运营类' AS kpi_type,@lack_amount_rate kpi, '严重缺货库存金额占比' AS kpi_name
UNION
SELECT @sdate3,4 AS seq,'运营类' AS kpi_type,@sku_satisfy kpi, '平畅爆SKU满足率' AS kpi_name
UNION 
SELECT @sdate3,4 AS seq,'运营类' AS kpi_type,AVG(satisfy_rate) AS satisfy_rate, satisfy_type
 FROM 
(
SELECT sdate,COUNT(IF(satisfy = '满足',1,NULL))/COUNT(*) AS satisfy_rate,CONCAT(product_type,"-",preware_sale_flag,"满足率") AS satisfy_type
FROM feods.d_sc_preware_sku_satisfy s
WHERE s.sdate >= @sdate2 AND s.sdate <= @sdate3
AND product_type IN ('原有','新增（试运行）') 
AND preware_sale_flag IN ('爆款','畅销','平销')
GROUP BY product_type,preware_sale_flag,sdate
) t
GROUP BY satisfy_type
UNION
SELECT @sdate3,5 AS seq,'运营类' AS kpi_type,@fill_rational_rate kpi,'备货订单合理率' AS kpi_name
UNION
SELECT @sdate3,5 AS seq,'运营类' AS kpi_type,@fill_intime_rate kpi,'备货订单及时率(爆畅平)' AS kpi_name
UNION
SELECT @sdate3,7 AS seq,'运营类' AS kpi_type,@oust_amount kpi, '淘汰品库存金额' AS kpi_name
UNION
SELECT @sdate3,7 AS seq,'运营类' AS kpi_type,@oust_amount_rate kpi, '淘汰品库存金额占比' AS kpi_name
-- UNION
-- SELECT @sdate3,'执行类' AS kpi_type,@oust_amount_rate kpi, '达标前置仓占比' AS kpi_name
UNION
SELECT @sdate3,10 AS seq, '执行类' AS kpi_type,@system_fill_rate kpi, '系统触发要货订单占比' AS kpi_name
UNION
SELECT @sdate3,9 AS seq,'执行类' AS kpi_type,@low_stock_rate, '低库存货架占比' AS kpi_name
UNION
SELECT @sdate3,11 AS seq,'执行类' AS kpi_type,@shelf_fill_preware kpi, '货架商品撤入量占比' AS kpi_name;
 
 
 	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'd_sc_preware_kpi',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('wuting@', @user, @timestamp)
  );
 
 
COMMIT;
    END