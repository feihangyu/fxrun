DELIMITER $$

USE `test`$$

DROP PROCEDURE IF EXISTS `dm_sc_preware_product_stat`$$

CREATE DEFINER=`wuting`@`%` PROCEDURE `dm_sc_preware_product_stat`(IN in_date DATE)
    SQL SECURITY INVOKER
BEGIN
    
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate = in_date ;
SET @sub_sdate = DATE_SUB(@sdate,INTERVAL 1 DAY);
SET @add_date = ADDDATE(@sdate,1);
-- SET @sdate2 = IF(@sdate >= '2020-01-31','2019-12-15',@sdate);
-- SET @sdate3 = IF(@sdate >= '2020-01-31','2019-12-16',@sdate);
SET @sa = 1, @sb = 0.4; -- 满足率系数
SET @pa = 1.45,@pb = 1.45; -- 备货量系数
SET @sdate2 = @sdate;
SET @sdate3 = @sdate;
-- 1、在途数据
#（1）7日内申请备货在途量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_fill_onload;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_fill_onload 
(KEY idx_warehouse_product(warehouse_id,product_id)) 
AS
SELECT
  -- DATE(b.fill_time) AS fill_date,
  a.shelf_id AS warehouse_id,
  a.product_id,
  SUM(a.ACTUAL_APPLY_NUM) AS onload_num,
  SUM(a.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM,
  SUM(a.ACTUAL_SIGN_NUM) AS ACTUAL_SIGN_NUM,
  SUM(a.ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM  
FROM fe_dwd.dwd_fill_day_inc a
JOIN fe_dwd.dwd_shelf_base_day_all s
ON a.SHELF_ID = s.shelf_id
AND s.shelf_type = 9
WHERE a.fill_type IN (1,2,4,8,10,12)
  AND a.order_status IN (1,2) #已申请、已发货
  AND a.apply_time >= DATE_SUB(@add_date,INTERVAL 7 DAY) 
  AND a.apply_time < @add_date
GROUP BY 
 --  DATE(b.fill_time),
   a.shelf_id,
   a.product_id;
-- 2、结余表基础数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_balance_tmp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_balance_tmp 
(KEY idx_warehouse_product(warehouse_id,product_id)) 
SELECT 
t.check_date AS sdate
, t.business_area 
, t.warehouse_id 
, t.product_id 
,pt.product_type 
, t.total_stock 
, t.AVAILABLE_STOCK 
, t.purchase_price 
, IFNULL(p.total_stock,0) AS last_total_stock
,IFNULL(po.send_num, 0) send_num
,IFNULL(pin.fill_num, 0) fill_num
,IFNULL(p.total_stock,0) + IFNULL(pin.fill_num,0) - IFNULL(po.send_num,0) stock_in_theory
,IFNULL(po.send_num_14,0) AS send_num_14
,IFNULL(po.send_noholiday_14,0)/14 AS avg_send_num_act
,IFNULL(po.send_noholiday_14 * t.purchase_price ,0)/14 AS avg_send_amount_act
,IFNULL(ps.qty,0) AS qty
,IFNULL(ps.GMV,0) AS GMV
,IFNULL(p.total_stock,0) + IFNULL(pin.fill_num,0) - IFNULL(po.send_num,0) - t.total_stock AS diff_stock
,IFNULL(pon.onload_num,0) onload_num
,IFNULL(ps.qty_15 ,0) qty_15 
,IFNULL(ps.sale_shelf_cnt_15,0) sale_shelf_cnt_15
,IFNULL(ps.sale_shelf_cnt,0) sale_shelf_cnt
, IFNULL(ps.discount_sale_qty_15 /15/ ps.discount_shelf_cnt_15,0) AS avg_shelf_sale
, IFNULL(ps.discount_sale_qty_15 /15,0) AS avg_sale_act 
, IFNULL(ps.sale_flag,5) AS sale_flag
,
CASE
WHEN ps.qty_15 > 0 AND pt.PRODUCT_TYPE IN ("新增（试运行）","原有") AND ps.sale_flag IN (1,2) AND IFNULL((po.send_noholiday_14/14)* @sa,0) * 2 <= t.AVAILABLE_STOCK THEN 1
WHEN ps.qty_15 > 0 AND pt.PRODUCT_TYPE IN ("新增（试运行）","原有") AND ps.sale_flag IN (1,2) AND t.AVAILABLE_STOCK >= 15 THEN 1
WHEN ps.qty_15 > 0 AND pt.PRODUCT_TYPE IN ("新增（试运行）","原有") AND ps.sale_flag IN (3,4,5) AND IFNULL((po.send_noholiday_14/14)* @sa,0) * 2 <= t.AVAILABLE_STOCK THEN 1
WHEN ps.qty_15 > 0 AND pt.PRODUCT_TYPE IN ("新增（试运行）","原有") AND ps.sale_flag IN (3,4,5) AND t.AVAILABLE_STOCK >= 10 THEN 1
WHEN ps.qty_15 > 0 AND pt.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 0
END AS satisfy
,fq.avg_fill_times
,fq.fill_frequency
,IF(s.shelf_status = 11,s.shelf_status,s.whether_close)  preware_status
,n.np_flag
, CASE 
WHEN ISNULL(fq.min_cycle) THEN 6 # 没有维护的仓默认为6
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND ps.sale_flag IN (1,2) THEN min_cycle -1
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND ps.sale_flag = 3 THEN min_cycle -1
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND (ps.sale_flag IN (4,5 ) OR ISNULL(ps.sale_flag)) THEN min_cycle - 1 
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1 AND ps.sale_flag IN (1,2) THEN min_cycle - 1
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1 AND ps.sale_flag = 3 THEN min_cycle - 1
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1 AND (ps.sale_flag IN (4,5 ) OR ISNULL(ps.sale_flag)) THEN min_cycle -1
WHEN ps.sale_flag IN (1,2) THEN 2 
WHEN (ps.sale_flag IN (3,4,5 ) OR ISNULL(ps.sale_flag)) THEN  2
END AS new_min_wave_cycle
, CASE 
WHEN ISNULL(fq.max_cycle) THEN 6 # 没有维护的仓默认为6
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND ps.sale_flag IN (1,2) THEN max_cycle + 3  + 1
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND ps.sale_flag = 3 THEN max_cycle + 3  + 1
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND (ps.sale_flag IN (4,5 ) OR ISNULL(ps.sale_flag)) THEN max_cycle + 2
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1  AND ps.sale_flag IN (1,2) THEN max_cycle + 3  + 1
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1  AND ps.sale_flag = 3 THEN max_cycle + 2  +1
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1  AND (ps.sale_flag IN (4,5 ) OR ISNULL(ps.sale_flag)) THEN max_cycle + 2  + 2
WHEN ps.sale_flag IN (1,2) THEN max_cycle + 2
WHEN (ps.sale_flag IN (3,4,5 ) OR ISNULL(ps.sale_flag)) THEN max_cycle + 2
END AS new_max_wave_cycle
, CASE 
WHEN ISNULL(fq.max_cycle) THEN 6 # 没有维护的仓默认为6
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND ps.sale_flag IN (1,2) THEN max_cycle + 3 
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND ps.sale_flag = 3 THEN max_cycle + 3
WHEN ((pt.product_type = '原有') OR (pt.product_type = '新增（试运行）' AND n.np_flag = 0)) AND (ps.sale_flag IN (4,5 ) OR ISNULL(ps.sale_flag)) THEN max_cycle
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1  AND ps.sale_flag IN (1,2) THEN max_cycle + 3 
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1  AND ps.sale_flag = 3 THEN max_cycle + 2 
WHEN pt.product_type = '新增（试运行）'  AND n.np_flag = 1  AND (ps.sale_flag IN (4,5 ) OR ISNULL(ps.sale_flag)) THEN max_cycle + 2 
WHEN ps.sale_flag IN (1,2) THEN max_cycle
WHEN (ps.sale_flag IN (3,4,5 ) OR ISNULL(ps.sale_flag)) THEN max_cycle
END AS new_mid_wave_cycle
,t.available_stock / IFNULL(po.send_noholiday_14/14,0)  AS turnover_days
,(t.available_stock + IFNULL(pon.onload_num,0))/ IFNULL(po.send_noholiday_14/14,0)  AS valid_turnover_days
, CASE 
  WHEN t.business_area = '安徽区' THEN 1.45
  WHEN t.business_area = '浙北区' THEN  1.45
  ELSE @pa 
  END AS senda
 ,CASE 
  WHEN t.business_area = '安徽区' THEN 1.45
  WHEN t.business_area = '浙北区' THEN 1.45
  ELSE @pb 
  END AS saleb
FROM 
(SELECT *
FROM fe_dm.dm_prewarehouse_stock_detail
WHERE check_date = @sdate
AND warehouse_id != 50341
-- and warehouse_id = 50625
-- and product_id = 985
 ) t 
LEFT JOIN fe_dm.dm_prewarehouse_stock_detail p 
ON p.check_date = @sub_sdate
AND t.warehouse_id = p.warehouse_id
AND t.product_id = p.product_id
LEFT JOIN fe_dwd.dwd_sc_preware_product_fill_stat pin # 入库宽表
ON t.check_date = pin.sdate
AND t.warehouse_id = pin.warehouse_id
AND t.product_id = pin.product_id
LEFT JOIN fe_dwd.dwd_sc_preware_product_outbound_stat po # 出库宽表
ON  t.check_date = po.sdate
AND t.warehouse_id = po.warehouse_id
AND t.product_id = po.product_id
AND po.sdate = @sdate 
LEFT JOIN fe_dwd.`dwd_sc_preware_product_sales_stat` ps # 销售宽表
ON t.check_date = ps.sdate
AND t.warehouse_id = ps.warehouse_id
AND t.product_id = ps.product_id
AND ps.sdate = @sdate
LEFT JOIN fe_dwd.dwd_preware_fill_onload pon
ON t.warehouse_id = pon.warehouse_id
AND t.product_id = pon.product_id
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp pt
ON t.business_area = pt.business_area
AND t.product_id  = pt.product_id
# 2020-04-02日修改，业务未添加的前置仓默认1周1配，最大和最小值都取6
LEFT JOIN fe_dm.dm_sc_preware_fill_frequency fq
ON t.warehouse_id = fq.warehouse_id
LEFT JOIN fe_dm.`dm_sc_preware_new_product_flag` n
ON t.warehouse_id = n.warehouse_id
AND t.product_id = n.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` s
ON t.warehouse_id = s.shelf_id
WHERE pt.product_type IN ("新增（试运行）","原有")
OR 
(t.total_stock >0
OR p.total_stock >0
OR pin.fill_num_7 >0
OR po.send_num_14 > 0
OR po.send_noholiday_14
OR ps.qty_15 > 0
OR pon.onload_num > 0)
;
# 3、参数，周转等级等
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_remain_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_remain_temp 
(KEY idx_preware_product (warehouse_id,product_id),
KEY idx_business_product(business_area,product_id),
KEY idx_preware (warehouse_id))
SELECT t.*
,CASE 
  WHEN t.avg_send_num_act  = 0 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 6
  WHEN t.turnover_days  < 2 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 3
  WHEN t.turnover_days  >= 2 AND t.turnover_days  <5 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 2 
  WHEN t.turnover_days  >= 5 AND t.turnover_days  <= 10 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 1
  WHEN t.turnover_days  > 10 AND t.turnover_days  <= 15 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 4
  WHEN t.turnover_days  > 15 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") AND t.available_stock <= pr.box_fill_model THEN 4
  WHEN t.turnover_days  > 15 AND t.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN 5
  ELSE 7
END AS turnover_level
,t.avg_send_amount_act *8 AS need_amount
,CASE
WHEN  t.turnover_days > 15 AND t.PRODUCT_TYPE ="原有"  AND t.available_stock > pr.box_fill_model THEN t.AVAILABLE_STOCK * t.purchase_price - avg_send_amount_act *8
END AS stag_amount #严重滞压金额
,CASE
WHEN  t.turnover_days  < 2 AND t.PRODUCT_TYPE = "原有" THEN  avg_send_amount_act * 8 - t.AVAILABLE_STOCK * t.purchase_price 
END AS lack_amount #"严重缺货金额" 
,pr.box_fill_model AS F_BGJ_FBOXEDSTANDARDS
,p.product_code2
,IF(p.`fname_type` = "水饮",50,20) AS fnamec
, avg_send_num_act * senda AS avg_send_num
, avg_send_amount_act * senda AS avg_send_amount
, avg_sale_act * saleb AS avg_sale
FROM fe_dwd.dwd_preware_balance_tmp t
JOIN fe_dwd.`dwd_product_base_day_all` p
ON t.product_id = p.product_id
JOIN fe_dwd.`dwd_sc_business_region` w
ON t.business_area = w.business_area
JOIN fe_dwd.dwd_product_area_pool pr
ON pr.`area_id` = w.`area_id`
AND pr.product_id = t.product_id
;
#(4)覆盖货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_shelf_cover_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_shelf_cover_temp 
(KEY idx_warehouse(warehouse_id))
AS
SELECT
    t.prewarehouse_id AS warehouse_id
    , COUNT(t.shelf_id) AS cover_shelf_cnt
    , CASE 
    WHEN SUM(IF(t.shelf_type IN (1,3),1,0))/COUNT(t.shelf_id) >= 0.6 THEN 1   -- '正常货架覆盖仓' 
    WHEN SUM(IF(t.shelf_type IN (2,5),1,0))/COUNT(t.shelf_id) >= 0.6 THEN 2 -- '冰箱覆盖仓'
    WHEN SUM(IF(t.shelf_type IN (6,7),1,0))/COUNT(t.shelf_id) >= 0.6 THEN 3 -- '智能机覆盖仓'
    WHEN SUM(IF(t.shelf_type = 4,1,0))/COUNT(t.shelf_id) >= 0.6 THEN 4 -- '虚拟货架覆盖仓'
    ELSE 5 -- "混合货架类型覆盖仓"
    END AS preware_type 
FROM
    fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all t
WHERE t.shelf_status = 2
GROUP BY t.prewarehouse_id ;
-- 5、有库存货架数
SELECT CONCAT(
"CREATE TEMPORARY TABLE fe_dm.dm_sc_shelf_sal_sto_tmp
(INDEX idx_warehouse_product(warehouse_id,product_id))
SELECT t1.prewarehouse_code AS shelf_code
, t1.prewarehouse_id AS  warehouse_id
, t3.product_id
,
"
, "SUM(t3.d"
, DAY(@add_date)
,") AS stock_quantity
, COUNT(DISTINCT(IF(t3.d"
, DAY(@add_date)
," >0 ,t3.SHELF_ID,NULL))) AS stock_shelf_cnt
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all t1
JOIN fe_dwd.dwd_shelf_product_stock_detail t3
ON t1.shelf_id = t3.shelf_id
AND t1.shelf_status = 2 
AND t3.`month_id` = '"
, DATE_FORMAT(@add_date,"%Y-%m")
,"' 
AND t1.`prewarehouse_id` != 50341
GROUP BY t1.prewarehouse_id,t3.product_id
;"
) INTO @str1;
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_shelf_sal_sto_tmp;
PREPARE sql_exe1 FROM @str1;
EXECUTE sql_exe1;
-- 6 ERP大仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_warehouse_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_warehouse_temp 
(KEY idx_business_product(business_area,PRODUCT_id))
AS
SELECT 
    t2.FPRODUCEDATE
    , t1.region_area
    , t1.business_area
    , t1.warehouse_number
    , t1.warehouse_name
    , p.product_id
    , t2.PRODUCT_BAR 
    , t2.PRODUCT_NAME
    , t2.QUALITYQTY 
    
FROM
    fe_dwd.dwd_pub_warehouse_business_area t1 
    JOIN fe_dwd.dwd_PJ_OUTSTOCK2_DAY t2 
        ON t1.warehouse_number = t2.warehouse_number 
        AND t1.to_preware = 1 
        AND t2.FPRODUCEDATE = @sdate
    JOIN fe_dwd.`dwd_product_base_day_all` p
    ON t2.product_bar = p.product_code2  ;
-- 第一次建议补货量计算
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_require_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_require_temp AS
SELECT t1.*,
  IFNULL(t3.cover_shelf_cnt,0) AS cover_shelf_cnt,
  IFNULL(t4.qualityqty,0) AS qualityqty, #正品库存
  IFNULL(t3.preware_type,0) AS preware_type, -- 0 "无激活状态货架"
  t8.next_fill_date, 
  IFNULL(st.stock_quantity,0) stock_quantity,
  IFNULL(st.stock_shelf_cnt,0) stock_shelf_cnt,   
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN 1 -- 吉林、山西、冀州、江西
WHEN t1.preware_status IN (1,11) THEN 2
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <=t1.new_mid_wave_cycle  AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3,4)
THEN 5
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle AND np_flag = 0 THEN 51
-- ) 库存小于15,else 
-- ) 库存小于15,else 
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24  AND t1.valid_turnover_days <=t1.new_mid_wave_cycle THEN 6
-- 2）库存大于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  >= 24  AND t1.valid_turnover_days <=t1.new_mid_wave_cycle THEN 7
-- 2020-08-10 拆分有库存，有出库，周转天>目标周转天
-- 1）库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3) 
THEN 8
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND t1.np_flag = 0 
THEN 81
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN 9
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN 10
-- 有库存，无出库，根据销售量来判断
-- 1)
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t1.avg_sale*t1.new_mid_wave_cycle   THEN 11
-- 2)2020-10-29 拆分仓新品，保底一箱或者2箱
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_mid_wave_cycle AND np_flag = 1 THEN 12
-- 3)原有品
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_mid_wave_cycle AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3) THEN 13
-- 4)
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_mid_wave_cycle  THEN 14
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" THEN 15
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" THEN 16
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3) THEN 17
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN 18 
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN 19
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN 20
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t1.avg_sale>0  AND t1.AVAILABLE_STOCK =0 THEN 21
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_mid_wave_cycle 
THEN 22
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_mid_wave_cycle THEN 23
END  AS sense,
# 建议补货量
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN "不允许备货" -- 吉林、山西、冀州、江西
WHEN t1.preware_status IN (1,11) THEN "不允许备货"
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle  AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3,4)
THEN IF(ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,IF(ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) =0,t1.F_BGJ_FBOXEDSTANDARDS,ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS))
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle AND t1.np_flag = 0 
THEN IF(CEILING(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,CEILING(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- ) 库存小于15,else 
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24  AND t1.valid_turnover_days <t1.new_mid_wave_cycle 
THEN IF(ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2）库存大于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  >= 24  AND t1.valid_turnover_days <t1.new_mid_wave_cycle 
THEN IF(ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2020-08-10 拆分有库存，有出库，周转天>目标周转天
-- 1）库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3) 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND t1.np_flag = 0
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN "不允许备货"
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN "不允许备货"
-- 有库存，无出库，根据销售量来判断
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t1.avg_sale*t1.new_mid_wave_cycle    THEN "不允许备货"
-- 2)2020-10-29 日新增拆分了新场景，增加仓新品保有量
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_mid_wave_cycle AND t1.np_flag = 1
THEN IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
, IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) = 0,IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 2),ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS ))
-- 3)原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3)
THEN IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
, IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) = 0, t1.F_BGJ_FBOXEDSTANDARDS,ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS ))
-- 4)
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_mid_wave_cycle    
THEN IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec, ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS )
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 3)
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" 
THEN "不允许备货"
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3)
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2) 
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN "不允许备货"  
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN "不允许备货"
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN "不允许备货" 
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t1.avg_sale>0  AND t1.AVAILABLE_STOCK =0 
THEN IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_mid_wave_cycle 
THEN IF(ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t1.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_mid_wave_cycle THEN "不允许备货"
END  AS suggest_fill_num,
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN "不允许备货" -- 吉林、山西、冀州、江西
WHEN t1.preware_status IN (1,11) THEN "不允许备货"
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_min_wave_cycle  AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3,4)
THEN IF(ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,IF(ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) =0,t1.F_BGJ_FBOXEDSTANDARDS,ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS))
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_min_wave_cycle AND t1.np_flag = 0 
THEN IF(CEILING(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,CEILING(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- ) 库存小于15,else 
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24  AND t1.valid_turnover_days <t1.new_min_wave_cycle 
THEN IF(ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2）库存大于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  >= 24  AND t1.valid_turnover_days <t1.new_min_wave_cycle 
THEN IF(ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2020-08-10 拆分有库存，有出库，周转天>目标周转天
-- 1）库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3) 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle AND t1.np_flag = 0 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle THEN "不允许备货"
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle THEN "不允许备货"
-- 有库存，无出库，根据销售量来判断
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t1.avg_sale*t1.new_min_wave_cycle    THEN "不允许备货"
-- 2)2020-10-29 日新增拆分了新场景，增加仓新品保有量
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_min_wave_cycle AND t1.np_flag = 1
THEN IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
, IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) = 0,IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 2),ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS ))
-- 3)原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3)
THEN IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
, IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) = 0, t1.F_BGJ_FBOXEDSTANDARDS,ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS ))
-- 4)
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_min_wave_cycle    
THEN IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec, ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS )
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 3)
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" 
THEN "不允许备货"
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3)
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2) 
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN "不允许备货"  
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN "不允许备货"
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN "不允许备货" 
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t1.avg_sale>0  AND t1.AVAILABLE_STOCK =0 
THEN IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_min_wave_cycle 
THEN IF(ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t1.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_min_wave_cycle THEN "不允许备货"
END AS min_suggest_qty,
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN "不允许备货" -- 吉林、山西、冀州、江西
WHEN t1.preware_status IN (1,11) THEN "不允许备货"
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <=t1.new_max_wave_cycle  AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3,4)
THEN IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) =0,t1.F_BGJ_FBOXEDSTANDARDS,ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS))
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_max_wave_cycle AND t1.np_flag = 0 
THEN IF(CEILING(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,CEILING(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- ) 库存小于15,else 
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24  AND t1.valid_turnover_days <=t1.new_max_wave_cycle 
THEN IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2）库存大于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  >= 24  AND t1.valid_turnover_days <=t1.new_max_wave_cycle 
THEN IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2020-08-10 拆分有库存，有出库，周转天>目标周转天
-- 1）库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle AND (t1.product_type = "原有" OR t1.np_flag = 1) AND t1.sale_flag IN (1,2,3) 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle AND t1.np_flag = 0 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle THEN "不允许备货"
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle THEN "不允许备货"
-- 有库存，无出库，根据销售量来判断
-- 1)
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t1.avg_sale*t1.new_max_wave_cycle    THEN "不允许备货"
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_max_wave_cycle AND t1.np_flag = 1
THEN IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
, IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) = 0,IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 2),ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS ))
-- 3)原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3)
THEN IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
, IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) = 0, t1.F_BGJ_FBOXEDSTANDARDS,ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS ))
-- 4)
WHEN t1.avg_send_num  = 0 AND t1.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t1.avg_sale*t1.new_max_wave_cycle    
THEN IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec, ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS )
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 3)
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" 
THEN "不允许备货"
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t1.sale_flag IN (1,2,3)
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS >=80,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2) 
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN "不允许备货"  
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN "不允许备货"
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t1.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN "不允许备货" 
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t1.avg_sale>0  AND t1.AVAILABLE_STOCK =0 
THEN IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_max_wave_cycle 
THEN IF(ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t1.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_max_wave_cycle THEN "不允许备货"
END  AS max_suggest_qty
FROM fe_dwd.dwd_preware_remain_temp t1
LEFT JOIN fe_dwd.dwd_preware_shelf_cover_temp t3
ON t1.warehouse_id = t3.warehouse_id
LEFT JOIN fe_dwd.dwd_preware_warehouse_temp t4
ON t1.business_area = t4.business_area
AND t1.product_id = t4.product_id
LEFT JOIN fe_dm.dm_sc_preware_fill_frequency t8
ON t1.warehouse_id = t8.warehouse_id
LEFT JOIN fe_dm.dm_sc_shelf_sal_sto_tmp st
ON t1.warehouse_id = st.warehouse_id
AND t1.product_id = st.product_id
;
-- 建议补货量调整，最终结果
DELETE FROM fe_dm.dm_sc_preware_product_stat WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_product_stat
( sdate,
  business_area,
  warehouse_id,
  preware_type,
  preware_status,
  product_id,
  product_type,
  np_flag,
  purchase_price,
  total_stock,
  available_stock,
  send_num,
  fill_num,
  onload_num,
  last_total_stock,
  stock_in_theory,
  diff_stock,
  qty,
  GMV,
  send_num_14,
  avg_send_num_act,
  qty_15,
  sale_shelf_cnt_15,
  avg_shelf_sale,
  avg_sale_act,
  sale_flag,
  satisfy,
  
  avg_fill_times,
  fill_frequency,
  new_min_wave_cycle,
  new_mid_wave_cycle,
  new_max_wave_cycle,
  turnover_days,
  valid_turnover_days,
  turnover_level,
  need_amount,
  stag_amount,
  lack_amount,
  
  avg_send_num,
  avg_sale,
  
  qualityqty,
  shelf_stock_qty,
  cover_shelf_cnt,
  stock_shelf_cnt,
  sale_shelf_cnt,
  
  suggest_fill_num , 
  min_suggest_qty ,
  max_suggest_qty ,
  warehouse_satitsfy ,
  suggest_fill_qty,
  suggest_fill_amend,
  fill_priority,
  next_fill_date
)
 SELECT 
  t.sdate,
  t.business_area,
  t.warehouse_id,
  t.preware_type,
  t.preware_status,
  t.product_id,
  t.product_type,
  t.np_flag,
  t.purchase_price,
  t.total_stock,
  t.available_stock,
  t.send_num,
  t.fill_num,
  t.onload_num,
  t.last_total_stock,
  t.stock_in_theory,
  t.diff_stock,
  t.qty,
  t.GMV,
  t.send_num_14,
  t.avg_send_num_act,
  t.qty_15,
  t.sale_shelf_cnt_15,
  t.avg_shelf_sale,
  t.avg_sale_act,
  t.sale_flag,
  t.satisfy,
  
  t.avg_fill_times,
  t.fill_frequency,
  t.new_min_wave_cycle,
  t.new_mid_wave_cycle,
  t.new_max_wave_cycle,
  t.turnover_days,
  t.valid_turnover_days,
  t.turnover_level,
  t.need_amount,
  t.stag_amount,
  t.lack_amount,
  
  t.avg_send_num,
  t.avg_sale,
  
  t.qualityqty,
  t.stock_quantity,
  t.cover_shelf_cnt,
  t.stock_shelf_cnt,
  t.sale_shelf_cnt,
  
CASE 
WHEN t.suggest_fill_num = '不允许备货' THEN "不允许备货"
WHEN t.valid_turnover_days <= 2 AND t.product_type IN ("原有","新增（试运行）" ) AND t.sale_flag IN (1,2,3) AND t.suggest_fill_num =0 THEN t.`F_BGJ_FBOXEDSTANDARDS`
ELSE t.suggest_fill_num
END AS suggest_fill_num, 
-- IF(t.min_suggest_qty = "不允许备货",0,IF(product_type IN("原有"," 新增（试运行）","新增（试运行）"),IF(t.min_suggest_qty > t.suggest_fill_num, 0, t.min_suggest_qty),0)) AS min_suggest_qty ,
CASE  
WHEN @add_date != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.min_suggest_qty = "不允许备货"  THEN 0
ELSE IF(product_type IN("原有"," 新增（试运行）"),IF(t.min_suggest_qty > t.suggest_fill_num, 0, t.min_suggest_qty),0) 
END AS min_suggest_qty ,
CASE 
WHEN @add_date != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.suggest_fill_num = '不允许备货' THEN 0
WHEN t.valid_turnover_days <= 2 AND t.product_type IN ("原有","新增（试运行）" ) AND t.sale_flag IN (1,2,3) AND t.suggest_fill_num =0 THEN t.`F_BGJ_FBOXEDSTANDARDS`
WHEN product_type IN("原有"," 新增（试运行）","新增（试运行）") THEN t.max_suggest_qty
WHEN (product_type NOT IN("原有"," 新增（试运行）","新增（试运行）") OR ISNULL(product_type)) AND t.qualityqty >= t.suggest_fill_num THEN t.suggest_fill_num
WHEN (product_type NOT IN("原有"," 新增（试运行）","新增（试运行）") OR ISNULL(product_type)) AND t.qualityqty < t.suggest_fill_num THEN t.qualityqty
END AS max_suggest_qty,
CASE 
WHEN @add_date != t.next_fill_date OR ISNULL(t.next_fill_date)  THEN 1  
WHEN t.suggest_fill_num = '不允许备货' THEN 1
WHEN t.qualityqty >= t.suggest_fill_num THEN  1
ELSE 0
END AS warehouse_satitsfy,
CASE 
WHEN @add_date != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.suggest_fill_num = '不允许备货' THEN 0
WHEN t.valid_turnover_days <= 2 AND t.product_type IN ("原有","新增（试运行）" ) AND t.sale_flag IN (1,2,3) AND t.suggest_fill_num =0 THEN t.`F_BGJ_FBOXEDSTANDARDS`
ELSE t.suggest_fill_num
END AS suggest_fill_qty,
CASE 
WHEN @add_date != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.suggest_fill_num = '不允许备货' THEN 0
WHEN t.qualityqty >= t.suggest_fill_num THEN t.suggest_fill_num
WHEN t.qualityqty < t.suggest_fill_num THEN t.qualityqty
END AS suggest_fill_amend,
 CASE
    WHEN @add_date != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 3  # 安徽区周五备货
    WHEN t.suggest_fill_num = '不允许备货' THEN 3 
    WHEN t.avg_send_num > 0 AND t.sale_flag IN (1,2) AND t.`valid_turnover_days` <= 4 THEN 1
    WHEN t.avg_send_num > 0 AND t.sale_flag = 3 AND t.`valid_turnover_days` <= 3 THEN 1  
    WHEN t.avg_send_num = 0 AND t.avg_sale > 0 AND t.sale_flag IN (1,2) AND (t.available_stock + t.`onload_num`) / t.avg_sale <= 5 THEN 1
    WHEN t.avg_send_num = 0 AND t.avg_sale > 0 AND t.sale_flag = 3 AND (t.available_stock + t.`onload_num`) / t.avg_sale <= 4 THEN 1
    WHEN t.avg_send_num = 0 AND t.avg_sale = 0 AND t.available_stock <= 0 AND t.product_type = "新增（试运行）" AND t.sale_flag IN (1,2,3) THEN 1
    ELSE 2      
    END AS fill_priority,
    t.next_fill_date
    FROM fe_dwd.dwd_preware_require_temp t
    WHERE t.product_code2 NOT LIKE "WZ%"
;       
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_preware_product_stat',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user), @timestamp);
  COMMIT;
END$$

DELIMITER ;