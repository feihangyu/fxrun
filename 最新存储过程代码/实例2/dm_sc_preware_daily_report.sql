CREATE DEFINER=`wuting`@`%` PROCEDURE `sp_d_sc_preware_daily_report`(in_sdate DATE)
    SQL SECURITY INVOKER
BEGIN
SET @sdate = in_sdate;
SET @sdate1 = DATE_ADD(@sdate,INTERVAL 1 DAY);
-- SET @sdate2 = IF(@sdate >= '2020-01-31','2019-12-15',@sdate);
-- SET @sdate3 = IF(@sdate >= '2020-01-31','2019-12-16',@sdate);
SET @sdate2 = @sdate;
SET @sdate3 = @sdate;
SET @a = 1.45,@b = 1.45;
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
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
  AND a.apply_time >= DATE_SUB(@sdate1,INTERVAL 7 DAY) 
  AND a.apply_time < @sdate1
GROUP BY 
 --  DATE(b.fill_time),
   a.shelf_id,
   a.product_id;
 
# (2)结余与箱规及商品类型
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_remain_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_remain_temp 
(KEY idx_preware_product (warehouse_id,product_id),
KEY idx_warehouse_productfe(warehouse_number,product_code2),
KEY idx_preware (warehouse_id))
AS
SELECT 
  t1.sdate,
  t1.region_area,
  t1.business_area,
  t1.warehouse_number,
  t1.warehouse_name,
  t1.warehouse_id,
  t1.shelf_name,
  t1.shelf_code,
  t1.product_id,
  t1.product_name,
  t1.product_code2,
  t1.fname,
  IF(t1.`fname` = "水饮", 50,20) AS fnamec,
  t1.total_stock,
  t1.available_stock,
  t1.purchase_price,
  t1.available_stock * t1.purchase_price AS available_amount,
  t1.total_stock_yesterday,
  t1.actual_send_num,
  t1.actual_fill_num,
  t1.stock_in_theory,
  t1.actual_send_forteen,
   CASE 
  WHEN t1.business_area = '安徽区' THEN 1.45
  WHEN t1.business_area = '浙北区' THEN  1.45
  ELSE @a 
  END AS senda,
  t1.avg_send_num * IF(t1.business_area IN ('安徽区','浙北区'),1.45,@a)  AS avg_send_num,
  t1.avg_send_num  AS avg_send_num_act, 
  t1.send_amount_forteen,
  -- t1.avg_send_amount * @a AS avg_send_amount,
  t1.avg_send_amount * IF(t1.business_area IN ('安徽区','浙北区'),1.45,@a) AS avg_send_amount,
  t1.available_stock / t1.avg_send_num  AS turnover_days,
  t1.quantity,
  t1.GMV,
  t2.box_fill_model AS F_BGJ_FBOXEDSTANDARDS,
  t3.product_type,
  IFNULL(t4.onload_num,0) AS onload_num,
  (t1.available_stock + IFNULL(t4.onload_num,0))/t1.avg_send_num  AS valid_turnover_days,
  t5.new_mid_wave_cycle,
  t5.new_min_wave_cycle,
  t5.new_max_wave_cycle,
--   t6.max_sale_flag,
  CASE 
 --  WHEN t1.actual_send_forteen = 0 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "无出库"
--   WHEN t1.available_stock / t1.avg_send_num < 3 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "严重缺货"
--   WHEN t1.available_stock / t1.avg_send_num >= 3 AND t1.available_stock / t1.avg_send_num <5 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "缺货" 
--   WHEN t1.available_stock / t1.avg_send_num >= 5 AND t1.available_stock / t1.avg_send_num <= 10 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "正常"
--   WHEN t1.available_stock / t1.avg_send_num > 10 AND t1.available_stock / t1.avg_send_num <= 15 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "滞压"
--   WHEN t1.available_stock / t1.avg_send_num > 15 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "严重滞压"
  WHEN t1.avg_send_num  = 0 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "无出库"
  WHEN t1.available_stock / t1.avg_send_num  < 2 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "严重缺货"
  WHEN t1.available_stock / t1.avg_send_num  >= 2 AND t1.available_stock / t1.avg_send_num  <5 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "缺货" 
  WHEN t1.available_stock / t1.avg_send_num  >= 5 AND t1.available_stock / t1.avg_send_num  <= 10 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "正常"
  WHEN t1.available_stock / t1.avg_send_num  > 10 AND t1.available_stock / t1.avg_send_num  <= 15 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "滞压"
  WHEN t1.available_stock / t1.avg_send_num  > 15 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") AND t1.available_stock <= t2.box_fill_model THEN '滞压'
  WHEN t1.available_stock / t1.avg_send_num  > 15 AND t3.PRODUCT_TYPE IN ("新增（试运行）","原有") THEN "严重滞压"
  ELSE "无等级"
END AS turnover_level,
t1.avg_send_amount *8 AS need_amount,
CASE
WHEN  t1.available_stock / t1.avg_send_num  > 15 AND t3.PRODUCT_TYPE ="原有"  AND t1.available_stock > t2.box_fill_model THEN t1.AVAILABLE_STOCK * t1.purchase_price - t1.avg_send_amount*8
END AS stag_amount, #严重滞压金额
CASE
WHEN  t1.available_stock / t1.avg_send_num  < 2 AND t3.PRODUCT_TYPE = "原有" THEN  t1.avg_send_amount *8 - t1.AVAILABLE_STOCK * t1.purchase_price 
END AS lack_amount #"严重缺货金额" 
FROM fe_dm.dm_sc_preware_balance  t1
-- JOIN fe_dwd.dwd_product_base_day_all t2
JOIN fe_dwd.`dwd_sc_business_region` w
ON t1.business_area = w.business_area
JOIN fe_dwd.dwd_product_area_pool t2
ON t2.`area_id` = w.`area_id`
AND t2.product_id = t1.product_id
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp t3
ON t1.business_area = t3.business_area
AND t1.product_code2 = t3.product_fe
LEFT JOIN fe_dwd.dwd_preware_fill_onload t4
ON t1.warehouse_id = t4.warehouse_id
AND t1.product_id = t4.product_id
JOIN fe_dm.dm_sc_preware_wave_cycle t5
ON t5.sdate = @sdate
AND t1.warehouse_id = t5.warehouse_id
AND t1.product_id = t5.product_id
-- LEFT JOIN fe_dm.dm_sc_preware_balance t6
-- ON t6.sdate = @sdate2
-- AND t6.warehouse_id = t1.warehouse_id
-- AND t6.product_id = t1.product_id
-- LEFT JOIN fe_dm.dm_sc_preware_outbound_seven_day t6
-- ON t6.sdate = @sdate2
-- AND t6.warehouse_id = t1.warehouse_id
-- AND t6.product_id = t1.product_id
WHERE t1.sdate = @sdate
AND t5.sdate = @sdate 
;
#(3) ERP大仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_warehouse_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_warehouse_temp 
(KEY idx_warehouse_product(warehouse_number,product_bar))
AS
SELECT 
    t2.FPRODUCEDATE
    , t1.region_area
    , t1.business_area
    , t1.warehouse_number
    , t1.warehouse_name
    , t2.PRODUCT_BAR
    , t2.PRODUCT_NAME
    , t2.QUALITYQTY 
FROM
    fe_dwd.dwd_pub_warehouse_business_area t1 
    JOIN fe_dwd.dwd_PJ_OUTSTOCK2_DAY t2 
        ON t1.warehouse_number = t2.warehouse_number 
        AND t1.to_preware = 1 
        AND t2.FPRODUCEDATE = @sdate ;
-- DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_warehouse_temp;
-- CREATE TEMPORARY TABLE fe_dwd.dwd_preware_warehouse_temp 
-- (KEY idx_warehouse_product(warehouse_number,product_bar))
-- AS
-- SELECT 
--     t2.sdate
--     , t1.region_area
--     , t1.business_area
--     , t1.warehouse_number
--     , t1.warehouse_name
--     , t2.sku_no AS PRODUCT_BAR
--     , t2.skuname AS PRODUCT_NAME
--     , t2.storage_amount AS QUALITYQTY 
-- FROM
--     fe_dwd.dwd_pub_warehouse_business_area t1 
--     JOIN fe_dwd.dwd_sc_bdp_warehouse_stock_daily t2 
--         ON t1.WAREHOUSE_NUMBER = t2.warehouse 
--         AND t1.data_flag = 1 
--         AND t2.sdate = @sdate ;
#(4)前置仓覆盖货架数及销量及销售等级
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_fifteen_sale_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_fifteen_sale_temp
(KEY idx_preware_product (warehouse_id,product_id) 
) 
AS
SELECT 
DATE_SUB(CURDATE(),INTERVAL 1 DAY) AS sdate,
a.warehouse_id,
a.SHELF_CODE,
a.PRODUCT_ID,
c.PRODUCT_CODE2,
c.PRODUCT_NAME,
a.stock_quantity,  #'前置仓覆盖货架库存量',
b.sale_flag, #'商品畅销等级',
-- IFNULL(d.sale_flag,"严重滞销") AS sale_flag_amend, #AS "畅销等级",
IFNULL(b.quantity,0)  AS sales_fifteen, #AS '近15天销量',
IFNULL(b.discount_sale_qty /15/ b.discount_shelf_cnt,0) AS avg_shelf_sale, #AS '近15天日架均',
IFNULL((b.discount_sale_qty * IF(s.`business_name` IN ("安徽区","浙北区"),1.35,@b))/ 15,0) AS avg_sale, #AS '近15天日架均',
IF(s.`business_name` IN ("安徽区","浙北区"),1.35,@b) saleb,
IFNULL(b.sale_shelf_cnt,0) AS sale_shelf_cnt,  #AS '近15天有销售货架数',
a.stock_shelf_cnt  #AS '有库存货架数'
FROM
(
SELECT t1.prewarehouse_code  AS shelf_code
, t1.prewarehouse_id AS  warehouse_id
, t3.product_id
, SUM(t3.stock_quantity) AS stock_quantity
, COUNT(DISTINCT(IF(t3.STOCK_QUANTITY >0 ,t3.SHELF_ID,NULL))) AS stock_shelf_cnt
FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all t1
JOIN fe_dwd.dwd_shelf_product_day_all t3
ON t1.shelf_id = t3.shelf_id
-- AND t3.stock_quantity > 0
AND t1.shelf_status = 2 
GROUP BY t1.prewarehouse_id,t3.product_id
)  a
JOIN 
fe_dwd.dwd_product_base_day_all c
ON a.product_id = c.product_id
LEFT JOIN #前置仓近15天销售
fe_dm.dm_preware_sales_fifteen b
ON b.sdate = @sdate3  # 过节期间修改
AND b.warehouse_id = a.warehouse_id
AND b.product_id = a.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` s
ON a.warehouse_id = s.`shelf_id`
-- LEFT JOIN #前置仓销售等级
-- fe_dm.dm_preware_shelf_sales_thirty d
-- ON d.sdate = @sdate 
-- AND d.warehouse_id  = a.warehouse_id
-- AND d.product_id = a.product_id
;
#(5)覆盖货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_shelf_cover_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_shelf_cover_temp 
(KEY idx_warehouse(warehouse_id))
AS
SELECT
    t.prewarehouse_id AS warehouse_id
    , COUNT(t.shelf_id) AS cover_shelf_cnt
    , CASE 
    WHEN SUM(IF(t.shelf_type IN (1,3),1,0))/COUNT(t.shelf_id) >= 0.6 THEN '正常货架覆盖仓'
    WHEN SUM(IF(t.shelf_type IN (2,5),1,0))/COUNT(t.shelf_id) >= 0.6 THEN '冰箱覆盖仓'
    WHEN SUM(IF(t.shelf_type IN (6,7),1,0))/COUNT(t.shelf_id) >= 0.6 THEN '智能机覆盖仓'
    WHEN SUM(IF(t.shelf_type = 4,1,0))/COUNT(t.shelf_id) >= 0.6 THEN '虚拟货架覆盖仓'
    ELSE "混合货架类型覆盖仓"
    END AS preware_type 
FROM
    fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all t
WHERE t.shelf_status = 2
GROUP BY t.prewarehouse_id ;
#(6)#爆款满足率
-- SELECT *
-- FROM fe_dm.dm_sc_preware_sku_satisfy t
-- WHERE t.sdate = @sdate;
#最终结果
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_preware_require_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_preware_require_temp AS
SELECT 
  t1.sdate,
  t1.region_area,
  t1.business_area,
  t1.warehouse_number,
  t1.warehouse_name,
  t1.warehouse_id,
  t1.shelf_name,
  t1.shelf_code,
  t1.product_id,
  t1.product_name,
  t1.product_code2,
  t1.fname,
  t1.total_stock,
  t1.available_stock,
  t1.purchase_price,
  t1.available_amount,
  t1.total_stock_yesterday,
  t1.actual_send_num,
  t1.actual_fill_num,
  t1.stock_in_theory,
  t1.actual_send_forteen,
  t1.avg_send_num,
  t1.avg_send_num_act,
  t1.send_amount_forteen,
  t1.avg_send_amount,
  t1.turnover_days,
  t1.quantity,
  t1.GMV,
  t1.F_BGJ_FBOXEDSTANDARDS,
  t1.product_type,
  t1.onload_num,
  t1.valid_turnover_days,
  t1.new_mid_wave_cycle,
  t1.turnover_level,
  IFNULL(t1.need_amount,0) AS need_amount ,
  IFNULL(t1.stag_amount,0) AS stag_amount,
  IFNULL(t1.lack_amount,0) AS lack_amount,
  IFNULL(t2.stock_quantity,0) AS stock_quantity,
  IFNULL(t2.sales_fifteen,0) AS sales_fifteen,
  IFNULL(t2.avg_shelf_sale,0) AS avg_shelf_sale,
  IFNULL(t2.avg_sale,0) AS avg_sale,
  IFNULL(t2.sale_shelf_cnt,0) AS sale_shelf_cnt,
  IFNULL(t2.stock_shelf_cnt,0) AS stock_shelf_cnt,
  IFNULL(t3.cover_shelf_cnt,0) AS cover_shelf_cnt,
  IFNULL(t4.qualityqty,0) AS qualityqty, #正品库存
  t5.satisfy, #满足率
  IF(t6.shelf_status = 11,t6.shelf_status,t6.whether_close) whether_close,
  t6.revoke_status,
--   t5.sale_flag,
  IFNULL(t7.sale_flag,"严重滞销") AS adjust_sale_flag ,
  IFNULL(t2.sale_flag,"严重滞销") AS sales_level,
  IFNULL(t7.discount_sale_qty,0) AS discount_sale_qty,
  IFNULL(t3.preware_type,"无激活状态货架") AS preware_type,
  t8.next_fill_date,
  
# 使用30天日均销量之后
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN 1 -- 吉林、山西、冀州、江西
WHEN t6.whether_close = 1 THEN 2
WHEN t6.revoke_status != 1 THEN 3
WHEN t6.shelf_status = 11 THEN 4
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle  AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销","滞销")
THEN 5
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle AND np_flag = 0 THEN 51
-- ) 库存小于15,else 
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24  AND t1.valid_turnover_days <t1.new_mid_wave_cycle THEN 6
-- 2）库存大于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  >= 24  AND t1.valid_turnover_days <t1.new_mid_wave_cycle THEN 7
-- 2020-08-10 拆分有库存，有出库，周转天>目标周转天
-- 1）库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销") 
THEN 8
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND t9.np_flag = 0 
THEN 81
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN 9
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN 10
-- 有库存，无出库，根据销售量来判断
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t2.avg_sale*t1.new_mid_wave_cycle   THEN 11
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t2.avg_sale*t1.new_mid_wave_cycle  THEN 12
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" THEN 13
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" THEN 14
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t2.sale_flag IN ("畅销","爆款","平销") THEN 15
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN 16 
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN 17
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN 18
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t2.avg_sale>0  AND t1.AVAILABLE_STOCK =0 THEN 19
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_mid_wave_cycle 
THEN 20
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_mid_wave_cycle THEN 21
END  AS sense,
# 建议补货量
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN "不允许备货" -- 吉林、山西、冀州、江西
WHEN t6.whether_close = 1 THEN "不允许备货"
WHEN t6.revoke_status != 1 THEN "不允许备货"
WHEN t6.shelf_status = 11 THEN "不允许备货"
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle  AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销","滞销")
THEN IF(ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,IF(ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) =0,t1.F_BGJ_FBOXEDSTANDARDS,ROUND(((t1.new_mid_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS))
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_mid_wave_cycle AND np_flag = 0 
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
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销") 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle AND t9.np_flag = 0 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN "不允许备货"
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_mid_wave_cycle THEN "不允许备货"
-- 有库存，无出库，根据销售量来判断
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t2.avg_sale*t1.new_mid_wave_cycle    THEN "不允许备货"
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t2.avg_sale*t1.new_mid_wave_cycle    
THEN IF(ROUND((t2.avg_sale*t1.new_mid_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec, ROUND((t2.avg_sale*t1.new_mid_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS )
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 3)
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" 
THEN "不允许备货"
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t2.sale_flag IN ("畅销","爆款","平销")
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2) 
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN "不允许备货"  
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN "不允许备货"
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN "不允许备货" 
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t2.avg_sale>0  AND t1.AVAILABLE_STOCK =0 
THEN IF(ROUND((t2.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t2.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_mid_wave_cycle 
THEN IF(ROUND((t2.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t2.avg_sale*t1.new_mid_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_mid_wave_cycle THEN "不允许备货"
END  AS suggest_fill_num,
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN "不允许备货" -- 吉林、山西、冀州、江西
WHEN t6.whether_close = 1 THEN "不允许备货"
WHEN t6.revoke_status != 1 THEN "不允许备货"
WHEN t6.shelf_status = 11 THEN "不允许备货"
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_min_wave_cycle  AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销","滞销")
THEN IF(ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,IF(ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) =0,t1.F_BGJ_FBOXEDSTANDARDS,ROUND(((t1.new_min_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS))
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_min_wave_cycle AND np_flag = 0 
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
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销") 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle AND t9.np_flag = 0 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle THEN "不允许备货"
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_min_wave_cycle THEN "不允许备货"
-- 有库存，无出库，根据销售量来判断
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t2.avg_sale*t1.new_min_wave_cycle    THEN "不允许备货"
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t2.avg_sale*t1.new_min_wave_cycle    
THEN IF(ROUND((t2.avg_sale*t1.new_min_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec, ROUND((t2.avg_sale*t1.new_min_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS )
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 3)
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" 
THEN "不允许备货"
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t2.sale_flag IN ("畅销","爆款","平销")
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2) 
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN "不允许备货"  
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN "不允许备货"
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN "不允许备货" 
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t2.avg_sale>0  AND t1.AVAILABLE_STOCK =0 
THEN IF(ROUND((t2.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t2.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_min_wave_cycle 
THEN IF(ROUND((t2.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t2.avg_sale*t1.new_min_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_min_wave_cycle THEN "不允许备货"
END AS min_suggest_qty,
CASE 
WHEN t1.business_area IN ("吉林区","山西区","冀州区","江西区") THEN "不允许备货" -- 吉林、山西、冀州、江西
WHEN t6.whether_close = 1 THEN "不允许备货"
WHEN t6.revoke_status != 1 THEN "不允许备货"
WHEN t6.shelf_status = 11 THEN "不允许备货"
-- 2020-08-10 拆分有库存，有出库，周转天<目标周转天
-- 1) 库存小于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_max_wave_cycle  AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销","滞销")
THEN IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) =0,t1.F_BGJ_FBOXEDSTANDARDS,ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS))
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  < 24 AND t1.valid_turnover_days <t1.new_max_wave_cycle AND np_flag = 0 
THEN IF(CEILING(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec
,CEILING(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2）库存大于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num  >= 24  AND t1.valid_turnover_days <t1.new_max_wave_cycle 
THEN IF(ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND(((t1.new_max_wave_cycle-t1.valid_turnover_days) * t1.avg_send_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS)
-- 2020-08-10 拆分有库存，有出库，周转天>目标周转天
-- 1）库存小于15
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle AND (t1.product_type = "原有" OR t9.np_flag = 1) AND t2.sale_flag IN ("畅销","爆款","平销") 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 2020-09-02增加拆分仓新品场景
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle AND t9.np_flag = 0 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>= 24,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2)
-- 1)库存小于15,else
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num < 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle THEN "不允许备货"
-- 2）库存大于15,
WHEN t1.avg_send_num  >0 AND t1.AVAILABLE_STOCK + t1.onload_num >= 24 AND t1.valid_turnover_days >= t1.new_max_wave_cycle THEN "不允许备货"
-- 有库存，无出库，根据销售量来判断
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num > t2.avg_sale*t1.new_max_wave_cycle    THEN "不允许备货"
WHEN t1.avg_send_num  = 0 AND t2.avg_sale > 0 AND t1.AVAILABLE_STOCK + t1.onload_num <= t2.avg_sale*t1.new_max_wave_cycle    
THEN IF(ROUND((t2.avg_sale*t1.new_max_wave_cycle   - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) >= fnamec, t1.F_BGJ_FBOXEDSTANDARDS * fnamec, ROUND((t2.avg_sale*t1.new_max_wave_cycle    - t1.AVAILABLE_STOCK - t1.onload_num)/t1.F_BGJ_FBOXEDSTANDARDS) * t1.F_BGJ_FBOXEDSTANDARDS )
-- 2020-08-10日修改，无出库，无销售，无库存
-- （1）新品无在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num = 0 AND t1.product_type = "新增（试运行）" 
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS, t1.F_BGJ_FBOXEDSTANDARDS* 3)
-- 2）新品有在途
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0  AND t1.onload_num > 0 AND t1.product_type = "新增（试运行）" 
THEN "不允许备货"
-- 3）原有品
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 AND t1.product_type = "原有" AND t2.sale_flag IN ("畅销","爆款","平销")
THEN IF(t1.F_BGJ_FBOXEDSTANDARDS>=80,t1.F_BGJ_FBOXEDSTANDARDS,t1.F_BGJ_FBOXEDSTANDARDS* 2) 
-- 4）除以上3者不允许备货
WHEN IFNULL(t1.avg_send_num,0) = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK =0 THEN "不允许备货"  
-- 5）补充场景
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK + t1.onload_num>0 THEN "不允许备货"
-- 6）补充# 2019-11-15日新增情况，可用库存为负的三无情况
WHEN t1.avg_send_num  = 0 AND IFNULL(t2.avg_sale,0) = 0 AND t1.AVAILABLE_STOCK <=0 AND  t1.onload_num =0 THEN "不允许备货" 
-- 无库存，无出库，有销量
WHEN t1.avg_send_num  = 0 AND t2.avg_sale>0  AND t1.AVAILABLE_STOCK =0 
THEN IF(ROUND((t2.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t2.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 无库存，有出库，销售不定
-- 1) 周转较小
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days <= t1.new_max_wave_cycle 
THEN IF(ROUND((t2.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS) >=  fnamec,t1.F_BGJ_FBOXEDSTANDARDS * fnamec,ROUND((t2.avg_sale*t1.new_max_wave_cycle    - t1.onload_num) / t1.F_BGJ_FBOXEDSTANDARDS)*t1.F_BGJ_FBOXEDSTANDARDS) 
-- 2)周转较大
WHEN t1.avg_send_num  > 0 AND t1.AVAILABLE_STOCK =0 AND t1.valid_turnover_days > t1.new_max_wave_cycle THEN "不允许备货"
END  AS max_suggest_qty
FROM fe_dwd.dwd_preware_remain_temp t1
LEFT JOIN fe_dwd.dwd_preware_shelf_cover_temp t3
ON t1.warehouse_id = t3.warehouse_id
LEFT JOIN fe_dwd.dwd_preware_fifteen_sale_temp t2
ON t1.warehouse_id = t2.warehouse_id
AND t1.product_id = t2.product_id
LEFT JOIN fe_dwd.dwd_preware_warehouse_temp t4
ON t1.warehouse_number = t4.warehouse_number
AND t1.product_code2 = t4.product_bar
LEFT JOIN fe_dm.dm_sc_preware_sku_satisfy t5
ON t1.warehouse_id = t5.warehouse_id
AND t1.product_id = t5.product_id
AND t5.sdate = @sdate # 过年期间修改
LEFT JOIN fe_dwd.dwd_shelf_base_day_all t6
ON t1.warehouse_id = t6.shelf_id
LEFT JOIN fe_dm.dm_preware_shelf_sales_thirty t7
ON t1.warehouse_id = t7.warehouse_id
AND t1.product_id = t7.product_id
AND t7.sdate = @sdate3 
LEFT JOIN fe_dm.dm_sc_preware_fill_frequency t8
ON t1.warehouse_id = t8.warehouse_id
LEFT JOIN fe_dm.`dm_sc_preware_new_product_flag` t9
ON t1.warehouse_id  = t9.warehouse_id
AND t1.product_id = t9.product_id
;
#修正结果
DELETE FROM fe_dm.dm_sc_preware_daily_report WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_daily_report
(sdate ,
  region_area,
  business_area,
  warehouse_number,
  warehouse_name,
  warehouse_id,
  shelf_name,
  shelf_code,
  product_id,
  product_name,
  product_code2,
  fname,
  total_stock,
  available_stock,
  purchase_price,
  available_amount,
  total_stock_yesterday,
  actual_send_num,
  actual_fill_num,
  stock_in_theory,
  actual_send_forteen_qty,
  avg_send_num,
   avg_send_num_act,
  actual_send_forteen_amount,
  avg_send_amount,
  turn_over_day,
  quantity,
  GMV,
  f_bgj_fboxedstandards,
  product_type,
  onload_num,
  valid_turnover_days,
   new_wave_cycle,
  turn_over_level,
  require_amount,
  seriously_stagnant_amount,
  seriously_lack_amount,
  cover_shelf_stock,
  sale_in_fifteen_days,
  per_shelf_dailysale,
  sale_shelf_cnt,
  stock_shelf_cnt,
  cover_shelf_cnt,
  qualityqty,
  satisfy,
  whether_close,
  revoke_status,
  sale_flag,
  sales_level,
   sales_level_flag,
  suggest_fill,
   min_suggest_qty,
   max_suggest_qty,
  warehouse_satitsfy_amend,
  suggest_fill_qty,
  suggest_fill_amend,
   turn_over_level_flag,
   fill_priority,
   preware_type,
   next_fill_date
  )
SELECT 
  t.sdate,
  t.region_area,
  t.business_area,
  t.warehouse_number,
  t.warehouse_name,
  t.warehouse_id,
  t.shelf_name,
  t.shelf_code,
  t.product_id,
  t.product_name,
  t.product_code2,
  t.fname,
  t.total_stock,
  t.available_stock,
  t.purchase_price,
  t.available_amount,
  t.total_stock_yesterday,
  t.actual_send_num,
  t.actual_fill_num,
  t.stock_in_theory,
  t.actual_send_forteen,
  t.avg_send_num,
  t.avg_send_num_act,
  t.send_amount_forteen,
  t.avg_send_amount,
  t.turnover_days,
  t.quantity,
  t.GMV,
  t.F_BGJ_FBOXEDSTANDARDS,
  t.product_type,
  t.onload_num,
  t.valid_turnover_days,
  t.new_mid_wave_cycle,
  t.turnover_level,
  t.need_amount ,
  t.stag_amount,
  t.lack_amount,
  t.stock_quantity,
  t.sales_fifteen,
  t.avg_shelf_sale,
  t.sale_shelf_cnt,
  t.stock_shelf_cnt,
  t.cover_shelf_cnt,
  t.qualityqty, #正品库存
  t.satisfy, #满足率
  t.whether_close,
  t.revoke_status,
  t.adjust_sale_flag,
  t.sales_level,
  CASE t.sales_level
    WHEN "爆款" THEN 1
    WHEN "畅销" THEN 2
    WHEN "平销" THEN 3
    WHEN "滞销" THEN 4
    WHEN "严重滞销" THEN 5
    END AS sales_level_flag, 
CASE 
WHEN t.suggest_fill_num = '不允许备货' THEN "不允许备货"
WHEN t.valid_turnover_days <= 2 AND t.product_type IN ("原有","新增（试运行）" ) AND t.sales_level IN ("畅销","爆款","平销") AND t.suggest_fill_num =0 THEN t.`F_BGJ_FBOXEDSTANDARDS`
ELSE t.suggest_fill_num
END AS suggest_fill_num, 
-- IF(t.min_suggest_qty = "不允许备货",0,IF(product_type IN("原有"," 新增（试运行）","新增（试运行）"),IF(t.min_suggest_qty > t.suggest_fill_num, 0, t.min_suggest_qty),0)) AS min_suggest_qty ,
CASE  
WHEN @sdate1 != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.min_suggest_qty = "不允许备货"  THEN 0
ELSE IF(product_type IN("原有"," 新增（试运行）"),IF(t.min_suggest_qty > t.suggest_fill_num, 0, t.min_suggest_qty),0) 
END AS min_suggest_qty ,
CASE 
WHEN @sdate1 != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.suggest_fill_num = '不允许备货' THEN 0
WHEN t.valid_turnover_days <= 2 AND t.product_type IN ("原有","新增（试运行）" ) AND t.sales_level IN ("畅销","爆款","平销") AND t.suggest_fill_num =0 THEN t.`F_BGJ_FBOXEDSTANDARDS`
WHEN product_type IN("原有"," 新增（试运行）","新增（试运行）") THEN t.max_suggest_qty
WHEN (product_type NOT IN("原有"," 新增（试运行）","新增（试运行）") OR ISNULL(product_type)) AND t.qualityqty >= t.suggest_fill_num THEN t.suggest_fill_num
WHEN (product_type NOT IN("原有"," 新增（试运行）","新增（试运行）") OR ISNULL(product_type)) AND t.qualityqty < t.suggest_fill_num THEN t.qualityqty
END AS max_suggest_qty,
CASE 
WHEN @sdate1 != t.next_fill_date OR ISNULL(t.next_fill_date)  THEN "满足备货量"  
WHEN t.suggest_fill_num = '不允许备货' THEN "满足备货量"
WHEN t.qualityqty >= t.suggest_fill_num THEN  "满足备货量"
ELSE "不满足"
END AS warehouse_satitsfy_amend,
CASE 
WHEN @sdate1 != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.suggest_fill_num = '不允许备货' THEN 0
WHEN t.valid_turnover_days <= 2 AND t.product_type IN ("原有","新增（试运行）" ) AND t.sales_level IN ("畅销","爆款","平销") AND t.suggest_fill_num =0 THEN t.`F_BGJ_FBOXEDSTANDARDS`
ELSE t.suggest_fill_num
END AS suggest_fill_qty,
CASE 
WHEN @sdate1 != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 0
WHEN t.suggest_fill_num = '不允许备货' THEN 0
WHEN t.qualityqty >= t.suggest_fill_num THEN t.suggest_fill_num
WHEN t.qualityqty < t.suggest_fill_num THEN t.qualityqty
END AS suggest_fill_amend,
CASE t.`turnover_level` 
    WHEN "正常" THEN 1
    WHEN "缺货" THEN 2
    WHEN "严重缺货" THEN 3
    WHEN "滞压" THEN 4
    WHEN "严重滞压" THEN 5
    WHEN "无出库" THEN 6
    WHEN "无等级" THEN 7 
    END AS turn_over_level_flag,
 CASE
    WHEN @sdate1 != t.next_fill_date OR ISNULL(t.next_fill_date) THEN 3  # 安徽区周五备货
    WHEN t.suggest_fill_num = '不允许备货' THEN 3 
    WHEN t.avg_send_num > 0 AND t.sales_level IN ("爆款","畅销") AND t.`valid_turnover_days` <= 4 THEN 1
    WHEN t.avg_send_num > 0 AND t.sales_level = '平销' AND t.`valid_turnover_days` <= 3 THEN 1  
    WHEN t.avg_send_num = 0 AND t.avg_sale > 0 AND t.sales_level IN ("爆款","畅销") AND (t.available_stock + t.`onload_num`) / t.avg_sale <= 5 THEN 1
    WHEN t.avg_send_num = 0 AND t.avg_sale > 0 AND t.sales_level = '平销' AND (t.available_stock + t.`onload_num`) / t.avg_sale <= 4 THEN 1
    WHEN t.avg_send_num = 0 AND t.avg_sale = 0 AND t.available_stock <= 0 AND t.product_type = "新增（试运行）" AND t.sales_level IN ("爆款","畅销","平销") THEN 1
    ELSE 2      
    END AS fill_priority,
    t.preware_type,
    t.next_fill_date
    FROM fe_dwd.dwd_preware_require_temp t
    WHERE t.product_code2 NOT LIKE "WZ%"
;     
-- 执行记录日志
CALL sh_process.sp_sf_dw_task_log (
'sp_d_sc_preware_daily_report',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user)
 , @timestamp); 
COMMIT;
    END