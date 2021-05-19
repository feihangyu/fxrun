CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_new_shelf_suggest_list`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate,1);
SET @week_end := SUBDATE(@sdate,WEEKDAY(@sdate) + 1);
SET @this_week := ADDDATE(@week_end,1);
SET @sub2_week := SUBDATE(@this_week,INTERVAL 2 WEEK);
SET @two_month_date := SUBDATE(@sdate,INTERVAL 2 MONTH);
-- 昨日前置仓+大仓有库存的地区商品
DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_product_tmp;
CREATE TEMPORARY TABLE fe_dm.stock_product_tmp (PRIMARY KEY(business_area,product_id))
SELECT business_area,
       product_id,
       SUM(stock)stock,
       SUM(pre_stock)pre_stock
FROM
(
SELECT business_area,
       sku_no,
       storage_amount stock,
       0 AS pre_stock 
FROM fe_dwd.dwd_sc_bdp_warehouse_stock_daily d
JOIN fe_dwd.dwd_pub_warehouse_business_area w ON d.warehouse = w.warehouse_number AND w.data_flag = 1
WHERE sdate = @sub_1
UNION ALL 
SELECT business_area,
       product_code2,
       0 AS stock,
       available_stock
FROM feods.pj_prewarehouse_stock_detail
WHERE check_date = @sub_1
AND available_stock > 0
)a
JOIN fe_dwd.dwd_product_base_day_all p ON a.sku_no = p.product_code2
GROUP BY business_area,product_id
HAVING stock > 0 OR pre_stock > 0;
-- 连续2周标记为热卖/非常好卖/好卖/一般/局部好卖的商品
DROP TEMPORARY TABLE IF EXISTS fe_dm.good_product_tmp;
CREATE TEMPORARY TABLE fe_dm.good_product_tmp (PRIMARY KEY(business_area,product_id))
SELECT business_area,
       a.product_id,
       COUNT(sdate)amount
FROM feods.zs_area_product_sale_flag a
JOIN fe_dwd.dwd_product_base_day_all p ON a.product_id = p.product_id AND p.second_type_name IN ('饮料','奶制品','速食食品','休闲食品','早餐食品','调味品') -- 2020/4/21增加
WHERE sdate >= @sub2_week
AND sdate < @this_week
AND !ISNULL(business_area)
AND sale_level IN ('热卖','好卖','非常好卖','一般','局部好卖')
GROUP BY business_area,product_id
HAVING amount >= 2;
-- 前2周地区二级分类商品gmv排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_tmp;
CREATE TEMPORARY TABLE fe_dm.gmv_tmp (PRIMARY KEY(business_name,product_id))
SELECT business_name,
       second_type_name,
       product_id,
       gmv,
       @rank := IF((@city = business_name AND @type_name = second_type_name),@rank + 1,1) gmv_rank,
       @city := business_name,
       @type_name := second_type_name
FROM
(
SELECT business_name,
       second_type_name,
       a.product_id,
       SUM(gmv)gmv
FROM
(
SELECT business_name,
       product_id,
       SUM(gmv)gmv
FROM feods.fjr_area_product_dgmv
WHERE sdate >= @sub2_week
AND sdate < @this_week
GROUP BY business_name,product_id
UNION ALL
SELECT a.business_name,
       t.product_id,
       SUM(total)gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht t
JOIN fe_dwd.dwd_shelf_base_day_all a ON t.shelf_id = a.shelf_id
WHERE pay_date >= @sub2_week
AND pay_date < @this_week
AND refund_status = '无'
GROUP BY a.business_name,t.product_id
)a
JOIN fe_dwd.dwd_product_base_day_all p ON a.product_id = p.product_id AND p.second_type_name IN ('饮料','奶制品','速食食品','休闲食品','早餐食品','调味品')
GROUP BY a.business_name,p.second_type_name,a.product_id
ORDER BY a.business_name ASC,p.second_type_name ASC,gmv DESC
)b,(SELECT @rank := 0,@city := NULL,@type_name = NULL)r;
-- 前2周地区二级分类商品销量排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.amount_tmp;
CREATE TEMPORARY TABLE fe_dm.amount_tmp (PRIMARY KEY(business_name,product_id))
SELECT business_name,
       second_type_name,
       product_id,
       amount,
       @rank := IF((@city = business_name AND @type_name = second_type_name),@rank + 1,1) amount_rank,
       @city := business_name,
       @type_name := second_type_name
FROM
(
SELECT business_name,
       second_type_name,
       a.product_id,
       SUM(amount)amount
FROM
(
SELECT business_name,
       product_id,
       SUM(qty_sal)amount
FROM feods.fjr_area_product_dgmv
WHERE sdate >= @sub2_week
AND sdate < @this_week
GROUP BY business_name,product_id
UNION ALL
SELECT a.business_name,
       t.product_id,
       SUM(amount)amount
FROM fe_dwd.dwd_op_out_of_system_order_yht t
JOIN fe_dwd.dwd_shelf_base_day_all a ON t.shelf_id = a.shelf_id
WHERE pay_date >= @sub2_week
AND pay_date < @this_week
AND refund_status = '无'
GROUP BY a.business_name,t.product_id
)a
JOIN fe_dwd.dwd_product_base_day_all p ON a.product_id = p.product_id AND p.second_type_name IN ('饮料','奶制品','速食食品','休闲食品','早餐食品','调味品')
GROUP BY a.business_name,p.second_type_name,a.product_id
ORDER BY a.business_name ASC,p.second_type_name ASC,amount DESC
)b,(SELECT @rank := 0,@city := NULL,@type_name = NULL)r;
-- 地区商品二级分类排名50%
DROP TEMPORARY TABLE IF EXISTS fe_dm.top_tmp;
CREATE TEMPORARY TABLE fe_dm.top_tmp (PRIMARY KEY(business_name,second_type_name))
SELECT business_name,
       second_type_name,
       COUNT(*),
       COUNT(*)*0.5 top
FROM fe_dm.gmv_tmp
GROUP BY business_name,second_type_name;
-- GMV排名前50% 或 销量前50%的商品
DROP TEMPORARY TABLE IF EXISTS fe_dm.top_product_tmp;
CREATE TEMPORARY TABLE fe_dm.top_product_tmp (PRIMARY KEY(business_name,product_id))
SELECT a.business_name,
       a.second_type_name,
       a.product_id,
       a.gmv,
       a.gmv_rank,
       b.amount,
       b.amount_rank,
       c.top,
       IF(a.gmv_rank <= c.top,1,0)is_gmv_top,
       IF(b.amount_rank <= c.top,1,0)is_amount_top
FROM fe_dm.gmv_tmp a
LEFT JOIN fe_dm.amount_tmp b ON a.business_name = b.business_name AND a.product_id = b.product_id
LEFT JOIN fe_dm.top_tmp c ON a.business_name = c.business_name AND a.second_type_name = c.second_type_name
WHERE a.gmv_rank <= c.top OR b.amount_rank <= c.top;
-- 当前有库存且连续两周部好卖好卖以上 或 2周gmv排名TOP50% 或 2周销量排名TOP50%
DROP TEMPORARY TABLE IF EXISTS fe_dm.product_tmp;
CREATE TEMPORARY TABLE fe_dm.product_tmp (PRIMARY KEY(business_name,product_id))
SELECT c.business_name,
       c.product_id,
       c.product_code2,
       c.product_name,
       c.second_type_name,
       c.sub_type_name,
       c.gmv,
       c.amount,
       c.is_good,
       c.stock,
       c.pre_stock,
       @rank := IF((@city = c.business_name AND @type_name = c.second_type_name),@rank + 1,1) gmv_rank,
       @city := business_name,
       @type_name := second_type_name
       
FROM
(
SELECT a.business_name,
       a.product_id,
       p.product_code2,
       p.product_name,
       p.second_type_name,
       p.sub_type_name,
       g.gmv,
       t.amount,
       SUM(is_good)is_good,
       s.stock,
       s.pre_stock
FROM
(
SELECT business_area business_name,
       product_id,
       1 AS is_good
FROM fe_dm.good_product_tmp a
UNION
SELECT business_name,
       product_id,
       0 AS is_good
FROM fe_dm.top_product_tmp
)a
JOIN fe_dwd.dwd_product_base_day_all p ON a.product_id = p.product_id
JOIN fe_dm.stock_product_tmp s ON a.business_name = s.business_area AND a.product_id = s.product_id
LEFT JOIN fe_dm.gmv_tmp g ON a.business_name = g.business_name AND a.product_id = g.product_id
LEFT JOIN fe_dm.amount_tmp t ON a.business_name = t.business_name AND a.product_id = t.product_id
GROUP BY a.business_name,a.product_id
ORDER BY a.business_name,p.second_type_name,gmv ASC
)c,(SELECT @rank := 0,@city := NULL,@type_name = NULL)r;
-- 地区推荐总sku数
DROP TEMPORARY TABLE IF EXISTS fe_dm.sku_total_tmp;
CREATE TEMPORARY TABLE fe_dm.sku_total_tmp AS
SELECT business_name,
       COUNT(CASE WHEN second_type_name IN('饮料','奶制品') THEN product_id END)fridge_sku_total,-- 冰箱可用sku
       COUNT(*)shelf_sku_total, -- 无人货架可用sku
       COUNT(CASE WHEN second_type_name IN('饮料','奶制品') THEN product_id END) - 20 out_fridge_sku,-- 冰箱多余sku
       COUNT(*) - 60 out_shelf_sku -- 无人货架多余sku
FROM fe_dm.product_tmp
GROUP BY business_name;
-- 地区二级分类推荐sku占比
DROP TEMPORARY TABLE IF EXISTS fe_dm.sku_rate_tmp;
CREATE TEMPORARY TABLE fe_dm.sku_rate_tmp AS
SELECT a.business_name,
       a.second_type_name,
       COUNT(CASE WHEN a.second_type_name IN ('饮料','奶制品') THEN a.product_id END) / b.fridge_sku_total fridge_sku_rate,
       COUNT(a.product_id) / b.shelf_sku_total shelf_sku_rate
FROM fe_dm.product_tmp a
LEFT JOIN fe_dm.sku_total_tmp b ON a.business_name = b.business_name
GROUP BY a.business_name,a.second_type_name;
-- 冰箱推荐应该删减的sku
DROP TEMPORARY TABLE IF EXISTS fe_dm.fridge_out_tmp;
CREATE TEMPORARY TABLE fe_dm.fridge_out_tmp AS
SELECT a.business_name,
       a.product_id,
       a.gmv,
       a.gmv_rank,
       a.second_type_name,
       IF(a.gmv_rank <= ROUND(s.fridge_sku_rate * t.out_fridge_sku,0),1,0)should_out -- 是否应该删减
FROM fe_dm.product_tmp a
LEFT JOIN fe_dm.sku_total_tmp t ON a.business_name = t.business_name
LEFT JOIN fe_dm.sku_rate_tmp s ON a.business_name = s.business_name AND a.second_type_name = s.second_type_name
WHERE t.out_fridge_sku > 0
AND a.second_type_name IN ('饮料','奶制品')
ORDER BY a.business_name,a.second_type_name,a.gmv_rank ASC;
-- 货架推荐应该删减的sku
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_out_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_out_tmp AS
SELECT a.business_name,
       a.product_id,
       a.gmv,
       a.gmv_rank,
       a.second_type_name,
       ROUND(s.shelf_sku_rate * t.out_shelf_sku,0)out_rank,
       IF(a.gmv_rank <= ROUND(s.shelf_sku_rate * t.out_shelf_sku,0),1,0)should_out -- 是否应该删减
FROM fe_dm.product_tmp a
LEFT JOIN fe_dm.sku_total_tmp t ON a.business_name = t.business_name
LEFT JOIN fe_dm.sku_rate_tmp s ON a.business_name = s.business_name AND a.second_type_name = s.second_type_name
WHERE t.out_shelf_sku > 0
ORDER BY a.business_name,a.second_type_name,a.gmv_rank ASC;
-- 实际删减与应该删减的对比得出还应该删减的sku数,将未删减的sku按gmv升序排名，依次删减gmv最小的
DROP TEMPORARY TABLE IF EXISTS fe_dm.out_actual_tmp;
CREATE TEMPORARY TABLE fe_dm.out_actual_tmp AS
SELECT business_name,
       need_out_fridge,
       need_out_shelf
FROM
(
SELECT t.business_name,
       t.out_fridge_sku,
       t.out_shelf_sku,
       t.out_fridge_sku - IFNULL(f.actual_fridge_out,0) need_out_fridge,
       t.out_shelf_sku - IFNULL(s.actual_shelf_out,0) need_out_shelf
FROM fe_dm.sku_total_tmp t 
LEFT JOIN 
(SELECT business_name,
        COUNT(CASE WHEN should_out = 1 THEN 1 END) actual_fridge_out
FROM fe_dm.fridge_out_tmp
GROUP BY business_name
)f ON t.business_name = f.business_name
LEFT JOIN
(SELECT business_name,
        COUNT(CASE WHEN should_out = 1 THEN 1 END) actual_shelf_out
FROM fe_dm.shelf_out_tmp
GROUP BY business_name
)s ON t.business_name = s.business_name
WHERE out_fridge_sku > 0 OR out_shelf_sku > 0
)b
HAVING need_out_fridge > 0 OR need_out_shelf > 0; 
-- 冰箱需继续删减的sku
DROP TEMPORARY TABLE IF EXISTS fe_dm.fridge_more_out_tmp;
CREATE TEMPORARY TABLE fe_dm.fridge_more_out_tmp AS
SELECT business_name,
       product_id,
       second_type_name,
       gmv,
       need_out_fridge,
       @rank := IF(@city = business_name,@rank + 1,1) rank,
       @city := business_name
FROM
(
SELECT o.business_name,
       o.product_id,
       o.second_type_name,
       o.gmv,
       a.need_out_fridge
FROM fe_dm.fridge_out_tmp o
JOIN fe_dm.out_actual_tmp a ON o.business_name = a.business_name
WHERE a.need_out_fridge > 0
AND o.should_out = 0
ORDER BY business_name,gmv ASC
)a,(SELECT @rank := 0,@city := NULL)r
HAVING rank <= need_out_fridge;
-- 货架需继续删减的sku
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_more_out_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_more_out_tmp AS
SELECT business_name,
       product_id,
       second_type_name,
       gmv,
       need_out_shelf,
       @rank := IF(@city = business_name,@rank + 1,1) rank,
       @city := business_name
FROM
(
SELECT o.business_name,
       o.product_id,
       o.second_type_name,
       o.gmv,
       a.need_out_shelf
FROM fe_dm.shelf_out_tmp o
JOIN fe_dm.out_actual_tmp a ON o.business_name = a.business_name
WHERE a.need_out_shelf > 0
AND o.should_out = 0
ORDER BY business_name,gmv ASC
)a,(SELECT @rank := 0,@city := NULL)r
HAVING rank <= need_out_shelf;
-- 新装货架建议商品清单
DELETE FROM fe_dm.`dm_op_new_shelf_suggest_list`  WHERE stat_date=@this_week  OR stat_date<@two_month_date;
INSERT INTO fe_dm.`dm_op_new_shelf_suggest_list`  
(stat_date
,business_name
,product_id
,product_code2
,product_name
,second_type_name
,sub_type_name
,product_type
,sale_level
,gmv
,amount
,gmv_rank
,amount_rank
,is_gmv_top
,is_good
,is_amount_top
,stock
,pre_stock
,fridge_suggest
,shelf_suggest
,load_time
)
SELECT @this_week stat_date,
       a.business_name,
       a.product_id,
       a.product_code2,
       a.product_name,
       a.second_type_name,
       a.sub_type_name,
       d.product_type,
       f.sale_level,
       a.gmv,
       a.amount,
       b.gmv_rank,
       b.amount_rank,
       b.is_gmv_top,
       a.is_good,
       b.is_amount_top,
       a.stock,
       a.pre_stock,
       IF(a.second_type_name IN ('饮料','奶制品') AND (o.should_out = 0 OR o.should_out IS NULL) AND mf.product_id IS NULL,1,0)fridge_suggest,
       IF((s.should_out = 0 OR s.should_out IS NULL) AND ms.product_id IS NULL,1,0)shelf_suggest,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.product_tmp a
LEFT JOIN fe_dm.top_product_tmp b ON a.business_name = b.business_name AND a.product_id = b.product_id
LEFT JOIN feods.zs_product_dim_sserp d ON a.business_name = d.business_area AND a.product_id = d.product_id
LEFT JOIN fe_dm.fridge_out_tmp o ON a.business_name = o.business_name AND a.product_id = o.product_id
LEFT JOIN fe_dm.shelf_out_tmp s ON a.business_name = s.business_name AND a.product_id = s.product_id
LEFT JOIN fe_dm.fridge_more_out_tmp mf ON a.business_name = mf.business_name AND a.product_id = mf.product_id
LEFT JOIN fe_dm.shelf_more_out_tmp ms ON a.business_name = ms.business_name AND a.product_id = ms.product_id
LEFT JOIN feods.zs_area_product_sale_flag f ON a.business_name = f.business_area AND a.product_id = f.product_id AND f.sdate = @this_week; 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_new_shelf_suggest_list',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('李世龙（朱星华）@', @user, @timestamp));
COMMIT;
	END