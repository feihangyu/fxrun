CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_op_shelf_product_price_dispose_monitor`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 地区商品池
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_product_tmp (PRIMARY KEY (area_name,product_id))
SELECT area_name,
       product_id,
       product_type,
       area_product_price,
       CONCAT(product_status,type_item)pool_product_type,
       price
FROM
(
SELECT b.area_name,
       a.product_id,
       d.product_type,
       a.area_product_price,
       CASE WHEN a.status = 1 THEN '新品'
            WHEN a.status = 2 THEN '正常运营'
            WHEN a.status = 3 THEN '淘汰'
            WHEN a.status = 4 THEN '退出'
       END AS product_status,
       IF(a.status = 1,l.new_type,'')type_item,
       d.price  -- 商品清单中新品标准售价
FROM fe_dwd.dwd_product_area_pool a
LEFT JOIN fe_dwd.dwd_sf_product_business_area b ON a.area_id = b.area_id AND b.data_flag = 1
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp d ON b.area_name = d.business_area AND a.product_id = d.product_id
LEFT JOIN 
(
SELECT a.psc_log_id,
       a.area_id,
       a.product_id,
       c1.item_name new_type
FROM
(
SELECT MAX(psc_log_id)psc_log_id,
       area_id,
       product_id,
       MAX(change_time)change_time
FROM fe_dwd.dwd_product_status_change_log
WHERE data_flag = 1
AND change_reason IS NOT NULL
GROUP BY area_id,product_id
HAVING !ISNULL(area_id)
)a
LEFT JOIN fe_dwd.dwd_product_status_change_log b ON a.psc_log_id = b.psc_log_id AND b.data_flag = 1
LEFT JOIN fe_dwd.dwd_pub_dictionary c1 ON b.change_reason = c1.item_value AND c1.dictionary_id = 434 -- 商品池新品模式
)l ON a.area_id = l.area_id AND a.product_id =  l.product_id
WHERE a.data_flag = 1
)b;
-- 全网商品池平均价格
DROP TEMPORARY TABLE IF EXISTS fe_dwd.avg_price_tmp;
CREATE TEMPORARY TABLE fe_dwd.avg_price_tmp (PRIMARY KEY (product_id))
SELECT product_id,
       AVG(area_product_price)avg_price
FROM fe_dwd.area_product_tmp
WHERE pool_product_type NOT IN ('淘汰','退出')
GROUP BY product_id;
-- 地区商品最近一次采购价
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_product_pruchase_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_product_pruchase_tmp (PRIMARY KEY(business_area,product_id))
SELECT a.stat_month,
       a.business_area,
       a.product_id,
       b.purchase_price
FROM
(
SELECT MAX(stat_month)stat_month,
       business_area,
       product_id
FROM fe_dwd.dwd_monthly_manual_purchase_price_insert
WHERE purchase_price IS NOT NULL
GROUP BY business_area,product_id
HAVING !ISNULL(product_id)
)a
LEFT JOIN fe_dwd.dwd_monthly_manual_purchase_price_insert b ON a.stat_month = b.stat_month AND a.business_area = b.business_area AND a.product_id = b.product_id;
-- 地区商品价格异常监控 每日更新，不需要结存历史数据
truncate table fe_dwd.dwd_op_area_product_price_monitor;
INSERT INTO fe_dwd.dwd_op_area_product_price_monitor
(area_name
,product_id
,product_type
,pool_product_type
,area_product_price
,avg_price
,price
,purchase_price
,is_abnormal_1
,is_abnormal_2
,is_abnormal_3
,load_time
)
SELECT a.area_name,
       a.product_id,
       a.product_type,
       a.pool_product_type,
       a.area_product_price,
       b.avg_price,
       a.price,
       c.purchase_price,
       IF((a.area_product_price - IFNULL(c.purchase_price,0)) / a.area_product_price <= 0.2,1,0)is_abnormal_1,
       IF(((area_product_price - IFNULL(avg_price,0)) / avg_price) >= 0.2 OR (area_product_price - IFNULL(avg_price,0)) >= 0.5 OR (area_product_price - IFNULL(avg_price,0)) <= (-0.5),1,0)is_abnormal_2,
       IF(a.pool_product_type = '新品试运行' AND a.area_product_price != a.price,1,0)is_abnormal_3,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.area_product_tmp a
LEFT JOIN fe_dwd.avg_price_tmp b ON a.product_id = b.product_id
LEFT JOIN fe_dwd.area_product_pruchase_tmp c ON a.area_name = c.business_area AND a.product_id = c.product_id;
-- 货架商品明细
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp (PRIMARY KEY(shelf_id,product_id))
SELECT a.business_name,
       a.shelf_id,
       LEFT(shelf_name,5)shelf_name5,
       if_bind,
       a.shelf_type,
       b.product_id,
       b.sale_price,
       b.shelf_fill_flag,
       b.max_quantity,
       c.purchase_price,
       IFNULL((b.sale_price - IFNULL(c.purchase_price,0)) / b.sale_price,0) profit,-- 毛利率
       f.product_type,
       e.area_product_price,
       g.fill_model,-- 补货规格
       g.fill_unit, -- 补货单位(袋/盒/瓶等)
       b.sale_price - IFNULL(e.area_product_price,0) s_value,
       (b.sale_price - IFNULL(e.area_product_price,0)) / e.area_product_price s_value_rate
FROM fe_dwd.dwd_shelf_product_day_all b
JOIN fe_dwd.dwd_shelf_base_day_all a ON a.shelf_id = b.shelf_id
LEFT JOIN fe_dwd.area_product_pruchase_tmp c ON a.business_name = c.business_area AND b.product_id = c.product_id
LEFT JOIN fe_dwd.area_product_tmp e ON a.business_name = e.area_name AND b.product_id = e.product_id
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp f ON a.business_name = f.business_area AND b.product_id = f.product_id
LEFT JOIN fe_dwd.dwd_product_base_day_all g ON b.product_id = g.product_id
WHERE a.shelf_status = 2
AND a.shelf_type IN(1,2,3,5,6,7);
-- 点位商品价格个数,找出同一个点位价格不一致的商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.location_price_num_tmp;
CREATE TEMPORARY TABLE fe_dwd.location_price_num_tmp (PRIMARY KEY(shelf_name5,product_id))
SELECT shelf_name5,
       product_id,
       COUNT(DISTINCT sale_price)price_num
FROM fe_dwd.shelf_product_tmp
WHERE shelf_type = 7
GROUP BY shelf_name5,product_id
HAVING price_num > 1;
-- 货架商品价格异常明细 每日更新，不需要结存历史数据
truncate table fe_dwd.dwd_op_shelf_product_price_monitor;
INSERT INTO fe_dwd.dwd_op_shelf_product_price_monitor
(business_name
,shelf_id
,product_id
,sale_price
,shelf_fill_flag
,purchase_price
,profit
,product_type
,s_value
,s_value_rate
,area_product_price
,is_abnormal_1
,is_abnormal_2
,is_abnormal_3
,is_abnormal_4
,is_abnormal_5
,load_time
)
SELECT business_name,
       shelf_id,
       a.product_id,
       sale_price,
       shelf_fill_flag,
       purchase_price,
       profit,
       product_type,
       s_value,
       s_value_rate,
       area_product_price,
       IF(sale_price < 0.99,1,0)is_abnormal_1,
       IF(profit < 0.15,1,0)is_abnormal_2,
       IF(shelf_type IN (1,2,3,5) AND (s_value >= 0.5 OR s_value <= (-0.5) OR s_value_rate > 0.2),1,0)is_abnormal_3,
       IF(shelf_type IN (6,7) AND sale_price < area_product_price,1,0)is_abnormal_4,
       IF(b.shelf_name5 IS NOT NULL,1,0)is_abnormal_5,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.shelf_product_tmp a
LEFT JOIN fe_dwd.location_price_num_tmp b ON a.shelf_name5 = b.shelf_name5  and a.product_id=b.product_id
HAVING is_abnormal_1 = 1 OR is_abnormal_2 = 1 OR is_abnormal_3 = 1 OR is_abnormal_4 = 1 OR is_abnormal_5 = 1;
-- 标配异常明细 每日更新，不需要结存历史数据
truncate table fe_dwd.dwd_op_shelf_product_dispose_monitor;
INSERT INTO fe_dwd.dwd_op_shelf_product_dispose_monitor
(shelf_id
,product_id
,max_quantity
,fill_unit
,fill_model
,is_abnormal_1
,is_abnormal_2
,is_abnormal_3
,load_time
)
SELECT shelf_id,
       product_id,
       max_quantity,
       fill_unit,
       fill_model,
       IF(shelf_fill_flag = 1 AND max_quantity < 2,1,0)is_abnormal_1,
       IF(fill_unit = '盒' AND max_quantity%fill_model = 1,1,0)is_abnormal_2,
       IF(fill_unit != '盒' AND ((if_bind = 0 AND max_quantity > 20) OR (if_bind = 1 AND max_quantity > 40)),1,0)is_abnormal_3,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.shelf_product_tmp
HAVING is_abnormal_1 = 1 OR is_abnormal_2 = 1 OR is_abnormal_3 = 1;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_op_shelf_product_price_dispose_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_area_product_price_monitor','dwd_op_shelf_product_price_dispose_monitor','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_shelf_product_price_monitor','dwd_op_shelf_product_price_dispose_monitor','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_shelf_product_dispose_monitor','dwd_op_shelf_product_price_dispose_monitor','朱星华');
  COMMIT;	
END