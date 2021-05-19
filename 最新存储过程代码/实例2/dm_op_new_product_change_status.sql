CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_new_product_change_status`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SELECT @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE) + 1),
       @add_1 := ADDDATE(@week_end,1),
       @sub4_week := SUBDATE(@week_end,21),-- 前四周
       @sub6_week := SUBDATE(@week_end,35);-- 前6周
-- 商品引入时间
DROP TEMPORARY TABLE IF EXISTS fe_dm.indate_tmp;
CREATE TEMPORARY TABLE fe_dm.indate_tmp (PRIMARY KEY (business_area,product_id))
SELECT business_area,
       product_id,
       indate_np
FROM fe_dwd.dwd_pub_product_dim_sserp;   -- zs_product_dim_sserp  
-- 近4周商品等级局部好卖及以上的次数
DROP TEMPORARY TABLE IF EXISTS fe_dm.4sale_flag_tmp;
CREATE TEMPORARY TABLE fe_dm.4sale_flag_tmp (PRIMARY KEY (business_area,product_id))
SELECT business_area,
       product_id,
       COUNT(week_end)good_amount
FROM fe_dm.dm_op_product_list_manager_week  -- fjr_product_list_manager_week
WHERE week_end >= @sub4_week
AND week_end <= @week_end
AND gmv_sale_level IN ('局部好卖','好卖','非常好卖','热卖','一般')
GROUP BY business_area,product_id;
-- 近6周为非常不好卖或难卖的次数
DROP TEMPORARY TABLE IF EXISTS fe_dm.6sale_flag_tmp;
CREATE TEMPORARY TABLE fe_dm.6sale_flag_tmp (PRIMARY KEY (business_area,product_id))
SELECT business_area,
       product_id,
       COUNT(week_end)bad_amount
FROM fe_dm.dm_op_product_list_manager_week   -- fjr_product_list_manager_week
WHERE gmv_sale_level IN ('非常不好卖','难卖')
AND week_end >= @sub6_week
AND week_end <= @week_end
GROUP BY business_area,product_id;
-- 统计周商品清单
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_product_tmp;
CREATE TEMPORARY TABLE fe_dm.area_product_tmp (PRIMARY KEY (business_area,product_id))
SELECT week_end,
       region_name,
       w.business_area,
       w.product_id,
       w.second_type_name,
       w.sub_type_name,
       product_type,
       shelfs_stock,
       gmv30,
       gmv_sale_level,
       first_fill_time,
       i.indate_np,
       CASE WHEN DATEDIFF(@add_1,first_fill_time) > 120 THEN DATEDIFF(@add_1,IF(i.indate_np IS NULL OR i.indate_np = '0000-00-00 00:00:00',first_fill_time,i.indate_np))-- 判断引入是否> 2个月,当前时间-首次上架时间>120天，则使用当前时间-商品引入时间
       ELSE DATEDIFF(@add_1,first_fill_time) END AS in_date
FROM fe_dm.dm_op_product_list_manager_week w  -- fjr_product_list_manager_week 
LEFT JOIN fe_dm.indate_tmp i ON w.business_area = i.business_area AND w.product_id = i.product_id
WHERE week_end = @week_end;
-- 地区原有品单sku近30天平均GMV
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_avg_tmp;
CREATE TEMPORARY TABLE fe_dm.area_avg_tmp (PRIMARY KEY (business_area))
SELECT business_area,
       AVG(gmv30)avg_30,
       AVG(shelfs_stock)avg_sto_shelf -- 地区原有品平均有库存货架数
FROM fe_dm.area_product_tmp
WHERE product_type = '原有'
GROUP BY business_area;
-- 地区三级品类原有品近30天平均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_subtype_avg_tmp;
CREATE TEMPORARY TABLE fe_dm.area_subtype_avg_tmp (PRIMARY KEY (business_area,sub_type_name))
SELECT business_area,
       sub_type_name,
       AVG(gmv30)sub_avg_30
FROM fe_dm.area_product_tmp
WHERE product_type = '原有'
GROUP BY business_area,sub_type_name;
-- 地区正常运营品三级品类gmv排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.normal_product_tmp;
CREATE TEMPORARY TABLE fe_dm.normal_product_tmp (PRIMARY KEY (business_area,product_id))
SELECT b.week_end,
       b.region_name,
       b.business_area,
       b.product_id,
       b.product_type,
       b.second_type_name,
       b.sub_type_name,
       b.shelfs_stock,
       b.gmv30,
       b.gmv_sale_level,
       b.first_fill_time,
       b.indate_np,
       b.is_over_60,
       b.good_amount,
       b.bad_amount,
       b.avg_30,
       b.avg_sto_shelf,
       b.sub_avg_30,
       b.predict_gmv,
       b.predict_gmv2,
       @rank := IF((@city = business_area AND @type_name = b.sub_type_name),@rank + 1,1) sub_type_rank,
       @city := business_area,
       @type_name := sub_type_name
FROM
(
SELECT a.week_end,
       a.region_name,
       a.business_area,
       a.product_id,
       a.product_type,
       a.second_type_name,
       a.sub_type_name,
       a.shelfs_stock,
       a.gmv30,
       a.gmv_sale_level,
       a.first_fill_time,
       a.indate_np,
       IF(a.in_date > 60,1,0)is_over_60,
       IFNULL(s4.good_amount,0)good_amount,
       IFNULL(s6.bad_amount,0)bad_amount,
       a1.avg_30,-- 地区原有品近30天单sku平均GMV
       a1.avg_sto_shelf,-- 地区原有品平均有库存货架数
       sa.sub_avg_30,-- 地区该品类原有品近30天单sku平均GMV
       gmv30 * (avg_sto_shelf / shelfs_stock)*1.15 predict_gmv, -- 近30天GMV *(地区原有品平均有库存货架数/该商品有库存货架数)*1.15
       gmv30 * (avg_sto_shelf / shelfs_stock) predict_gmv2      -- 进30天GMV * (地区原有品平均有库存货架数/该商品有库存货架数)
FROM fe_dm.area_product_tmp a
LEFT JOIN fe_dm.4sale_flag_tmp s4  ON a.business_area = s4.business_area AND a.product_id = s4.product_id
LEFT JOIN fe_dm.6sale_flag_tmp s6 ON a.business_area = s6.business_area AND a.product_id = s6.product_id
LEFT JOIN fe_dm.area_avg_tmp a1 ON a.business_area = a1.business_area
LEFT JOIN fe_dm.area_subtype_avg_tmp sa ON a.business_area = sa.business_area AND a.sub_type_name = sa.sub_type_name
WHERE a.product_type IN ('原有','新增（试运行）')
ORDER BY business_area,a.sub_type_name,predict_gmv DESC
)b,(SELECT @rank := 0,@city := NULL,@type_name := NULL)r;
-- 地区三级品类2/3后的排名及sku
DROP TEMPORARY TABLE IF EXISTS fe_dm.rank_tmp;
CREATE TEMPORARY TABLE fe_dm.rank_tmp (PRIMARY KEY (business_area,sub_type_name))
SELECT business_area,
       sub_type_name,
       COUNT(*)sku,
       MAX(sub_type_rank)*(2/3) out_rank
FROM fe_dm.normal_product_tmp
GROUP BY business_area,sub_type_name;
-- 先标签1-4
DROP TEMPORARY TABLE IF EXISTS fe_dm.label_tmp1;
CREATE TEMPORARY TABLE fe_dm.label_tmp1 AS
SELECT a.region_name,
       a.business_area,
       a.product_id,
       a.product_type,
       a.shelfs_stock,
       a.second_type_name,
       a.sub_type_name,
       a.gmv30,
       a.gmv_sale_level,
       a.first_fill_time,
       a.indate_np,
       a.is_over_60,
       a.good_amount,
       a.bad_amount,
       a.avg_30,
       a.avg_sto_shelf,
       a.sub_avg_30,
       a.predict_gmv,
       a.predict_gmv2,
       a.sub_type_rank,
       r.out_rank,
       CASE WHEN a.product_type = '原有' THEN '原有品'
            WHEN a.is_over_60 = 0 AND a.product_type = '新增（试运行）' THEN '上架未满60天'
            WHEN a.is_over_60 = 1 AND a.product_type = '新增（试运行）' AND r.sku < b.sku_min THEN 1 -- 当地区正常运营sku低于地区三级品类下限时取标签1
            WHEN a.is_over_60 = 1 AND a.product_type = '新增（试运行）' AND a.gmv30 >= a.avg_30 THEN 2
            WHEN a.is_over_60 = 1 AND a.product_type = '新增（试运行）' AND a.good_amount = 4 AND a.predict_gmv >= a.sub_avg_30 THEN 3
            WHEN a.is_over_60 = 1 AND a.product_type = '新增（试运行）' AND a.bad_amount < 6 AND a.sub_type_rank < r.out_rank THEN 4
       ELSE '0' END AS labe1_1
FROM fe_dm.normal_product_tmp a
LEFT JOIN fe_dm.dm_op_area_product_type_sku_limit_insert b ON a.business_area = b.business_area AND a.second_type_name = b.second_type_name AND a.sub_type_name = b.sub_type_name -- 此表待建
LEFT JOIN fe_dm.rank_tmp r ON a.business_area = r.business_area AND a.sub_type_name = r.sub_type_name;
-- 排除标签1-4后的地区正常运营品gmv排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.normal_product_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.normal_product_rank_tmp (PRIMARY KEY (business_area,product_id))
SELECT region_name,
       business_area,
       product_id,
       is_over_60,
       bad_amount,
       product_type,
       avg_30,
       predict_gmv,
       predict_gmv2,
       @rank := IF(@city = business_area ,@rank + 1,1) rank,
       @city := business_area
       
FROM
(
SELECT region_name,
       business_area,
       product_id,
       is_over_60,
       bad_amount,
       product_type,
       avg_30,
       predict_gmv,
       predict_gmv2
FROM fe_dm.label_tmp1
WHERE product_type IN ('原有','新增（试运行）')
AND labe1_1 IN( '0','原有品')
ORDER BY business_area,predict_gmv2 DESC
)a,(SELECT @rank := 0,@city := NULL)r;
-- 地区9/10后的排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.area_rank_tmp AS
SELECT business_area,MAX(rank)*(9/10) out_rank
FROM fe_dm.normal_product_rank_tmp
GROUP BY business_area;
delete from fe_dm.dm_op_new_product_change_status where week_end = @week_end;
INSERT INTO fe_dm.dm_op_new_product_change_status
(
week_end,
business_area,
product_id,
product_type,
shelfs_stock,
gmv30,
gmv_sale_level,
first_fill_time,
indate_np,
is_over_60,
good_amount,
bad_amount,
avg_30,
avg_sto_shelf,
sub_avg_30,
sub_type_rank,
rank,
change_label
)
SELECT @week_end week_end,
       a.business_area,
       a.product_id,
       a.product_type,
       a.shelfs_stock,
       a.gmv30,
       a.gmv_sale_level,
       a.first_fill_time,
       a.indate_np,
       a.is_over_60,
       a.good_amount,
       a.bad_amount,
       a.avg_30,
       a.avg_sto_shelf,
       a.sub_avg_30,
       a.sub_type_rank,
       r2.rank,
       CASE WHEN a.labe1_1 != '0'  THEN a.labe1_1
            WHEN a.is_over_60 = 1 AND a.product_type = '新增（试运行）' AND r2.rank < ar.out_rank THEN 5
       ELSE 6 END AS change_label
FROM fe_dm.label_tmp1 a
LEFT JOIN fe_dm.normal_product_rank_tmp r2 ON a.business_area = r2.business_area AND a.product_id = r2.product_id -- 正常运营品gmv排名
LEFT JOIN fe_dm.rank_tmp r ON a.business_area = r.business_area AND a.sub_type_name = r.sub_type_name
LEFT JOIN fe_dm.area_rank_tmp ar ON a.business_area = ar.business_area;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_new_product_change_status',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_new_product_change_status','dm_op_new_product_change_status','朱星华');
  COMMIT;	
END