CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_shelf_product_start_fill_label`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate,1);
SET @weekend := SUBDATE(@sdate,WEEKDAY(@sdate) + 1);
SET @sub_28 := SUBDATE(@sdate,28); 
SET @one_month_date := SUBDATE(@sdate,INTERVAL 1 MONTH);
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.shelf_type,                             
       i.prewarehouse_id AS warehouse_id,                          
       a.is_monthly_balance,                      
       IFNULL(a.grade,'新装')grade,               
       s.fill_sku,
       p.package_id,
       p.package_name,
       p.package_type_name
FROM fe_dwd.dwd_shelf_base_day_all a
LEFT JOIN fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all i ON a.shelf_id = i.shelf_id
-- LEFT JOIN feods.d_op_package_shelf p ON a.shelf_id = p.shelf_id AND p.stat_date = @sdate
LEFT JOIN 
(SELECT s.shelf_id,
        s.package_id,
        a.package_name,
        b.package_type_name
FROM fe.sf_shelf_package_detail s
LEFT JOIN fe.sf_package a ON s.package_id = a.package_id AND a.data_flag = 1 AND a.statu_flag = 1
LEFT JOIN fe.sf_package_type b ON a.package_type_id = b.package_type_id AND b.data_flag = 1 AND b.statu_flag = 1
WHERE s.data_flag = 1
) p ON a.shelf_id = p.shelf_id
LEFT JOIN fe_dm.dm_op_shelf_sku_situation s ON a.shelf_id = s.shelf_id AND s.sdate = @sdate
WHERE a.shelf_type IN (1,2,3,6)  -- 四层、冰箱、五层、智能货柜              
AND a.shelf_status = 2         -- 已激活                       
AND a.revoke_status = 1        -- 正在运营
AND a.activate_time < @sdate
AND (relation_flag = 1 AND main_shelf_id IS NULL OR relation_flag = 0);       -- 非关联或者关联货架的主货架
-- 大仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_dc_stock_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_dc_stock_tmp
SELECT business_area,
       product_id,
       SUM(QUALITYQTY)stock
FROM feods.PJ_OUTSTOCK2_DAY d
JOIN fe_dwd.dwd_product_base_day_all a ON d.product_bar = a.product_code2
WHERE FPRODUCEDATE = @sub_1
AND QUALITYQTY > 0
GROUP BY business_area,product_id;
-- 前置仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_pre_stock_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_pre_stock_tmp(PRIMARY KEY (warehouse_id, product_id))
SELECT warehouse_id,
       product_id,
       IFNULL(SUM(available_stock),0)pre_stock
FROM feods.pj_prewarehouse_stock_detail
WHERE check_date = @sub_1
AND available_stock > 0
GROUP BY warehouse_id,product_id;
-- 爆畅平停补明细
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp(PRIMARY KEY (shelf_id, product_id))
SELECT a.shelf_id,
       a.product_id,
       product_type
FROM feods.d_op_fill_day_sale_qty a -- fe_dwd.dwd_shelf_product_day_all,改成使用补货组爆畅平等级
JOIN fe_dwd.dwd_shelf_product_day_all ba ON a.shelf_id = ba.shelf_id AND a.product_id = ba.product_id
JOIN fe_dwd.shelf_tmp s ON a.shelf_id = s.shelf_id
JOIN feods.zs_product_dim_sserp p ON s.business_name = p.business_area AND a.product_id = p.product_id
JOIN fe_dwd.dwd_product_base_day_all b ON a.product_id = b.product_id
LEFT JOIN fe_dwd.shelf_dc_stock_tmp st ON s.business_name = st.business_area AND p.product_id = st.product_id
LEFT JOIN fe_dwd.shelf_pre_stock_tmp pr ON s.warehouse_id = pr.warehouse_id AND a.product_id = pr.product_id
WHERE ba.shelf_fill_flag = 2
AND a.stat_date = @sub_1
AND a.fill_level IN (1,2,3) -- 销量标识(1:爆款、2:畅销、3:平销、4:滞销、5:严重滞销)(DICT)
AND (ba.danger_flag < 4 OR ba.danger_flag IS NULL)  -- 剔除风险4、5
AND (st.stock IS NOT NULL OR pr.pre_stock IS NOT NULL) -- 剔除大仓&前置仓库存均为0
AND (s.shelf_type IN (1,3,6) OR (s.shelf_type = 2 AND b.second_type_name IN ('饮料','奶制品')))
AND (product_type IN ('原有','新增（试运行）') OR (product_type NOT IN ('原有','新增（试运行）') AND (s.warehouse_id IS NOT NULL AND pre_stock > 10 AND stock > 100 OR (s.warehouse_id IS NULL AND stock > 50))));
-- 可补sku不足的普通货架、智能货柜关联地区正常运营品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.normal_shelf_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.normal_shelf_product_tmp(PRIMARY KEY (shelf_id, product_id))
SELECT s.shelf_id,
       p.product_id,
       p.product_type
FROM fe_dwd.shelf_tmp s
JOIN feods.zs_product_dim_sserp p ON s.business_name = p.business_area AND p.product_type IN ('原有','新增（试运行）')
WHERE (s.shelf_type IN (1,3) AND s.fill_sku < 25) OR (s.shelf_type = 6 AND s.fill_sku < 15);
-- 可补sku不足的冰箱关联地区正常运营品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.normal_fridge_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.normal_fridge_product_tmp(PRIMARY KEY (shelf_id, product_id))
SELECT s.shelf_id,
       p.product_id,
       p.product_type
FROM fe_dwd.shelf_tmp s
JOIN feods.zs_product_dim_sserp p ON s.business_name = p.business_area AND p.product_type IN ('原有','新增（试运行）')
JOIN fe_dwd.dwd_product_base_day_all a ON p.product_id = a.product_id
WHERE s.shelf_type = 2 AND s.fill_sku < 10
AND a.second_type_name IN ('饮料','奶制品');
-- 货架未有的地区正常运营品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.normal_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.normal_product_tmp(PRIMARY KEY (shelf_id, product_id))
SELECT a.shelf_id,
       a.product_id,
       product_type,
       b.shelf_fill_flag
FROM
(
SELECT shelf_id,
       product_id,
       product_type
FROM fe_dwd.normal_shelf_product_tmp
UNION
SELECT shelf_id,
       product_id,
       product_type
FROM fe_dwd.normal_fridge_product_tmp
)a
LEFT JOIN fe_dwd.dwd_shelf_product_day_all b ON a.shelf_id = b.shelf_id AND a.product_id = b.product_id
WHERE b.product_id IS NULL OR (b.shelf_fill_flag = 2 AND (b.danger_flag < 4 OR b.danger_flag IS NULL));
-- 总清单(地区正常运营品+停补爆畅平)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.product_tmp;
CREATE TEMPORARY TABLE fe_dwd.product_tmp(PRIMARY KEY (shelf_id, product_id))
SELECT a.shelf_id,
       a.product_id,
       b.is_common_product,
       product_type,
       IFNULL(st.stock,0)stock,
       IFNULL(pr.pre_stock,0)pre_stock,
       IF(s.warehouse_id IS NOT NULL AND pre_stock > 0 OR (s.warehouse_id IS NULL AND stock > 0),1,0)if_stock,
       SUM(in_shelf)in_shelf
FROM
(
SELECT shelf_id,
       product_id,
       product_type,
       IF(shelf_fill_flag IS NULL,0,1) in_shelf
FROM fe_dwd.normal_product_tmp
UNION
SELECT shelf_id,
       product_id,
       product_type,
       1 AS in_shelf
FROM  fe_dwd.shelf_product_tmp
)a
JOIN fe_dwd.dwd_product_base_day_all b ON a.product_id = b.product_id
JOIN fe_dwd.shelf_tmp s ON a.shelf_id = s.shelf_id
LEFT JOIN fe_dwd.shelf_dc_stock_tmp st ON s.business_name = st.business_area AND a.product_id = st.product_id
LEFT JOIN fe_dwd.shelf_pre_stock_tmp pr ON s.warehouse_id = pr.warehouse_id AND a.product_id = pr.product_id
LEFT JOIN fe_dwd.dwd_op_product_type_blacklist_insert tb ON a.shelf_id = tb.shelf_id AND b.second_type_name = tb.second_type_name -- 此表还未建,二级品类黑名单
LEFT JOIN fe_dwd.dwd_op_product_type_blacklist_insert tb2 ON a.shelf_id = tb2.shelf_id AND b.sub_type_name = tb2.sub_type_name -- 此表还未建,三级品类黑名单
LEFT JOIN fe_dwd.dwd_op_shelf_product_blacklist_insert pb ON a.shelf_id = pb.shelf_id AND a.product_id = pb.product_id -- 此表还未建,货架商品黑名单
WHERE tb.second_type_name IS NULL 
AND tb2.sub_type_name IS NULL 
AND pb.product_id IS NULL
GROUP BY a.shelf_id,a.product_id;
-- 货架商品近30天销售
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_sale_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_sale_tmp (PRIMARY KEY (shelf_id, product_id))
SELECT s.business_name,
       i.shelf_id,
       i.product_id,
       d.product_type,
       i.days_sal_sto30,
       i.qty_sal30 qty_sal30,
       i.qty_sal30 * a.sale_price gmv_30,
       i.qty_sal30 / i.days_sal_sto30 30_avg_qty
FROM feods.d_op_sp_avgsal30 i
JOIN fe_dwd.shelf_tmp s ON i.shelf_id = s.shelf_id
LEFT JOIN fe_dwd.dwd_shelf_product_day_all a ON i.shelf_id = a.shelf_id AND i.product_id = a.product_id
LEFT JOIN feods.zs_product_dim_sserp d ON s.business_name = d.business_area AND i.product_id = d.product_id AND d.product_type IN ('原有','新增（试运行）');
-- 地区单品近30天日架均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.product_avg_sale_tmp;
CREATE TEMPORARY TABLE fe_dwd.product_avg_sale_tmp (PRIMARY KEY (business_name, product_id))
SELECT business_name,
       product_id,
       AVG(gmv_30) avg_30_gmv
FROM fe_dwd.shelf_product_sale_tmp
WHERE product_type IN ('原有','新增（试运行）')
GROUP BY business_name,product_id;
-- 地区近30天日均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_sale_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_sale_tmp (PRIMARY KEY (business_name))
SELECT business_name,
       AVG(gmv_30)area_avg_gmv
FROM fe_dwd.shelf_product_sale_tmp
WHERE product_type IN ('原有','新增（试运行）')
GROUP BY business_name;
-- 地区近30天总销量\gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_sale_total_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_sale_total_tmp (PRIMARY KEY (business_name))
SELECT business_name,
       SUM(qty_sal30)area_amount,
       SUM(gmv_30)area_gmv
FROM fe_dwd.shelf_product_sale_tmp
GROUP BY business_name;
-- 地区商品top20(单sku销量/地区总销量*0.4 + 单skugmv/地区总gmv*0.6后降序排名)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_top_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_top_product_tmp (PRIMARY KEY (business_name, product_id))
SELECT business_name,
       product_id,
       rank
FROM
(
SELECT business_name,
       product_id,
       devote,
       @rank := IF(@city = business_name ,@rank + 1,1) rank,
       @city := business_name
FROM
(
SELECT m.business_name,
       m.product_id,
       (SUM(m.qty_sal30) / b.area_amount * 0.4)+ (SUM(m.gmv_30) / b.area_gmv *0.6) devote
FROM fe_dwd.shelf_product_sale_tmp m
LEFT JOIN fe_dwd.area_sale_total_tmp b ON m.business_name = b.business_name
GROUP BY m.business_name,m.product_id
ORDER BY m.business_name,devote DESC
)a,(SELECT @rank := 0,@city := NULL)r
)b
WHERE b.rank <= 20;
-- 地区水饮top10(单sku销量/地区总销量*0.4 + 单skugmv/地区总gmv*0.6后降序排名)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_top_drink_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_top_drink_tmp (PRIMARY KEY (business_name, product_id))
SELECT business_name,
       product_id,
       rank
FROM
(
SELECT business_name,
       product_id,
       second_type_name,
       devote,
       @rank := IF(@city = business_name ,@rank + 1,1) rank,
       @city := business_name
FROM
(
SELECT m.business_name,
       m.product_id,
       a.second_type_name,
       (SUM(m.qty_sal30) / b.area_amount * 0.4)+ (SUM(m.gmv_30) / b.area_gmv *0.6) devote
FROM fe_dwd.shelf_product_sale_tmp m
LEFT JOIN fe_dwd.area_sale_total_tmp b ON m.business_name = b.business_name
JOIN fe_dwd.dwd_product_base_day_all a ON m.product_id = a.product_id
WHERE a.second_type_name IN ('饮料','奶制品')
GROUP BY m.business_name,m.product_id
ORDER BY m.business_name,devote DESC
)a,(SELECT @rank := 0,@city := NULL)r
)b
WHERE b.rank <= 10;
-- 上架超过28天且商品等级为好卖及以上的商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_good_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_good_product_tmp (PRIMARY KEY (business_area, product_id))
SELECT business_area,
       product_id,
       first_fill_time,
       product_sale_level,
       IF(first_fill_time < @sub_28 AND product_sale_level IN ('好卖','热卖','非常好卖'),1,0)is_good
FROM feods.fjr_product_list_manager_week
WHERE week_end = @weekend;
-- 指定货架商品 9s
DELETE FROM fe_dm.dm_op_shelf_product_start_fill_label WHERE stat_date=@sdate OR stat_date<@one_month_date;
INSERT INTO fe_dm.dm_op_shelf_product_start_fill_label
(stat_date
,business_name
,shelf_id
,grade
,fill_sku
,package_id
,package_name
,package_type_name
,product_id
,is_common_product
,days_sal_sto30
,product_type
,qty_sal30
,gmv_30
,30_avg_qty
,first_fill_time
,shelf_fill_flag
,sales_flag
,stock
,pre_stock
,is_top20
,is_top10
,is_good
,label  
)
SELECT @sdate stat_date,
       s.business_name,
       a.shelf_id,
       s.grade,
       s.fill_sku,
       s.package_id,
       s.package_name,
       s.package_type_name,
       a.product_id,
       a.is_common_product,
       sa.days_sal_sto30,
       a.product_type,
       sa.qty_sal30,
       sa.gmv_30,
       sa.30_avg_qty,
       g.first_fill_time,                    -- 地区首次上架时间
       p.shelf_fill_flag,
       p.sales_flag,
       a.stock,
       a.pre_stock,
       IF(top1.product_id IS NULL,0,1)is_top20,
       IF(top2.product_id IS NULL,0,1)is_top10,
       g.is_good,                            -- 上架超过28天且商品等级为好卖及以上
       CASE WHEN a.product_type IN('原有','新增（试运行）') AND sales_flag IN (1,2,3) THEN 1
            WHEN a.product_type NOT IN ('原有','新增（试运行）') AND sales_flag IN (1,2,3) THEN 2
            WHEN shelf_type IN (1,2,3) AND grade IN('丙','丁') AND a.is_common_product = 1 AND if_stock = 1 THEN 3
            WHEN shelf_type IN (1,2,3) AND grade IN ('甲','乙') AND in_shelf = 0 AND if_stock = 1 THEN 4
            WHEN shelf_type IN (1,2,3) AND grade IN('丙','丁') AND in_shelf = 0 AND g.is_good = 1 AND if_stock = 1 THEN 5
            WHEN shelf_type = 6 AND in_shelf = 0 AND a.is_common_product = 1 AND if_stock = 1 THEN 6
            END AS label       
FROM fe_dwd.product_tmp a
JOIN fe_dwd.shelf_tmp s ON a.shelf_id = s.shelf_id
LEFT JOIN fe_dwd.dwd_shelf_product_day_all p ON a.shelf_id = p.shelf_id AND a.product_id = p.product_id
LEFT JOIN fe_dwd.shelf_product_sale_tmp sa ON a.shelf_id = sa.shelf_id AND a.product_id = sa.product_id
LEFT JOIN fe_dwd.product_avg_sale_tmp pa ON s.business_name = pa.business_name AND a.product_id = pa.product_id
LEFT JOIN fe_dwd.area_sale_tmp av ON s.business_name = av.business_name
LEFT JOIN fe_dwd.area_good_product_tmp g ON s.business_name = g.business_area AND a.product_id = g.product_id
LEFT JOIN fe_dwd.area_top_product_tmp top1 ON s.business_name = top1.business_name AND a.product_id = top1.product_id
LEFT JOIN fe_dwd.area_top_drink_tmp top2 ON s.business_name = top2.business_name AND a.product_id = top2.product_id;
UPDATE fe_dm.dm_op_shelf_product_start_fill_label t1
LEFT JOIN 
(SELECT a.shelf_id,
        a.product_id,
        CASE WHEN a.label IN(1,3,4,5) THEN 1
             WHEN (a.label = 2 AND b.shelf_type = 6) OR label = 6 THEN 0
             WHEN a.label = 2 THEN 1
        END AS should_add
FROM fe_dm.dm_op_shelf_product_start_fill_label a
JOIN fe_dwd.dwd_shelf_base_day_all b ON a.shelf_id = b.shelf_id WHERE a.stat_date=CURRENT_DATE
)t2 ON t2.shelf_id = t1.shelf_id AND t1.product_id = t2.product_id
SET t1.should_add= t2.should_add
WHERE t1.stat_date=CURRENT_DATE;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_product_start_fill_label',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('李世龙（朱星华）@', @user, @timestamp));
COMMIT;
	END