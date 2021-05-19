CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_shelf_product_fill_suggest_label`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate,1);
SET @one_month_date := SUBDATE(@sdate,INTERVAL 1 MONTH);
-- 货架可补货sku
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_sku_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_sku_tmp (PRIMARY KEY(shelf_id))
SELECT shelf_id,
       COUNT(*)fill_sku
FROM fe_dwd.dwd_shelf_product_day_all
WHERE shelf_fill_flag = 1
GROUP BY shelf_id;
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY(shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.shelf_type,
       a.type_name,
       a.relation_flag AS rel_flag,                                -- 是否关联货架
       IF(i.prewarehouse_id IS NULL,'0','1') prewh_falg,           -- 是否前置仓覆盖
       i.prewarehouse_id AS warehouse_id,                          -- 绑定前置仓id
       i.prewarehouse_name AS warehouse_name,                      -- 绑定前置仓名称
       a.is_monthly_balance,                      -- 是否月结用户
       IFNULL(a.grade,'新装')grade,               -- 上月货架等级
       p.fill_sku                                 -- 货架可补货sku数
FROM fe_dwd.dwd_shelf_base_day_all a
LEFT JOIN fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all i ON a.shelf_id = i.shelf_id
-- LEFT JOIN feods.d_op_shelf_info i ON a.shelf_id = i.shelf_id
-- LEFT JOIN feods.d_op_package_shelf p ON a.shelf_id = p.shelf_id AND p.stat_date = @sdate
LEFT JOIN fe_dwd.shelf_sku_tmp p ON a.shelf_id = p.shelf_id
WHERE a.shelf_type IN (1,2,3,6)                   -- 四层\冰箱\五层\智能货柜
AND a.shelf_status = 2                            -- 已激活
AND a.revoke_status = 1;                          -- 正在运营
-- 货架可补货商品明细
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp (PRIMARY KEY(shelf_id,product_id))
SELECT t.business_name,
       d.shelf_id,
       t.shelf_type,
       t.type_name,
       t.rel_flag,
       t.prewh_falg,
       t.warehouse_id,
       t.warehouse_name,
       t.is_monthly_balance,
       t.grade,
       t.fill_sku,
       d.product_id,
       p.is_common_product,
       d.first_fill_time,
       d.shelf_fill_flag,
       IFNULL(a.qty_sal30,0)qty_sal30,
       IFNULL(a.days_sal_sto30,0)days_sal_sto30,
       z.product_type,
       IFNULL(a.qty_sal30 * d.sale_price,0) gmv_30,
       IFNULL(s.stock,0)stock,
       IFNULL(pr.pre_stock,0)pre_stock,
       d.sales_flag,
       IF((DATEDIFF(@sdate,d.first_fill_time) + 1) < 30 OR d.first_fill_time IS NULL,'否','是') fill_over30day  -- 上架是否超过30天
FROM fe_dwd.dwd_shelf_product_day_all d
JOIN fe_dwd.shelf_tmp t ON d.shelf_id = t.shelf_id
JOIN fe_dwd.dwd_product_base_day_all p ON d.product_id = p.product_id
LEFT JOIN feods.d_op_sp_avgsal30 a ON d.shelf_id = a.shelf_id AND d.product_id = a.product_id
LEFT JOIN feods.zs_product_dim_sserp z ON t.business_name = z.business_area AND d.product_id = z.product_id
-- LEFT JOIN -- 大仓库存
-- (SELECT business_area,
--         sku_no,
--         storage_amount stock
-- FROM fe_dwd.dwd_sc_bdp_warehouse_stock_daily d
-- JOIN fe_dwd.dwd_pub_warehouse_business_area w ON d.warehouse = w.warehouse_number AND w.data_flag = 1
-- WHERE sdate = @sub_1
-- )s ON t.business_name = s.business_area AND p.product_code2 = s.sku_no
LEFT JOIN-- 大仓库存
(SELECT business_area,
        product_bar,
        SUM(QUALITYQTY)stock
FROM feods.PJ_OUTSTOCK2_DAY
WHERE FPRODUCEDATE = @sub_1
AND QUALITYQTY > 0
GROUP BY business_area,product_bar
)s ON t.business_name = s.business_area AND p.product_code2 = s.product_bar
LEFT JOIN -- 前置仓库存
(SELECT warehouse_id,
        product_id,
        IFNULL(SUM(available_stock),0)pre_stock
FROM feods.pj_prewarehouse_stock_detail
WHERE check_date = @sub_1
AND available_stock > 0
GROUP BY warehouse_id,product_id
)pr ON t.warehouse_id = pr.warehouse_id AND p.product_id = pr.product_id
WHERE d.shelf_fill_flag = 1;
-- 货架可补货商品标签1-5
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_label_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_label_tmp (PRIMARY KEY(shelf_id,product_id))
SELECT business_name,
       shelf_id,
       shelf_type,
       type_name,
       rel_flag,
       prewh_falg,
       warehouse_id,
       warehouse_name,
       is_monthly_balance,
       grade,
       fill_sku,
       product_id,
       is_common_product,
       first_fill_time,
       shelf_fill_flag,
       gmv_30,
       qty_sal30,
       days_sal_sto30,
       IFNULL(qty_sal30 / days_sal_sto30,0)30_avg_qty,
       product_type,
       stock,
       pre_stock,
       sales_flag,
       fill_over30day,
       CASE WHEN product_type IN ('淘汰（替补）','退出','预淘汰') AND stock = 0 AND pre_stock = 0 THEN 1
            WHEN is_monthly_balance = 0 AND fill_over30day = '是' AND days_sal_sto30 >= 20 AND qty_sal30 = 0 AND sales_flag IN (4,5) THEN 2
            WHEN is_monthly_balance = 0 AND fill_over30day = '否' AND product_type IN ('原有','新增（试运行）') THEN 3
            WHEN is_monthly_balance = 0 AND product_type IN ('原有','新增（试运行）') AND sales_flag IN (1,2) THEN 4
            WHEN is_monthly_balance = 0 AND product_type IN ('原有','新增（试运行）') AND is_common_product = 1 THEN 5 END AS label_1
FROM fe_dwd.shelf_product_tmp;
-- 无人货架(55-需保留sku),静态柜(22-需保留sku),动态柜(30-需保留sku)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_keep_sku_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_keep_sku_tmp (PRIMARY KEY(shelf_id))
SELECT shelf_id,
       -- IF(shelf_type = 6,30-COUNT(*),55-COUNT(*)) out_rank
       IF(type_name LIKE '%静态',22-COUNT(*),IF(type_name LIKE '%动态',30-COUNT(*),55-COUNT(*)))out_rank
FROM fe_dwd.shelf_product_label_tmp
WHERE label_1 IN (3,4,5)
GROUP BY shelf_id;
-- 货架未有标签商品近30天日均销排名
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_avg_qty_rank_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_avg_qty_rank_tmp (PRIMARY KEY(shelf_id,product_id))
SELECT shelf_id,
       product_id,
       30_avg_qty,
       @rank := IF(@shelf_id = shelf_id,@rank + 1,1) qty_rank,
       @shelf_id := shelf_id
FROM
(
SELECT shelf_id,
       product_id,
       30_avg_qty
FROM fe_dwd.shelf_product_label_tmp
WHERE grade != '新装'
AND (shelf_type IN (1,2,3) AND fill_sku > 55 OR (type_name LIKE'%静态柜' AND fill_sku > 22 OR (type_name LIKE'%动态柜' AND fill_sku > 30)))
AND is_monthly_balance = 0
AND rel_flag = 0    -- rel_flag = '否'
AND label_1 IS NULL
ORDER BY shelf_id,30_avg_qty DESC
)a,(SELECT @rank := 0,@shelf_id := NULL)r;
-- 货架未有标签商品近30天gmv排名
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_30gmv_rank_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_30gmv_rank_tmp (PRIMARY KEY(shelf_id,product_id))
SELECT shelf_id,
       product_id,
       gmv_30,
       @rank := IF(@shelf_id = shelf_id,@rank + 1,1)gmv_rank,
       @shelf_id := shelf_id
FROM
(
SELECT shelf_id,
       product_id,
       gmv_30
FROM fe_dwd.shelf_product_label_tmp
WHERE grade != '新装'
AND (shelf_type IN (1,2,3) AND fill_sku > 55 OR (type_name LIKE'%静态柜' AND fill_sku > 22 OR (type_name LIKE'%动态柜' AND fill_sku > 30)))
AND is_monthly_balance = 0
AND rel_flag = 0    -- rel_flag = '否'
AND label_1 IS NULL
ORDER BY shelf_id,gmv_30 DESC
)a,(SELECT @rank := 0,@shelf_id := NULL)r;
-- 货架未有标签商品近30天日均销及gmv综合排名
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_final_rank_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_final_rank_tmp (PRIMARY KEY(shelf_id,product_id))
SELECT b.shelf_id,
       product_id,
       30_avg_qty,
       gmv_30,
       qty_rank,
       gmv_rank,
       final_rank,
       k.out_rank
FROM
(
SELECT a.shelf_id,
       a.product_id,
       SUM(30_avg_qty)30_avg_qty,
       SUM(gmv_30)gmv_30,
       SUM(qty_rank)qty_rank,
       SUM(gmv_rank)gmv_rank,
       SUM(qty_rank) * 0.4 + SUM(gmv_rank) *0.6 final_rank
FROM
(
SELECT shelf_id,
       product_id,
       30_avg_qty,
       0 AS gmv_30,
       qty_rank,
       0 AS gmv_rank
FROM fe_dwd.shelf_avg_qty_rank_tmp
UNION ALL
SELECT shelf_id,
       product_id,
       0 AS 30_avg_qty,
       gmv_30,
       0 AS qty_rank,
       gmv_rank
FROM fe_dwd.shelf_30gmv_rank_tmp
)a
GROUP BY a.shelf_id,a.product_id
ORDER BY a.shelf_id,final_rank ASC
)b
LEFT JOIN fe_dwd.shelf_keep_sku_tmp k ON b.shelf_id = k.shelf_id
WHERE b.final_rank > k.out_rank;
-- 货架最终停补或保留的标签明细
DELETE FROM  fe_dm.dm_op_shelf_product_fill_suggest_label WHERE stat_date=@sdate OR stat_date<@one_month_date;
INSERT INTO fe_dm.dm_op_shelf_product_fill_suggest_label
(stat_date
,business_name
,shelf_id
,shelf_type
,rel_flag
,prewh_falg
,warehouse_id
,warehouse_name
,is_monthly_balance
,grade
,fill_sku
,product_id
,is_common_product
,first_fill_time
,shelf_fill_flag
,gmv_30
,qty_sal30
,days_sal_sto30
,30_avg_qty
,product_type
,stock
,pre_stock
,sales_flag
,fill_over30day
,label
)
SELECT @sdate stat_date,
       business_name,
       a.shelf_id,
       shelf_type,
       rel_flag,
       prewh_falg,
       warehouse_id,
       warehouse_name,
       is_monthly_balance,
       grade,
       fill_sku,
       a.product_id,
       is_common_product,
       first_fill_time,
       shelf_fill_flag,
       a.gmv_30,
       qty_sal30,
       days_sal_sto30,
       a.30_avg_qty,
       product_type,
       stock,
       pre_stock,
       sales_flag,
       fill_over30day,
       IF(a.label_1 IS NULL AND b.product_id IS NOT NULL,6,a.label_1)label
FROM fe_dwd.shelf_product_label_tmp a
LEFT JOIN fe_dwd.shelf_final_rank_tmp b ON a.shelf_id = b.shelf_id AND a.product_id = b.product_id
HAVING !ISNULL(label);
UPDATE fe_dm.dm_op_shelf_product_fill_suggest_label t1
LEFT JOIN 
(SELECT a.shelf_id,
        a.product_id,
        CASE WHEN a.label IN(1,6) OR (a.label = 2 AND (b.grade IN('甲','乙','新装') OR b.grade_cur_month = '新装')) THEN 1
        ELSE 0 END AS should_add
FROM fe_dm.dm_op_shelf_product_fill_suggest_label a
JOIN fe_dwd.dwd_shelf_base_day_all b ON a.shelf_id = b.shelf_id WHERE a.stat_date=CURRENT_DATE
)t2 ON t2.shelf_id = t1.shelf_id AND t1.product_id = t2.product_id
SET t1.should_add= t2.should_add
WHERE t1.stat_date=CURRENT_DATE;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_product_fill_suggest_label',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('李世龙（朱星华）@', @user, @timestamp));
COMMIT;
	END