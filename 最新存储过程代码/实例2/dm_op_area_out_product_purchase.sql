CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_out_product_purchase`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SELECT @sdate := CURRENT_DATE,
       @sub_1 := SUBDATE(@sdate,1),
       @week_end := SUBDATE(@sdate,DAYOFWEEK(@sdate) - 1);
-- 淘汰品
DROP TEMPORARY TABLE IF EXISTS fe_dm.out_product_tmp;
CREATE TEMPORARY TABLE fe_dm.out_product_tmp (PRIMARY KEY (business_area,product_id))
SELECT a.business_area,
       a.product_id,
       a.product_fe,
       a.product_type,
       f.sale_level,
       a.out_date
FROM fe_dwd.dwd_pub_product_dim_sserp a  -- zs_product_dim_sserp
LEFT JOIN fe_dm.dm_area_product_sale_flag f ON a.business_area = f.business_area AND a.product_id = f.product_id AND f.sdate = ADDDATE(@week_end,1)  -- zs_area_product_sale_flag
WHERE product_type IN('淘汰（替补）','退出','预淘汰')
AND out_date IS NOT NULL;
-- 大仓及前置仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_tmp;
CREATE TEMPORARY TABLE fe_dm.stock_tmp (PRIMARY KEY (business_area,product_id))
SELECT business_area,
       product_id,
       SUM(stock)stock,
       SUM(pre_stock)pre_stock
FROM
(
SELECT business_area,
       product_id,
       SUM(QUALITYQTY)stock,
       0 AS pre_stock
FROM fe_dwd.dwd_pj_outstock2_day d  -- PJ_OUTSTOCK2_DAY
JOIN fe_dwd.`dwd_product_base_day_all` a ON d.product_bar = a.product_code2
WHERE FPRODUCEDATE = @sub_1
AND QUALITYQTY > 0
GROUP BY business_area,product_id
UNION ALL
SELECT business_area,
       product_id,
       0 AS stock,
       IFNULL(SUM(available_stock),0)pre_stock
FROM fe_dm.dm_prewarehouse_stock_detail   -- pj_prewarehouse_stock_detail
WHERE check_date = @sub_1
AND available_stock > 0
GROUP BY business_area,product_id
)a
GROUP BY business_area,product_id;
-- 淘汰后采购数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.purchase_tmp;
CREATE TEMPORARY TABLE fe_dm.purchase_tmp (PRIMARY KEY (business_area,product_id))
SELECT d.business_area,
       o.product_id,
       o.out_date,
       SUM(actual_instock_qty)actual_instock_qty
FROM fe_dm.dm_sc_poorderlist_day  d            -- pj_poorderlist_day
JOIN fe_dm.out_product_tmp o ON d.business_area = o.business_area AND d.product_code2 = o.product_fe
WHERE d.purchase_time > o.out_date
AND fgiveaway = '非赠品'
AND invalid_status = '未作废'
AND F_BGJ_FISINSTOCK = 1 -- 需要入库
GROUP BY d.business_area,o.product_id;
-- 采购在途数量
DROP TEMPORARY TABLE IF EXISTS fe_dm.onload_tmp;
CREATE TEMPORARY TABLE fe_dm.onload_tmp (PRIMARY KEY (business_area,product_id))
SELECT d.business_area,
       d.product_id,
       SUM(onload_qty)onload_qty
FROM fe_dm.dm_sc_warehouse_onload d   -- d_sc_warehouse_onload
JOIN fe_dm.out_product_tmp o ON d.business_area = o.business_area AND d.product_id = o.product_id
WHERE d.sdate = @sub_1
GROUP BY d.business_area,d.product_id;
-- 每日更新结存
DELETE FROM fe_dm.dm_op_area_out_product_purchase WHERE sdate = @sub_1;
INSERT INTO fe_dm.dm_op_area_out_product_purchase
(sdate
,business_area
,product_id
,product_type
,sale_level
,out_date
,stock
,pre_stock
,actual_instock_qty
,onload_qty
)
SELECT @sub_1 sdate,
       s.business_area,
       s.product_id,
       s.product_type,
       s.sale_level,
       s.out_date,
       st.stock,
       st.pre_stock,
       p.actual_instock_qty,
       o.onload_qty
FROM fe_dm.out_product_tmp s
LEFT JOIN fe_dm.stock_tmp st ON s.business_area = st.business_area AND s.product_id = st.product_id
LEFT JOIN fe_dm.purchase_tmp p ON s.business_area = p.business_area AND s.product_id = p.product_id
LEFT JOIN fe_dm.onload_tmp o ON s.business_area = o.business_area AND s.product_id = o.product_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_out_product_purchase',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_out_product_purchase','dm_op_area_out_product_purchase','朱星华');
  COMMIT;	
END