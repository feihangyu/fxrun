CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_shelf_sku_situation`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
-- 已激活货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT business_name,
       shelf_id,
       shelf_type,
       relation_flag,
       type_name,
       activate_time,
       IF(grade IS NULL AND activate_time >= DATE_FORMAT(SUBDATE(@sdate,1),'%Y-%m-01'),'新装',grade)grade
FROM fe_dwd.dwd_shelf_base_day_all s
WHERE s.shelf_status = 2
LIMIT 99999999999;
-- 货架sku概况
DROP TEMPORARY TABLE IF EXISTS fe_dm.sku_tmp;
CREATE TEMPORARY TABLE fe_dm.sku_tmp (PRIMARY KEY (shelf_id))
SELECT d.shelf_id,
       s.grade,
       COUNT(CASE WHEN sale_price < 2 AND stock_quantity > 0 THEN d.product_id END)lowprice_sku,-- 低单价有货sku
       COUNT(CASE WHEN d.shelf_fill_flag = 1 THEN d.product_id END)fill_sku,-- 可补货sku
       COUNT(CASE WHEN d.shelf_fill_flag = 1 AND p.product_type IN ('新增（试运行）','原有') THEN d.product_id END)fill_normal_sku,-- 可补货正常运营sku
       COUNT(CASE WHEN d.shelf_fill_flag = 1 AND p.product_type IN ('淘汰（替补）','退出') THEN d.product_id END)fill_out_sku,-- 可补货淘汰sku
       COUNT(CASE WHEN d.stock_quantity > 0 THEN d.product_id END)sto_sku,-- 有库存sku
       COUNT(CASE WHEN d.stock_quantity > 0  AND p.product_type IN ('新增（试运行）','原有') THEN d.product_id END)sto_normal_sku,-- 有库存正常运营sku
       COUNT(CASE WHEN d.stock_quantity > 0  AND p.product_type IN ('淘汰（替补）','退出') THEN d.product_id END)sto_out_sku,-- 有库存淘汰sku
       COUNT(CASE WHEN sales_flag IN (1,2,3) AND d.shelf_fill_flag = 2 THEN d.product_id END)nofill_flag3_sku,-- 爆畅平停补sku
       COUNT(CASE WHEN sales_flag IN (1,2,3) AND d.stock_quantity > 0 THEN d.product_id END)flag3_sto_sku,-- 爆畅平有货sku
       COUNT(CASE WHEN sales_flag IN (1,2,3) AND d.stock_quantity > 0 AND p.product_type IN ('新增（试运行）','原有') THEN d.product_id END)flag3_normal_sto_sku,-- 正常运营品爆畅平有货sku
       COUNT(CASE WHEN sales_flag IN (1,2,3) AND d.stock_quantity > 0 AND p.product_type IN ('淘汰（替补）','退出') THEN d.product_id END)flag3_out_sto_sku,-- 淘汰品爆畅平有货sku
       COUNT(CASE WHEN sales_flag = 5 AND d.stock_quantity > 0 THEN d.product_id END)flag5_sto_sku,-- 严重滞销有货sku
       COUNT(CASE WHEN sales_flag = 5 AND d.stock_quantity > 0 AND p.product_type IN ('新增（试运行）','原有') THEN d.product_id END)flag5_normal_sku,-- 正常运营品严重滞销有货sku
       COUNT(CASE WHEN sales_flag = 5 AND d.stock_quantity > 0 AND p.product_type IN ('淘汰（替补）','退出')THEN d.product_id END)flag5_out_sku,-- 淘汰品严重滞销有货sku
       SUM(stock_quantity)sto_num,-- 总库存量
       SUM(stock_quantity*sale_price)sto_val,-- 总库存金额
       SUM(CASE WHEN sales_flag IN (1,2,3) THEN stock_quantity*sale_price END)flag3_sto_val,-- 爆畅平库存金额
       SUM(CASE WHEN sales_flag = 5 THEN stock_quantity*sale_price END)flag5_sto_val,-- 严重滞销库存金额
       SUM(CASE WHEN p.product_type = '新增（试运行）' THEN stock_quantity*sale_price END)new_sto_val,-- 新品库存金额
       SUM(CASE WHEN p.product_type IN ('新增（试运行）','原有') THEN stock_quantity*sale_price END)normal_sto_val,-- 正常运营品库存金额
       SUM(CASE WHEN p.product_type IN ('淘汰（替补）','退出') THEN stock_quantity*sale_price END) out_sto_val -- 淘汰品库存金额
FROM fe_dwd.dwd_shelf_product_day_all d
JOIN fe_dm.shelf_tmp s ON d.shelf_id = s.shelf_id
LEFT JOIN feods.zs_product_dim_sserp p ON s.business_name = p.business_area AND d.product_id = p.product_id
GROUP BY shelf_id;
-- 需每日更新并截存。
DELETE FROM fe_dm.dm_op_shelf_sku_situation WHERE sdate=@sdate;
INSERT INTO fe_dm.dm_op_shelf_sku_situation
(sdate
,shelf_id
,lowprice_sku
,fill_sku
,fill_normal_sku
,fill_out_sku
,sto_sku
,sto_normal_sku
,sto_out_sku
,nofill_flag3_sku
,flag3_sto_sku
,flag3_normal_sto_sku
,flag3_out_sto_sku
,flag5_sto_sku
,flag5_normal_sku
,flag5_out_sku
,sto_num
,sto_val
,flag3_sto_val
,flag5_sto_val
,new_sto_val
,out_sto_val
,fill_sku_situation
)
SELECT @sdate sdate,
       s.shelf_id,
       t.lowprice_sku,
       t.fill_sku,
       t.fill_normal_sku,
       t.fill_out_sku,
       t.sto_sku,
       t.sto_normal_sku,
       t.sto_out_sku,
       t.nofill_flag3_sku,
       t.flag3_sto_sku,
       t.flag3_normal_sto_sku,
       t.flag3_out_sto_sku,
       t.flag5_sto_sku,
       t.flag5_normal_sku,
       t.flag5_out_sku,
       t.sto_num,
       t.sto_val,
       t.flag3_sto_val,
       t.flag5_sto_val,
       t.new_sto_val,
      -- t.normal_sto_val,
       t.out_sto_val,
       CASE WHEN (s.relation_flag = 1 AND fill_sku <= 25)
            OR (s.shelf_type IN(1,3) AND s.grade = '新装' AND fill_sku < 35)
            OR (s.shelf_type = 2 AND s.grade = '新装' AND fill_sku < 15)
            OR (s.shelf_type IN (6,7) AND fill_sku < 15) 
            OR ((s.shelf_type IN (1,3) AND fill_sku < 25)  
            OR (s.shelf_type = 2 AND fill_sku < 10)) THEN 'sku偏少'
            WHEN ((s.relation_flag = 1 AND fill_sku > 25) 
            OR (s.type_name LIKE '%静态%' AND fill_sku >= 15 AND fill_sku <= 25) -- 2020/07/01修改,OR (s.type_name LIKE '%静态%' AND fill_sku >= 15 AND fill_sku <= 22) 
            OR (s.type_name LIKE '%动态%' AND fill_sku >= 15 AND fill_sku <= 35) -- 2020/07/01修改,OR ((s.type_name LIKE '%动态%' OR s.shelf_type = 7) AND fill_sku >= 15 AND fill_sku <= 30) 
            OR (s.shelf_type = 7 AND fill_sku >= 15 AND fill_sku <= 30)
            OR (s.shelf_type IN (1,3) AND s.grade = '新装' AND fill_sku >= 35 AND fill_sku <= 55)
            OR (s.shelf_type = 2 AND s.grade = '新装' AND fill_sku >= 15 AND fill_sku <= 25)
            OR (s.shelf_type IN (1,3) AND fill_sku >= 25 AND fill_sku <= 55)
            OR (s.shelf_type = 2 AND fill_sku >= 10 AND fill_sku <= 20)) THEN 'sku正常'
       ELSE 'sku偏多' END AS fill_sku_situation
FROM fe_dm.shelf_tmp s
LEFT JOIN fe_dm.sku_tmp t ON s.shelf_id = t.shelf_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_sku_situation',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('朱星华（唐进）@', @user, @timestamp));
 
  COMMIT;
END