CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_op_shelf6_should_start_fill_item`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SELECT @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE) + 1);
-- 智能货柜模板
DROP TEMPORARY TABLE IF EXISTS fe_dm.template_tmp;
CREATE TEMPORARY TABLE fe_dm.template_tmp (PRIMARY KEY (template_id))
SELECT b.area_name,
       t.template_id,
       t.template_name
FROM fe_dwd.dwd_sf_shelf_smart_product_template t --  sf_shelf_smart_product_template t
LEFT JOIN fe_dwd.dwd_sf_product_business_area b ON t.area_id = b.area_id AND b.data_flag = 1  --  sf_product_business_area
WHERE t.template_status = 1
AND t.data_flag = 1;
-- 智能柜商品模板明细
DROP TEMPORARY TABLE IF EXISTS fe_dm.template_item_tmp;
CREATE TEMPORARY TABLE fe_dm.template_item_tmp (PRIMARY KEY (template_id,product_id))
SELECT t.area_name,
       t.template_id,
       t.template_name,
       i.product_id
FROM fe_dm.template_tmp t
LEFT JOIN fe_dwd.dwd_sf_shelf_smart_product_template_item i ON t.template_id = i.template_id AND i.data_flag = 1;   -- sf_shelf_smart_product_template_item
-- 地区激活货架明细(不统计已注销)
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp; 
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.shelf_code,
       a.activate_time,
       a.shelf_type,
       i.prewarehouse_id AS warehouse_id,
       a.type_name
FROM fe_dwd.dwd_shelf_base_day_all a
LEFT JOIN fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all i ON a.shelf_id = i.shelf_id
WHERE a.shelf_status = 2
AND a.shelf_type IN (1,2,3,6);-- 四层/五层/冰箱/智能货柜
-- 地区激活货架数
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_active_tmp;
CREATE TEMPORARY TABLE fe_dm.area_active_tmp (PRIMARY KEY (business_name))
SELECT business_name,
       COUNT(CASE WHEN shelf_type = 6 THEN shelf_id END)active_machine,
       COUNT(CASE WHEN shelf_type IN (1,2,3) THEN shelf_id END)active_shelf,
       COUNT(CASE WHEN shelf_type = 6 THEN shelf_id END)*(1/3)top_active_machine, -- 1/3智能货柜激活量
       COUNT(CASE WHEN shelf_type IN (1,2,3) THEN shelf_id END)*(1/3)top_active_shelf -- 1/3货架激活量
FROM fe_dm.shelf_tmp
GROUP BY business_name;
-- 地区商品覆盖数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_product_cover_tmp;
CREATE TEMPORARY TABLE fe_dm.area_product_cover_tmp (PRIMARY KEY (business_name,product_id))
SELECT s.business_name,
       a.product_id,
       COUNT(CASE WHEN a.shelf_fill_flag = 1 AND shelf_type = 6 THEN a.shelf_id END)fill_machine,-- 开启补货智能货柜数
       COUNT(CASE WHEN a.shelf_fill_flag = 1 AND shelf_type IN(1,2,3) THEN a.shelf_id END)fill_shelf,-- 开启补货货架数
       COUNT(CASE WHEN a.stock_quantity > 0 AND shelf_type = 6 THEN a.shelf_id END)sto_machine,-- 有库存智能货柜数
       COUNT(CASE WHEN a.stock_quantity > 0 AND shelf_type = 6 AND sales_flag IN (1,2) THEN a.shelf_id END)sto_good_machine, -- 有库存爆畅智能货柜数
       COUNT(CASE WHEN a.stock_quantity > 0 AND shelf_type IN (1,2,3) THEN a.shelf_id END)sto_shelf,-- 有库存货架数
       COUNT(CASE WHEN a.stock_quantity > 0 AND shelf_type IN (1,2,3) AND sales_flag IN (1,2) THEN a.shelf_id END)sto_good_shelf-- 有库存爆畅货架数
FROM fe_dwd.dwd_shelf_product_day_all a
JOIN fe_dm.shelf_tmp s ON a.shelf_id = s.shelf_id
GROUP BY s.business_name,a.product_id;
-- 大仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_dc_stock_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_dc_stock_tmp
SELECT business_area,
       product_id,
       SUM(QUALITYQTY)stock
FROM fe_dwd.dwd_pj_outstock2_day d             -- PJ_OUTSTOCK2_DAY d
JOIN fe_dwd.dwd_product_base_day_all a ON d.product_bar = a.product_code2
WHERE FPRODUCEDATE = SUBDATE(CURRENT_DATE,1)
AND QUALITYQTY > 0
GROUP BY business_area,product_id;
-- 前置仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_pre_stock_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_pre_stock_tmp(PRIMARY KEY (warehouse_id, product_id))
SELECT warehouse_id,
       product_id,
       IFNULL(SUM(available_stock),0)pre_stock
FROM fe_dm.dm_prewarehouse_stock_detail          -- pj_prewarehouse_stock_detail
WHERE check_date = SUBDATE(CURRENT_DATE,1)
AND available_stock > 0
GROUP BY warehouse_id,product_id;
-- 正常运营品覆盖无人货架、智能柜情况
DROP TEMPORARY TABLE IF EXISTS fe_dm.normal_product_cover_tmp;
CREATE TEMPORARY TABLE fe_dm.normal_product_cover_tmp AS
SELECT a.business_area,
       a.product_id product_id1,
       a.product_type,
       IFNULL(c.sto_machine,0)sto_machine,-- 有库存智能货柜数
       IFNULL(c.fill_machine,0) / IFNULL(b.active_machine,0) fill_machine_rate,-- 可补货智能货柜占比
       IFNULL(c.sto_good_machine,0) / IFNULL(c.sto_machine,0) good_machine_rate,-- 爆畅智能柜占比
       t.template_id,
       t.template_name,
       i.product_id product_id2,-- 智能柜商品模板中的商品id
       IFNULL(c.sto_shelf,0)sto_shelf,-- 有库存货架数
       IFNULL(b.top_active_shelf,0)top_active_shelf,-- 1/3激活货架数
       IFNULL(b.top_active_machine,0)top_active_machine,-- 1/3激活智能货柜数
       IFNULL(c.sto_good_shelf,0) / IFNULL(c.sto_shelf,0)good_shelf_rate -- 爆畅货架占比
FROM fe_dwd.dwd_pub_product_dim_sserp a          -- zs_product_dim_sserp a
LEFT JOIN fe_dm.area_active_tmp b ON a.business_area = b.business_name
LEFT JOIN fe_dm.area_product_cover_tmp c ON a.business_area = c.business_name AND a.product_id = c.product_id
LEFT JOIN fe_dm.template_tmp t ON a.business_area = t.area_name
LEFT JOIN fe_dm.template_item_tmp i ON t.template_id = i.template_id AND a.product_id = i.product_id
WHERE a.product_type IN('原有','新增（试运行）');
-- 是否应该添加至智能柜商品模板商品明细  每周一更新并结存
delete from fe_dwd.dwd_op_shelf6_should_add_template where week_end=@week_end;
INSERT INTO fe_dwd.dwd_op_shelf6_should_add_template
(
week_end,
business_area,
product_id,
product_type,
fill_machine_rate,
good_machine_rate,
template_id,
template_name,
is_in_template,
is_in_logic,
should_add
)
SELECT @week_end week_end,
       business_area,
       product_id1 AS product_id,
       product_type,
       fill_machine_rate,-- 智能货柜可补货货架占比
       good_machine_rate,-- 爆畅智能柜货架占比
       template_id,
       template_name,
       IF(product_id2 IS NULL,0,1)is_in_template,-- 是否在商品模板中
       IF(sto_shelf > top_active_shelf AND good_shelf_rate >= 0.3,1,0)is_in_logic,-- 是否符合逻辑:覆盖地区无人货架有库存货架数超过三分之一,覆盖货架数≥30%商品销售等级为爆畅级别,正常运营品
       IF(sto_shelf > top_active_shelf AND good_shelf_rate >= 0.3 AND product_id2 IS NULL AND template_id IS NOT NULL,1,0)should_add -- 是否符合需要添加道商品模板的逻辑:符合逻辑2且不在商品模板中
FROM fe_dm.normal_product_cover_tmp a;
-- 智能柜应该开启补货的地区商品
DROP TEMPORARY TABLE IF EXISTS fe_dm.product_tmp;
CREATE TEMPORARY TABLE fe_dm.product_tmp (PRIMARY KEY(business_area,product_id))
SELECT business_area,
       product_id1 AS product_id,
       product_type
FROM fe_dm.normal_product_cover_tmp a
WHERE product_id2 IS NOT NULL -- 在商品模板中
AND sto_machine > top_active_machine -- 覆盖地区智能柜有库存货架数超过三分之一
AND good_machine_rate >= 0.5 -- 覆盖货架数≥50%销售等级为爆畅
GROUP BY business_area,product_id1; 
-- 需要开启补货的智能货柜货架商品明细  每周一更新并结存
delete from fe_dwd.dwd_op_shelf6_should_start_fill_item where week_end=@week_end;
INSERT INTO fe_dwd.dwd_op_shelf6_should_start_fill_item
(
week_end,
shelf_id,
product_id,
product_type,
sales_flag,
shelf_fill_flag,
stock,
pre_stock
)
SELECT @week_end week_end,
       a.shelf_id,
       b.product_id,
       c.product_type,
       b.sales_flag,
       b.shelf_fill_flag,
       dc.stock,
       pre.pre_stock
FROM fe_dm.shelf_tmp a
JOIN fe_dwd.dwd_shelf_product_day_all b ON a.shelf_id = b.shelf_id
JOIN fe_dm.product_tmp c ON a.business_name = c.business_area AND b.product_id = c.product_id
LEFT JOIN fe_dm.shelf_dc_stock_tmp dc ON a.business_name = dc.business_area AND b.product_id = dc.product_id
LEFT JOIN fe_dm.shelf_pre_stock_tmp pre ON a.warehouse_id = pre.warehouse_id AND b.product_id = pre.product_id
WHERE a.shelf_type = 6
AND b.shelf_fill_flag = 2;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_op_shelf6_should_start_fill_item',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_shelf6_should_add_template','dwd_op_shelf6_should_start_fill_item','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_shelf6_should_start_fill_item','dwd_op_shelf6_should_start_fill_item','朱星华');
  COMMIT;	
END