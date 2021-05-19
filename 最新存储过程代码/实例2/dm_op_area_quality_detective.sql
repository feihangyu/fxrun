CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_quality_detective`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;
SET @sub_1 := SUBDATE(@sdate,1);
SET @month_id := DATE_FORMAT(@sub_1,'%Y-%m');
SET @m_id := DATE_FORMAT(@sub_1,'%Y%m');
SET @month_start := CONCAT(@month_id,'-01');
SET @month_end := ADDDATE(LAST_DAY(@month_start),1);
SET @sub_7 := SUBDATE(@sdate,7);
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 货架近7天销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.7_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.7_sale_tmp (PRIMARY KEY(shelf_id))
SELECT shelf_id,
       SUM(IFNULL(gmv,0))gmv,
       SUM(IFNULL(gmv,0))+SUM(IFNULL(after_payment_money,0)) gmv_total
FROM
(
SELECT shelf_id,
       SUM(gmv)gmv,
       SUM(after_payment_money)after_payment_money
FROM fe_dwd.dwd_shelf_day_his
WHERE sdate >= @sub_7
AND sdate < @sdate
GROUP BY shelf_id
UNION ALL
SELECT shelf_id,
       SUM(total)total,
       0 AS after_payment_money
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @sub_7
AND pay_date < @sdate
AND refund_status = '无'
GROUP BY shelf_id
)a
GROUP BY shelf_id;
-- 货架月销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.month_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.month_sale_tmp (PRIMARY KEY(shelf_id))
SELECT b.business_name,
       a.shelf_id,
       SUM(IFNULL(gmv,0))gmv,
       SUM(IFNULL(gmv,0))+SUM(IFNULL(after_payment_money,0)) gmv_total
FROM
(
SELECT shelf_id,
       SUM(gmv)gmv,
       SUM(after_payment_money)after_payment_money
FROM fe_dwd.dwd_shelf_day_his
WHERE sdate >= @month_start
AND sdate < @month_end
GROUP BY shelf_id
UNION ALL
SELECT shelf_id,
       SUM(total)total,
       0 AS after_payment_money
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @month_start
AND pay_date < @month_end
AND refund_status = '无'
GROUP BY shelf_id
)a
JOIN fe_dwd.dwd_shelf_base_day_all b ON a.shelf_id = b.shelf_id
GROUP BY a.shelf_id;
-- 地区月总gmv
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.area_sale_tmp (PRIMARY KEY(business_name))
SELECT business_name,
       SUM(gmv)gmv
FROM fe_dm.month_sale_tmp
GROUP BY business_name;
-- 货架月盗损数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.damage_tmp;
CREATE TEMPORARY TABLE fe_dm.damage_tmp (PRIMARY KEY(shelf_id))
SELECT d.shelf_id,
      IFNULL(d.huosun,0) + IFNULL(d.bk_money,0) - IFNULL(d.total_error_value,0) lose_val,-- 月盗损金额
     (IFNULL(d.huosun,0) + IFNULL(d.bk_money,0) - IFNULL(d.total_error_value,0)) / (IFNULL(d.GMV,0) + IFNULL(d.bk_money,0)) month_lose_rate, -- 月盗损率
     IFNULL(d.GMV,0) + IFNULL(d.bk_money,0)
FROM fe_dm.dm_pj_zs_goods_damaged d   -- pj_zs_goods_damaged
JOIN fe_dwd.`dwd_shelf_base_day_all` s ON (s.shelf_id = d.shelf_id AND s.data_flag=1)
WHERE d.smonth = @m_id 
AND d.operate_time >= @month_start
AND (d.shelf_status IN (2,5) OR (d.revoke_time >= @month_start AND d.revoke_time < ADDDATE(@sdate,1)));
-- 已激活货架基础信息
DROP TEMPORARY TABLE IF EXISTS fe_dm.board_tmp;
CREATE TEMPORARY TABLE fe_dm.board_tmp (PRIMARY KEY(shelf_id))
SELECT a.business_name,
       a.zone_name,
       a.shelf_id,
       d.lose_val,-- 盗损金额
       d.month_lose_rate,-- 盗损率
       IFNULL(s.gmv_total,0)7_gmv,-- 近7日gmv含补付款
       IFNULL(ms.gmv_total,0)m_gmv,-- 本月gmv含补付款
       IF(a.whether_close = 1,1,0)whether_close, -- 是否关闭
       IF(IFNULL(s.gmv,0) = 0,1,0)zero_sale, -- 近7天是否0销
       IF(d.lose_val < -30 AND d.month_lose_rate < -0.15,1,0)high_steal -- 是否高盗损
FROM fe_dwd.dwd_shelf_base_day_all a 
LEFT JOIN fe_dm.damage_tmp d ON a.shelf_id = d.shelf_id
LEFT JOIN fe_dm.7_sale_tmp s ON a.shelf_id = s.shelf_id
LEFT JOIN fe_dm.month_sale_tmp ms ON a.shelf_id = ms.shelf_id
WHERE a.shelf_status = 2  -- 已激活
AND a.shelf_type NOT IN(4,8,9); -- 剔除虚拟货架、校园货架、前置仓
-- 门店异常货架占比
DROP TEMPORARY TABLE IF EXISTS fe_dm.bad_zone_item_tmp;
CREATE TEMPORARY TABLE fe_dm.bad_zone_item_tmp AS
SELECT business_name,
       zone_name,
       ROUND(COUNT(CASE WHEN whether_close = 1 OR zero_sale = 1 OR high_steal = 1 THEN shelf_id END)/COUNT(*),2) bad_shelf_rate -- 不达标货架占比
FROM fe_dm.board_tmp
GROUP BY business_name,zone_name;
-- 异常货架占比大于20%的门店占比
DROP TEMPORARY TABLE IF EXISTS fe_dm.bad_zone_tmp;
CREATE TEMPORARY TABLE fe_dm.bad_zone_tmp (PRIMARY KEY(business_name))
SELECT business_name,
       COUNT(CASE WHEN bad_shelf_rate > 0.2 THEN 1 END)bad_zone,
       COUNT(1)zone
FROM fe_dm.bad_zone_item_tmp
WHERE zone_name IS NOT NULL
GROUP BY business_name;
-- 异常货架明细 每日更新
TRUNCATE TABLE fe_dm.dm_op_abnormal_shelf ;
INSERT INTO fe_dm.dm_op_abnormal_shelf
(
sdate,
business_name,
zone_name,
shelf_id,
lose_val,
month_lose_rate,
7_gmv,
m_gmv,
whether_close,
zero_sale,
high_steal,
is_abnormal,
is_abnormal_zone
)
SELECT @sdate sdate,
       b.business_name,
       b.zone_name,
       shelf_id,
       lose_val,-- 盗损金额
       month_lose_rate,-- 盗损率
       7_gmv,-- 近7日gmv含补付款(含未对接自贩机)
       m_gmv,-- 本月gmv含补付款(含未对接自贩机)
       whether_close, -- 是否关闭
       zero_sale, -- 近7天是否0销
       high_steal, -- 是否高盗损
       IF(whether_close = 1 OR  zero_sale = 1 OR high_steal= 1,1,0)is_abnormal, -- 判断是否异常
       IF(i.bad_shelf_rate > 0.2,1,0)is_abnormal_zone  -- 是否异常货架占比>20%门店
FROM fe_dm.board_tmp b
LEFT JOIN fe_dm.bad_zone_item_tmp i ON b.business_name = i.business_name AND b.zone_name = i.zone_name
WHERE whether_close = 1 OR zero_sale = 1 OR high_steal = 1;
-- 地区新品月gmv
DROP TEMPORARY TABLE IF EXISTS fe_dm.newgmv_tmp;
CREATE TEMPORARY TABLE fe_dm.newgmv_tmp (PRIMARY KEY(business_area))
SELECT business_area,
       SUM(gmv)new_gmv
FROM fe_dm.dm_new_product_gmv    -- zs_new_product_gmv
WHERE order_date >= @month_start
AND order_date < @month_end
AND product_type IN('新增（试运行）','新增（免费货）')
GROUP BY business_area;
-- 新品近两个月最早的到仓时间,只取到仓时间>=15天的
DROP TEMPORARY TABLE IF EXISTS fe_dm.new_sku_dc_tmp;
CREATE TEMPORARY TABLE fe_dm.new_sku_dc_tmp AS
SELECT business_area,
       product_id,
       days,
       CASE WHEN days >= 15 AND days < 30 THEN 0.25
            WHEN days >= 30 AND days < 45 THEN 0.5
            WHEN days >= 45 THEN 0.7
       END AS aim -- 目标由师佳提供
FROM
(
SELECT t.business_area,
       d.product_id,
       DATE(MIN(t.fproducedate)) minday, -- 近2个月仓库最早有货时间
       DATEDIFF(@sdate,DATE(MIN(t.fproducedate)))days
FROM fe_dwd.dwd_pj_outstock2_day  t   -- PJ_OUTSTOCK2_DAY
JOIN fe_dwd.dwd_pub_product_dim_sserp  d ON t.business_area = d.business_area AND t.product_bar = d.product_fe AND d.product_type = '新增（试运行）'  -- zs_product_dim_sserp
WHERE t.fbaseqty > 0 -- 库存数量>0
AND ! ISNULL(t.business_area)
AND t.fproducedate >= SUBDATE(@sdate,INTERVAL 2 MONTH)
AND t.fproducedate < @sdate
GROUP BY t.business_area,d.product_id
)a
HAVING days >= 15;
-- 新品投放达标率 每日更新
TRUNCATE TABLE fe_dm.dm_op_lowstorate_area_product ;
INSERT INTO fe_dm.dm_op_lowstorate_area_product
(
sdate,
business_name,
product_id,
days,
aim,
sto_rate,
at_dim
)
SELECT @sdate sdate,
       b.business_name,
       a.product_id,
       c.days,
       c.aim,
       IFNULL(ROUND(COUNT(CASE WHEN stock_quantity > 0 THEN a.shelf_id END) / COUNT(CASE WHEN shelf_fill_flag = 1 OR (shelf_fill_flag = 2 AND stock_quantity > 0) THEN a.shelf_id END),4),0) sto_rate, -- 上架率
       IF(COUNT(CASE WHEN stock_quantity > 0 THEN a.shelf_id END) / COUNT(CASE WHEN shelf_fill_flag = 1 OR (shelf_fill_flag = 2 AND stock_quantity > 0) THEN a.shelf_id END) >= c.aim,1,0) at_dim -- 判断是否达标
FROM fe_dwd.dwd_shelf_product_day_all a
JOIN fe_dwd.dwd_shelf_base_day_all b ON a.shelf_id = b.shelf_id
JOIN fe_dm.new_sku_dc_tmp c ON b.business_name = c.business_area AND a.product_id = c.product_id
GROUP BY b.business_name,a.product_id;
-- 近60天驳回撤架申请的货架是否撤架挽回成功明细 每日更新
TRUNCATE TABLE fe_dm.dm_op_save_revoke_shelf;
INSERT INTO fe_dm.dm_op_save_revoke_shelf
(
sdate,
business_name,
shelf_id,
shelf_status,
revoke_status,
add_time,
gmv,
is_save
)
SELECT @sdate sdate,
       c.business_name,
       r.shelf_id,
       a.shelf_status,
       a.revoke_status,
       MAX(r.add_time) add_time,-- 近60天最近一次申请撤架时间
       IFNULL(b.gmv,0) gmv,
       IF(IFNULL(b.gmv,0) > 70,1,0) is_save
FROM fe_dwd.dwd_sf_shelf_revoke  r    -- sf_shelf_revoke
LEFT JOIN fe_dm.7_sale_tmp b ON r.shelf_id = b.shelf_id
JOIN fe_dwd.dwd_shelf_base_day_all  a ON r.shelf_id = a.shelf_id AND a.shelf_status = 2 AND a.revoke_status = 1   -- sf_shelf
JOIN fe_dwd.dwd_city_business c ON a.city = c.city
WHERE r.data_flag = 1
AND r.add_time >= SUBDATE(@sdate,60)
AND r.add_time < @sdate
GROUP BY r.shelf_id;
-- 超期激活量明细 每日更新
TRUNCATE TABLE fe_dm.dm_op_outtime_active_shelf;
INSERT INTO fe_dm.dm_op_outtime_active_shelf
(
sdate,
business_name,
record_id,
apply_code,
audit_status,
shelf_type,
apply_time,
create_time,
audit_time,
region_audit_time,
apply_num,
add_time,
shelf_id,
shelf_status,
activate_time,
create_id_time,
execute_result,
execute_finish_time,
is_active_outtime,
is_create_outtime,
is_execute_outtime,
is_active_outtime2
)
SELECT @sdate sdate,
       business_name,
       record_id,
       apply_code,
       audit_status,
       shelf_type,-- 申请货架类型
       apply_time,-- 首次申请时间
       create_time,-- 最新申请时间
       audit_time, -- 总部审核时间
       region_audit_time,-- 地区审核时间
       apply_num,-- 申请货架量
       add_time,-- 创建货架时间
       shelf_id,
       shelf_status,-- 货架状态
       activate_time,-- 激活时间
       create_id_time,-- 创建id时间
       execute_result,-- 安装任务执行结果
       execute_finish_time,-- 安装任务完成时间
       is_active_outtime, -- 是否超时待激活
       is_create_outtime, -- 是否超时未创建
       is_execute_outtime,-- 是否超时未安装
       is_active_outtime2 -- 已安装未在时效内激活
FROM
(
SELECT s.business_name, 
       r.record_id,
       r.apply_code,
       CASE WHEN r.region_audit_status = 2 AND r.audit_status = 2 THEN '总部审核通过' END AS audit_status, 
       d1.item_name AS shelf_type,
       r.apply_time,
       re.create_time,
       r.audit_time,
       r.region_audit_time,
       r.apply_num,
       al.add_time,
       a.shelf_id,
       d9.item_name shelf_status,
       al.activate_time,
       cr.create_time create_id_time,
       t.execute_result,
       t.execute_finish_time,
       IF((al.activate_time IS NULL OR a.shelf_id = 0) AND TIMESTAMPDIFF(SECOND,r.audit_time,CURRENT_TIMESTAMP) > 604800,1,0)is_active_outtime,-- 是否超时激活，总部审核7日内未激活货架，旧逻辑
       IF(cr.create_time IS NULL AND TIMESTAMPDIFF(SECOND,r.audit_time,CURRENT_TIMESTAMP) > 86400,1,0)is_create_outtime,-- 是否超时未创建，审核通过24小时内未创建
       IF(cr.create_time IS NOT NULL AND t.execute_result = 0 AND TIMESTAMPDIFF(SECOND,cr.create_time,CURRENT_TIMESTAMP) > 604800,1,0)is_execute_outtime, -- 是否超时未安装，创建ID之日开始算起超7个自然日，安装任务仍未执行成功的即为超时未安装
       IF(t.execute_result = 1 AND (TIMESTAMPDIFF(SECOND,t.execute_finish_time,IF(al.activate_time IS NULL,CURRENT_TIMESTAMP,al.activate_time))> 86400),1,0)is_active_outtime2-- 是否超时激活，安装任务执行成功但未在24小时内激活成功的，即为已安装未在时效内激活
FROM fe_dwd.dwd_sf_shelf_apply_record  r   -- sf_shelf_apply_record
LEFT JOIN fe_dwd.dwd_sf_shelf_apply a ON r.record_id = a.record_id AND a.data_flag = 1 AND a.region_audit_status = 2 AND a.audit_status = 2   -- sf_shelf_apply
LEFT JOIN fe_dwd.dwd_pub_dictionary d1 ON a.shelf_type = d1.item_value AND d1.dictionary_id = 8   -- pub_dictionary_item
LEFT JOIN fe_dwd.dwd_shelf_base_day_all al ON a.shelf_id = al.shelf_id AND al.data_flag = 1   -- sf_shelf
LEFT JOIN fe_dwd.dwd_pub_dictionary  d9 ON al.shelf_status = d9.item_value AND d9.dictionary_id = 9  -- pub_dictionary_item
LEFT JOIN fe_dwd.dwd_city_business s ON r.city = s.city
LEFT JOIN fe_dwd.dwd_pub_shelf_manager  c ON r.add_user_id = c.manager_id   -- pub_shelf_manager
LEFT JOIN fe_dwd.dwd_sf_company_protocol_apply pa ON r.record_id = pa.shelf_apply_record_id AND pa.data_flag = 1   -- sf_company_protocol_apply
LEFT JOIN 
(SELECT shelf_id,
        MAX(logistics_task_id)logistics_task_id
FROM fe_dwd.dwd_sf_shelf_logistics_task_install
GROUP BY shelf_id
) ti ON a.shelf_id = ti.shelf_id -- 此表待同步到实例2
LEFT JOIN fe_dwd.dwd_sf_shelf_logistics_task t ON t.logistics_task_id = ti.logistics_task_id AND t.task_type = 1 AND t.data_flag = 1 -- 获取货架安装数据
LEFT JOIN 
(SELECT record_id,
        MAX(create_time)create_time
FROM fe_dwd.dwd_sf_shelf_apply_log   -- sf_shelf_apply_log
WHERE fill_flag IN(1,5)
AND remark != '申请能量站保存草稿'
GROUP BY record_id
)re ON r.record_id = re.record_id
LEFT JOIN -- 获取创建能量站时间
(SELECT record_id,
        create_time
FROM  fe_dwd.dwd_sf_shelf_apply_log
WHERE fill_flag = 4 -- 创建能量站
)cr ON r.record_id = cr.record_id
WHERE r.data_flag = 1
AND c.user_type <> 10  -- 非学生
AND (pa.apply_status IN (1,2,5) OR pa.apply_status IS NULL) 
AND r.region_audit_status = 2 -- 地区审核通过
AND r.audit_status = 2 -- 总部审核通过
AND (al.shelf_name NOT LIKE '%测试%' OR al.shelf_name IS NULL) -- 剔除测试
AND r.audit_time >= SUBDATE(@sdate,30) -- 近30天审核
AND (d9.item_name != '已失效' OR d9.item_name IS NULL)
UNION ALL
-- 智能设备申请
SELECT b.business_name,
       t.record_id,
       t.record_id apply_code,
       CASE WHEN t.machines_apply_status = 4 THEN '总部审核通过' END AS audit_status,
       CASE WHEN t.machine_type IN (1,2,7) THEN '自贩机' WHEN t.machine_type IN (3,6) THEN '智能柜' ELSE '' END AS shelf_type,
       t.apply_time,
       rea.create_time,
       t.hq_audit_time,
       t.region_audit_time, 
       t.machines_num,
       al.add_time,
       cr.shelf_id,
       d9.item_name shelf_status,
       al.activate_time,
       cr.create_time create_id_time,
       cr.execute_result,
       cr.execute_finish_time,
       IF((al.activate_time IS NULL OR cr.shelf_id IS NULL) AND TIMESTAMPDIFF(SECOND,t.hq_audit_time,CURRENT_TIMESTAMP) > 604800,1,0)is_active_outtime,
       IF(cr.create_time IS NULL AND TIMESTAMPDIFF(SECOND,t.hq_audit_time,CURRENT_TIMESTAMP) > 86400,1,0)is_create_outtime, 
       IF(cr.create_time IS NOT NULL AND cr.execute_result = 0 AND TIMESTAMPDIFF(SECOND,cr.create_time,CURRENT_TIMESTAMP) > 604800,1,0)is_execute_outtime,
       IF(cr.execute_result = 1 AND (TIMESTAMPDIFF(SECOND,cr.execute_finish_time,IF(al.activate_time IS NULL,CURRENT_TIMESTAMP,al.activate_time))> 86400),1,0)is_active_outtime2   
FROM fe_dwd.dwd_sf_machines_apply_record t   -- sf_machines_apply_record
LEFT JOIN fe_dwd.`dwd_sf_machines_apply_record_extend`  re ON t.record_id = re.record_id AND re.data_flag = 1   -- sf_machines_apply_record_extend
JOIN fe_dwd.dwd_city_business b ON t.city = b.city
LEFT JOIN fe_dwd.dwd_pub_shelf_manager  c ON t.apply_user_id = c.manager_id  -- pub_shelf_manager
LEFT JOIN fe_dwd.dwd_sf_company_protocol_apply  pa ON t.record_id = pa.shelf_apply_record_id AND pa.data_flag = 1   -- sf_company_protocol_apply
LEFT JOIN 
(SELECT machine_apply_record_id record_id,
        MAX(operation_time)create_time
FROM fe_dwd.dwd_sf_machines_apply_operation   -- sf_machines_apply_operation
WHERE operation_item IN ('申请','申诉')
AND data_flag = 1
GROUP BY machine_apply_record_id
)rea ON t.record_id = rea.record_id
LEFT JOIN 
(SELECT machine_apply_record_id record_id,
        ao.operation_detail,
        ao.operation_time create_time,
        ta.execute_result,
        ta.execute_finish_time,
        s.shelf_id,
        s.activate_time
FROM fe_dwd.dwd_sf_machines_apply_operation ao   -- sf_machines_apply_operation
LEFT JOIN fe_dwd.dwd_pub_number  t1 ON t1.number > 0 AND t1.number <= LENGTH(ao.operation_detail) - LENGTH(REPLACE(ao.operation_detail, ',', ''))   -- fjr_number
LEFT JOIN fe_dwd.dwd_shelf_base_day_all s ON SUBSTRING_INDEX(SUBSTRING_INDEX(ao.operation_detail,',',t1.number),'ID',- 1) = s.shelf_id
LEFT JOIN 
(SELECT shelf_id,
        MAX(logistics_task_id)logistics_task_id
FROM fe_dwd.dwd_sf_shelf_logistics_task_install
GROUP BY shelf_id
) ti ON s.shelf_id = ti.shelf_id
LEFT JOIN fe_dwd.dwd_sf_shelf_logistics_task ta ON ta.logistics_task_id = ti.logistics_task_id AND ta.task_type = 1 AND ta.data_flag = 1
WHERE ao.operation_item = '创建货架'
)cr ON cr.record_id = t.record_id
LEFT JOIN fe_dwd.dwd_shelf_base_day_all al ON cr.shelf_id = al.shelf_id AND al.data_flag = 1   -- sf_shelf
LEFT JOIN fe_dwd.dwd_pub_dictionary  d9 ON al.shelf_status = d9.item_value AND d9.dictionary_id = 9  -- pub_dictionary_item
WHERE t.data_flag = 1 
AND t.machines_apply_status = 4 -- 总部审核通过
AND (pa.apply_status IN (1,2,5) OR pa.apply_status IS NULL) 
AND c.user_type <> 10 -- 非学生
AND (al.shelf_name NOT LIKE '%测试%' OR al.shelf_name IS NULL) -- 剔除测试
AND t.hq_audit_time >= SUBDATE(@sdate,30)
AND (d9.item_name != '已失效' OR d9.item_name IS NULL )
)z
HAVING (is_active_outtime = 1 OR is_create_outtime = 1 OR is_execute_outtime = 1 OR is_active_outtime2 = 1)AND !ISNULL(business_name);
-- 建表-地区运营质量诊断 结存每日数据
DELETE FROM  fe_dm.dm_op_area_operate_quality WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_op_area_operate_quality
(
sdate,
business_name,
active_shelf,
bad_shelf,
close_shelf,
zero_shelf,
high_steal_shelf,
bad_zone,
zone,
new_gmv,
m_gmv,
outtime_active_shelf,
at_aim,
new_num,
is_save,
save_num,
outtime_create_shelf,
outtime_execute_shelf,
outtime_active2_shelf
)
SELECT @sdate sdate,
       a.business_name,
       COUNT(*)active_shelf,-- 总激活货架量
       COUNT(CASE WHEN whether_close = 1 OR zero_sale = 1 OR high_steal = 1 THEN a.shelf_id END)bad_shelf,-- 异常货架量
       SUM(whether_close)close_shelf,-- 关闭货架量
       SUM(zero_sale)zero_shelf,-- 近7日零销货架量
       SUM(high_steal)high_steal_shelf,-- 高盗损货架量
       b.bad_zone,-- 异常货架占比>20%且不为空的门店数
       b.zone,-- 不为空的总门店数
       c.new_gmv,-- 新品gmv
       s.gmv AS m_gmv,-- 月gmv(不含补付款)
       d.outtime_active_shelf,-- 超期激活量
       e.at_aim, -- 新品投放达标量
       e.new_num, -- 到仓天数>=15的新品量
       f.is_save,-- 挽救成功量
       f.save_num, -- 总撤架申请量
       d.outtime_create_shelf,-- 超期创建量
       d.outtime_execute_shelf,-- 超期未安装量
       d.outtime_active2_shelf -- 已安装未在时效内激活量
FROM fe_dm.board_tmp a
LEFT JOIN fe_dm.area_sale_tmp s ON a.business_name = s.business_name
LEFT JOIN fe_dm.bad_zone_tmp b ON a.business_name = b.business_name
LEFT JOIN fe_dm.newgmv_tmp c ON a.business_name = c.business_area
LEFT JOIN
(SELECT business_name,
        COUNT(CASE WHEN is_active_outtime = 1 THEN record_id END)outtime_active_shelf,-- 超期激活量
        COUNT(CASE WHEN is_create_outtime = 1 THEN record_id END)outtime_create_shelf,-- 超期创建量
        COUNT(CASE WHEN is_execute_outtime = 1 THEN record_id END)outtime_execute_shelf,-- 超期未安装量
        COUNT(CASE WHEN is_active_outtime2 = 1 THEN record_id END)outtime_active2_shelf-- 已安装未在时效内激活量
FROM fe_dm.dm_op_outtime_active_shelf
GROUP BY business_name
)d ON a.business_name = d.business_name
LEFT JOIN 
(SELECT business_name,
       SUM(at_dim)at_aim,
       COUNT(*)new_num
FROM fe_dm.dm_op_lowstorate_area_product
GROUP BY business_name
)e ON a.business_name = e.business_name
LEFT JOIN
(SELECT business_name,
        SUM(is_save)is_save,
        COUNT(*)save_num
FROM fe_dm.dm_op_save_revoke_shelf
GROUP BY business_name
)f ON a.business_name = f.business_name
GROUP BY a.business_name;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_quality_detective',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_operate_quality','dm_op_area_quality_detective','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_abnormal_shelf','dm_op_area_quality_detective','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_lowstorate_area_product','dm_op_area_quality_detective','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_save_revoke_shelf','dm_op_area_quality_detective','朱星华');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_outtime_active_shelf','dm_op_area_quality_detective','朱星华');
  COMMIT;	
END