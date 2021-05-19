CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf7_slot_analysis`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE,
    @sub1 := SUBDATE(@sdate, 1),
    @sub7 := SUBDATE(@sub1,6),
    @month_id := DATE_FORMAT(@sub1,'%Y-%m'),
    @month_start := CONCAT(@month_id,'-01');
-- 已激活自贩机
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.shelf_code,
       a.shelf_name LIKE '%测试%' is_test,
       s.slot_num,-- 总货道数
       w.sto_sku,-- 近7天有库存sku
       w.sale_sku,-- 近7天有销售sku
       IF(o.first_order_date IS NULL,0,DATEDIFF(@sdate,o.first_order_date))sale_days,-- 历史销售运营天数
       IF(a.activate_time <= @sub7,1,0)active_full_week, -- 激活是否满7天
       IF(a.activate_time < @month_start,1,0)is_history_shelf, -- 激活在本月1日前的记为存量(不含当月1号)
       IF(f.firstfill_all <= @sub7,1,0)history_fill, -- 首次补货是否在7天前
       IF(o.first_order_date <= @sub7,1,0)history_order -- 首次销售是否在7天前
FROM fe_dwd.`dwd_shelf_base_day_all` a
LEFT JOIN fe_dm.dm_op_shelf_firstfill f ON a.shelf_id = f.shelf_id  -- d_op_shelf_firstfill
LEFT JOIN fe_dwd.dwd_pub_shelf_first_order_info o ON a.shelf_id = o.shelf_id
LEFT JOIN -- 货架近7天有库存sku、有销售sku
(SELECT shelf_id,
        COUNT(CASE WHEN qty_sal7 > 0 THEN product_id END)sale_sku,
        COUNT(CASE WHEN days_sal_sto7 > 0 THEN product_id END)sto_sku
FROM fe_dm.dm_op_sp_avgsal_recent_week    -- d_op_sp_avgsal7
GROUP BY shelf_id
) w ON a.shelf_id = w.shelf_id
LEFT JOIN
(SELECT shelf_id,
         COUNT(*)slot_num
FROM fe_dwd.dwd_sf_shelf_machine_slot
WHERE data_flag = 1
GROUP BY shelf_id
)s ON a.shelf_id = s.shelf_id
WHERE a.shelf_type = 7
AND a.shelf_status = 2;
-- 自贩机历史gmv、近7天销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_cum_gmv_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_cum_gmv_tmp (PRIMARY KEY (shelf_id))
SELECT shelf_id,
       SUM(gmv)gmv,
       SUM(7gmv)7gmv,
       SUM(7amount)7amount
FROM
(
SELECT d.shelf_id,
       SUM(gmv)gmv,
       SUM(CASE WHEN sdate >= @sub7 AND sdate < @sdate THEN gmv END)7gmv,
       SUM(CASE WHEN sdate >= @sub7 AND sdate < @sdate THEN sal_qty END)7amount
FROM fe_dwd.dwd_shelf_day_his d  -- fjr_shelf_dgmv 此处业务需求计算自贩机历史gmv，但是实例2的货架结存宽表数据只有2019-12月份后的数据，和业务沟通先通过该结存宽表计算历史日均gmv，如果后续有问题，将再做修改
JOIN fe_dm.shelf_tmp s ON d.shelf_id = s.shelf_id
GROUP BY d.shelf_id
UNION ALL
SELECT shelf_id,
       SUM(total)gmv,
       SUM(CASE WHEN pay_date >= @sub7 AND pay_date < @sdate THEN total END)7gmv,
       SUM(CASE WHEN pay_date >= @sub7 AND pay_date < @sdate THEN amount END)amount
FROM fe_dwd.dwd_op_out_of_system_order_yht o
WHERE refund_status = '无'
GROUP BY shelf_id
)b
GROUP BY b.shelf_id;
-- 历史自然日日均gmv
DROP TEMPORARY TABLE IF EXISTS fe_dm.avg_gmv_tmp;
CREATE TEMPORARY TABLE fe_dm.avg_gmv_tmp (PRIMARY KEY (shelf_id))
SELECT s.shelf_id,
       IFNULL(s.gmv,0) / t.sale_days avg_gmv
FROM fe_dm.shelf_cum_gmv_tmp s
LEFT JOIN fe_dm.shelf_tmp t ON s.shelf_id = t.shelf_id;
-- 自贩机全天在线率、12小时在线率
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_online_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_online_tmp (PRIMARY KEY (shelf_id))
SELECT b.shelf_id,
       SUM(CASE WHEN b.online_status = 1 THEN b.duration END) / SUM(b.duration)online_rate,
       SUM(CASE WHEN b.online_status = 1 THEN b.duration820 ELSE 0 END) / SUM(b.duration820) 12hour_online_rate
FROM fe_dm.dm_op_machine_online_stat b -- d_op_machine_online_stat 
WHERE duration820 >= 0
AND sdate >= @sub7
AND sdate < @sdate
GROUP BY b.shelf_id;
-- 货道库存量及销量
DROP TEMPORARY TABLE IF EXISTS fe_dm.slot_sto_and_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.slot_sto_and_sale_tmp (PRIMARY KEY (shelf_id,slot_id))
SELECT s.shelf_id,
       s.manufacturer_slot_code slot_id,
       IFNULL(c.stock,0)stock,
       IFNULL(c.amount,0)amount
FROM fe_dwd.dwd_sf_shelf_machine_slot s
LEFT JOIN 
(
SELECT shelf_id,
       slot_id,
       IFNULL(SUM(stock),0)stock,
       IFNULL(SUM(amount),0)amount
FROM
(-- 货道库存量
SELECT shelf_id,
       manufacturer_slot_code slot_id,
       SUM(stock_num)stock,
       0 AS amount
FROM fe_dm.dm_op_slot_his h  -- d_op_slot_his
WHERE sdate >= @sub7
AND sdate < @sdate
AND stock_num >= 0 
GROUP BY shelf_id,slot_id
UNION ALL
-- 货道销量
SELECT c.shelf_id,
       c.slot_code,
       0 AS stock,
       SUM(o.quantity_act)amount
FROM fe_dwd.dwd_sf_shelf_machine_command_log c
JOIN fe_dwd.dwd_pub_order_item_recent_one_month o ON c.order_id = o.order_id
WHERE c.dispatch_result = 1
AND c.data_flag = 1
AND o.pay_date >= @sub7 
AND o.pay_date < @sdate
GROUP BY c.shelf_id,c.slot_code
UNION ALL
-- 澳柯玛货道销量
SELECT IFNULL(t.shelf_id, s.shelf_id) shelf_id,
       oi.locationId,
       0 AS stock,
       SUM(oi.product_count)amount
FROM fe_dwd.dwd_sf_order_yht t
JOIN fe_dwd.dwd_sf_order_yht_item oi  ON t.order_id = oi.order_id
LEFT JOIN fe_dm.shelf_tmp s ON t.asset_id = s.shelf_code AND ! ISNULL(s.shelf_code) AND s.shelf_code != ''
WHERE t.data_flag = 1
AND t.pay_status = 1
AND t.deliver_status = 0
AND t.paytime >= @sub7
AND t.paytime < @sdate
AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
AND ! ISNULL(oi.goods_id)
GROUP BY IFNULL(t.shelf_id, s.shelf_id),oi.locationId
)a
GROUP BY a.shelf_id,slot_id
)c ON s.shelf_id = c.shelf_id AND s.manufacturer_slot_code = c.slot_id
WHERE s.data_flag = 1;
-- 货架有库存货道数、有销售货道数
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_slot_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_slot_tmp (PRIMARY KEY (shelf_id))
SELECT a.shelf_id,
       COUNT(CASE WHEN stock > 0 OR amount > 0 THEN slot_id END)sto_slot, -- 有库存货道数(含有销售货道)
       COUNT(CASE WHEN amount > 0 THEN slot_id END)sale_slot,-- 有销售货道数
       COUNT(CASE WHEN amount = 0 THEN slot_id END)nosale_slot,-- 零销货道数
       COUNT(CASE WHEN stock > 0 AND amount = 0 THEN slot_id END)sto_zero_sale_slot,-- 有库存零销货道数,
       COUNT(CASE WHEN stock = 0 AND amount = 0 THEN slot_id END)nosto_nosale_slot -- 无库存零销货道数
FROM fe_dm.slot_sto_and_sale_tmp a
JOIN fe_dm.shelf_tmp b ON a.shelf_id = b.shelf_id -- 2020/07/14增加
GROUP BY a.shelf_id;
-- 零销货道原因归类
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_slot_nosale_reason_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_slot_nosale_reason_tmp (PRIMARY KEY (shelf_id,slot_id))
SELECT s.shelf_id,
       s.slot_id,
       CASE WHEN IFNULL(o.12hour_online_rate,0) = 0 THEN '1、设备异常-周在线率为0'
            WHEN s.stock = 0 THEN '2、货道无库存零销'
            WHEN s.stock > 0 AND IFNULL(o.12hour_online_rate,0) < 0.3 THEN '3、设备异常-周在线率≤30%'
            WHEN s.stock > 0 AND (IFNULL(a.avg_gmv,0) < 10 OR (IFNULL(a.avg_gmv,0) >= 10 AND IFNULL(a.avg_gmv,0) < 15 AND t.sto_slot >= 25)) THEN '4、设备异常-本身低销'
       ELSE '5、商品配置问题' END AS reason
FROM fe_dm.slot_sto_and_sale_tmp s
JOIN fe_dm.shelf_tmp b ON s.shelf_id = b.shelf_id -- 2020/07/14增加
LEFT JOIN fe_dm.shelf_online_tmp o ON s.shelf_id = o.shelf_id
LEFT JOIN fe_dm.avg_gmv_tmp a ON s.shelf_id = a.shelf_id
LEFT JOIN fe_dm.shelf_slot_tmp t ON s.shelf_id = t.shelf_id
WHERE amount = 0;
-- 各原因零销货道数
DROP TEMPORARY TABLE IF EXISTS fe_dm.reason_slot_num_tmp;
CREATE TEMPORARY TABLE fe_dm.reason_slot_num_tmp (PRIMARY KEY (shelf_id,reason))
SELECT shelf_id,
       reason,
       COUNT(DISTINCT slot_id)nosale_slot_num
FROM fe_dm.shelf_slot_nosale_reason_tmp
GROUP BY shelf_id,reason;
-- 每周日更新
DELETE FROM fe_dm.dm_op_shelf7_slot_analysis WHERE sdate=@sub1;
INSERT INTO fe_dm.dm_op_shelf7_slot_analysis
(sdate
,business_name
,shelf_id
,is_test
,slot_num
,sto_sku
,sale_sku
,sale_days
,active_full_week
,is_history_shelf
,history_fill
,history_order
,7gmv
,7amount
,avg_gmv
,online_rate
,12hour_online_rate
,sto_slot
,sale_slot
,nosale_slot
,sto_zero_sale_slot
,nosto_nosale_slot
,reason
,nosale_slot_num
)
SELECT @sub1 AS sdate,
       a.business_name,
       a.shelf_id,
       a.is_test,
       a.slot_num,
       a.sto_sku,
       a.sale_sku,
       a.sale_days,
       a.active_full_week,
       a.is_history_shelf,
       a.history_fill,
       a.history_order,
       b.7gmv,
       b.7amount,
       e.avg_gmv,
       c.online_rate,
       c.12hour_online_rate,
       d.sto_slot,
       d.sale_slot,
       d.nosale_slot,
       d.sto_zero_sale_slot,
       d.nosto_nosale_slot,
       IFNULL(r.reason,'0'),
       r.nosale_slot_num
FROM fe_dm.shelf_tmp a
LEFT JOIN fe_dm.shelf_cum_gmv_tmp b ON a.shelf_id = b.shelf_id
LEFT JOIN fe_dm.shelf_online_tmp c ON a.shelf_id = c.shelf_id
LEFT JOIN fe_dm.shelf_slot_tmp d ON a.shelf_id = d.shelf_id
LEFT JOIN fe_dm.avg_gmv_tmp e ON a.shelf_id = e.shelf_id
LEFT JOIN fe_dm.reason_slot_num_tmp r ON a.shelf_id = r.shelf_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf7_slot_analysis',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf7_slot_analysis','dm_op_shelf7_slot_analysis','朱星华');
  COMMIT;	
END