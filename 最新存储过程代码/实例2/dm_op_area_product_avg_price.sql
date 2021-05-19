CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_avg_price`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 平均单价：=GMV/销量
SET @month_id := DATE_FORMAT(CURRENT_DATE,'%Y-%m');
SET @month_start := CONCAT(@month_id, '-01');
SET @month_end := ADDDATE(LAST_DAY(@month_start),1);
SET @add_user := CURRENT_USER;
SET @timestamp := CURRENT_TIMESTAMP;
-- 统计月最新清单版本
SELECT @version_id :=
(
SELECT MAX(h.version)
FROM fe_dwd.dwd_pub_product_dim_sserp_his h
WHERE pub_time >= @month_start
AND pub_time < @month_end
);
-- 货架信息       
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT business_name,
       shelf_id,
       shelf_type_desc shelf_type
FROM fe_dwd.dwd_shelf_base_day_all
WHERE shelf_type IN(1,2,3,5,6,7,8);
-- 系统内月销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_product_sale_tmp1;
CREATE TEMPORARY TABLE fe_dm.shelf_product_sale_tmp1 AS
SELECT s.business_name,
       s.shelf_type,
       i.product_id,
       SUM(i.quantity_act)amount,
       SUM(i.quantity_act * i.sale_price)gmv
FROM fe_dwd.dwd_pub_order_item_recent_two_month i   
JOIN fe_dm.shelf_tmp s ON i.shelf_id = s.shelf_id
WHERE i.pay_date >= @month_start
AND i.pay_date < @month_end
GROUP BY s.business_name,s.shelf_type,i.product_id;
-- 系统外月销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_product_sale_tmp2;
CREATE TEMPORARY TABLE fe_dm.shelf_product_sale_tmp2 AS
SELECT s.business_name,
       s.shelf_type,
       i.product_id,
       SUM(amount)amount,
       SUM(total)gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht i
JOIN fe_dm.shelf_tmp s ON i.shelf_id = s.shelf_id
WHERE pay_date >= @month_start
AND pay_date < @month_end
AND refund_status = '无'
GROUP BY s.business_name,s.shelf_type,i.product_id;
-- 地区商品平均销售单价需每日更新，每月1日截存上月数据
DELETE FROM fe_dm.dm_op_area_product_avg_price WHERE month_id = @month_id;
INSERT INTO fe_dm.dm_op_area_product_avg_price (
`month_id`,
`business_name`,
`shelf_type`,
`product_id`,
`version`,
`product_type`,
`amount`,
`gmv`,
`load_time`
)
SELECT @month_id AS month_id,
       business_name,
       shelf_type,
       a.product_id,
       h.version,
       h.product_type,
       SUM(amount)amount,
       SUM(gmv)gmv,
       @timestamp AS load_time
FROM 
(
SELECT business_name,
       shelf_type,
       product_id,
       amount,
       gmv
FROM fe_dm.shelf_product_sale_tmp1
UNION ALL
SELECT business_name,
       shelf_type,
       product_id,
       amount,
       gmv
FROM fe_dm.shelf_product_sale_tmp2
)a
JOIN fe_dwd.dwd_pub_product_dim_sserp_his h ON a.business_name = h.business_area AND a.product_id = h.product_id AND h.version = @version_id
GROUP BY business_name,shelf_type,a.product_id;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_avg_price',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华（唐进）@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_avg_price','dm_op_area_product_avg_price','朱星华（唐进）');
 
END