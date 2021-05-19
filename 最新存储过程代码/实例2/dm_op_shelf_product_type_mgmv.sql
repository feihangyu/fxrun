CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_product_type_mgmv`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
#SET @month_id = month_id;
SET @month_id = date_format(subdate(current_date,interval 1 day),'%Y-%m');
SET @month_start = CONCAT(@month_id, '-01');
SET @month_end = ADDDATE(LAST_DAY(@month_start),1);
-- 每日更新，每月1日结存上月数据，需要历史2018年至今的数据
DELETE a.* FROM fe_dm.dm_op_shelf_product_type_mgmv a WHERE a.month_id = @month_id;
INSERT INTO fe_dm.dm_op_shelf_product_type_mgmv 
(month_id
,business_name 
,shelf_id
,shelf_type
,second_type_name
,sub_type_name
,amount
,gmv
,orders
,users
,skus
,discount
,load_time
)
SELECT @month_id AS month_id,
       b.business_name,
       t.shelf_id, 
       b.shelf_type_desc AS shelf_type,
       a.second_type_name,
       a.sub_type_name,
       SUM(t.quantity_act)amount, 
       SUM(IF(t.refund_amount > 0,t.quantity_act,t.quantity) * t.sale_price)gmv,
       COUNT(DISTINCT t.order_id)orders, 
       COUNT(DISTINCT t.user_id)users, 
       COUNT(DISTINCT t.product_id)skus, 
       SUM(t.discount_amount)discount,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.dwd_order_item_refund_day t
JOIN fe_dwd.dwd_product_base_day_all a ON t.product_id = a.product_id
JOIN fe_dwd.dwd_shelf_base_day_all b ON t.shelf_id = b.shelf_id
WHERE t.pay_date >= @month_start
AND t.pay_date < @month_end
GROUP BY t.shelf_id,a.second_type_name,a.sub_type_name;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_product_type_mgmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_product_type_mgmv','dm_op_shelf_product_type_mgmv','朱星华');
  COMMIT;	
END