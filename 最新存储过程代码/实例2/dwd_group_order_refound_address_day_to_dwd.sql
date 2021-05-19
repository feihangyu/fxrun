CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_group_order_refound_address_day_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 企业订单宽表数据从fe_temp 到 fe_dwd 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_tmp1;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_tmp1 AS
SELECT a.order_id,a.order_item_id    -- 因为对应的宽表存储过程update有对 补货订单号和订单明细编号 进行更新字段 
FROM 
fe_temp.dwd_group_order_refound_address_day b -- 小表   小表要放在前面作为驱动表，加快速度
 JOIN fe_dwd.dwd_group_order_refound_address_day a   -- 大表 
    ON a.order_id = b.order_id AND a.order_item_id=b.order_item_id
    AND b.load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;   
CREATE INDEX idx_order_item_id
ON fe_dwd.dwd_group_order_refound_address_day_tmp1(order_id,order_item_id);
DELETE a.* FROM fe_dwd.dwd_group_order_refound_address_day a   -- 先删除共同的部分  按照订单号删除即可
JOIN  fe_dwd.dwd_group_order_refound_address_day_tmp1  b
    ON a.order_id = b.order_id AND a.order_item_id=b.order_item_id;
	
INSERT INTO fe_dwd.dwd_group_order_refound_address_day(
pay_time,
finish_time,
supply_channel,
parent_order_pay_id,
order_id,
order_item_id,
order_date,
order_status_name,
order_type,
sale_channel,
pay_type,
pay_type_desc,
order_type_number,
pay_state,
gateway_pay_id,
order_pay_id,
order_user_id,
three_transaction_id,
product_spec_id,
product_name,
quantity,
purchase_unit_price,
sale_unit_price,
origin_sale_unit_price,
order_discount_amount,
coupon_total_amount_order,
discount_total_amount,
coupon_total_amount,
sale_total_amount,
purchase_total_amount,
real_total_amount,
order_total_amount,
pay_amount,
pay_discount_amount,
cost_percent,
sale_from,
order_from,
link_name,
mobile,
province,
city,
district,
address,
add_time,
delivery_link_assign_id,
express_number,
express_group_name,
freight_amount,
freight_amount_item,
supplyerid,
group_name,
gateway_pay_id_rufound,
refund_amount,
refund_amount_order,
load_time)
SELECT 
pay_time,
finish_time,
supply_channel,
parent_order_pay_id,
order_id,
order_item_id,
order_date,
order_status_name,
order_type,
sale_channel,
pay_type,
pay_type_desc,
order_type_number,
pay_state,
gateway_pay_id,
order_pay_id,
order_user_id,
three_transaction_id,
product_spec_id,
product_name,
quantity,
purchase_unit_price,
sale_unit_price,
origin_sale_unit_price,
order_discount_amount,
coupon_total_amount_order,
discount_total_amount,
coupon_total_amount,
sale_total_amount,
purchase_total_amount,
real_total_amount,
order_total_amount,
pay_amount,
pay_discount_amount,
cost_percent,
sale_from,
order_from,
link_name,
mobile,
province,
city,
district,
address,
add_time,
delivery_link_assign_id,
express_number,
express_group_name,
freight_amount,
freight_amount_item,
supplyerid,
group_name,
gateway_pay_id_rufound,
refund_amount,
refund_amount_order,
load_time
FROM fe_temp.dwd_group_order_refound_address_day 
WHERE load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;   
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_group_order_refound_address_day_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_group_order_refound_address_day','dwd_group_order_refound_address_day_to_dwd','李世龙');
  COMMIT;	
END