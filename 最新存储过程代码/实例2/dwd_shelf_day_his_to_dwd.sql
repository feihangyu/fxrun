CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_day_his_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 货架日结存宽表数据从fe_temp 到 fe_dwd 
DELETE FROM fe_dwd.dwd_shelf_day_his WHERE sdate=SUBDATE(CURRENT_DATE,INTERVAL 2 DAY);
REPLACE INTO fe_dwd.dwd_shelf_day_his(
sdate,
shelf_id,
zone_code,
shelf_code,
ACTIVATE_TIME,
shelf_type,
shelf_status,
revoke_status,
whether_close,
manager_id,
manager_type,
prewarehouse_id,
main_shelf_id,
shelf_level,
grade,
stock_quantity,
stock_skus,
stock_sum,
sal_qty,
sal_qty_act,
skus,
gmv,
before_refund_GMV,
refunding_GMV,
o_product_total_amount,
o_discount_amount,
o_coupon_amount,
o_third_discount_amount,
AFTER_PAYMENT_MONEY,
pay_amount,
pay_amount_act,
refund_finish_amount,
before_refund_amount,
onway_num,
ACTUAL_FILL_NUM,
orders,
users,
load_time) 
SELECT 
sdate,
shelf_id,
zone_code,
shelf_code,
ACTIVATE_TIME,
shelf_type,
shelf_status,
revoke_status,
whether_close,
manager_id,
manager_type,
prewarehouse_id,
main_shelf_id,
shelf_level,
grade,
stock_quantity,
stock_skus,
stock_sum,
sal_qty,
sal_qty_act,
skus,
gmv,
before_refund_GMV,
refunding_GMV,
o_product_total_amount,
o_discount_amount,
o_coupon_amount,
o_third_discount_amount,
AFTER_PAYMENT_MONEY,
pay_amount,
pay_amount_act,
refund_finish_amount,
before_refund_amount,
onway_num,
ACTUAL_FILL_NUM,
orders,
users,
load_time
FROM fe_temp.dwd_shelf_day_his 
WHERE load_time >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) ;  
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_day_his_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_day_his','dwd_shelf_day_his_to_dwd','李世龙');
 
  COMMIT;	
END