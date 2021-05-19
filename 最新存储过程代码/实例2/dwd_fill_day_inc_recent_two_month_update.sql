CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_fill_day_inc_recent_two_month_update`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 建存储过程更新补货订单宽表及结存两个月宽表的数据 ，主要是针对于实例1上fe库源表出现data_flag由之前的1变为2的情况，该存储过程是用于删除实例2上该部分的数据
DELETE a.*
FROM fe_dwd.dwd_fill_day_inc a
JOIN fe_dwd.`dwd_fill_order_item_data_flag_2` b
ON a.`order_id`=b.`order_id` AND a.`ORDER_ITEM_ID`=b.`order_item_id`;
DELETE a.*
FROM fe_dwd.dwd_fill_day_inc_recent_two_month a
JOIN fe_dwd.`dwd_fill_order_item_data_flag_2` b
ON a.`order_id`=b.`order_id` AND a.`ORDER_ITEM_ID`=b.`order_item_id`;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dwd_fill_day_inc_recent_two_month_update',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('唐进@', @user),@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_fill_day_inc','dwd_fill_day_inc_recent_two_month_update','唐进');
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_fill_day_inc_recent_two_month','dwd_fill_day_inc_recent_two_month_update','唐进');
END