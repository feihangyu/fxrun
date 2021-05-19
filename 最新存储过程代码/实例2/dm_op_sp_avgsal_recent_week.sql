CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_sp_avgsal_recent_week`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dm.sale_tmp;
CREATE TEMPORARY TABLE fe_dm.sale_tmp 
SELECT 
shelf_id,product_id,
SUM(IFNULL(sal_qty,0)) AS qty_sal7,
SUM(CASE WHEN stock_quantity=0 AND sal_qty=0 THEN 0 ELSE 1 END) AS days_sal_sto7
FROM fe_dwd.`dwd_shelf_product_sto_sal_30_days` FORCE INDEX(sdate)
WHERE sdate>=SUBDATE(CURRENT_DATE, INTERVAL 7 DAY) AND sdate<CURRENT_DATE
GROUP BY shelf_id,product_id;
CREATE INDEX idx_shelf_product_id
ON fe_dm.sale_tmp(shelf_id,product_id);
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_sp_avgsal_recent_week","@time_1--@time_2",@time_1,@time_2);
    
-- 未对接系统澳柯玛销售(0510增加)
DROP TEMPORARY TABLE IF EXISTS fe_dm.akm_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.akm_sale_tmp (PRIMARY KEY (shelf_id,product_id))
SELECT shelf_id,
       product_id,
       SUM(amount)amount
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= SUBDATE(CURRENT_DATE, INTERVAL 7 DAY)
AND pay_date < CURRENT_DATE
AND refund_status = '无'
GROUP BY shelf_id,product_id;
    
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_sp_avgsal_recent_week","@time_2--@time_3",@time_2,@time_3);
TRUNCATE fe_dm.dm_op_sp_avgsal_recent_week;
INSERT INTO fe_dm.dm_op_sp_avgsal_recent_week
(
shelf_id,
product_id,
qty_sal7,
days_sal_sto7
) 
SELECT t.shelf_id,
       t.product_id,
       t.qty_sal7 + IFNULL(s.amount,0) AS qty_sal7, -- 0510修改
       t.days_sal_sto7 AS days_sal_sto7 
FROM fe_dm.sale_tmp t 
LEFT JOIN fe_dm.akm_sale_tmp s ON t.shelf_id = s.shelf_id AND t.product_id = s.product_id -- 0510修改
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_sp_avgsal_recent_week","@time_3--@time_4",@time_3,@time_4);
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_sp_avgsal_recent_week',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_sp_avgsal_recent_week','dm_op_sp_avgsal_recent_week','朱星华');
  COMMIT;	
END