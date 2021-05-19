CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_week_product_stock_detail_tmp`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET    @week_end := SUBDATE(CURRENT_DATE,DAYOFWEEK(CURRENT_DATE) - 1),
       @add_day := ADDDATE(@week_end, 1),
       @week_start := SUBDATE(@week_end, 6);
truncate table fe_dm.dm_op_shelf_week_product_stock_detail_tmp;
insert into fe_dm.dm_op_shelf_week_product_stock_detail_tmp(week_end,shelf_id,product_id,qty_sal_week,days_sal_week)
SELECT 
@week_end as week_end,
shelf_id,product_id,
SUM(IFNULL(sal_qty,0)) AS qty_sal_week,
SUM(CASE WHEN stock_quantity=0 AND sal_qty=0 THEN 0 ELSE 1 END) AS days_sal_week
FROM fe_dwd.`dwd_shelf_product_sto_sal_30_days` FORCE INDEX(sdate)
WHERE sdate>=@week_start AND sdate<=@week_end
GROUP BY shelf_id,product_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_week_product_stock_detail_tmp',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_week_product_stock_detail_tmp','dm_op_shelf_week_product_stock_detail_tmp','朱星华');
  COMMIT;	
END