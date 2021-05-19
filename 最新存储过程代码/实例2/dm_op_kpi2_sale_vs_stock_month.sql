CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_sale_vs_stock_month`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   set @month_id := DATE_FORMAT(SUBDATE(DATE_FORMAT(current_date,'%Y-%m-01'),INTERVAL 1 DAY),'%Y-%m'),
  @add_user := current_user,
  @timestamp := current_timestamp,
  @str := '';
  set @month_start := concat(@month_id, '-01');
  SET @month_end := LAST_DAY(@month_start);
  SET @add_day := adddate(@month_end, 1);
  set @ym := date_format(@month_start, '%Y%m');
  SET @month_first_day := CONCAT(@month_id, '-01');
  SET @month_last_day := LAST_DAY(@month_first_day);
  SET @month_add_day := ADDDATE(@month_last_day, 1);
  SET @month_last_weekend := SUBDATE(
    @month_last_day, DAYOFWEEK(@month_last_day) - 1
  );
  
  
  drop temporary table if exists fe_dm.shelf_tmp;
  create temporary table fe_dm.shelf_tmp as
  select
    s.shelf_id,
    s.business_name
  from
    fe_dwd.dwd_shelf_base_day_all s;
  
  
  create index idx_shelf_id
  on fe_dm.shelf_tmp (shelf_id);
  drop temporary table if exists fe_dm.sal_tmp;
  CREATE TEMPORARY TABLE fe_dm.sal_tmp AS
  SELECT
    s.business_name,
    t.product_id,
    COUNT(DISTINCT t.shelf_id) shelfs_sal
  FROM
    fe_dwd.dwd_order_item_refund_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @month_start
    and t.pay_date < @add_day
  GROUP BY s.business_name,
    t.product_id;
	
	
	
  DROP TEMPORARY TABLE if exists fe_dm.sto_tmp;
  CREATE TEMPORARY TABLE fe_dm.sto_tmp AS
 SELECT
        business_name,
        product_id,
        SUM(cur_month_stock_days > 0) AS shelfs_sto
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_day30`
		WHERE stat_date >= @month_first_day  
		AND stat_date < @month_add_day 
GROUP BY business_name,product_id
;	
	
	
	
  delete
  from
    fe_dm.dm_op_kpi2_sale_vs_stock_month
  where month_id = @month_id;
  insert into fe_dm.dm_op_kpi2_sale_vs_stock_month (
    month_id,
    business_name,
    product_id,
    shelfs_sal,
    shelfs_sto,
    add_user
  )
  select
    @month_id month_id,
    t.business_name,
    t.product_id,
    sum(t.shelfs_sal) shelfs_sal,
    sum(t.shelfs_sto) shelfs_sto,
    @add_user add_user
  from
    (select
      t.business_name,
      t.product_id,
      t.shelfs_sal,
      0 shelfs_sto
    from
      fe_dm.sal_tmp t
    union
    all
    select
      t.business_name,
      t.product_id,
      0 shelfs_sal,
      t.shelfs_sto
    from
      fe_dm.sto_tmp t) t
  group by t.business_name,
    t.product_id;
  delete
  from
    fe_dm.dm_op_kpi2_monitor
  where sdate = @month_start
    and indicate_type = 'm'
    and indicate_id = 103;
  insert into fe_dm.dm_op_kpi2_monitor (
    sdate,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  select
    @month_start sdate,
    'm' indicate_type,
    103 indicate_id,
    'dm_op_kpi2_sale_vs_stock_month' indicate_name,
    round(
      sum(t.shelfs_sal) / sum(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  from
    fe_dm.dm_op_kpi2_sale_vs_stock_month t
  where t.month_id = @month_id;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 103;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate,
    business_name,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @month_start sdate,
    t.business_name,
    'm' indicate_type,
    103 indicate_id,
    'dm_op_kpi2_sale_vs_stock_month' indicate_name,
    ROUND(
      SUM(t.shelfs_sal) / SUM(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_sale_vs_stock_month t
  WHERE t.month_id = @month_id
  group by t.business_name;
  
  
  
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_sale_vs_stock_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_sale_vs_stock_month','dm_op_kpi2_sale_vs_stock_month','李世龙');
COMMIT;
    END