CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_sale_vs_stock_month`(in_month_id char(7))
begin
  #run after dwd_order_item_refund_day
   set @month_id := in_month_id,
  @add_user := current_user,
  @timestamp := current_timestamp,
  @str := '';
  set @month_start := concat(@month_id, '-01');
  SET @month_end := LAST_DAY(@month_start);
  SET @add_day := adddate(@month_end, 1);
  set @ym := date_format(@month_start, '%Y%m');
  drop temporary table if exists feods.shelf_tmp;
  create temporary table feods.shelf_tmp as
  select
    s.shelf_id,
    b.business_name
  from
    fe.sf_shelf s
    join feods.fjr_city_business b
      on s.city = b.city
  where s.data_flag = 1;
  create index idx_shelf_id
  on feods.shelf_tmp (shelf_id);
  drop temporary table if exists feods.sal_tmp;
  CREATE TEMPORARY TABLE feods.sal_tmp AS
  SELECT
    s.business_name,
    t.product_id,
    COUNT(DISTINCT t.shelf_id) shelfs_sal
  FROM
    fe_dwd.dwd_order_item_refund_day t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @month_start
    and t.pay_date < @add_day
  GROUP BY s.business_name,
    t.product_id;
  drop temporary table if exists feods.sto_tmp;
  create temporary table feods.sto_tmp as
  select
    s.business_name,
    t.product_id,
    count(*) shelfs_sto
  from
    fe.sf_shelf_product_stock_detail t
    join feods.shelf_tmp s
      on t.shelf_id = s.shelf_id
  where t.stat_date = @month_id
    and (
      t.day1_quantity > 0
      or t.day2_quantity > 0
      or t.day3_quantity > 0
      or t.day4_quantity > 0
      or t.day5_quantity > 0
      or t.day6_quantity > 0
      or t.day7_quantity > 0
      or t.day8_quantity > 0
      or t.day9_quantity > 0
      or t.day10_quantity > 0
      or t.day11_quantity > 0
      or t.day12_quantity > 0
      or t.day13_quantity > 0
      or t.day14_quantity > 0
      or t.day15_quantity > 0
      or t.day16_quantity > 0
      or t.day17_quantity > 0
      or t.day18_quantity > 0
      or t.day19_quantity > 0
      or t.day20_quantity > 0
      or t.day21_quantity > 0
      or t.day22_quantity > 0
      or t.day23_quantity > 0
      or t.day24_quantity > 0
      or t.day25_quantity > 0
      or t.day26_quantity > 0
      or t.day27_quantity > 0
      or t.day28_quantity > 0
      or t.day29_quantity > 0
      or t.day30_quantity > 0
      or t.day31_quantity > 0
    )
  group by s.business_name,
    t.product_id;
  delete
  from
    feods.fjr_kpi2_sale_vs_stock_month
  where month_id = @month_id;
  insert into feods.fjr_kpi2_sale_vs_stock_month (
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
      feods.sal_tmp t
    union
    all
    select
      t.business_name,
      t.product_id,
      0 shelfs_sal,
      t.shelfs_sto
    from
      feods.sto_tmp t) t
  group by t.business_name,
    t.product_id;
  delete
  from
    feods.fjr_kpi2_monitor
  where sdate = @month_start
    and indicate_type = 'm'
    and indicate_id = 103;
  insert into feods.fjr_kpi2_monitor (
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
    'fjr_kpi2_sale_vs_stock_month' indicate_name,
    round(
      sum(t.shelfs_sal) / sum(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  from
    feods.fjr_kpi2_sale_vs_stock_month t
  where t.month_id = @month_id;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 103;
  INSERT INTO feods.fjr_kpi2_monitor_area (
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
    'fjr_kpi2_sale_vs_stock_month' indicate_name,
    ROUND(
      SUM(t.shelfs_sal) / SUM(t.shelfs_sto),
      6
    ) indicate_value,
    @add_user add_user
  FROM
    feods.fjr_kpi2_sale_vs_stock_month t
  WHERE t.month_id = @month_id
  group by t.business_name;
  call feods.sp_task_log (
    'sp_kpi2_sale_vs_stock_month',
    @month_start,
    concat(
      'fjr_m_00e510140df9e7378ec866a8eeb2340c',
      @timestamp,
      @add_user
    )
  );
  commit;
end