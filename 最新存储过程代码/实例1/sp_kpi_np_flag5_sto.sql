CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_np_flag5_sto`()
begin
  set @sdate := current_date, @add_user := current_user, @timestamp := CURRENT_TIMESTAMP;
  delete
  from
    feods.fjr_kpi_np_flag5_sto
  where sdate = @sdate;
  insert into feods.fjr_kpi_np_flag5_sto (
    sdate, region, business_area, product_id, product_fe, product_name, sales_flag, stoqty, stoval, add_user
  )
  select
    @sdate, b.region_name, b.business_name, d.product_id, pd.product_fe, pd.product_name, ifnull(f.sales_flag, 0), IFNULL(sum(d.stock_quantity), 0) stock_quantity, IFNULL(
      sum(d.stock_quantity * d.sale_price), 0
    ) sto_val, @add_user
  from
    fe.sf_shelf_product_detail d
    left join fe.sf_shelf_product_detail_flag f
      on d.shelf_id = f.shelf_id
      and d.product_id = f.product_id
      and f.data_flag = 1
      and f.new_flag = 2
    join fe.sf_shelf s
      on d.shelf_id = s.shelf_id
      and s.data_flag = 1
      and s.shelf_status = 2
      and s.shelf_type in (1, 2, 3, 4, 5)
      and s.activate_time < subdate(@sdate, 14)
    join feods.fjr_city_business b
      on s.city = b.city
    join feods.zs_product_dim_sserp pd
      on b.business_name = pd.business_area
      and f.product_id = pd.product_id
      and pd.product_type = '新增（试运行）'
  where d.data_flag = 1
  group by b.business_name, d.product_id, ifnull(f.sales_flag, 0);
  CALL feods.sp_task_log (
    'sp_kpi_np_flag5_sto', @sdate, CONCAT(
      'fjr_d_cceb15598ec2686013016e8c8cbc7864', @timestamp, @add_user
    )
  );
  commit;
end