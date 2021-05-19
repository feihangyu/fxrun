CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_op_unstock_area_product_week_two`(in_week_end DATE)
BEGIN
  SET group_concat_max_len = 102400;
  SET @week_end := SUBDATE(
    in_week_end,
    DAYOFWEEK(in_week_end) - 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @add_day := ADDDATE(@week_end, 1);
  SET @y_m := DATE_FORMAT(@add_day, '%Y-%m'),
  @d := DAY(@add_day);
    SET @time_1 := CURRENT_TIMESTAMP();
DELETE FROM  fe_dwd.`dwd_op_unstock_detail_week`  WHERE week_end = @week_end;
INSERT INTO fe_dwd.`dwd_op_unstock_detail_week`
(
        week_end,
        shelf_id,
        product_id,
        sales_flag,
        sto_qty,
        add_user
) 
SELECT 
        @week_end AS week_end,
        t.shelf_id,
        t.product_id,
        t.sales_flag,
        t.STOCK_QUANTITY AS sto_qty,
        @add_user AS add_user 
FROM
        fe_dwd.`dwd_shelf_product_day_all` t 
WHERE t.STOCK_QUANTITY <= 0 
        AND t.sales_flag < 4 
;
  
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_op_unstock_area_product_week_two","@time_1--@time_2",@time_1,@time_2);
  
  DELETE FROM fe_dwd.`dwd_op_unstock_area_product_week` WHERE week_end = @week_end;
  INSERT INTO fe_dwd.`dwd_op_unstock_area_product_week`(
    week_end,
    business_name,
    product_id,
    sales_flag,
    shelfs,
    shelf_list,
    add_user
  )
  SELECT
    @week_end week_end,
    s.business_name,
    t.product_id,
    t.sales_flag,
    COUNT(*) shelfs,
    GROUP_CONCAT(t.shelf_id
      ORDER BY t.shelf_id) shelf_list,
    @add_user add_user
  FROM
    fe_dwd.`dwd_op_unstock_detail_week` t
    JOIN fe_dwd.`dwd_shelf_base_day_all`  s
      ON t.shelf_id = s.shelf_id
  WHERE t.week_end = @week_end
  GROUP BY s.business_name,
    t.product_id,
    t.sales_flag;
	
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_op_unstock_area_product_week_two","@time_2--@time_3",@time_2,@time_3);
	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_op_unstock_area_product_week_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_unstock_detail_week','dwd_op_unstock_area_product_week_two','宋英南');
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_unstock_area_product_week','dwd_op_unstock_area_product_week_two','宋英南');
	
  COMMIT;
END