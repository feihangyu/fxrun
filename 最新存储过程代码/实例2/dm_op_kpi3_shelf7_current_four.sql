CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi3_shelf7_current_four`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1), @week_flag := (WEEKDAY(@sdate) = 6), @week_start := SUBDATE(@sdate, WEEKDAY(@sdate)), @month_flag := (@sdate = LAST_DAY(@sdate)), @month_start := SUBDATE(@sdate, DAY(@sdate) - 1);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, t.shelf_name LIKE '%测试%' is_test
  FROM
     fe_dwd.dwd_shelf_base_day_all t
  WHERE  t.shelf_type = 7
    AND ! ISNULL(t.shelf_id);
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.price_tmp;
  CREATE TEMPORARY TABLE fe_dm.price_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id, t.product_id, t.sale_price
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_slot_stock_day
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_slot_stock_day (
    sdate, shelf_id, skus, pskus, slots, pslots, slots1, pslots1, slots2, pslots2, slots3, pslots3, slots4, pslots4, stock_num, pstock_num, stock_val, pstock_val, stock_num1, pstock_num1, stock_val1, pstock_val1, stock_num2, pstock_num2, stock_val2, pstock_val2, stock_num3, pstock_num3, stock_val3, pstock_val3, stock_num4, pstock_num4, stock_val4, pstock_val4, slot_capacity_limit, slot_capacity_limit1, slot_capacity_limit2, slot_capacity_limit3, slot_capacity_limit4, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, COUNT(DISTINCT t.product_id) skus, COUNT(
      DISTINCT IF(t.stock_num > 0, t.product_id, NULL)
    ) pskus, COUNT(*) slots, SUM(t.stock_num > 0) pslots, SUM(t.slot_status = 1) slots1, SUM(t.slot_status = 1 && t.stock_num > 0) pslots1, SUM(t.slot_status = 2) slots2, SUM(t.slot_status = 2 && t.stock_num > 0) pslots2, SUM(t.slot_status = 3) slots3, SUM(t.slot_status = 3 && t.stock_num > 0) pslots3, SUM(t.slot_status = 4) slots4, SUM(t.slot_status = 4 && t.stock_num > 0) pslots4, SUM(t.stock_num) stock_num, SUM(IF(t.stock_num > 0, t.stock_num, 0)) pstock_num, SUM(t.stock_num * p.sale_price) stock_val, SUM(
      IF(
        t.stock_num > 0, t.stock_num * p.sale_price, 0
      )
    ) pstock_val, SUM(
      IF(t.slot_status = 1, t.stock_num, 0)
    ) stock_num1, SUM(
      IF(
        t.slot_status = 1 && t.stock_num > 0, t.stock_num, 0
      )
    ) pstock_num1, SUM(
      IF(
        t.slot_status = 1, t.stock_num * p.sale_price, 0
      )
    ) stock_val1, SUM(
      IF(
        t.slot_status = 1 && t.stock_num > 0, t.stock_num * p.sale_price, 0
      )
    ) pstock_val1, SUM(
      IF(t.slot_status = 2, t.stock_num, 0)
    ) stock_num2, SUM(
      IF(
        t.slot_status = 2 && t.stock_num > 0, t.stock_num, 0
      )
    ) pstock_num2, SUM(
      IF(
        t.slot_status = 2, t.stock_num * p.sale_price, 0
      )
    ) stock_val2, SUM(
      IF(
        t.slot_status = 2 && t.stock_num > 0, t.stock_num * p.sale_price, 0
      )
    ) pstock_val2, SUM(
      IF(t.slot_status = 3, t.stock_num, 0)
    ) stock_num3, SUM(
      IF(
        t.slot_status = 3 && t.stock_num > 0, t.stock_num, 0
      )
    ) pstock_num3, SUM(
      IF(
        t.slot_status = 3, t.stock_num * p.sale_price, 0
      )
    ) stock_val3, SUM(
      IF(
        t.slot_status = 3 && t.stock_num > 0, t.stock_num * p.sale_price, 0
      )
    ) pstock_val3, SUM(
      IF(t.slot_status = 4, t.stock_num, 0)
    ) stock_num4, SUM(
      IF(
        t.slot_status = 4 && t.stock_num > 0, t.stock_num, 0
      )
    ) pstock_num4, SUM(
      IF(
        t.slot_status = 4, t.stock_num * p.sale_price, 0
      )
    ) stock_val4, SUM(
      IF(
        t.slot_status = 4 && t.stock_num > 0, t.stock_num * p.sale_price, 0
      )
    ) pstock_val4, SUM(t.slot_capacity_limit) slot_capacity_limit, SUM(
      IF(
        t.slot_status = 1, t.slot_capacity_limit, 0
      )
    ) slot_capacity_limit1, SUM(
      IF(
        t.slot_status = 2, t.slot_capacity_limit, 0
      )
    ) slot_capacity_limit2, SUM(
      IF(
        t.slot_status = 3, t.slot_capacity_limit, 0
      )
    ) slot_capacity_limit3, SUM(
      IF(
        t.slot_status = 4, t.slot_capacity_limit, 0
      )
    ) slot_capacity_limit4, @add_user add_user
  FROM
    fe_dwd.dwd_shelf_machine_slot_type t
    LEFT JOIN fe_dm.price_tmp p
      ON t.shelf_id = p.shelf_id
      AND t.product_id = p.product_id
  WHERE  ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id;
  
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_slot_sale_day
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_slot_sale_day (
    sdate, shelf_id, records, records_nsto, records_nenough, slots, skus, orders, change_quantity, change_val, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, COUNT(*) records, SUM(t.old_quantity <= 0) records_nsto, SUM(
      t.old_quantity + t.change_quantity < 0
    ) records_nenough, COUNT(DISTINCT t.slot_code) slots, COUNT(DISTINCT t.product_id) skus, COUNT(DISTINCT t.source_id) orders, - SUM(t.change_quantity) change_quantity, - SUM(t.change_quantity * p.sale_price) change_val, @add_user add_user
  FROM
    fe_dwd.dwd_sf_shelf_slot_stock_record t
    LEFT JOIN fe_dm.price_tmp p
      ON t.shelf_id = p.shelf_id
      AND t.product_id = p.product_id
  WHERE  t.source_type = 1
    AND t.add_time >= @sdate
    AND t.add_time < @add_day
  GROUP BY t.shelf_id;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_shelf_stock_day
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_shelf_stock_day (
    sdate, shelf_id, sales_flag0, new_flag0, stock_quantity, stock_val, pskus, nskus, pstock_quantity, pstock_val, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, IFNULL(t.sales_flag, 0) sales_flag0, 
	IFNULL(t.new_flag, 0) = 1 new_flag0, SUM(t.stock_quantity) stock_quantity, 
	SUM(t.stock_quantity * t.sale_price) stock_val, SUM(t.stock_quantity > 0) pskus, 
	SUM(t.stock_quantity < 0) nskus, SUM(
      IF(
        t.stock_quantity > 0, t.stock_quantity, 0
      )
    ) pstock_quantity, SUM(
      IF(
        t.stock_quantity > 0, t.stock_quantity * t.sale_price, 0
      )
    ) pstock_val, @add_user add_user
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON t.shelf_id = s.shelf_id
      AND  s.shelf_type = 7
  GROUP BY t.shelf_id, sales_flag0, new_flag0
  HAVING SUM(t.stock_quantity) != 0
    OR pstock_quantity != 0;
	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_monitor
  WHERE sdate = @sdate
    AND indicate_name IN (
      '货道动销率', '严重滞销金额占比', '缺货货道率'
    );
  SELECT
    @pslots := SUM(t.pslots), @slots := SUM(t.slots)
  FROM
    fe_dm.dm_op_kpi3_shelf7_slot_stock_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! is_test
  WHERE t.sdate = @sdate;
  
  
  SELECT
    @slots_sale := SUM(t.slots)
  FROM
    fe_dm.dm_op_kpi3_shelf7_slot_sale_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! is_test
  WHERE t.sdate = @sdate;
  
  
  SELECT
    @flag5_rate_d := SUM(
      IF(
        t.sales_flag0 = 5 && t.new_flag0 = 0, t.pstock_val, 0
      )
    ) / SUM(t.pstock_val)
  FROM
    fe_dm.dm_op_kpi3_shelf7_shelf_stock_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! is_test
  WHERE t.sdate = @sdate;
  
  
  SELECT
    @flag5_rate_w := SUM(
      IF(
        t.sales_flag0 = 5 && t.new_flag0 = 0, t.pstock_val, 0
      )
    ) / SUM(t.pstock_val) indicate_value
  FROM
    fe_dm.dm_op_kpi3_shelf7_shelf_stock_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! is_test
  WHERE @week_flag
    AND t.sdate BETWEEN @week_start
    AND @sdate;
	
	
  SELECT
    @flag5_rate_m := SUM(
      IF(
        t.sales_flag0 = 5 && t.new_flag0 = 0, t.pstock_val, 0
      )
    ) / SUM(t.pstock_val) indicate_value
  FROM
    fe_dm.dm_op_kpi3_shelf7_shelf_stock_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! is_test
  WHERE @month_flag
    AND t.sdate BETWEEN @month_start
    AND @sdate;
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.monitor_tmp;
  CREATE TEMPORARY TABLE fe_dm.monitor_tmp (
    indicate_type CHAR(1), indicate_name VARCHAR (48), indicate_value DECIMAL (18, 6), PRIMARY KEY (indicate_type, indicate_name)
  );
  SELECT
    @sql_str := CONCAT(
      "INSERT INTO fe_dm.monitor_tmp(indicate_type,indicate_name,indicate_value)VALUES", 
	  "('d','货道动销率',@slots_sale / @slots),", "('d','严重滞销金额占比',@flag5_rate_d),
	  ('w','严重滞销金额占比',@flag5_rate_w),('m','严重滞销金额占比',@flag5_rate_m),", 
	  "('d','缺货货道率',1- @pslots / @slots);"
    );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_monitor (
    indicate_type, sdate, indicate_name, indicate_value, add_user
  )
  SELECT
    t.indicate_type, @sdate sdate, t.indicate_name, t.indicate_value, @add_user add_user
  FROM
    fe_dm.monitor_tmp t
  WHERE ! ISNULL(t.indicate_value)
    AND (
      t.indicate_type = 'd'
      OR (
        @week_flag
        AND t.indicate_type = 'w'
      )
      OR (
        @month_flag
        AND t.indicate_type = 'm'
      )
    );
  
	
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi3_shelf7_current_four',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_monitor','dm_op_kpi3_shelf7_current_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_slot_stock_day','dm_op_kpi3_shelf7_current_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_slot_sale_day','dm_op_kpi3_shelf7_current_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_shelf_stock_day','dm_op_kpi3_shelf7_current_four','李世龙');
COMMIT;
    END