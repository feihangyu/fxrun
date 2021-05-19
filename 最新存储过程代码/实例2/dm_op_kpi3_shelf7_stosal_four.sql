CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi3_shelf7_stosal_four`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE,INTERVAL 1 DAY), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1), @week_flag := (WEEKDAY(@sdate) = 6),
  @week_start := SUBDATE(@sdate, WEEKDAY(@sdate)),
  @month_flag := (@sdate = LAST_DAY(@sdate)),
  @month_start := SUBDATE(@sdate, DAY(@sdate) - 1), 
  @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  -- 注意，如果当天执行失败。第二天不能补数据。因为库存会发生变化
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (
    PRIMARY KEY (shelf_id), KEY (shelf_code)
  ) AS
  SELECT
    t.shelf_id, t.shelf_code, t.business_name, 
	IFNULL(m.machine_type_id, 0) machine_type_id
  FROM
   fe_dwd.dwd_shelf_base_day_all  t
    LEFT JOIN fe_dwd.dwd_shelf_machine_info m
      ON t.shelf_id = m.shelf_id
  WHERE  t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.psale_day_tmp;
  CREATE TEMPORARY TABLE fe_dm.psale_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_sale, 
	COUNT(
      DISTINCT IF(
        t.quantity_shipped > 0, t.shelf_id, NULL
      )
    ) shelfs_sale_shipped
  FROM
  fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.machine_type_id)
  WHERE t.order_type = 3
    AND  t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.psale_week_tmp;
  CREATE TEMPORARY TABLE fe_dm.psale_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_sale, 
	COUNT(
      DISTINCT IF(
        t.quantity_shipped > 0, t.shelf_id, NULL
      )
    ) shelfs_sale_shipped
  FROM
   fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.machine_type_id)
  WHERE @week_flag
    AND t.order_type = 3
    AND t.pay_date >= @week_start
    AND t.pay_date < @add_day
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.psale_month_tmp;
  CREATE TEMPORARY TABLE fe_dm.psale_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_sale, COUNT(
      DISTINCT IF(
        t.quantity_shipped > 0, t.shelf_id, NULL
      )
    ) shelfs_sale_shipped
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.machine_type_id)
  WHERE @month_flag
    AND t.order_type = 3
    AND t.pay_date >= @month_start
    AND t.pay_date < @add_day
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.psale_yht_day_tmp;
  CREATE TEMPORARY TABLE fe_dm.psale_yht_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name,
	s.machine_type_id, COUNT(DISTINCT s.shelf_id) shelfs_sale, 
	COUNT(DISTINCT s.shelf_id) shelfs_sale_shipped
  FROM
   fe_dwd.dwd_pub_order_shelf_product_yht t
    JOIN fe_dm.shelf_tmp s
      ON (
        ! ISNULL(t.shelf_id)
        AND t.shelf_id = s.shelf_id
      )
      OR (
        ISNULL(t.shelf_id)
        AND t.asset_id = s.shelf_code
      )
  WHERE t.payTime >= @sdate
    AND t.payTime < @add_day
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY product_id, s.business_name, s.machine_type_id;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.psale_yht_week_tmp;
  CREATE TEMPORARY TABLE fe_dm.psale_yht_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, 
	COUNT(DISTINCT s.shelf_id) shelfs_sale, COUNT(DISTINCT s.shelf_id) shelfs_sale_shipped
  FROM
    fe_dwd.dwd_pub_order_shelf_product_yht t
    JOIN fe_dm.shelf_tmp s
      ON (
        ! ISNULL(t.shelf_id)
        AND t.shelf_id = s.shelf_id
      )
      OR (
        ISNULL(t.shelf_id)
        AND t.asset_id = s.shelf_code
      )
  WHERE @week_flag
    AND t.payTime >= @week_start
    AND t.payTime < @add_day
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY product_id, s.business_name, s.machine_type_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.psale_yht_month_tmp;
  CREATE TEMPORARY TABLE fe_dm.psale_yht_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id , s.business_name, s.machine_type_id, 
	COUNT(DISTINCT s.shelf_id) shelfs_sale, COUNT(DISTINCT s.shelf_id) shelfs_sale_shipped
  FROM
    fe_dwd.dwd_pub_order_shelf_product_yht t
    JOIN fe_dm.shelf_tmp s
      ON (
        ! ISNULL(t.shelf_id)
        AND t.shelf_id = s.shelf_id
      )
      OR (
        ISNULL(t.shelf_id)
        AND t.asset_id = s.shelf_code
      )
  WHERE @month_flag
    AND t.payTime >= @month_start
    AND t.payTime < @add_day
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY product_id, s.business_name, s.machine_type_id;
  
  DELETE
    t
  FROM
    fe_dm.psale_day_tmp t
    JOIN fe_dm.psale_yht_day_tmp yht
      ON t.product_id = yht.product_id
      AND t.business_name = yht.business_name
      AND t.machine_type_id = yht.machine_type_id;
	  
  DELETE
    t
  FROM
    fe_dm.psale_week_tmp t
    JOIN fe_dm.psale_yht_week_tmp yht
      ON t.product_id = yht.product_id
      AND t.business_name = yht.business_name
      AND t.machine_type_id = yht.machine_type_id
  WHERE @week_flag;
  
  DELETE
    t
  FROM
    fe_dm.psale_month_tmp t
    JOIN fe_dm.psale_yht_month_tmp yht
      ON t.product_id = yht.product_id
      AND t.business_name = yht.business_name
      AND t.machine_type_id = yht.machine_type_id
  WHERE @month_flag;
  
  
  INSERT INTO fe_dm.psale_day_tmp
  SELECT
    *
  FROM
    fe_dm.psale_yht_day_tmp;
	
	
  INSERT INTO fe_dm.psale_week_tmp
  SELECT
    *
  FROM
    fe_dm.psale_yht_week_tmp;
	
  INSERT INTO fe_dm.psale_month_tmp
  SELECT
    *
  FROM
    fe_dm.psale_yht_month_tmp;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sp_stock_tmp;
  
 
  CREATE TEMPORARY TABLE fe_dm.sp_stock_tmp (product_id INT, shelf_id INT);
  
INSERT INTO fe_dm.sp_stock_tmp (product_id, shelf_id) 
SELECT 
  t.product_id,
  t.shelf_id 
FROM
  fe_dwd.dwd_shelf_product_day_all t 
  JOIN fe_dm.shelf_tmp s 
    ON t.shelf_id = s.shelf_id 
WHERE t.stock_quantity >0;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.pstock_day_tmp;
  CREATE TEMPORARY TABLE fe_dm.pstock_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(*) shelfs_stock
  FROM
    fe_dm.sp_stock_tmp t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.pstock_week_tmp;
  CREATE TEMPORARY TABLE fe_dm.pstock_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(DISTINCT t.shelf_id) shelfs_stock
  FROM
    fe_dm.sp_stock_tmp t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE @week_flag
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(s.machine_type_id)
  GROUP BY t.product_id, s.business_name, s.machine_type_id
  HAVING @week_flag;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.pstock_month_tmp;
  CREATE TEMPORARY TABLE fe_dm.pstock_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, s.business_name, s.machine_type_id, COUNT(*) shelfs_stock
  FROM
    fe_dwd.dwd_shelf_product_sto_sal_30_days t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(t.product_id)
      AND ! ISNULL(s.business_name)
      AND ! ISNULL(s.machine_type_id)
  WHERE @month_flag
    AND t.sdate >= @month_start
    AND t.stock_quantity > 0
  GROUP BY t.product_id, s.business_name, s.machine_type_id
  HAVING @month_flag;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.product_day_tmp, fe_dm.product_week_tmp, fe_dm.product_month_tmp;
  CREATE TEMPORARY TABLE fe_dm.product_day_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    fe_dm.psale_day_tmp t
  UNION
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    fe_dm.pstock_day_tmp t;
  CREATE TEMPORARY TABLE fe_dm.product_week_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    fe_dm.psale_week_tmp t
  UNION
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    fe_dm.pstock_week_tmp t;
  CREATE TEMPORARY TABLE fe_dm.product_month_tmp (
    PRIMARY KEY (
      product_id, business_name, machine_type_id
    )
  ) AS
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    fe_dm.psale_month_tmp t
  UNION
  SELECT
    t.product_id, t.business_name, t.machine_type_id
  FROM
    fe_dm.pstock_month_tmp t;
	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_product_sale_stock_day
  WHERE sdate = @sdate;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_product_sale_stock_week
  WHERE @week_flag
    AND sdate = @sdate;
	
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_product_sale_stock_month
  WHERE @month_flag
    AND sdate = @sdate;
	
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_product_sale_stock_day (
    sdate, product_id, business_name, machine_type_id, shelfs_sale, shelfs_sale_shipped, shelfs_stock, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, t.machine_type_id, sal.shelfs_sale, sal.shelfs_sale_shipped, sto.shelfs_stock, @add_user add_user
  FROM
    fe_dm.product_day_tmp t
    LEFT JOIN fe_dm.psale_day_tmp sal
      ON t.product_id = sal.product_id
      AND t.business_name = sal.business_name
      AND t.machine_type_id = sal.machine_type_id
    LEFT JOIN fe_dm.pstock_day_tmp sto
      ON t.product_id = sto.product_id
      AND t.business_name = sto.business_name
      AND t.machine_type_id = sto.machine_type_id;
	  
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_product_sale_stock_week (
    sdate, product_id, business_name, machine_type_id, shelfs_sale, shelfs_sale_shipped, shelfs_stock, add_user
  )
  SELECT
    @sdate sdate, t.product_id, t.business_name, t.machine_type_id, sal.shelfs_sale, sal.shelfs_sale_shipped, sto.shelfs_stock, @add_user add_user
  FROM
    fe_dm.product_week_tmp t
    LEFT JOIN fe_dm.psale_week_tmp sal
      ON t.product_id = sal.product_id
      AND t.business_name = sal.business_name
      AND t.machine_type_id = sal.machine_type_id
    LEFT JOIN fe_dm.pstock_week_tmp sto
      ON t.product_id = sto.product_id
      AND t.business_name = sto.business_name
      AND t.machine_type_id = sto.machine_type_id
  WHERE @week_flag;
  
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_product_sale_stock_month (
    sdate, month_id, product_id, business_name, machine_type_id, shelfs_sale, shelfs_sale_shipped, shelfs_stock, add_user
  )
  SELECT
    @sdate sdate, @y_m month_id, t.product_id, t.business_name, t.machine_type_id, sal.shelfs_sale, sal.shelfs_sale_shipped, sto.shelfs_stock, @add_user add_user
  FROM
    fe_dm.product_month_tmp t
    LEFT JOIN fe_dm.psale_month_tmp sal
      ON t.product_id = sal.product_id
      AND t.business_name = sal.business_name
      AND t.machine_type_id = sal.machine_type_id
    LEFT JOIN fe_dm.pstock_month_tmp sto
      ON t.product_id = sto.product_id
      AND t.business_name = sto.business_name
      AND t.machine_type_id = sto.machine_type_id
  WHERE @month_flag;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi3_shelf7_monitor
  WHERE sdate = @sdate
    AND indicate_name IN ('商品动销率');
  INSERT INTO fe_dm.dm_op_kpi3_shelf7_monitor (
    indicate_type, sdate, indicate_name, indicate_value, add_user
  )
  SELECT
    t.indicate_type, @sdate sdate, '商品动销率' indicate_name, t.indicate_value, @add_user add_user
  FROM
    (SELECT
      'd' indicate_type, SUM(t.shelfs_sale_shipped) / SUM(t.shelfs_stock) indicate_value
    FROM
      fe_dm.dm_op_kpi3_shelf7_product_sale_stock_day t
    WHERE t.sdate = @sdate
    UNION
    ALL
    SELECT
      'w' indicate_type, SUM(t.shelfs_sale_shipped) / SUM(t.shelfs_stock) indicate_value
    FROM
      fe_dm.dm_op_kpi3_shelf7_product_sale_stock_week t
    WHERE @week_flag
      AND t.sdate = @sdate
    UNION
    ALL
    SELECT
      'm' indicate_type, SUM(t.shelfs_sale_shipped) / SUM(t.shelfs_stock) indicate_value
    FROM
      fe_dm.dm_op_kpi3_shelf7_product_sale_stock_month t
    WHERE @month_flag
      AND t.sdate = @sdate) t
  WHERE ! ISNULL(t.indicate_value);
  
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi3_shelf7_stosal_four',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_monitor','dm_op_kpi3_shelf7_stosal_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_product_sale_stock_day','dm_op_kpi3_shelf7_stosal_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_product_sale_stock_month','dm_op_kpi3_shelf7_stosal_four','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi3_shelf7_product_sale_stock_week','dm_op_kpi3_shelf7_stosal_four','李世龙');
COMMIT;
    END