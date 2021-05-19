CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_subtype_price_stat_three`(in_week_end DATE)
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @week_end := SUBDATE(
    in_week_end, DAYOFWEEK(in_week_end) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @week_start := SUBDATE(@week_end, 6), @add_day := ADDDATE(@week_end, 1), @next_month_last_day := LAST_DAY(
    ADDDATE(@week_end, INTERVAL 1 MONTH)
  );
  SET @next_month_add_day := ADDDATE(@next_month_last_day, 1), @next_month_id := DATE_FORMAT(@next_month_last_day, '%Y-%m');
  SET @last_year_first_day := SUBDATE(
    @next_month_add_day, INTERVAL 14 MONTH
  ), @last_year_add_day := SUBDATE(
    @next_month_add_day, INTERVAL 11 MONTH
  );
  
  DELETE
  FROM
    fe_dm.dm_op_product_price_salqty
  WHERE week_end = @week_end;
  INSERT INTO fe_dm.dm_op_product_price_salqty (
    week_end, business_name, product_id, sale_price, salqty, add_user
  )
  SELECT
    @week_end, v.business_name, oi.product_id, oi.sale_price, SUM(oi.quantity) quantity, @add_user
  FROM
  fe_dwd.dwd_pub_order_item_recent_one_month oi
    JOIN
      fe_dwd.dwd_shelf_base_day_all v
      ON oi.shelf_id = v.shelf_id
  WHERE  oi.pay_date >= @week_start
    AND oi.pay_date < @add_day
  GROUP BY v.business_name, oi.product_id, oi.sale_price;
  
  
  DELETE
  FROM
    fe_dm.dm_op_subtype_price_salqty
  WHERE week_end = @week_end;
  
  INSERT INTO fe_dm.dm_op_subtype_price_salqty (
    week_end, business_name, sub_type_id, sale_price, salqty, add_user
  )
  SELECT
    @week_end, t.business_name, p.sub_type_id, t.sale_price, SUM(t.salqty) salqty, @add_user
  FROM
    fe_dm.dm_op_product_price_salqty t
    JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_id = p.product_id
      AND p.sub_type_id IS NOT NULL
  GROUP BY t.business_name, p.sub_type_id, t.sale_price;
  
  
  DELETE
  FROM
    fe_dm.dm_subtype_price_stat
  WHERE month_id = @next_month_id;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.subtype_area_tmp;
  CREATE TEMPORARY TABLE fe_dm.subtype_area_tmp AS
  SELECT
    t.business_name, t.sub_type_id, t.sale_price, SUM(t.salqty * t.sale_price) gmv
  FROM
    fe_dm.dm_op_subtype_price_salqty t
  WHERE t.week_end < @add_day
  GROUP BY t.business_name, t.sub_type_id, t.sale_price;
  SET @order_num := 0, @business_name := '', @sub_type_id := 0;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.for_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.for_order_tmp AS
  SELECT
    t.order_num, t.business_name, t.sub_type_id, t.sale_price
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          WHEN @sub_type_id = t.sub_type_id
          AND @business_name = t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @business_name := t.business_name business_name,
	  @sub_type_id := t.sub_type_id sub_type_id, t.sale_price
    FROM
      fe_dm.subtype_area_tmp t
    ORDER BY t.business_name, t.sub_type_id, t.gmv DESC) t
  WHERE t.order_num < 4;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.max_tmp;
  CREATE TEMPORARY TABLE fe_dm.max_tmp AS
  SELECT
    t.business_name, t.sub_type_id, MIN(t.sale_price) low_price, MAX(t.sale_price) high_price
  FROM
    fe_dm.for_order_tmp t
  GROUP BY t.business_name, t.sub_type_id;
  INSERT INTO fe_dm.dm_subtype_price_stat (
    month_id, data_range, business_name, sub_type_id, top_price, low_price, high_price, add_user
  )
  SELECT
    @next_month_id month_id, 0 data_range, t.business_name business_name, t.sub_type_id, t.sale_price top_price, m.low_price, m.high_price, @add_user
  FROM
    fe_dm.for_order_tmp t
    JOIN fe_dm.max_tmp m
      ON t.business_name = m.business_name
      AND t.sub_type_id = m.sub_type_id
  WHERE t.order_num = 1;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.subtype_tmp;
  CREATE TEMPORARY TABLE fe_dm.subtype_tmp AS
  SELECT
    t.sub_type_id, t.sale_price, SUM(t.gmv) gmv
  FROM
    fe_dm.subtype_area_tmp t
  GROUP BY t.sub_type_id, t.sale_price;
  SET @order_num := 0, @sub_type_id := 0;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.for_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.for_order_tmp AS
  SELECT
    t.order_num, t.sub_type_id, t.sale_price
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @sub_type_id
          WHEN t.sub_type_id
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @sub_type_id := t.sub_type_id sub_type_id, t.sale_price
    FROM
      fe_dm.subtype_tmp t
    ORDER BY t.sub_type_id, t.gmv DESC) t
  WHERE t.order_num < 4;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.max_tmp;
  CREATE TEMPORARY TABLE fe_dm.max_tmp AS
  SELECT
    t.sub_type_id, MIN(t.sale_price) low_price, MAX(t.sale_price) high_price
  FROM
    fe_dm.for_order_tmp t
  GROUP BY t.sub_type_id;
  INSERT INTO fe_dm.dm_subtype_price_stat (
    month_id, data_range, business_name, sub_type_id, top_price, low_price, high_price, add_user
  )
  SELECT
    @next_month_id month_id, 0 data_range, '全国' business_name, t.sub_type_id, t.sale_price top_price, m.low_price, m.high_price, @add_user
  FROM
    fe_dm.for_order_tmp t
    JOIN fe_dm.max_tmp m
      ON t.sub_type_id = m.sub_type_id
  WHERE t.order_num = 1;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.subtype_area_tmp;
  CREATE TEMPORARY TABLE fe_dm.subtype_area_tmp AS
  SELECT
    t.business_name, t.sub_type_id, t.sale_price, SUM(t.salqty * t.sale_price) gmv
  FROM
    fe_dm.dm_op_subtype_price_salqty t
  WHERE t.week_end >= @last_year_first_day
    AND t.week_end < @last_year_add_day
  GROUP BY t.business_name, t.sub_type_id, t.sale_price;
  SET @order_num := 0, @business_name := '', @sub_type_id := 0;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.for_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.for_order_tmp AS
  SELECT
    t.order_num, t.business_name, t.sub_type_id, t.sale_price
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          WHEN @sub_type_id = t.sub_type_id
          AND @business_name = t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @business_name := t.business_name business_name, @sub_type_id := t.sub_type_id sub_type_id, t.sale_price
    FROM
      fe_dm.subtype_area_tmp t
    ORDER BY t.business_name, t.sub_type_id, t.gmv DESC) t
  WHERE t.order_num < 4;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.max_tmp;
  CREATE TEMPORARY TABLE fe_dm.max_tmp AS
  SELECT
    t.business_name, t.sub_type_id, MIN(t.sale_price) low_price, MAX(t.sale_price) high_price
  FROM
    fe_dm.for_order_tmp t
  GROUP BY t.business_name, t.sub_type_id;
  INSERT INTO fe_dm.dm_subtype_price_stat (
    month_id, data_range, business_name, sub_type_id, top_price, low_price, high_price, add_user
  )
  SELECT
    @next_month_id month_id, 1 data_range, t.business_name business_name, t.sub_type_id, t.sale_price top_price, m.low_price, m.high_price, @add_user
  FROM
    fe_dm.for_order_tmp t
    JOIN fe_dm.max_tmp m
      ON t.business_name = m.business_name
      AND t.sub_type_id = m.sub_type_id
  WHERE t.order_num = 1;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.subtype_tmp;
  CREATE TEMPORARY TABLE fe_dm.subtype_tmp AS
  SELECT
    t.sub_type_id, t.sale_price, SUM(t.gmv) gmv
  FROM
    fe_dm.subtype_area_tmp t
  GROUP BY t.sub_type_id, t.sale_price;
  SET @order_num := 0, @sub_type_id := 0;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.for_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.for_order_tmp AS
  SELECT
    t.order_num, t.sub_type_id, t.sale_price
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @sub_type_id
          WHEN t.sub_type_id
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @sub_type_id := t.sub_type_id sub_type_id, t.sale_price
    FROM
      fe_dm.subtype_tmp t
    ORDER BY t.sub_type_id, t.gmv DESC) t
  WHERE t.order_num < 4;
  DROP TEMPORARY TABLE IF EXISTS fe_dm.max_tmp;
  CREATE TEMPORARY TABLE fe_dm.max_tmp AS
  SELECT
    t.sub_type_id, MIN(t.sale_price) low_price, MAX(t.sale_price) high_price
  FROM
    fe_dm.for_order_tmp t
  GROUP BY t.sub_type_id;
  INSERT INTO fe_dm.dm_subtype_price_stat (
    month_id, data_range, business_name, sub_type_id, top_price, low_price, high_price, add_user
  )
  SELECT
    @next_month_id month_id, 1 data_range, '全国' business_name, t.sub_type_id, t.sale_price top_price, m.low_price, m.high_price, @add_user
  FROM
    fe_dm.for_order_tmp t
    JOIN fe_dm.max_tmp m
      ON t.sub_type_id = m.sub_type_id
  WHERE t.order_num = 1;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_subtype_price_stat_three',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_price_salqty','dm_subtype_price_stat_three','李世龙');
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_subtype_price_salqty','dm_subtype_price_stat_three','李世龙');
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_subtype_price_stat','dm_subtype_price_stat_three','李世龙');
COMMIT;
	END