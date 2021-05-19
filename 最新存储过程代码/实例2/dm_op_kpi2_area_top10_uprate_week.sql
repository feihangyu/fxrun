CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_area_top10_uprate_week`(in_week_end DATE)
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   SET @week_end := SUBDATE(
    in_week_end, DAYOFWEEK(in_week_end) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@week_end, 1), @last_week_end := SUBDATE(@week_end, 7), @week_start := SUBDATE(@week_end, 6), @last_week_start := SUBDATE(@week_end, 13);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp AS
  SELECT
    s.shelf_id, s.business_name
  FROM
     fe_dwd.dwd_shelf_base_day_all s
  WHERE  s.shelf_type IN (1, 2, 3, 5)
    AND s.activate_time < @last_week_start
    AND (
      s.revoke_time IS NULL
      OR s.revoke_time >= @add_day
    );
	
	
  CREATE INDEX idx_shelf_id
  ON fe_dm.shelf_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp AS
  SELECT
    t.shelf_id, t.product_id, SUM(t.sale_price * t.quantity) gmv
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
  WHERE t.pay_date >= @week_start
    AND t.pay_date < @add_day
  GROUP BY t.shelf_id, t.product_id;
  
  
  CREATE INDEX idx_shelf_id
  ON fe_dm.oi_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_tmp;
  
  CREATE TEMPORARY TABLE fe_dm.gmv_tmp AS
  SELECT
    s.business_name, t.product_id, SUM(t.gmv) gmv
  FROM
    fe_dm.oi_tmp t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY s.business_name, t.product_id;
  
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS fe_dm.order_gmv_tmp;
  CREATE TEMPORARY TABLE fe_dm.order_gmv_tmp AS
  SELECT
    t.business_name, t.order_num, t.product_id, t.gmv
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @order_area
          WHEN t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @order_area := t.business_name business_name, t.product_id, t.gmv
    FROM
      fe_dm.gmv_tmp t
    ORDER BY t.business_name, t.gmv DESC) t
  WHERE t.order_num <= 10;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_lw_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_lw_tmp AS
  SELECT
    t.shelf_id, t.product_id, SUM(t.sale_price * t.quantity) gmv
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
  WHERE  t.pay_date >= @last_week_start
    AND t.pay_date < @week_start
  GROUP BY t.shelf_id, t.product_id;
  
  CREATE INDEX idx_shelf_id
  ON fe_dm.oi_lw_tmp (shelf_id);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_lw_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_lw_tmp AS
  SELECT
    s.business_name, t.product_id, SUM(t.gmv) gmv
  FROM
    fe_dm.oi_lw_tmp t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY s.business_name, t.product_id;
  
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS fe_dm.order_gmv_lw_tmp;
  CREATE TEMPORARY TABLE fe_dm.order_gmv_lw_tmp AS
  SELECT
    t.business_name, t.order_num, t.product_id, t.gmv
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @order_area
          WHEN t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @order_area := t.business_name business_name, t.product_id, t.gmv
    FROM
      fe_dm.gmv_lw_tmp t
    ORDER BY t.business_name, t.gmv DESC) t
  WHERE t.order_num <= 10;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_lwp_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_lwp_tmp AS
  SELECT
    t.business_name, l.order_num, t.gmv
  FROM
    fe_dm.gmv_tmp t
    JOIN fe_dm.order_gmv_lw_tmp l
      ON t.business_name = l.business_name
      AND t.product_id = l.product_id;
	  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.area10_tmp;
  CREATE TEMPORARY TABLE fe_dm.area10_tmp AS
  SELECT
    t.business_name, n.order_num
  FROM
    (SELECT DISTINCT
      t.business_name
    FROM
      fe_dwd.dwd_city_business t) t
    JOIN
      (SELECT
        t.number + 1 order_num
      FROM
        fe_dwd.dwd_pub_number t
      WHERE t.number < 10) n
      ON 1;
	  
	  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_week
  WHERE week_end = @week_end;
  INSERT INTO fe_dm.dm_op_kpi2_area_top10_uprate_week (
    week_end, business_name, order_num, product_id, gmv, product_id_lw, gmv_lw, gmv_lwp, add_user
  )
  SELECT
    @week_end week_end, t.business_name, t.order_num, IFNULL(b1.product_id, 0) product_id, IFNULL(b1.gmv, 0) gmv, IFNULL(b2.product_id, 0) product_id_lw, IFNULL(b2.gmv, 0) gmv_lw, IFNULL(b12.gmv, 0) gmv_lwp, @add_user add_user
  FROM
    fe_dm.area10_tmp t
    LEFT JOIN fe_dm.order_gmv_tmp b1
      ON t.business_name = b1.business_name
      AND t.order_num = b1.order_num
    LEFT JOIN fe_dm.order_gmv_lw_tmp b2
      ON t.business_name = b2.business_name
      AND t.order_num = b2.order_num
    LEFT JOIN fe_dm.gmv_lwp_tmp b12
      ON t.business_name = b12.business_name
      AND t.order_num = b12.order_num;
	  
	  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 102;
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, 'w' indicate_type, 102 indicate_id, 'dm_op_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 102;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, t.business_name, 'w' indicate_type, 102 indicate_id, 'dm_op_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 202;
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, 'w' indicate_type, 202 indicate_id, 'dm_op_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv_lwp) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE sdate = @week_end
    AND indicate_type = 'w'
    AND indicate_id = 202;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end sdate, t.business_name, 'w' indicate_type, 202 indicate_id, 'dm_op_kpi2_area_top10_uprate_week' indicate_name, ROUND(SUM(gmv_lwp) / SUM(gmv_lw) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_week t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name;
 
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_area_top10_uprate_week',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_area_top10_uprate_week','dm_op_kpi2_area_top10_uprate_week','李世龙');
COMMIT;
    END