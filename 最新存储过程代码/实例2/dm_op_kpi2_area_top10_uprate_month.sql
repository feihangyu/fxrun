CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_area_top10_uprate_month`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @month_id := DATE_FORMAT(SUBDATE(DATE_FORMAT(CURRENT_DATE,'%Y-%m-01'),INTERVAL 1 DAY),'%Y-%m'), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := CONCAT(@month_id, '-01');
  SET @month_end := LAST_DAY(@month_start);
  SET @add_day := ADDDATE(@month_end, 1);
  SET @last_month_start := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @ym := DATE_FORMAT(@month_start, '%Y%m');
  SET @last_ym := DATE_FORMAT(@last_month_start, '%Y%m');
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.shelf_id, s.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all s
  WHERE s.shelf_type IN (1, 2, 3, 5)
    AND s.activate_time < @last_month_start
    AND (
      s.revoke_time IS NULL
      OR s.revoke_time >= @add_day
    )
    AND ! ISNULL(s.shelf_id);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, SUM(t.sale_price * t.quantity_act) gmv
  FROM
    fe_dwd.dwd_order_item_refund_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @month_start
    AND t.pay_date < @add_day
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
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
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_lm_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_lm_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, SUM(t.sale_price * t.quantity_act) gmv
  FROM
    fe_dwd.dwd_order_item_refund_day t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @last_month_start
    AND t.pay_date < @month_start
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY s.business_name, t.product_id;
  
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS fe_dm.order_gmv_lm_tmp;
  CREATE TEMPORARY TABLE fe_dm.order_gmv_lm_tmp AS
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
      fe_dm.gmv_lm_tmp t
    ORDER BY t.business_name, t.gmv DESC) t
  WHERE t.order_num <= 10;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_lmp_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_lmp_tmp AS
  SELECT
    t.business_name, l.order_num, t.gmv
  FROM
    fe_dm.gmv_tmp t
    JOIN fe_dm.order_gmv_lm_tmp l
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
    fe_dm.dm_op_kpi2_area_top10_uprate_month
  WHERE month_id = @month_id;
  INSERT INTO fe_dm.dm_op_kpi2_area_top10_uprate_month (
    month_id, business_name, order_num, product_id, gmv, product_id_lm, gmv_lm, gmv_lmp, add_user
  )
  SELECT
    @month_id month_id, t.business_name, t.order_num, IFNULL(b1.product_id, 0) product_id, IFNULL(b1.gmv, 0) gmv, IFNULL(b2.product_id, 0) product_id_lm, IFNULL(b2.gmv, 0) gmv_lm, IFNULL(b12.gmv, 0) gmv_lmp, @add_user add_user
  FROM
    fe_dm.area10_tmp t
    LEFT JOIN fe_dm.order_gmv_tmp b1
      ON t.business_name = b1.business_name
      AND t.order_num = b1.order_num
    LEFT JOIN fe_dm.order_gmv_lm_tmp b2
      ON t.business_name = b2.business_name
      AND t.order_num = b2.order_num
    LEFT JOIN fe_dm.gmv_lmp_tmp b12
      ON t.business_name = b12.business_name
      AND t.order_num = b12.order_num;
	  
	  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 102;
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, 'm' indicate_type, 102 indicate_id, 'dm_op_kpi2_area_top10_uprate_month' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lm) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_month t
  WHERE t.month_id = @month_id;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 102;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, t.business_name, 'm' indicate_type, 102 indicate_id, 'dm_op_kpi2_area_top10_uprate_month' indicate_name, ROUND(SUM(gmv) / SUM(gmv_lm) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_month t
  WHERE t.month_id = @month_id
  GROUP BY t.business_name;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 202;
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, 'm' indicate_type, 202 indicate_id, 'dm_op_kpi2_area_top10_uprate_month' indicate_name, ROUND(SUM(gmv_lmp) / SUM(gmv_lm) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_month t
  WHERE t.month_id = @month_id;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE sdate = @month_start
    AND indicate_type = 'm'
    AND indicate_id = 202;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, t.business_name, 'm' indicate_type, 202 indicate_id, 'dm_op_kpi2_area_top10_uprate_month' indicate_name, ROUND(SUM(gmv_lmp) / SUM(gmv_lm) - 1, 6) indicate_value, @add_user add_user
  FROM
    fe_dm.dm_op_kpi2_area_top10_uprate_month t
  WHERE t.month_id = @month_id
  GROUP BY t.business_name;
 
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_area_top10_uprate_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_area_top10_uprate_month','dm_op_kpi2_area_top10_uprate_month','李世龙');
COMMIT;
    END