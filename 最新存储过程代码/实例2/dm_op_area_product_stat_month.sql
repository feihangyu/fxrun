CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_stat_month`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @month_id := DATE_FORMAT(SUBDATE(DATE_FORMAT(current_date,'%Y-%m-01'),INTERVAL 1 DAY),'%Y-%m'), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_first_day := CONCAT(@month_id, '-01');
  SET @month_last_day := LAST_DAY(@month_first_day);
  SET @month_add_day := ADDDATE(@month_last_day, 1);
  SET @month_last_weekend := SUBDATE(
    @month_last_day, DAYOFWEEK(@month_last_day) - 1
  );
  
SET @time_1 := CURRENT_TIMESTAMP();
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_area_tmp, fe_dm.stat_inventory_tmp, fe_dm.fil_tmp, fe_dm.sto_tmp, fe_dm.order_month_tmp, fe_dm.order_tmp, fe_dm.order_re_tmp, fe_dm.order_area_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_area_tmp AS
 SELECT
    s.shelf_id,s.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all s
  WHERE  ! ISNULL(s.shelf_id);
  
  
  CREATE INDEX idx_shelf_area_tmp_shelf_id
  ON fe_dm.shelf_area_tmp (shelf_id);
  
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_1--@time_2",@time_1,@time_2);
  CREATE TEMPORARY TABLE fe_dm.stat_inventory_tmp AS
  SELECT
    b.business_name, t.product_id, SUM(t.pre_stock_num) pre_stock_num, SUM(t.curr_fill_num) curr_fill_num, SUM(t.curr_actual_stock) curr_actual_stock, SUM(t.curr_should_stock) curr_should_stock
  FROM
    fe_dwd.dwd_sf_statistics_product_inventory t
    JOIN fe_dwd.dwd_city_business b
      ON t.city = b.city
  WHERE t.stat_month = @month_id
  GROUP BY b.business_name, t.product_id;
  
  
  CREATE INDEX idx_stat_inventory_tmp_business_name_product_id
  ON fe_dm.stat_inventory_tmp (business_name, product_id);
  
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_2--@time_3",@time_2,@time_3);
  CREATE TEMPORARY TABLE fe_dm.fil_tmp AS
  SELECT
    s.business_name, f.product_id, SUM(f.actual_fill_num) actual_fill_num3
  FROM
  fe_dwd.dwd_fill_day_inc f 
    JOIN fe_dm.shelf_area_tmp s
      ON s.shelf_id = f.shelf_id
  WHERE  f.order_status IN (3, 4)
    AND f.fill_type = 3
    AND f.fill_time >= @month_first_day
    AND f.fill_time < @month_add_day
  GROUP BY s.business_name, f.product_id;
  
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_3--@time_4",@time_3,@time_4);
 
   CREATE TEMPORARY TABLE fe_dm.sto_tmp AS
 SELECT
        business_name,
        product_id,
        SUM(cur_month_stock_days > 0) AS shelfs,
        SUM(cur_month_stock_days) AS skudays
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_day30`
		WHERE stat_date >= @month_first_day  
		AND stat_date < @month_add_day 
GROUP BY business_name,product_id
;
  CREATE INDEX idx_sto_tmp_business_name_product_id
  ON fe_dm.sto_tmp (business_name, product_id);
  
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_4--@time_5",@time_4,@time_5);
  CREATE TEMPORARY TABLE fe_dm.order_month_tmp AS
  SELECT
    o.order_id, o.pay_date order_date, o.user_id, o.shelf_id, o.product_id, o.quantity, o.sale_price, IFNULL(
      o.purchase_price, o.cost_price
    ) purchase_price, o.discount_amount, 
	IFNULL(o.product_total_amount, 0) + IFNULL(o.discount_amount, 0) + IFNULL(o.coupon_amount, 0) ogmv, 
	o.discount_amount o_discount_amount, o.coupon_amount o_coupon_amount
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month o
    WHERE  o.pay_date >= @month_first_day
      AND o.pay_date < @month_add_day;
	 
  CREATE INDEX idx_order_month_tmp_shelf_id_product_id
  ON fe_dm.order_month_tmp (shelf_id, product_id);
  CREATE INDEX idx_order_month_tmp_user_id
  ON fe_dm.order_month_tmp (user_id);
  
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_5--@time_6",@time_5,@time_6);
  CREATE TEMPORARY TABLE fe_dm.order_tmp AS
  SELECT
    sa.business_name, t.product_id, SUM(t.sale_price * t.quantity) gmv, SUM(t.quantity) quantity, SUM(t.discount_amount) discount_amount, COUNT(DISTINCT t.user_id) users, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.shelf_id) shelfs, COUNT(
      DISTINCT
      CASE
        WHEN t.ogmv > t.sale_price * t.quantity + .1
        THEN t.order_id
      END
    ) orders_related
  FROM
    fe_dm.order_month_tmp t
    JOIN fe_dm.shelf_area_tmp sa
      ON t.shelf_id = sa.shelf_id
  GROUP BY sa.business_name, t.product_id;
  CREATE INDEX idx_order_tmp_business_name_product_id
  ON fe_dm.order_tmp (business_name, product_id);
  
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_6--@time_7",@time_6,@time_7);
  CREATE TEMPORARY TABLE fe_dm.order_re_tmp AS
  SELECT
    t.business_name, t.product_id, COUNT(*) users_re
  FROM
    (SELECT
      sa.business_name, t.product_id
    FROM
      fe_dm.order_month_tmp t
      JOIN fe_dm.shelf_area_tmp sa
        ON t.shelf_id = sa.shelf_id
    GROUP BY sa.business_name, t.product_id, t.user_id
    HAVING COUNT(DISTINCT t.order_id) > 1) t
  GROUP BY t.business_name, t.product_id;
  CREATE INDEX idx_order_re_tmp_business_name_product_id
  ON fe_dm.order_re_tmp (business_name, product_id);
  
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_7--@time_8",@time_7,@time_8);
  CREATE TEMPORARY TABLE fe_dm.order_area_tmp AS
  SELECT
    sa.business_name, COUNT(DISTINCT t.user_id) users
  FROM
    fe_dm.order_month_tmp t
    JOIN fe_dm.shelf_area_tmp sa
      ON t.shelf_id = sa.shelf_id
  GROUP BY sa.business_name;
  CREATE INDEX idx_order_area_tmp_business_name
  ON fe_dm.order_area_tmp (business_name);
  
  
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_8--@time_9",@time_8,@time_9);
  DELETE
  FROM
    fe_dm.dm_op_area_product_stat_month
  WHERE month_id = @month_id;
  INSERT INTO fe_dm.dm_op_area_product_stat_month (
    month_id, region_name, business_name, product_id, product_code2, product_name, second_type_id, second_type_name, product_type, gmv, quantity, discount_amount, pre_stock_num, curr_fill_num, initial_fill_num, curr_actual_stock, curr_should_stock, shelfs_sto, shelfs_sal, skudays, orders, orders_related, users, users_area, users_re, add_user
  )
  SELECT
    @month_id, b.region_name, t.business_name, t.product_id, p.product_code2, p.product_name, p.second_type_id, 
	p.sub_type_name type_name, pdh.product_type, IFNULL(o.gmv, 0), IFNULL(o.quantity, 0), IFNULL(o.discount_amount, 0), IFNULL(t.pre_stock_num, 0), IFNULL(t.curr_fill_num, 0), IFNULL(fil.actual_fill_num3, 0), IFNULL(t.curr_actual_stock, 0), IFNULL(t.curr_should_stock, 0), IFNULL(sto.shelfs, 0), IFNULL(o.shelfs, 0), IFNULL(sto.skudays, 0), IFNULL(o.orders, 0), IFNULL(o.orders_related, 0), IFNULL(o.users, 0), IFNULL(oa.users, 0) users_area, IFNULL(oe.users_re, 0), @add_user
  FROM
    fe_dm.stat_inventory_tmp t
    LEFT JOIN fe_dm.fil_tmp fil
      ON t.business_name = fil.business_name
      AND t.product_id = fil.product_id
    LEFT JOIN fe_dm.sto_tmp sto
      ON t.business_name = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN fe_dm.order_tmp o
      ON t.business_name = o.business_name
      AND t.product_id = o.product_id
    LEFT JOIN fe_dm.order_re_tmp oe
      ON t.business_name = oe.business_name
      AND t.product_id = oe.product_id
    LEFT JOIN fe_dm.order_area_tmp oa
      ON t.business_name = oa.business_name
    JOIN
      (SELECT DISTINCT
        b.region_name, b.business_name
      FROM
        fe_dwd.dwd_city_business b) b
      ON t.business_name = b.business_name
    JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_id = p.product_id
    LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp_his pdh
      ON t.business_name = pdh.business_area
      AND t.product_id = pdh.product_id
      AND pdh.version =
      (SELECT
        vv.version_id
      FROM
        fe_dwd.dwd_op_dim_date vv              -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
      WHERE vv.sdate <= @month_last_day     -- vv.min_date <= @month_last_day
        AND vv.edate > @month_last_day);    -- vv.max_date > @month_last_day)
		
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_area_product_stat_month","@time_9--@time_10",@time_9,@time_10);
		
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_stat_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_stat_month','dm_op_area_product_stat_month','李世龙');
COMMIT;
    END