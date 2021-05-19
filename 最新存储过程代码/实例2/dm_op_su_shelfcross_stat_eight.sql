CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_su_shelfcross_stat_eight`()
BEGIN
  SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := SUBDATE(@sdate, DAY(@sdate) - 1), @add_day := ADDDATE(@sdate, 1), @month_flag := (@sdate = LAST_DAY(@sdate));
  DELETE
  FROM
    fe_dm.dm_op_su_month_stat
  WHERE sdate >= @month_start
    AND sdate < @add_day;
SET @time_3 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_month_stat (
    sdate, shelf_id, user_id, orders, skus, quantity_act, gmv, discount_amount, coupon_amount, first_order_id, min_order_date, last_order_id, max_order_date, min_ogmv, max_ogmv, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, t.user_id, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.product_id) skus, SUM(t.quantity_act) quantity_act, SUM(t.quantity_act * t.sale_price) gmv, SUM(
      t.o_discount_amount * t.quantity_act * t.sale_price / t.ogmv
    ) discount_amount, SUM(
      t.o_coupon_amount * t.quantity_act * t.sale_price / t.ogmv
    ) coupon_amount, SUBSTRING_INDEX(
      GROUP_CONCAT(t.order_id
        ORDER BY t.pay_date), ',', 1
    ) first_order_id, MIN(t.pay_date) min_order_date, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.order_id
        ORDER BY t.pay_date DESC
      ), ',', 1
    ) last_order_id, MAX(t.pay_date) max_order_date, MIN(t.ogmv) min_ogmv, MAX(t.ogmv) max_ogmv, @add_user add_user
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
  WHERE t.pay_date >= @month_start
    AND t.pay_date < @add_day
  GROUP BY t.shelf_id, t.user_id;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_3--@time_5",@time_3,@time_5);
  DELETE
  FROM
    fe_dm.dm_op_su_shelf_month_stat
  WHERE sdate >= @month_start
    AND sdate < @add_day;
SET @time_8 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_shelf_month_stat (
    sdate, shelf_id, users, orders, quantity_act, gmv, discount_amount, coupon_amount, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, COUNT(*) users, SUM(t.orders) orders, SUM(t.quantity_act) quantity_act, SUM(t.gmv) gmv, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, @add_user add_user
  FROM
    fe_dm.dm_op_su_month_stat t
  WHERE t.sdate = @sdate
  GROUP BY t.shelf_id;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_8--@time_10",@time_8,@time_10);
  DELETE
  FROM
    fe_dm.dm_op_su_user_month_stat
  WHERE sdate >= @month_start
    AND sdate < @add_day;
SET @time_13 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_user_month_stat (
    sdate, user_id, shelfs, orders, quantity_act, gmv, discount_amount, coupon_amount, first_order_id, min_order_date, first_shelf_id, last_order_id, max_order_date, last_shelf_id, min_ogmv, max_ogmv, add_user
  )
  SELECT
    @sdate sdate, t.user_id, COUNT(*) shelfs, SUM(t.orders) orders, SUM(t.quantity_act) quantity_act, SUM(t.gmv) gmv, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.first_order_id
        ORDER BY t.min_order_date
      ), ',', 1
    ) first_order_id, MIN(t.min_order_date) min_order_date, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.shelf_id
        ORDER BY t.min_order_date
      ), ',', 1
    ) first_shelf_id, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.last_order_id
        ORDER BY t.max_order_date DESC
      ), ',', 1
    ) last_order_id, MAX(t.max_order_date) max_order_date, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.shelf_id
        ORDER BY t.max_order_date DESC
      ), ',', 1
    ) last_shelf_id, MIN(t.min_ogmv) min_ogmv, MAX(t.max_ogmv) max_ogmv, @add_user add_user
  FROM
    fe_dm.dm_op_su_month_stat t
  WHERE t.sdate = @sdate
  GROUP BY t.user_id;
SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_13--@time_15",@time_13,@time_15);
  #TRUNCATE TABLE fe_dm.dm_op_su_uptolm_stat;
SET @time_18 := CURRENT_TIMESTAMP();
#INSERT INTO fe_dm.dm_op_su_uptolm_stat ( user_id, shelf_id, orders, quantity_act, gmv, discount_amount, coupon_amount, first_order_id, min_order_date, last_order_id, max_order_date, min_ogmv, max_ogmv, add_user ) SELECT t.user_id, t.shelf_id, SUM(t.orders) orders, SUM(t.quantity_act) quantity_act, SUM(t.gmv) gmv, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUBSTRING_INDEX( GROUP_CONCAT(t.first_order_id ORDER BY t.min_order_date), ',', 1 ) first_order_id,MIN(t.min_order_date)min_order_date, SUBSTRING_INDEX( GROUP_CONCAT(t.last_order_id ORDER BY t.max_order_date DESC), ',', 1 ) last_order_id,MAX(t.max_order_date)max_order_date,MIN(t.min_ogmv)min_ogmv,MAX(t.max_ogmv)max_ogmv,@add_user add_user FROM fe_dm.dm_op_su_month_stat t WHERE t.sdate < subdate(current_date,day(current_date)-1) GROUP BY t.user_id, t.shelf_id;
SET @time_20 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_18--@time_20",@time_18,@time_20);
 
   TRUNCATE TABLE fe_dm.dm_op_su_stat;
SET @time_23 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_stat (
    detail_id, user_id, shelf_id, orders, quantity_act, gmv, discount_amount, coupon_amount, first_order_id, min_order_date, last_order_id, max_order_date, min_ogmv, max_ogmv, add_user
  )
  SELECT
    t.user_id * 1e7+ t.shelf_id detail_id, t.user_id, t.shelf_id, t.orders + IFNULL(s.orders, 0) orders, t.quantity_act + IFNULL(s.quantity_act, 0) quantity_act, t.gmv + IFNULL(s.gmv, 0) gmv, t.discount_amount + IFNULL(s.discount_amount, 0) discount_amount, t.coupon_amount + IFNULL(s.coupon_amount, 0) coupon_amount, t.first_order_id, t.min_order_date, IFNULL(
      s.last_order_id, t.last_order_id
    ) last_order_id, IFNULL(
      s.max_order_date, t.max_order_date
    ) max_order_date, IF(
      s.min_ogmv < t.min_ogmv, s.min_ogmv, t.min_ogmv
    ) min_ogmv, IF(
      s.max_ogmv > t.max_ogmv, s.max_ogmv, t.max_ogmv
    ) max_ogmv, @add_user add_user
  FROM
    fe_dm.dm_op_su_uptolm_stat t
    LEFT JOIN fe_dm.dm_op_su_month_stat s
      ON s.sdate = @sdate
      AND t.user_id = s.user_id
      AND t.shelf_id = s.shelf_id;
SET @time_25 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_23--@time_25",@time_23,@time_25);
SET @time_27 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_stat (
    detail_id, user_id, shelf_id, orders, quantity_act, gmv, discount_amount, coupon_amount, first_order_id, min_order_date, last_order_id, max_order_date, min_ogmv, max_ogmv, add_user
  )
  SELECT
    t.user_id * 1e7+ t.shelf_id detail_id, t.user_id, t.shelf_id, t.orders, t.quantity_act, t.gmv, t.discount_amount, t.coupon_amount, t.first_order_id, t.min_order_date, t.last_order_id, t.max_order_date, t.min_ogmv, t.max_ogmv, @add_user add_user
  FROM
    fe_dm.dm_op_su_month_stat t
    LEFT JOIN fe_dm.dm_op_su_uptolm_stat s
      ON t.user_id = s.user_id
      AND t.shelf_id = s.shelf_id
  WHERE t.sdate = @sdate
    AND ISNULL(s.user_id);
SET @time_29 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_27--@time_29",@time_27,@time_29);
  SET @sql_str := IF(
    @month_flag, 'truncate table fe_dm.dm_op_su_uptolm_stat', 'set @sql_str:=null'
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
SET @time_34 := CURRENT_TIMESTAMP();
  SET @sql_str := IF(
    @month_flag, 'insert into fe_dm.dm_op_su_uptolm_stat select * from fe_dm.dm_op_su_stat', 'set @sql_str:=null'
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_36 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_34--@time_36",@time_34,@time_36);
  TRUNCATE TABLE fe_dm.dm_op_su_u_stat;
SET @time_41 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_u_stat (
    user_id, shelfs, orders, quantity_act, gmv, discount_amount, coupon_amount, first_order_id, min_order_date, first_shelf_id, last_order_id, max_order_date, last_shelf_id, min_ogmv, max_ogmv, add_user
  )
  SELECT
    t.user_id, COUNT(*) shelfs, SUM(t.orders) orders, SUM(t.quantity_act) quantity_act, SUM(t.gmv) gmv, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.first_order_id
        ORDER BY t.min_order_date
      ), ',', 1
    ) first_order_id, MIN(t.min_order_date) min_order_date, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.shelf_id
        ORDER BY t.min_order_date
      ), ',', 1
    ) first_shelf_id, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.last_order_id
        ORDER BY t.max_order_date DESC
      ), ',', 1
    ) last_order_id, MAX(t.max_order_date) max_order_date, SUBSTRING_INDEX(
      GROUP_CONCAT(
        t.shelf_id
        ORDER BY t.max_order_date DESC
      ), ',', 1
    ) last_shelf_id, MIN(t.min_ogmv) min_ogmv, MAX(t.max_ogmv) max_ogmv, @add_user add_user
  FROM
    fe_dm.dm_op_su_stat t
  GROUP BY t.user_id;
SET @time_43 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_41--@time_43",@time_41,@time_43);
  TRUNCATE TABLE fe_dm.dm_op_su_s_stat;
SET @time_46 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_s_stat (
    shelf_id, users, users_first, add_user
  )
  SELECT
    t.shelf_id, t.users, f.users_first, @add_user add_user
  FROM
    (SELECT
      t.shelf_id, COUNT(*) users
    FROM
      fe_dm.dm_op_su_stat t
    GROUP BY t.shelf_id) t
    LEFT JOIN
      (SELECT
        t.first_shelf_id, COUNT(*) users_first
      FROM
        fe_dm.dm_op_su_u_stat t
      GROUP BY t.first_shelf_id) f
      ON t.shelf_id = f.first_shelf_id;
SET @time_48 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_46--@time_48",@time_46,@time_48);
  TRUNCATE TABLE fe_dm.dm_op_su_shelfcross_stat;
SET @time_51 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_shelfcross_stat (
    shelf_id1, shelf_id2, users, add_user
  )
  SELECT
    t1.shelf_id shelf_id1, t2.shelf_id shelf_id2, COUNT(*) users, @add_user add_user
  FROM
    fe_dm.dm_op_su_stat t1
    JOIN fe_dm.dm_op_su_stat t2
      ON t1.user_id = t2.user_id
      AND t1.shelf_id < t2.shelf_id
  GROUP BY t1.shelf_id, t2.shelf_id;
SET @time_53 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_51--@time_53",@time_51,@time_53);
SET @time_55 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dm.dm_op_su_shelfcross_stat (
    shelf_id1, shelf_id2, users, add_user
  )
  SELECT
    t.shelf_id2 shelf_id1, t.shelf_id1 shelf_id2, t.users, @add_user add_user
  FROM
    fe_dm.dm_op_su_shelfcross_stat t;
	
SET @time_57 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_su_shelfcross_stat_eight","@time_55--@time_57",@time_55,@time_57);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_su_shelfcross_stat_eight',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('李世龙@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_month_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_shelf_month_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_user_month_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_uptolm_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_u_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_s_stat','dm_op_su_shelfcross_stat_eight','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_su_shelfcross_stat','dm_op_su_shelfcross_stat_eight','李世龙');
END