CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_ds7p_sal_fil`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := subdate(current_date,interval 1 day);
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
 DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id
  FROM
     fe_dwd.dwd_shelf_base_day_all t
  WHERE t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
	
	
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.quantity_act) quantity_act, SUM(t.quantity_act * t.sale_price) gmv
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_tmp;
  CREATE TEMPORARY TABLE fe_dm.fill_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(
      IF(
        t.fill_type IN (1, 2, 3, 4, 7, 8, 9), t.actual_sign_num, 0
      )
    ) actual_sign_num_in, SUM(
      IF(
        t.fill_type IN (5, 6, 11), t.actual_sign_num, 0
      )
    ) actual_sign_num_out
  FROM
	  fe_dwd.dwd_fill_day_inc_recent_two_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE  t.order_status IN (3, 4)
    AND t.fill_time >= @sdate
    AND t.fill_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.main_tmp;
  CREATE TEMPORARY TABLE fe_dm.main_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.oi_tmp
  UNION
  SELECT
    shelf_id, product_id
  FROM
    fe_dm.fill_tmp;
	
	
  DELETE
  FROM
    fe_dm.dm_op_ds7p_sal_fil  
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_ds7p_sal_fil (
    sdate, shelf_id, product_id, quantity_act, gmv, actual_sign_num_in, actual_sign_num_out, add_user
  )
  SELECT
    @sdate sdate, t.shelf_id, t.product_id, IFNULL(o.quantity_act, 0) quantity_act, IFNULL(o.gmv, 0) gmv, IFNULL(f.actual_sign_num_in, 0) actual_sign_num_in, IFNULL(f.actual_sign_num_out, 0) actual_sign_num_out, @add_user add_user
  FROM
    fe_dm.main_tmp t
    LEFT JOIN fe_dm.oi_tmp o
      ON t.shelf_id = o.shelf_id
      AND t.product_id = o.product_id
    LEFT JOIN fe_dm.fill_tmp f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id;
	  
	  
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_ds7p_sal_fil',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_ds7p_sal_fil','dm_op_ds7p_sal_fil','李世龙');
COMMIT;
    END