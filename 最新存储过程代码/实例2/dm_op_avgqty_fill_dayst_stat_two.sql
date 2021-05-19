CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_avgqty_fill_dayst_stat_two`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := subdate(current_date,interval 1 day), @str := '', @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_date := ADDDATE(@sdate, 1), @month_id := DATE_FORMAT(@sdate, '%Y-%m'), @day_id := DAY(@sdate);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    b.region_name, b.business_name, b.shelf_id
  FROM
    fe_dwd.dwd_shelf_base_day_all b;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp (
    KEY (
      product_id, business_name, first_fill_date
    )
  )
  SELECT
    st.region_name, st.business_name, t.product_id, DATE(f.first_fill_time) first_fill_date, COUNT(DISTINCT t.shelf_id) shelfs_sal, SUM(t.quantity_act) salqty
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN fe_dm.shelf_tmp st
      ON st.shelf_id = t.shelf_id
    LEFT JOIN fe_dwd.dwd_shelf_product_day_all f
      ON f.shelf_id = t.shelf_id
      AND f.product_id = t.product_id
      AND f.first_fill_time < @add_date
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_date
  GROUP BY t.product_id, st.business_name, first_fill_date;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dayst_tmp;
  CREATE TEMPORARY TABLE fe_dm.dayst_tmp (
    KEY (
      product_id, business_name, first_fill_date
    )
  )
  SELECT
    t.region_name, t.business_name, t.product_id, t.first_fill_date, DATEDIFF(@sdate, t.first_fill_date) ddiff, SUM(t.salqty) salqty, SUM(t.shelfs_sal) shelfs_sal
  FROM
    fe_dm.oi_tmp t
  GROUP BY t.product_id, t.business_name, t.first_fill_date;
  
  
  DELETE
  FROM
    fe_dm.dm_op_avgqty_fill_dayst
  WHERE sdate = @sdate;
  
  INSERT INTO fe_dm.dm_op_avgqty_fill_dayst (
    sdate, region_name, business_name, product_id, fill_date, salqty, shelfs_sal, add_user
  )
  SELECT
    @sdate, t.region_name, t.business_name, t.product_id, t.first_fill_date, t.salqty, t.shelfs_sal, @add_user
  FROM
    fe_dm.dayst_tmp t;
  SELECT
    COUNT(*) = 0 INTO @nrun_flag
  FROM
    fe_dwd.dwd_sf_dw_task_log t
  WHERE t.end_time > CURRENT_DATE
    AND t.task_name = 'dm_op_avgqty_fill_dayst_stat_two';  -- 注意这个存储过程名
	
	
  UPDATE
    fe_dm.dm_op_avgqty_fill_dayst_stat t
    JOIN fe_dm.dayst_tmp d
      ON t.product_id = d.product_id
      AND t.business_name = d.business_name
      AND t.ddiff = d.ddiff SET t.salqty = t.salqty + d.salqty, t.shelfs_sal = t.shelfs_sal + d.shelfs_sal
  WHERE @nrun_flag;
  
  
  INSERT INTO fe_dm.dm_op_avgqty_fill_dayst_stat (
    region_name, business_name, product_id, ddiff, salqty, shelfs_sal, add_user
  )
  SELECT
    t.region_name, t.business_name, t.product_id, t.ddiff, t.salqty, t.shelfs_sal, @add_user add_user
  FROM
    fe_dm.dayst_tmp t
    LEFT JOIN fe_dm.dm_op_avgqty_fill_dayst_stat d
      ON t.product_id = d.product_id
      AND t.business_name = d.business_name
      AND t.ddiff = d.ddiff
  WHERE @nrun_flag
    AND ISNULL(d.product_id);
	
	
	
	
	  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_avgqty_fill_dayst_stat_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_avgqty_fill_dayst','dm_op_avgqty_fill_dayst_stat_two','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_avgqty_fill_dayst_stat','dm_op_avgqty_fill_dayst_stat_two','李世龙');
COMMIT;
    END