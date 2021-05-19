CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_product_price_tag`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  
   SET @week_end := SUBDATE(
    CURRENT_DATE, DAYOFWEEK(CURRENT_DATE) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @week_add := ADDDATE(@week_end, 1), @add_day := CURRENT_DATE;
  
SET @time_1 := CURRENT_TIMESTAMP();
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.shelf_id,s.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all s
  WHERE s.shelf_status = 2 #AND s.shelf_type NOT IN (6, 7)
     AND ! ISNULL(s.shelf_id);
	 
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_1--@time_2",@time_1,@time_2);
	 
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.wgmv_tmp;
  CREATE TEMPORARY TABLE fe_dwd.wgmv_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, t.sale_price, SUM(t.sale_price * t.salqty) gmv
  FROM
    fe_dm.dm_op_product_price_salqty t
  GROUP BY t.business_name, t.product_id, t.sale_price;
  
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_2--@time_3",@time_2,@time_3);
  
  INSERT INTO fe_dwd.wgmv_tmp (
    business_name, product_id, sale_price, gmv
  )
  SELECT
    s.business_name, oi.product_id, oi.sale_price, SUM(oi.quantity_act * oi.sale_price) gmv
  FROM
     fe_dwd.dwd_pub_order_item_recent_one_month oi
    JOIN fe_dwd.shelf_tmp s
      ON oi.shelf_id = s.shelf_id
  WHERE oi.pay_date >= @week_add
    AND oi.pay_date < @add_day
  GROUP BY s.business_name, oi.product_id, oi.sale_price;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_3--@time_4",@time_3,@time_4);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.gmv_tmp;
  CREATE TEMPORARY TABLE fe_dwd.gmv_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, t.sale_price, SUM(t.gmv) gmv
  FROM
    fe_dwd.wgmv_tmp t
  GROUP BY t.business_name, t.product_id, t.sale_price;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_4--@time_5",@time_4,@time_5);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.max_gmv_tmp;
  CREATE TEMPORARY TABLE fe_dwd.max_gmv_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_name, t.product_id, MAX(t.gmv) mgmv
  FROM
    fe_dwd.gmv_tmp t
  WHERE ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_5--@time_6",@time_5,@time_6);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.rec_price_tmp;
  CREATE TEMPORARY TABLE fe_dwd.rec_price_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_name, t.product_id, MAX(t.sale_price) rec_price
  FROM
    fe_dwd.gmv_tmp t
    JOIN fe_dwd.max_gmv_tmp m
      ON t.business_name = m.business_name
      AND t.product_id = m.product_id
      AND t.gmv = m.mgmv
  WHERE ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
  
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_6--@time_7",@time_6,@time_7);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp (
    PRIMARY KEY (shelf_id, product_id), KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.shelf_id, t.product_id, pm.package_id, t.sale_price
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dwd.dwd_package_information pm
      ON t.item_id = pm.item_id
  WHERE ! ISNULL(s.shelf_id)
    AND ! ISNULL(t.product_id)
    AND t.stock_quantity > 0;
	
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_7--@time_8",@time_7,@time_8);
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_price_tmp;
  CREATE TEMPORARY TABLE fe_dwd.area_price_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, t.sale_price, COUNT(1) ct
  FROM
    fe_dwd.shelf_product_tmp t
  GROUP BY t.business_name, t.product_id, t.sale_price;
  
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_8--@time_9",@time_8,@time_9);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.max_count_tmp;
  CREATE TEMPORARY TABLE fe_dwd.max_count_tmp (KEY (business_name, product_id)) AS
  SELECT
    t.business_name, t.product_id, MAX(t.ct) ct
  FROM
    fe_dwd.area_price_tmp t
  GROUP BY t.business_name, t.product_id;
  
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_9--@time_10",@time_9,@time_10);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.most_price_tmp;
  CREATE TEMPORARY TABLE fe_dwd.most_price_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_name, t.product_id, MAX(t.sale_price) most_price
  FROM
    fe_dwd.area_price_tmp t
    JOIN fe_dwd.max_count_tmp m
      ON t.business_name = m.business_name
      AND t.product_id = m.product_id
      AND t.ct = m.ct
  WHERE ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
  
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_10--@time_11",@time_10,@time_11);
  
  TRUNCATE TABLE fe_dwd.dwd_shelf_product_price_tag;
  INSERT INTO fe_dwd.dwd_shelf_product_price_tag (
    business_name, shelf_id, product_id, PACKAGE_ID, sale_price, rec_price, most_price, price_tag, add_user
  )
  SELECT
    t.business_name, t.shelf_id, t.product_id, t.PACKAGE_ID, t.sale_price, rp.rec_price, mp.most_price, 2 * (t.sale_price = mp.most_price) + (t.sale_price > rp.rec_price) price_tag, @add_user add_user
  FROM
    fe_dwd.shelf_product_tmp t
    JOIN fe_dwd.rec_price_tmp rp
      ON t.business_name = rp.business_name
      AND t.product_id = rp.product_id
      AND t.sale_price != rp.rec_price
    JOIN fe_dwd.most_price_tmp mp
      ON t.business_name = mp.business_name
      AND t.product_id = mp.product_id;
	  
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_price_tag","@time_11--@time_12",@time_11,@time_12);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_product_price_tag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_price_tag','dwd_shelf_product_price_tag','李世龙');
COMMIT;
    END