CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_product_area_disrate_two`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @sdate30 := SUBDATE(@sdate, 30), @sdate12m := SUBDATE(@sdate, INTERVAL 12 MONTH);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, t.business_name, t.shelf_status
  FROM
    fe_dwd.dwd_shelf_base_day_all t 
  WHERE  ! ISNULL(t.shelf_id);
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_tmp;
  CREATE TEMPORARY TABLE fe_dm.sto_tmp (
    PRIMARY KEY (product_id, business_name)
  )
  SELECT
    s.business_name, t.product_id, SUM(t.stock_quantity) stock_quantity, SUM(t.stock_quantity * t.sale_price) stock_val
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE  t.stock_quantity > 0
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY s.business_name, t.product_id;
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sal_tmp;
  CREATE TEMPORARY TABLE fe_dm.sal_tmp (
    PRIMARY KEY (product_id, business_name)
  )
  SELECT
    t.business_name, t.product_id, SUM(t.qty_sal) qty_sal, SUM(t.gmv) gmv
  FROM
    fe_dm.dm_area_product_dgmv t
  WHERE t.sdate >= @sdate30
    AND ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.business_name, t.product_id;
  
  
  TRUNCATE fe_dm.dm_op_product_area_disrate;
  INSERT INTO fe_dm.dm_op_product_area_disrate (
    product_id, business_name, product_type, out_date, stock_quantity, stock_val, qty_sal, gmv, stock_days, disrate, add_user
  )
  SELECT
    t.product_id, t.business_area business_name, t.product_type, t.out_date, sto.stock_quantity, sto.stock_val, sal.qty_sal, sal.gmv, @tdays := ROUND(
      sto.stock_quantity / sal.qty_sal * 30
    ) stock_days,
    CASE
      WHEN t.out_date >= @sdate30
      THEN .9
      WHEN t.out_date <= @sdate12m
      THEN .5
      WHEN @tdays >= 100
      THEN .6
      WHEN @tdays >= 60
      THEN .7
      WHEN @tdays >= 30
      THEN .8
      WHEN @tdays < 30
      THEN .9
      WHEN IFNULL(@tdays, 0) = 0
      THEN .4
      ELSE 1.0
    END disrate, @add_user add_user
  FROM
    fe_dwd.dwd_pub_product_dim_sserp t
    JOIN fe_dm.sto_tmp sto
      ON t.business_area = sto.business_name
      AND t.product_id = sto.product_id
    LEFT JOIN fe_dm.sal_tmp sal
      ON t.business_area = sal.business_name
      AND t.product_id = sal.product_id
  WHERE t.product_type IN (
      '淘汰（替补）', '淘汰', '停补（替补）', '停补', '退出'
    );
  -- ①    淘汰时间一个月内的，统一9折
-- ②    淘汰时间12个月以上的，统一5折
-- ③    架上周转天数≥100天，6折
-- ④    架上周转天数≥60天，7折
-- ⑤    架上周转天数≥30天，8折
-- ⑥    架上周转天数＜30,9折
-- ⑦    架上周转天数显示空/无，4折
   TRUNCATE fe_dm.dm_op_sp_disrate;
  INSERT INTO fe_dm.dm_op_sp_disrate (
    shelf_id, business_name, product_id, product_name, out_date, stock_frag, add_user
  )
  SELECT
    s.shelf_id, t.business_name, t.product_id, p.product_name, t.out_date,
    CASE
      WHEN t.stock_days >= 100
      THEN '[100,)'
      WHEN t.stock_days >= 60
      THEN '[60,100)'
      WHEN t.stock_days >= 30
      THEN '[30,60)'
      WHEN t.stock_days < 30
      THEN '[0,30)'
      ELSE NULL
    END stock_frag, @add_user add_user
  FROM
    fe_dm.dm_op_product_area_disrate t
    JOIN fe_dm.shelf_tmp s
      ON t.business_name = s.business_name
      AND s.shelf_status = 2
    JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_id = p.product_id;
	  
	  
	  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_product_area_disrate_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_area_disrate','dm_op_product_area_disrate_two','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_sp_disrate','dm_op_product_area_disrate_two','李世龙');
COMMIT;
    END