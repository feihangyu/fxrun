CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf7_area_product_stat`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SELECT
    @sdate30 := SUBDATE(@sdate, 29), @add_day := ADDDATE(@sdate, 1), @month_start := SUBDATE(@sdate, DAY(@sdate) - 1), @y_m := DATE_FORMAT(@sdate, '%Y-%m'), @y_m_last := DATE_FORMAT(@sdate30, '%Y-%m');
 
 DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, t.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all t
  WHERE t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id);
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.detail_tmp;
  CREATE TEMPORARY TABLE fe_dm.detail_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, COUNT(*) shelfs, SUM(t.stock_quantity > 0) shelfs_sto, 
	SUM(t.sales_flag = 5) shelfs5, SUM(
      IF(
        t.stock_quantity > 0, t.stock_quantity, 0
      )
    ) stock_quantity, SUM(
      IF(
        t.sales_flag = 5 && t.stock_quantity > 0, t.stock_quantity, 0
      )
    ) stock_quantity5
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE  ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY s.business_name, t.product_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.slot_tmp;
  CREATE TEMPORARY TABLE fe_dm.slot_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, COUNT(*) slots, SUM(t.stock_num > 0) slots_sto
  FROM
    fe_dwd.dwd_shelf_machine_slot_type t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE  ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
  GROUP BY s.business_name, t.product_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dc_tmp;
  CREATE TEMPORARY TABLE fe_dm.dc_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    t.business_area business_name, p.product_id, FLOOR(SUM(t.fbaseqty)) dcqty
  FROM
    fe_dwd.dwd_PJ_OUTSTOCK2_DAY t
    JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_bar = p.product_code2
  WHERE t.fproducedate = SUBDATE(CURRENT_DATE, 1)
    AND ! ISNULL(p.product_id)
    AND ! ISNULL(t.business_area)
  GROUP BY t.business_area, p.product_id
  HAVING dcqty > 0;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp (
    PRIMARY KEY (business_name, product_id)
  ) AS
  SELECT
    s.business_name, t.product_id, COUNT(DISTINCT t.shelf_id) shelfs_sal, SUM(t.quantity_act) quantity_act, SUM(t.quantity_act * t.sale_price) gmv, SUM(
      t.quantity_act * t.purchase_price
    ) gmv_cost
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate30
    AND t.pay_date < @add_day
    AND t.order_type = 3
    AND t.quantity_act > 0
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(s.business_name)
  GROUP BY s.business_name, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmpl;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmpl (PRIMARY KEY (shelf_id)) AS
  SELECT
    *
  FROM
    fe_dm.shelf_tmp;
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_tmp;
  CREATE TEMPORARY TABLE fe_dm.sto_tmp(primary key(business_name, product_id)) as 
  SELECT
        business_name,
        product_id,
        COUNT(*) AS shelfs_sto
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_day30`
WHERE stock_day30 > 0
GROUP BY business_name,product_id
;
  
  -- 原来的写法。保留一下
-- SELECT
--   @sql_str := CONCAT(
--     (SELECT
--       CONCAT(
--         "CREATE TEMPORARY TABLE fe_dm.sto_tmp(primary key(business_name, product_id)) as select t.business_name,t.product_id,count(*)shelfs_sto from(SELECT s.business_name,t.shelf_id,t.product_id FROM fe.sf_shelf_product_stock_detail t JOIN fe_dm.shelf_tmp s ON t.shelf_id=s.shelf_id WHERE t.stat_date=@y_m  AND !ISNULL(s.business_name) AND !ISNULL(t.product_id) AND (1 ", GROUP_CONCAT(
--           CONCAT(
--             " or t.day", DAY(t.sdate), "_quantity>0"
--           ) SEPARATOR ''
--         ), ")"
--       )
--     FROM
--       fe_dwd.dwd_pub_work_day t
--     WHERE t.sdate BETWEEN @sdate30
--       AND @sdate
--       AND t.sdate >= @month_start),
--     (SELECT
--       IFNULL(
--         CONCAT(
--           " union SELECT s.business_name,t.shelf_id,t.product_id FROM fe.sf_shelf_product_stock_detail t JOIN fe_dm.shelf_tmpl s ON t.shelf_id=s.shelf_id WHERE t.stat_date=@y_m_last AND (1 ", GROUP_CONCAT(
--             CONCAT(
--               " or t.day", DAY(t.sdate), "_quantity>0"
--             ) SEPARATOR ''
--           ), ")"
--         ), ''
--       )
--     FROM
--       fe_dwd.dwd_pub_work_day t
--     WHERE t.sdate BETWEEN @sdate30
--       AND @sdate
--       AND t.sdate < @month_start), ")t group by t.business_name,t.product_id"
--   );
--	
-- PREPARE sql_exe FROM @sql_str;
-- EXECUTE sql_exe;
  
  
  TRUNCATE TABLE fe_dm.dm_op_shelf7_area_product_stat;
  INSERT INTO fe_dm.dm_op_shelf7_area_product_stat (
    business_name, product_id, shelfs, shelfs_sto, shelfs5, stock_quantity, stock_quantity5, slots, slots_sto, dcqty, shelfs_sal30, quantity_act, gmv, gmv_cost, shelfs_sto30, add_user
  )
  SELECT
    t.business_name, t.product_id, t.shelfs, t.shelfs_sto, t.shelfs5, t.stock_quantity, t.stock_quantity5, sl.slots, sl.slots_sto, dc.dcqty, oi.shelfs_sal shelfs_sal30, oi.quantity_act, oi.gmv, oi.gmv_cost, st.shelfs_sto shelfs_sto30, @add_user add_user
  FROM
    fe_dm.detail_tmp t
    LEFT JOIN fe_dm.slot_tmp sl
      ON t.business_name = sl.business_name
      AND t.product_id = sl.product_id
    LEFT JOIN fe_dm.dc_tmp dc
      ON t.business_name = dc.business_name
      AND t.product_id = dc.product_id
    LEFT JOIN fe_dm.oi_tmp oi
      ON t.business_name = oi.business_name
      AND t.product_id = oi.product_id
    LEFT JOIN fe_dm.sto_tmp st
      ON t.business_name = st.business_name
      AND t.product_id = st.product_id;
  
  
  
   -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf7_area_product_stat',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf7_area_product_stat','dm_op_shelf7_area_product_stat','李世龙');
COMMIT;
    END