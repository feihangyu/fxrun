CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_np_gmv_week_three`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+1),
  @tmp_str := '',
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
   DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp,
  fe_dm.pd_tmp,
  fe_dm.pd_distinct_tmp,
  fe_dm.pd_distinct2_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp AS
	SELECT
    DATE(oi.pay_date) order_date,
    oi.shelf_id,
    oi.product_id,
	SUM(IF(oi.refund_amount>0,oi.quantity_act,oi.`QUANTITY`) * oi.`SALE_PRICE`) AS GMV ,
    SUM(
      oi.quantity * (
        oi.sale_price - IFNULL(
          oi.purchase_price,
          oi.cost_price
        )
      )
    ) profit
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month oi
      WHERE  oi.pay_date >= SUBDATE(@week_end, 6)
      AND oi.pay_date < ADDDATE(@week_end, 1)
  GROUP BY DATE(oi.pay_date),
    oi.shelf_id,
    oi.product_id;
	
	
	
	
	
  CREATE INDEX idx_oi_tmp_shelf_id_product_id
  ON fe_dm.oi_tmp (shelf_id, product_id);
  CREATE TEMPORARY TABLE fe_dm.pd_tmp AS
  SELECT
    vv.sdate AS min_date,    -- vv.min_date,
    vv.edate AS max_date,    -- vv.max_date,
    pd.business_area,
    pd.product_id
  FROM
    fe_dwd.dwd_pub_product_dim_sserp_his pd
    JOIN fe_dwd.dwd_op_dim_date vv             -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
      ON vv.version_id = pd.version         -- vv.version = pd.version
      AND vv.sdate <= @week_end             -- vv.min_date <= @week_end
      AND vv.edate > SUBDATE(@week_end, 7)  -- vv.max_date > SUBDATE(@week_end, 7)
  WHERE pd.product_type IN (
      '新增（试运行）',
      '新增（免费货）'
    );
	
  CREATE INDEX idx_pd_tmp_business_area_product_id
  ON fe_dm.pd_tmp (business_area, product_id);
  CREATE TEMPORARY TABLE fe_dm.pd_distinct_tmp AS
  SELECT DISTINCT
    pd.business_area,
    pd.product_id
  FROM
    fe_dm.pd_tmp pd;
  CREATE INDEX idx_pd_distinct_tmp_business_area_product_id
  ON fe_dm.pd_distinct_tmp (business_area, product_id);
  CREATE TEMPORARY TABLE fe_dm.pd_distinct2_tmp AS
  SELECT
    *
  FROM
    fe_dm.pd_distinct_tmp pd;
  CREATE INDEX idx_pd_distinct2_tmp_business_area_product_id
  ON fe_dm.pd_distinct2_tmp (business_area, product_id);
  DELETE
  FROM
    fe_dm.dm_op_kpi_np_gmv_week
  WHERE week_end = @week_end;
  
  
  INSERT INTO fe_dm.dm_op_kpi_np_gmv_week (
    week_end,
    region,
    business_area,
    product_id,
    product_fe,
    product_name,
    gmv,
    gmv_profit,
    add_user
  )
  SELECT
    @week_end,
    b.region_name,
    b.business_name,
    t.product_id,
    p.product_code2,
    p.product_name,
    IFNULL(SUM(t.gmv), 0),
    IFNULL(SUM(t.profit), 0),
    @add_user
  FROM
    fe_dm.oi_tmp t
       JOIN fe_dwd.dwd_shelf_base_day_all b
	ON t.shelf_id = b.shelf_id
    JOIN fe_dm.pd_tmp pd
      ON t.order_date >= pd.min_date
      AND t.order_date < pd.max_date
      AND b.business_name = pd.business_area
      AND t.product_id = pd.product_id
  JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_id = p.product_id
  GROUP BY b.business_name,
    t.product_id;
	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi_np_sal_sto_week
  WHERE week_end = @week_end;
  
  
 INSERT INTO fe_dm.dm_op_kpi_np_sal_sto_week (
    week_end,
    region,
    business_area,
    product_id,
    product_fe,
    product_name,
    shelfs_sal,
    shelfs_sto,
    add_user
  )
  SELECT
    @week_end,
    t.region_name,
    t.business_name,
    t.product_id,
    p.product_code2,
    p.product_name,
    IFNULL(SUM(t.shelfs_sal), 0),
    IFNULL(SUM(t.shelfs_sto), 0),
    @add_user
  FROM
    (SELECT
      b.region_name,
      b.business_name,
      t.product_id,
      COUNT(DISTINCT t.shelf_id) shelfs_sal,
      0 shelfs_sto
    FROM
      fe_dm.oi_tmp t
      JOIN fe_dwd.dwd_shelf_base_day_all b
        ON t.shelf_id = b.shelf_id
      JOIN fe_dm.pd_distinct_tmp pd
        ON b.business_name = pd.business_area
        AND t.product_id = pd.product_id
    GROUP BY b.business_name,
      t.product_id
    UNION ALL
    SELECT
      b.region_name,
      b.business_name,
      t.product_id,
      0 shelfs_sal,
      SUM(t.stock_quantity > 0) shelfs_sto
    FROM
   fe_dwd.dwd_shelf_product_sto_sal_30_days t  
 JOIN fe_dwd.dwd_shelf_base_day_all b
        ON t.shelf_id = b.shelf_id
      JOIN fe_dm.pd_distinct2_tmp pd
        ON b.business_name = pd.business_area
        AND t.product_id = pd.product_id
    WHERE t.sdate =@week_end
    GROUP BY b.business_name,
      t.product_id) t
    JOIN fe_dwd.dwd_product_base_day_all p
      ON t.product_id = p.product_id
  GROUP BY t.business_name,
    t.product_id;
	
	
  DELETE
  FROM
    fe_dm.dm_op_kpi_gmv_week
  WHERE week_end = @week_end;
  INSERT INTO fe_dm.dm_op_kpi_gmv_week (
    week_end,
    region,
    business_area,
    gmv,
    add_user
  )
  SELECT
    @week_end,
    b.region_name,
    b.business_name,
    IFNULL(SUM(t.gmv), 0),
    @add_user
  FROM
    fe_dm.oi_tmp t
    JOIN fe_dwd.dwd_shelf_base_day_all b
	ON t.shelf_id = b.shelf_id
  GROUP BY b.business_name;
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi_np_gmv_week_three',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_np_flag5_sto','dm_op_kpi_np_gmv_week_three','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_np_sal_sto_week','dm_op_kpi_np_gmv_week_three','李世龙');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_gmv_week','dm_op_kpi_np_gmv_week_three','李世龙');
COMMIT;
    END