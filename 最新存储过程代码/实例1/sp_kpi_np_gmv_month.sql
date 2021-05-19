CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_np_gmv_month`(IN in_month_id CHAR(7))
BEGIN
  SET @month_id := in_month_id,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp,
  feods.pd_tmp,
  feods.pd_distinct_tmp,
  feods.pd_distinct2_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp AS
  SELECT
    DATE(o.order_date) order_date,
    o.shelf_id,
    oi.product_id,
    SUM(oi.quantity * oi.sale_price) gmv,
    SUM(
      oi.quantity * (
        oi.sale_price - IFNULL(
          oi.purchase_price,
          oi.cost_price
        )
      )
    ) profit
  FROM
    fe.sf_order_item oi
    JOIN fe.sf_order o
      ON oi.order_id = o.order_id
      AND o.order_status = 2
      AND o.order_date >= CONCAT(@month_id, '-01')
      AND o.order_date < ADDDATE(
        LAST_DAY(CONCAT(@month_id, '-01')),
        1
      )
  GROUP BY DATE(o.order_date),
    o.shelf_id,
    oi.product_id;
  CREATE INDEX idx_oi_tmp_shelf_id_product_id
  ON feods.oi_tmp (shelf_id, product_id);
  CREATE TEMPORARY TABLE feods.pd_tmp AS
  SELECT
    vv.sdate as min_date,                 -- vv.min_date,
    vv.edate as max_date,                 -- vv.max_date,
    pd.business_area,
    pd.product_id
  FROM
    feods.zs_product_dim_sserp_his pd
    JOIN feods.d_op_dim_date vv             -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
      ON vv.version_id = pd.version         -- vv.version = pd.version
      AND vv.sdate <= LAST_DAY(CONCAT(@month_id, '-01'))  -- vv.min_date <= LAST_DAY(CONCAT(@month_id, '-01'))
      AND vv.edate > CONCAT(@month_id, '-01')             -- vv.max_date > CONCAT(@month_id, '-01')
  WHERE pd.product_type IN (
      '新增（试运行）',
      '新增（免费货）'
    );
  CREATE INDEX idx_pd_tmp_business_area_product_id
  ON feods.pd_tmp (business_area, product_id);
  CREATE TEMPORARY TABLE feods.pd_distinct_tmp AS
  SELECT DISTINCT
    pd.business_area,
    pd.product_id
  FROM
    feods.pd_tmp pd;
  CREATE INDEX idx_pd_distinct_tmp_business_area_product_id
  ON feods.pd_distinct_tmp (business_area, product_id);
  CREATE TEMPORARY TABLE feods.pd_distinct2_tmp AS
  SELECT
    *
  FROM
    feods.pd_distinct_tmp pd;
  CREATE INDEX idx_pd_distinct2_tmp_business_area_product_id
  ON feods.pd_distinct2_tmp (business_area, product_id);
  DELETE
  FROM
    feods.fjr_kpi_np_gmv_month
  WHERE month_id = @month_id;
  INSERT INTO feods.fjr_kpi_np_gmv_month (
    month_id,
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
    @month_id,
    b.region_name,
    b.business_name,
    t.product_id,
    p.product_code2,
    p.product_name,
    IFNULL(SUM(t.gmv), 0),
    IFNULL(SUM(t.profit), 0),
    @add_user
  FROM
    feods.oi_tmp t
    JOIN fe.sf_shelf s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 1
    JOIN feods.fjr_city_business b
      ON s.city = b.city
    JOIN feods.pd_tmp pd
      ON t.order_date >= pd.min_date
      AND t.order_date < pd.max_date
      AND b.business_name = pd.business_area
      AND t.product_id = pd.product_id
    JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
  GROUP BY b.business_name,
    t.product_id;
  DELETE
  FROM
    feods.fjr_kpi_np_sal_sto_month
  WHERE month_id = @month_id;
  INSERT INTO feods.fjr_kpi_np_sal_sto_month (
    month_id,
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
    @month_id,
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
      feods.oi_tmp t
      JOIN fe.sf_shelf s
        ON t.shelf_id = s.shelf_id
        AND s.data_flag = 1
      JOIN feods.fjr_city_business b
        ON s.city = b.city
      JOIN feods.pd_distinct_tmp pd
        ON b.business_name = pd.business_area
        AND t.product_id = pd.product_id
    GROUP BY b.business_name,
      t.product_id
    UNION
    ALL
    SELECT
      b.region_name,
      b.business_name,
      t.product_id,
      0 shelfs_sal,
      SUM(t.day1_quantity > 0) shelfs_sto
    FROM
      fe.sf_shelf_product_stock_detail t
      JOIN fe.sf_shelf s
        ON t.shelf_id = s.shelf_id
        AND s.data_flag = 1
      JOIN feods.fjr_city_business b
        ON s.city = b.city
      JOIN feods.pd_distinct2_tmp pd
        ON b.business_name = pd.business_area
        AND t.product_id = pd.product_id
    WHERE t.stat_date = DATE_FORMAT(
        ADDDATE(
          CONCAT(@month_id, '-01'),
          INTERVAL 1 MONTH
        ),
        '%Y-%m'
      )
    GROUP BY b.business_name,
      t.product_id) t
    JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
  GROUP BY t.business_name,
    t.product_id;
  DELETE
  FROM
    feods.fjr_kpi_gmv_month
  WHERE month_id = @month_id;
  INSERT INTO feods.fjr_kpi_gmv_month (
    month_id,
    region,
    business_area,
    gmv,
    add_user
  )
  SELECT
    @month_id,
    b.region_name,
    b.business_name,
    IFNULL(SUM(t.gmv), 0),
    @add_user
  FROM
    feods.oi_tmp t
    JOIN fe.sf_shelf s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 1
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  GROUP BY b.business_name;
  CALL feods.sp_task_log (
    'sp_kpi_np_gmv_month',
    @month_id,
    CONCAT(
      'fjr_m_1f8289b73c83767192818cc458bbb8ac',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END