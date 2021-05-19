CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_np_gmv_week`(IN in_week_end DATE)
BEGIN
  SET @week_end := SUBDATE(
    in_week_end,
    DAYOFWEEK(in_week_end) - 1
  ),
  @tmp_str := '',
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp,
  feods.pd_tmp,
  feods.pd_distinct_tmp,
  feods.pd_distinct2_tmp;
  
  
  CREATE TEMPORARY TABLE feods.oi_tmp AS
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
  ON feods.oi_tmp (shelf_id, product_id);
  CREATE TEMPORARY TABLE feods.pd_tmp AS
  SELECT
    vv.sdate AS min_date,    -- vv.min_date,
    vv.edate AS max_date,    -- vv.max_date,
    pd.business_area,
    pd.product_id
  FROM
    feods.zs_product_dim_sserp_his pd
    JOIN feods.d_op_dim_date vv             -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
      ON vv.version_id = pd.version         -- vv.version = pd.version
      AND vv.sdate <= @week_end             -- vv.min_date <= @week_end
      AND vv.edate > SUBDATE(@week_end, 7)  -- vv.max_date > SUBDATE(@week_end, 7)
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
    feods.fjr_kpi_np_gmv_week
  WHERE week_end = @week_end;
  
  
  INSERT INTO feods.fjr_kpi_np_gmv_week (
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
    feods.fjr_kpi_np_sal_sto_week
  WHERE week_end = @week_end;
  SELECT
    @tmp_str := CONCAT(
      "INSERT INTO feods.fjr_kpi_np_sal_sto_week(week_end,region,business_area,product_id,product_fe,product_name,shelfs_sal,shelfs_sto,add_user) ",
      "SELECT @week_end,t.region_name,t.business_name,t.product_id,p.product_code2,p.product_name ",
      ",ifnull(SUM(t.shelfs_sal),0) ",
      ",ifnull(SUM(t.shelfs_sto),0),@add_user ",
      "FROM ( ",
      "SELECT b.region_name,b.business_name,t.product_id ",
      ",COUNT(DISTINCT t.shelf_id)shelfs_sal,0 shelfs_sto ",
      "FROM feods.oi_tmp t ",
      "JOIN fe.sf_shelf s ON t.shelf_id=s.shelf_id AND s.data_flag=1	 ",
      "JOIN feods.fjr_city_business b ON s.city=b.city ",
      "JOIN feods.pd_distinct_tmp pd ON pd.business_area=b.business_name AND t.product_id=pd.product_id ",
      "GROUP BY b.business_name,t.product_id ",
      "UNION ALL ",
      "SELECT b.region_name,b.business_name,t.product_id ",
      ",0 shelfs_sal,SUM(t.day",
      DAYOFMONTH(ADDDATE(@week_end, 1)),
      "_quantity>0) shelfs_sto ",
      "FROM fe.sf_shelf_product_stock_detail t ",
      "JOIN fe.sf_shelf s ON t.shelf_id=s.shelf_id AND s.data_flag=1	 ",
      "JOIN feods.fjr_city_business b ON s.city=b.city ",
      "JOIN feods.pd_distinct2_tmp pd ON pd.business_area=b.business_name AND t.product_id=pd.product_id ",
      "WHERE t.stat_date=DATE_FORMAT(ADDDATE(@week_end,1),'%Y-%m') ",
      "GROUP BY b.business_name,t.product_id ",
      ")t ",
      "JOIN fe.sf_product p ON t.product_id=p.product_id AND p.data_flag=1 ",
      "GROUP BY t.business_name,t.product_id "
    );
  PREPARE exe_str FROM @tmp_str;
  EXECUTE exe_str;
  DELETE
  FROM
    feods.fjr_kpi_gmv_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi_gmv_week (
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
    feods.oi_tmp t
    JOIN fe.sf_shelf s
      ON t.SHELF_ID = s.SHELF_ID
      AND s.data_flag = 1
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  GROUP BY b.business_name;
  CALL feods.sp_task_log (
    'sp_kpi_np_gmv_week',
    @week_end,
    CONCAT(
      'fjr_w_068c50f098ca43e873cdf87cbbe49119',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END