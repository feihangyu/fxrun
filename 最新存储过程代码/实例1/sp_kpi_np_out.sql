CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_np_out`()
BEGIN
  SET @sdate := CURRENT_DATE,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  
  
  DELETE
  FROM
    feods.fjr_kpi_np_out_week
  WHERE sdate = @sdate;
  
  
  INSERT INTO feods.fjr_kpi_np_out_week (
    sdate,
    VERSION,
    region,
    business_area,
    product_id,
    product_fe,
    product_name,
    out_flag,
    stoval,
    add_user
  )
  SELECT
    @sdate,
    pdh.VERSION,
    b.REGION_NAME,
    b.BUSINESS_NAME,
    pdh.PRODUCT_ID,
    pdh.PRODUCT_FE,
    pdh.PRODUCT_NAME,
    pde.PRODUCT_ID IS NOT NULL,
    IFNULL(
      SUM(
        de.STOCK_QUANTITY * de.SALE_PRICE
      ),
      0
    ),
    @add_user
  FROM
    fe.sf_shelf_product_detail de
    JOIN fe.sf_shelf s
      ON s.SHELF_ID = de.SHELF_ID
      AND s.SHELF_STATUS = 2
      AND s.DATA_FLAG = 1
      AND s.SHELF_TYPE IN (1, 2, 3, 4, 5, 9)
    JOIN feods.fjr_city_business b
      ON s.CITY = b.CITY
    JOIN feods.d_op_dim_date vv     -- feods.vv_fjr_product_dim_sserp_period3 vv
      on vv.edate=                  -- ON vv.max_date =      
      (SELECT
        pd.PUB_TIME
      FROM
        feods.zs_product_dim_sserp pd
      LIMIT 1)
    JOIN feods.zs_product_dim_sserp_his pdh
      ON pdh.version = vv.version_id    -- vv.version
      AND de.PRODUCT_ID = pdh.PRODUCT_ID
      AND b.BUSINESS_NAME = pdh.BUSINESS_AREA
      AND pdh.PRODUCT_TYPE = '新增（试运行）'
    LEFT JOIN feods.zs_product_dim_sserp pde
      ON pde.BUSINESS_AREA = pdh.BUSINESS_AREA
      AND pde.PRODUCT_id = pdh.PRODUCT_id
      AND pde.PRODUCT_TYPE = '淘汰（替补）'
  WHERE de.DATA_FLAG = 1
  GROUP BY b.BUSINESS_NAME,
    pdh.PRODUCT_ID;
	
	
  CALL feods.sp_task_log (
    'sp_kpi_np_out',
    @sdate,
    CONCAT(
      'fjr_d_19cfd9feee430cc7192770b1baab4c09',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END