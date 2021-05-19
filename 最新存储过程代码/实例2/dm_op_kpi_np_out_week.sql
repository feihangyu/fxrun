CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_np_out_week`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi_np_out_week  
  WHERE sdate = @sdate;
  
  
  INSERT INTO fe_dm.dm_op_kpi_np_out_week (
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
    s.REGION_NAME,
    s.BUSINESS_NAME,
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
    fe_dwd.dwd_shelf_product_day_all de
    JOIN  fe_dwd.dwd_shelf_base_day_all s
      ON s.SHELF_ID = de.SHELF_ID
      AND s.SHELF_STATUS = 2  
      AND s.SHELF_TYPE IN (1, 2, 3, 4, 5, 9)
    JOIN fe_dwd.dwd_op_dim_date vv     -- feods.vv_fjr_product_dim_sserp_period3 vv
      on vv.edate=                  -- ON vv.max_date =      
      (SELECT
        pd.PUB_TIME
      FROM
        fe_dwd.dwd_pub_product_dim_sserp pd
      LIMIT 1)
    JOIN fe_dwd.dwd_pub_product_dim_sserp_his pdh
      ON pdh.version = vv.version_id    -- vv.version
      AND de.PRODUCT_ID = pdh.PRODUCT_ID
      AND s.BUSINESS_NAME = pdh.BUSINESS_AREA
      AND pdh.PRODUCT_TYPE = '新增（试运行）'
    LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp pde
      ON pde.BUSINESS_AREA = pdh.BUSINESS_AREA
      AND pde.PRODUCT_id = pdh.PRODUCT_id
      AND pde.PRODUCT_TYPE = '淘汰（替补）'
  GROUP BY s.BUSINESS_NAME,
    pdh.PRODUCT_ID;
	
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi_np_out_week',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_np_out_week','dm_op_kpi_np_out_week','李世龙');
COMMIT;
    END