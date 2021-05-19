CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_area_product_sat_rate_week`()
BEGIN
  SET @week_end := SUBDATE(
    CURRENT_DATE,
    WEEKDAY(CURRENT_DATE) + 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    feods.fjr_kpi_area_product_sat_rate
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_kpi_area_product_sat_rate (
    week_end,
    region,
    business_area,
    product_id,
    product_fe,
    shelfs_stock,
    shelfs_active,
    qty_dcsto,
    qty_dcout,
    add_user
  )
  SELECT
    @week_end,
    b.region_name,
    pd.BUSINESS_AREA,
    d.PRODUCT_ID,
    pd.PRODUCT_FE,
    IFNULL(SUM(d.STOCK_QUANTITY > 0), 0) shelfs_stock,
    IFNULL(sa.ct, 0),
    IFNULL(dc.fbaseqty, 0),
    IFNULL(dc.fqty, 0),
    @add_user
  FROM
    fe.sf_shelf_product_detail d
    join fe.sf_shelf s
      on d.shelf_id = s.shelf_id
      and s.data_flag = 1
    join feods.fjr_city_business b
      on s.city = b.city
    join feods.zs_product_dim_sserp pd
      on d.product_id = pd.product_id
      and b.business_name = pd.business_area
      and pd.product_type in (
        '原有',
        '新增（正式运行）'
      )
    LEFT JOIN
      (SELECT
        b.BUSINESS_NAME BUSINESS_AREA,
        COUNT(1) ct
      FROM
        fe.sf_shelf s,
        feods.fjr_city_business b
      WHERE s.CITY = b.CITY
        AND s.DATA_FLAG = 1
        AND s.SHELF_STATUS = 2
      GROUP BY b.BUSINESS_NAME) sa
      ON sa.BUSINESS_AREA = pd.BUSINESS_AREA
    LEFT JOIN
      (SELECT
        t1.business_area,
        t1.product_code2,
        SUM(t1.fbaseqty) fbaseqty,
        SUM(t2.fqty) fqty
      FROM
        (SELECT
          f.business_area,
          e.fnumber dc_code,
          d.fnumber product_code2,
          SUM(a.fbaseqty) fbaseqty
        FROM
          sserp.T_STK_INVENTORY a
          LEFT JOIN sserp.T_BD_MATERIAL d
            ON a.fmaterialid = d.fmaterialid
          LEFT JOIN sserp.T_BD_STOCK e
            ON a.fstockid = e.fstockid
          LEFT JOIN sserp.ZS_DC_BUSINESS_AREA f
            ON e.fnumber = f.dc_code
        GROUP BY f.business_area,
          e.fnumber,
          d.fnumber) t1
        LEFT JOIN
          (SELECT
            b.f_bgj_fstorehouset dc_code,
            d.fnumber product_code2,
            SUM(fqty) fqty
          FROM
            sserp.T_STK_OUTSTOCKAPPLYENTRY a
            JOIN sserp.T_STK_OUTSTOCKAPPLY b
              ON a.fid = b.fid
            LEFT JOIN sserp.T_BD_MATERIAL d
              ON a.fmaterialid = d.fmaterialid
          WHERE b.fbilltypeid = '5b0e65b177a7e1'
            AND b.fcloseflag = 0
            AND b.fclosestatus = 'B'
            AND b.fcancelstatus = 'A'
            AND b.fdate >= subdate(@week_end, 6)
            AND b.fdate < ADDDATE(@week_end, 1)
          GROUP BY b.f_bgj_fstorehouset,
            d.fnumber) t2
          ON t1.dc_code = t2.dc_code
          AND t1.product_code2 = t2.product_code2
      GROUP BY t1.business_area,
        t1.product_code2) dc
      ON dc.business_area = pd.BUSINESS_AREA
      AND dc.product_code2 = pd.PRODUCT_FE
  WHERE d.DATA_FLAG = 1
  GROUP BY pd.BUSINESS_AREA,
    d.PRODUCT_ID;
  CALL feods.sp_task_log (
    'sp_kpi_area_product_sat_rate_week',
    @week_end,
    CONCAT(
      'fjr_w_778dbf93976fa85360432c61eea7a62d',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END