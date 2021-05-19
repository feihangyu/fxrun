CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_area_product_sat_rate`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @week_end := SUBDATE(
    CURRENT_DATE,
    WEEKDAY(CURRENT_DATE) + 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    fe_dm.dm_op_kpi_area_product_sat_rate
  WHERE week_end = @week_end;
  INSERT INTO fe_dm.dm_op_kpi_area_product_sat_rate (
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
    s.region_name,
    pd.BUSINESS_AREA,
    d.PRODUCT_ID,
    pd.PRODUCT_FE,
    IFNULL(SUM(d.STOCK_QUANTITY > 0), 0) shelfs_stock,
    IFNULL(sa.ct, 0),
    IFNULL(dc.fbaseqty, 0),
    IFNULL(dc.fqty, 0),
    @add_user
  FROM
    fe_dwd.dwd_shelf_product_day_all d
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON d.shelf_id = s.shelf_id
    JOIN fe_dwd.dwd_pub_product_dim_sserp pd
      ON d.product_id = pd.product_id
      AND s.business_name = pd.business_area
      AND pd.product_type IN (
        '原有',
        '新增（正式运行）'
      )
    LEFT JOIN
      (SELECT
        s.BUSINESS_NAME BUSINESS_AREA,
        COUNT(1) ct
      FROM
        fe_dwd.dwd_shelf_base_day_all s
      WHERE s.SHELF_STATUS = 2
      GROUP BY s.BUSINESS_NAME) sa
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
          fe_dwd.dwd_sserp_t_stk_inventory a
          LEFT JOIN fe_dwd.dwd_sserp_t_bd_material d
            ON a.fmaterialid = d.fmaterialid
          LEFT JOIN fe_dwd.dwd_sserp_t_bd_stock e
            ON a.fstockid = e.fstockid
          LEFT JOIN fe_dwd.dwd_sserp_zs_dc_business_area f
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
            fe_dwd.dwd_sserp_t_stk_outstockapplyentry a
            JOIN fe_dwd.dwd_sserp_t_stk_outstockapply b
              ON a.fid = b.fid
            LEFT JOIN fe_dwd.dwd_sserp_t_bd_material d
              ON a.fmaterialid = d.fmaterialid
          WHERE b.fbilltypeid = '5b0e65b177a7e1'
            AND b.fcloseflag = 0
            AND b.fclosestatus = 'B'
            AND b.fcancelstatus = 'A'
            AND b.fdate >= SUBDATE(@week_end, 6)
            AND b.fdate < ADDDATE(@week_end, 1)
          GROUP BY b.f_bgj_fstorehouset,
            d.fnumber) t2
          ON t1.dc_code = t2.dc_code
          AND t1.product_code2 = t2.product_code2
      GROUP BY t1.business_area,
        t1.product_code2) dc
      ON dc.business_area = pd.BUSINESS_AREA
      AND dc.product_code2 = pd.PRODUCT_FE
  GROUP BY pd.BUSINESS_AREA,
    d.PRODUCT_ID;
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi_area_product_sat_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_area_product_sat_rate','dm_op_kpi_area_product_sat_rate','李世龙');
COMMIT;
    END