CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_monitor`()
BEGIN
  #run after sh_process.sp_kpi_np_gmv_week
#run after sh_process.sp_kpi_area_product_sat_rate_week
#run after sh_process.sp_kpi_shelf_nps
#run after sh_process.sp_kpi_np_flag5_sto
#run after sh_process.sp_kpi_np_inqty
#run after sh_process.sp_kpi_avggmv_month
#run after sh_process.sp_kpi_sto_val_rate
#run after sh_process.sp_kpi_np_gmv_month
#run after sh_process.sh_area_product_sale_flag
#run after sh_process.sp_kpi_np_out
#run after sh_process.sp_kpi_avggmv_week
#run after sh_process.sp_kpi_unsku
   SET @week_end := SUBDATE(
    CURRENT_DATE, WEEKDAY(CURRENT_DATE) + 1
  ), @month_id := DATE_FORMAT(
    SUBDATE(CURRENT_DATE, INTERVAL 1 MONTH), '%Y-%m'
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @business_last_month := '2017-11', @business_areas := 0;
  SELECT
    @business_last_month := t.business_last_month
  FROM
    feods.fjr_kpi_avggmv_week t
  WHERE t.week_end = @week_end
  LIMIT 1;
  SELECT
    @business_areas := COUNT(DISTINCT b.BUSINESS_CODE)
  FROM
    feods.fjr_city_business b;
  SET @week_start := SUBDATE(@week_end, 6);
  SET @next_week_start := ADDDATE(@week_end, 1);
  SET @month_first_day := CONCAT(@month_id, '-01');
  SET @month_last_day := LAST_DAY(@month_first_day);
  SET @last2_month_id := DATE_FORMAT(
    SUBDATE(
      @month_first_day, INTERVAL 2 MONTH
    ), '%Y-%m'
  );
  SET @last_month_id := DATE_FORMAT(
    SUBDATE(
      @month_first_day, INTERVAL 1 MONTH
    ), '%Y-%m'
  );
  SET @last_business_last_month_id := DATE_FORMAT(
    SUBDATE(
      CONCAT(@business_last_month, '-01'), INTERVAL 1 MONTH
    ), '%Y-%m'
  );
  SET @next_month_first_day := ADDDATE(@month_last_day, 1);
  SET @next_month_first2_day := ADDDATE(@month_last_day, 2);
  SET @month_2day := CONCAT(@month_id, '-02');
  SET @next_date := ADDDATE(CURRENT_DATE, 1);
-- 以下三个临时表用于取代后面的视图 vv_fjr_product_dim_sserp_period 
drop temporary table if exists feods.`vv_fjr_product_dim_sserp_p2_tmp`;
CREATE temporary table feods.`vv_fjr_product_dim_sserp_p2_tmp` AS 
SELECT
  CONCAT('V',ROUND((SUBSTR(`d`.`VERSION`,2) + 0.1),1)) AS `VERSION`,
  `d`.`PUB_TIME` AS `max_date`
FROM feods.`zs_product_dim_sserp_his` `d`
GROUP BY `d`.`VERSION` UNION ALL 
SELECT
   MIN(`t`.`VERSION`) AS `MIN(t.version)`,
   '2017-11-11' AS `2017-11-11`
FROM feods.`zs_product_dim_sserp_his` `t`;
drop temporary table if exists feods.`vv_fjr_product_dim_sserp_p1_tmp`;
CREATE temporary table feods.`vv_fjr_product_dim_sserp_p1_tmp` AS 
select
  `d`.`VERSION`  AS `VERSION`,
  `d`.`PUB_TIME` AS `max_date`
from feods.`zs_product_dim_sserp_his` `d`
group by `d`.`VERSION`;
drop temporary table if exists feods.`vv_fjr_product_dim_sserp_period_tmp`;
CREATE temporary table feods.`vv_fjr_product_dim_sserp_period_tmp` AS 
select
  `t1`.`VERSION`  AS `version`,
  `t2`.`max_date` AS `min_date`,
  `t1`.`max_date` AS `max_date`
from (feods.`vv_fjr_product_dim_sserp_p1_tmp` `t1`
   join feods.`vv_fjr_product_dim_sserp_p2_tmp` `t2`)
where (`t1`.`VERSION` = `t2`.`VERSION`);
  
 
  DELETE
  FROM
    feods.fjr_kpi_monitor
  WHERE sdate = @week_end
    OR sdate = @month_first_day;
  DELETE
  FROM
    feods.fjr_kpi_avggmv_week_tran;
  INSERT INTO feods.fjr_kpi_avggmv_week_tran (shelf_id, pgmv, days_wd)
  SELECT
    t.shelf_id, IFNULL(t.gmv, 0) + IFNULL(t.payment_money, 0) pgmv, IFNULL(t.days_wd, 0)
  FROM
    feods.fjr_kpi_avggmv_week t
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    feods.fjr_kpi_avggmv_month_tran;
  INSERT INTO feods.fjr_kpi_avggmv_month_tran (month_id, shelf_id, pgmv, days_wd)
  SELECT
    t.month_id, t.shelf_id, IFNULL(t.gmv, 0) + IFNULL(t.payment_money, 0) pgmv, IFNULL(t.days_wd, 0)
  FROM
    feods.fjr_kpi_avggmv_month t
  WHERE t.month_id >= @last2_month_id;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 101, 'avggmv', IFNULL(
      (
        (SELECT
          AVG(w.pgmv / w.days_wd)
        FROM
          feods.fjr_kpi_avggmv_week_tran w
          LEFT JOIN feods.fjr_kpi_avggmv_month_tran m
            ON w.shelf_id = m.shelf_id
            AND m.month_id = @business_last_month
        WHERE m.month_id IS NULL
          OR m.days_wd = 0
          OR w.pgmv / w.days_wd >= .45 * m.pgmv / m.days_wd) /
        (SELECT
          AVG(m.pgmv / m.days_wd)
        FROM
          feods.fjr_kpi_avggmv_month_tran m
          LEFT JOIN feods.fjr_kpi_avggmv_month_tran m2
            ON m.shelf_id = m2.shelf_id
            AND m2.month_id = @last_business_last_month_id
        WHERE m.month_id = @business_last_month
          AND (
            m2.month_id IS NULL
            OR m2.days_wd = 0
            OR m.pgmv / m.days_wd >= .45 * m2.pgmv / m2.days_wd
          )) - 1
      ), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 101, 'avggmv', IFNULL(
      (
        (SELECT
          AVG(m.pgmv / m.days_wd)
        FROM
          feods.fjr_kpi_avggmv_month_tran m
          LEFT JOIN feods.fjr_kpi_avggmv_month_tran m2
            ON m.shelf_id = m2.shelf_id
            AND m2.month_id = @last_month_id
        WHERE m.month_id = @month_id
          AND (
            m2.month_id IS NULL
            OR m2.days_wd = 0
            OR m.pgmv / m.days_wd >= .45 * m2.pgmv / m2.days_wd
          )) /
        (SELECT
          AVG(m2.pgmv / m2.days_wd)
        FROM
          feods.fjr_kpi_avggmv_month_tran m2
          LEFT JOIN feods.fjr_kpi_avggmv_month_tran m3
            ON m2.shelf_id = m3.shelf_id
            AND m3.month_id = @last2_month_id
        WHERE m2.month_id = @last_month_id
          AND (
            m3.month_id IS NULL
            OR m3.days_wd = 0
            OR m2.pgmv / m2.days_wd >= .45 * m3.pgmv / m3.days_wd
          )) - 1
      ), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 102, 'area_product_sat_rate', IFNULL(
      (SELECT
        COUNT(*) / @business_areas
      FROM
        (SELECT
          t.business_area, COUNT(*) ct
        FROM
          feods.fjr_kpi_area_product_sat_rate t
        WHERE t.week_end = @week_end
          AND (
            t.shelfs_active IS NULL
            OR t.shelfs_active = 0
            OR t.shelfs_stock >= .3 * shelfs_active
          )
          AND (
            t.qty_dcout IS NULL
            OR t.qty_dcout = 0
            OR t.qty_dcsto >= 5 / 7 * qty_dcout
          )
        GROUP BY t.business_area
        HAVING ct > 35) t), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 102, 'area_product_sat_rate', IFNULL(
      (SELECT
        COUNT(DISTINCT t.business_area) / @business_areas
      FROM
        (SELECT
          t.week_end, t.business_area, COUNT(*) ct
        FROM
          feods.fjr_kpi_area_product_sat_rate t
        WHERE t.week_end >= @month_first_day
          AND t.week_end < @next_month_first_day
          AND (
            t.shelfs_active IS NULL
            OR t.shelfs_active = 0
            OR t.shelfs_stock >= .3 * shelfs_active
          )
          AND (
            t.qty_dcout IS NULL
            OR t.qty_dcout = 0
            OR t.qty_dcsto >= 5 / 7 * qty_dcout
          )
        GROUP BY t.week_end, t.business_area
        HAVING ct > 35) t), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 103, 'product_gsal_rate', IFNULL(
      (SELECT
        SUM(
          CASE
            WHEN t.sale_level IN (
              '热卖', '好卖', '非常好卖'
            )
            THEN 1
          END
        ) / COUNT(*)
      FROM
        feods.zs_area_product_sale_flag t, feods.zs_product_dim_sserp_his d, feods.d_op_dim_date vv,  -- 用d_op_dim_date取代 vv_fjr_product_dim_sserp_period3 
        (SELECT DISTINCT
          b.REGION_NAME, b.BUSINESS_NAME
        FROM
          feods.fjr_city_business b) b
      WHERE 1
        AND t.business_area = d.BUSINESS_AREA
        AND t.PRODUCT_ID = d.PRODUCT_ID
        AND t.sdate = @next_week_start
        AND t.sdate >= vv.sdate     -- vv.min_date
        AND t.sdate < vv.edate      -- vv.max_date
        AND d.version = vv.version_id  -- vv.version
        AND t.business_area = b.BUSINESS_NAME
        AND d.PRODUCT_TYPE IN (
          '原有', '新增（正式运行）', '新增（试运行）'
        )), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 103, 'product_gsal_rate', IFNULL(
      (SELECT
        SUM(
          CASE
            WHEN t.sale_level IN (
              '热卖', '好卖', '非常好卖'
            )
            THEN 1
          END
        ) / COUNT(*)
      FROM
        feods.zs_area_product_sale_flag t, feods.zs_product_dim_sserp_his d, feods.d_op_dim_date vv,  -- 用d_op_dim_date取代 vv_fjr_product_dim_sserp_period3
        (SELECT DISTINCT
          b.REGION_NAME, b.BUSINESS_NAME
        FROM
          feods.fjr_city_business b) b
      WHERE 1
        AND t.business_area = d.BUSINESS_AREA
        AND t.PRODUCT_ID = d.PRODUCT_ID
        AND t.sdate >= @month_2day
        AND t.sdate < @next_month_first2_day
        AND t.sdate >= vv.sdate  -- vv.min_date
        AND t.sdate < vv.edate   -- vv.max_date
        AND d.version = vv.version_id  -- vv.version
        AND t.business_area = b.BUSINESS_NAME
        AND d.PRODUCT_TYPE IN (
          '原有', '新增（正式运行）', '新增（试运行）'
        )), 0
    ), @add_user;
	
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 104, 'sto_val_rate_out', IFNULL(
      (SELECT
        SUM(t.sto_val_out) / SUM(t.sto_val)
      FROM
        feods.fjr_kpi_sto_val_rate t
      WHERE t.sdate = @week_end), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 104, 'sto_val_rate_out', IFNULL(
      (SELECT
        SUM(t.sto_val_out) / SUM(t.sto_val)
      FROM
        feods.fjr_kpi_sto_val_rate t
      WHERE t.sdate >= @month_first_day
        AND t.sdate < @next_month_first_day), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 105, 'sto_val_rate_flag5', IFNULL(
      (SELECT
        SUM(t.sto_val_flag5) / SUM(t.sto_val)
      FROM
        feods.fjr_kpi_sto_val_rate t
      WHERE t.sdate = @week_end), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 105, 'sto_val_rate_flag5', IFNULL(
      (SELECT
        SUM(t.sto_val_flag5) / SUM(t.sto_val)
      FROM
        feods.fjr_kpi_sto_val_rate t
      WHERE t.sdate = @month_last_day), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 106, 'shelf_nps', IFNULL(
      (SELECT
        SUM(t.new_products > 2) / COUNT(*)
      FROM
        feods.fjr_kpi_shelf_nps t
      WHERE t.sdate = @week_end
        AND NOT EXISTS
        (SELECT
          1
        FROM
          fe.sf_shelf_relation_record r
        WHERE r.data_flag = 1
          AND r.shelf_handle_status IN (9, 10)
          AND r.secondary_shelf_id = t.shelf_id
          AND r.add_time <= @next_week_start
          AND IFNULL(r.unbind_time, @next_date) > @next_week_start)), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 106, 'shelf_nps', IFNULL(
      (SELECT
        SUM(t.new_products > 2) / COUNT(*)
      FROM
        feods.fjr_kpi_shelf_nps t
      WHERE t.sdate = @month_last_day
        AND NOT EXISTS
        (SELECT
          1
        FROM
          fe.sf_shelf_relation_record r
        WHERE r.data_flag = 1
          AND r.shelf_handle_status IN (9, 10)
          AND r.secondary_shelf_id = t.shelf_id
          AND r.add_time <= @next_month_first_day
          AND IFNULL(r.unbind_time, @next_date) > @next_month_first_day)), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 107, 'unsku', IFNULL(
      (SELECT
        AVG(t.ct)
      FROM
        (SELECT
          t.sdate, COUNT(*) ct
        FROM
          feods.fjr_kpi_unsku t
        WHERE t.sdate >= @week_start
          AND t.sdate < @next_week_start
        GROUP BY t.sdate) t), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 107, 'unsku', IFNULL(
      (SELECT
        AVG(t.ct)
      FROM
        (SELECT
          t.sdate, COUNT(*) ct
        FROM
          feods.fjr_kpi_unsku t
        WHERE t.sdate >= @month_first_day
          AND t.sdate < @next_month_first_day
        GROUP BY t.sdate) t), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 108, 'np_inqty', IFNULL(
      (SELECT
        SUM(
          IFNULL(t.salqty, 0) + IFNULL(t.stoqty, 0) >= .9 * t.inqty
        ) / COUNT(*)
      FROM
        feods.fjr_kpi_np_inqty t
      WHERE t.sdate >= @week_start
        AND t.sdate < @next_week_start), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 108, 'np_inqty', IFNULL(
      (SELECT
        SUM(
          IFNULL(t.salqty, 0) + IFNULL(t.stoqty, 0) >= .9 * t.inqty
        ) / COUNT(*)
      FROM
        feods.fjr_kpi_np_inqty t
      WHERE t.sdate >= @month_first_day
        AND t.sdate < @next_month_first_day), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 109, 'np_flag5_sto', IFNULL(
      (SELECT
        SUM(
          CASE
            t.sales_flag
            WHEN 5
            THEN t.stoval
          END
        ) / SUM(t.stoval)
      FROM
        feods.fjr_kpi_np_flag5_sto t
      WHERE t.sdate = @week_end), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 109, 'np_flag5_sto', IFNULL(
      (SELECT
        SUM(
          CASE
            t.sales_flag
            WHEN 5
            THEN t.stoval
          END
        ) / SUM(t.stoval)
      FROM
        feods.fjr_kpi_np_flag5_sto t
      WHERE t.sdate = @month_last_day), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 110, 'ns_avggmv', IFNULL(
      (SELECT
        AVG(
          (
            IFNULL(t.gmv, 0) + IFNULL(t.payment_money, 0)
          ) / t.days_wd
        )
      FROM
        feods.fjr_kpi_ns_avggmv_week t
      WHERE t.week_end = @week_end), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 110, 'ns_avggmv', IFNULL(
      (SELECT
        AVG(
          (
            IFNULL(t.gmv, 0) + IFNULL(t.payment_money, 0)
          ) / t.days_wd
        )
      FROM
        feods.fjr_kpi_ns_avggmv_month t
      WHERE t.month_id = @month_id), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 201, 'np_gmv', IFNULL(
      (SELECT
        (SELECT
          SUM(t.gmv)
        FROM
          feods.fjr_kpi_np_gmv_week t
        WHERE t.week_end = @week_end) /
        (SELECT
          SUM(t.gmv)
        FROM
          feods.fjr_kpi_gmv_week t
        WHERE t.week_end = @week_end)), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 201, 'np_gmv', IFNULL(
      (SELECT
        (SELECT
          SUM(t.gmv)
        FROM
          feods.fjr_kpi_np_gmv_month t
        WHERE t.month_id = @month_id) /
        (SELECT
          SUM(t.gmv)
        FROM
          feods.fjr_kpi_gmv_month t
        WHERE t.month_id = @month_id)), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 202, 'np_sal_sto_week', IFNULL(
      (SELECT
        SUM(t.shelfs_sal) / SUM(t.shelfs_sto)
      FROM
        feods.fjr_kpi_np_sal_sto_week t
      WHERE t.week_end = @week_end), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 202, 'np_sal_sto_week', IFNULL(
      (SELECT
        SUM(t.shelfs_sal) / SUM(t.shelfs_sto)
      FROM
        feods.fjr_kpi_np_sal_sto_month t
      WHERE t.month_id = @month_id), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 203, 'np_out', IFNULL(
      (SELECT
        SUM(
          CASE
            t.out_flag
            WHEN 1
            THEN t.stoval
          END
        ) / SUM(t.stoval)
      FROM
        feods.fjr_kpi_np_out_week t
      WHERE t.sdate >= @week_start
        AND t.sdate < @next_week_start), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 203, 'np_out', IFNULL(
      (SELECT
        SUM(
          CASE
            t.out_flag
            WHEN 1
            THEN t.stoval
          END
        ) / SUM(t.stoval)
      FROM
        feods.fjr_kpi_np_out_week t
      WHERE t.sdate >= @month_first_day
        AND t.sdate < @next_month_first_day), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 204, 'hotsal', IFNULL(
      (SELECT
        COUNT(*)
      FROM
        feods.zs_area_product_sale_flag t
        JOIN feods.vv_fjr_product_dim_sserp_period_tmp vv   -- 用 vv_fjr_product_dim_sserp_period_tmp 取代 vv_fjr_product_dim_sserp_period
          ON vv.min_date <= DATE(t.sdate)
          AND vv.max_date > t.sdate
        JOIN feods.zs_product_dim_sserp_his pdh
          ON pdh.business_area = t.business_area
          AND pdh.product_id = t.product_id
          AND pdh.product_type = '新增（试运行）'
          AND pdh.version = vv.version
      WHERE t.sale_level = '热卖'
        AND t.sdate = @next_week_start), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 204, 'hotsal', IFNULL(
      (SELECT
        COUNT(
          DISTINCT t.business_area, t.product_id
        )
      FROM
        feods.zs_area_product_sale_flag t
        JOIN feods.vv_fjr_product_dim_sserp_period_tmp vv    -- 用 vv_fjr_product_dim_sserp_period_tmp 取代 vv_fjr_product_dim_sserp_period
          ON vv.min_date <= DATE(t.sdate)
          AND vv.max_date > t.sdate
        JOIN feods.zs_product_dim_sserp_his pdh
          ON pdh.business_area = t.business_area
          AND pdh.product_id = t.product_id
          AND pdh.product_type = '新增（试运行）'
          AND pdh.version = vv.version
      WHERE t.sale_level = '热卖'
        AND t.sdate >= @month_2day
        AND t.sdate < @next_month_first2_day), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 205, 'city_product_sat_rate', IFNULL(
      (SELECT
        COUNT(*) / @business_areas
      FROM
        (SELECT
          t.business_area, COUNT(*) ct
        FROM
          feods.fjr_kpi_area_product_sat_rate t
        WHERE t.week_end = @week_end
        GROUP BY t.business_area
        HAVING ct > 45) t), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 205, 'city_product_sat_rate', IFNULL(
      (SELECT
        COUNT(DISTINCT t.business_area) / @business_areas
      FROM
        (SELECT
          t.week_end, t.business_area, COUNT(*) ct
        FROM
          feods.fjr_kpi_area_product_sat_rate t
        WHERE t.week_end >= @month_first_day
          AND t.week_end < @next_month_first_day
        GROUP BY t.week_end, t.business_area
        HAVING ct > 45) t), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @week_end, 'w', 206, 'np_profit', IFNULL(
      (SELECT
        SUM(t.gmv_profit) / SUM(t.gmv)
      FROM
        feods.fjr_kpi_np_gmv_week t
      WHERE t.week_end = @week_end), 0
    ), @add_user;
  INSERT INTO feods.fjr_kpi_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_first_day, 'm', 206, 'np_profit', IFNULL(
      (SELECT
        SUM(t.gmv_profit) / SUM(t.gmv)
      FROM
        feods.fjr_kpi_np_gmv_month t
      WHERE t.month_id = @month_id), 0
    ), @add_user;
  CALL feods.sp_task_log (
    'sp_kpi_monitor', @week_end, CONCAT(
      'fjr_d_eb51f36e2e735ccb5c49e85031509c12', @timestamp, @add_user
    )
  );
  COMMIT;
END