CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_board`()
BEGIN
  #run after sh_process.sh_shelf_level_ab
#run after sh_process.sp_shelf_dgmv
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, group_concat_max_len = 40960;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @sub_day := SUBDATE(@sdate, 1);
  SET @sub6_day := SUBDATE(@sdate, 6);
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_end := LAST_DAY(@sdate);
  SET @month_start_last := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @month_start_last2 := SUBDATE(@month_start, INTERVAL 2 MONTH);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @y_m_last := DATE_FORMAT(@month_start_last, '%Y-%m');
  SET @y_m_last2 := DATE_FORMAT(@month_start_last2, '%Y-%m');
  SET @ym := DATE_FORMAT(@sdate, '%Y%m');
  SET @ym_last := DATE_FORMAT(@month_start_last, '%Y%m');
  SET @w := WEEKDAY(@sdate);
  SET @week_end := ADDDATE(@sdate, 6- @w);
  SET @week_end_last := SUBDATE(@week_end, 7);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  SET @time_18 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT t.shelf_id,
         b.business_name,
         t.shelf_type,
         t.shelf_status = 2 shelf_status2,
         t.shelf_status = 2 && (t.whether_close = 2 || t.close_type = 12) && t.revoke_status = 1 flag_status,
         !ISNULL(sr.shelf_id) if_relation
  FROM fe.sf_shelf t
  JOIN fe_dwd.dwd_city_business b ON t.city = b.city
  LEFT JOIN -- 判断是否为关联货架
  (SELECT  t.main_shelf_id shelf_id
   FROM fe.sf_shelf_relation_record t
   WHERE t.data_flag = 1
   AND t.shelf_handle_status = 9
   UNION
   SELECT t.secondary_shelf_id shelf_id
   FROM fe.sf_shelf_relation_record t
   WHERE t.data_flag = 1
   AND t.shelf_handle_status = 9
   ) sr ON t.shelf_id = sr.shelf_id
  WHERE t.data_flag = 1;
--     SELECT
--     t.shelf_id, t.business_name, t.shelf_type, t.shelf_status = 2 shelf_status2, t.shelf_status = 2 && (
--       t.whether_close = 2 || t.close_type = 12
--     ) && t.revoke_status = 1 flag_status, t.relation_flag if_relation
--   FROM fe_dwd.dwd_shelf_base_day_all t
--    where  ! ISNULL(t.shelf_id);
  
  SET @time_20 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_18--@time_20", @time_18, @time_20
  );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_second_tmp;
  SET @time_23 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.shelf_second_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT distinct  -- 0703报错加distinct处理
    t.secondary_shelf_id shelf_id, s.shelf_status2, s.flag_status
  FROM
    fe.sf_shelf_relation_record t
    JOIN feods.shelf_tmp s
      ON t.main_shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.shelf_handle_status = 9
    AND ! ISNULL(t.secondary_shelf_id);
  SET @time_25 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_23--@time_25", @time_23, @time_25
  );
  SET @time_27 := CURRENT_TIMESTAMP();
  UPDATE
    feods.shelf_tmp t
    JOIN feods.shelf_second_tmp s
      ON t.shelf_id = s.shelf_id SET t.shelf_status2 = s.shelf_status2, t.flag_status = s.flag_status;
  SET @time_29 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_27--@time_29", @time_27, @time_29
  );
  SELECT
    @alpha := IFNULL(
      SUM(IF(t.if_work_day, 1, .5)) / SUM(
        CASE
          WHEN t.sdate > @sdate
          THEN 0
          WHEN t.if_work_day
          THEN 1
          ELSE .5
        END
      ), 0
    ) alpha
  FROM
    feods.fjr_work_days t
  WHERE t.sdate >= @month_start
    AND t.sdate <= @month_end
    AND t.holiday = '';
  DROP TEMPORARY TABLE IF EXISTS feods.maxgmv_tmp;
  SET @time_33 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.maxgmv_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, MAX(t.gmv + t.payment_money) pgmv
  FROM
    feods.fjr_shelf_mgmv t
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  SET @time_35 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_33--@time_35", @time_33, @time_35
  );
  DROP TEMPORARY TABLE IF EXISTS feods.d7gmv_tmp;
  SET @time_38 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.d7gmv_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.gmv + t.payment_money) pgmv
  FROM
    feods.fjr_shelf_dgmv t
  WHERE t.sdate >= @sub6_day
    AND t.sdate < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  SET @time_40 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_38--@time_40", @time_38, @time_40
  );
  DROP TEMPORARY TABLE IF EXISTS feods.mgmv_tmp;
  SET @time_43 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.mgmv_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, t.pgmv pgmv_mmax, l2.gmv + l2.payment_money pgmv_lm2, l.gmv + l.payment_money pgmv_lm, m.gmv + m.payment_money pgmv_m, m.payment_money afterpay_m, ROUND((m.gmv + m.payment_money) * @alpha, 2) pgmv_mp, d7.pgmv pgmv_d7
  FROM
    feods.maxgmv_tmp t
    LEFT JOIN feods.fjr_shelf_mgmv m
      ON t.shelf_id = m.shelf_id
      AND m.month_id = @y_m
    LEFT JOIN feods.fjr_shelf_mgmv l
      ON t.shelf_id = l.shelf_id
      AND l.month_id = @y_m_last
    LEFT JOIN feods.fjr_shelf_mgmv l2
      ON t.shelf_id = l2.shelf_id
      AND l2.month_id = @y_m_last2
    LEFT JOIN feods.d7gmv_tmp d7
      ON t.shelf_id = d7.shelf_id
  WHERE ! ISNULL(t.shelf_id);
  SET @time_45 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_43--@time_45", @time_43, @time_45
  );
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_level_tmp;
  SET @time_48 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.shelf_level_tmp (PRIMARY KEY (shelf_id)) AS
    SELECT t.shelf_id,
         MAX(IF(t.month_id = @y_m, t.grade, NULL)) shelf_level,
         MAX(IF(t.month_id = @y_m_last, t.grade, NULL)) shelf_level_lm
  FROM feods.d_op_shelf_grade t
  WHERE t.month_id IN(@y_m, @y_m_last)
  AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  
  
--   SELECT
--     t.shelf_id, MAX(
--       IF(
--         t.smonth = @ym, t.shelf_level_t, NULL
--       )
--     ) shelf_level, MAX(
--       IF(
--         t.smonth = @ym_last, t.shelf_level, NULL
--       )
--     ) shelf_level_lm
--   FROM
--     feods.pj_shelf_level_ab t
--   WHERE t.smonth IN (@ym, @ym_last)
--     AND ! ISNULL(t.shelf_id)
--   GROUP BY t.shelf_id;
  
  SET @time_50 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_48--@time_50", @time_48, @time_50
  );
  DROP TEMPORARY TABLE IF EXISTS feods.check_error_tmp;
  SET @time_53 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.check_error_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    c.shelf_id, SUM(cd.error_num * cd.sale_price) val_error
  FROM
    fe.sf_shelf_check c
    JOIN fe.sf_shelf_check_detail cd
      ON c.check_id = cd.check_id
      AND cd.data_flag = 1
      AND cd.error_num != 0
      AND cd.error_reason = 3
  WHERE c.data_flag = 1
    AND c.operate_time >= @month_start
    AND c.operate_time < @add_day
    AND ! ISNULL(c.shelf_id)
  GROUP BY c.shelf_id;
  SET @time_55 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_53--@time_55", @time_53, @time_55
  );
  DROP TEMPORARY TABLE IF EXISTS feods.last_check_id_tmp;
  SET @time_58 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.last_check_id_tmp (PRIMARY KEY (check_id))
  SELECT
    MAX(t.check_id) check_id
  FROM
    fe.sf_shelf_check t
    JOIN
      (SELECT
        shelf_id, MAX(operate_time) operate_time
      FROM
        fe.sf_shelf_check
      WHERE data_flag = 1
        AND ! ISNULL(check_id)
      GROUP BY shelf_id) mo
      ON t.SHELF_ID = mo.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.check_id)
  GROUP BY t.SHELF_ID;
  SET @time_60 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_58--@time_60", @time_58, @time_60
  );
  DROP TEMPORARY TABLE IF EXISTS feods.last_check_tmp;
  SET @time_63 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.last_check_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    c.shelf_id, DATE(c.operate_time) last_check, IF(
      r.audit_status = 1, r.is_fake_check, 0
    ) check_audit_status, IF(
      c.operate_time >= @month_start && c.operate_time < @add_day, c.month_lose_rate, 0
    ) month_lose_rate
  FROM
    fe.sf_shelf_check c
    JOIN feods.last_check_id_tmp m
      ON c.check_id = m.check_id
    LEFT JOIN fe.sf_check_audit_record r
      ON c.check_id = r.check_id
      AND r.data_flag = 1
  WHERE c.data_flag = 1
    AND ! ISNULL(c.shelf_id);
  SET @time_65 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_63--@time_65", @time_63, @time_65
  );
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  SET @time_68 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, DATE(
      MAX(
        IF(
          t.order_status = 2, NULL, t.fill_time
        )
      )
    ) last_fill, DATEDIFF(
      @sdate, MIN(
        IF(
          t.order_status = 2, t.send_time, NULL
        )
      )
    ) + 1 onway_day
  FROM
    fe.sf_product_fill_order t
  WHERE t.data_flag = 1
    AND t.order_status IN (2, 3, 4)
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  SET @time_70 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_68--@time_70", @time_68, @time_70
  );
  DROP TEMPORARY TABLE IF EXISTS feods.detail_tmp;
  SET @time_73 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.detail_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, SUM(
      IF(
        f.danger_flag > 3, IF(
          t.stock_quantity > 0, t.stock_quantity, 0
        ) * t.sale_price, 0
      )
    ) val_sto_danger, SUM(
      IF(
        t.stock_quantity > 0, t.stock_quantity, 0
      )
    ) qty_sto, SUM(
      IF(
        t.stock_quantity > 0, t.stock_quantity, 0
      ) * t.sale_price
    ) val_sto, SUM(t.stock_quantity > 0) skus_sto, SUM(
      t.stock_quantity > 0 && f.new_flag = 1
    ) skus_sto_new, SUM(
      t.stock_quantity <= 0 && f.sales_flag < 3 && t.shelf_fill_flag = 1
    ) skus_nsto_flag12, SUM(
      t.shelf_fill_flag = 1 && ! ISNULL(d.product_id)
    ) skus_fflag, SUM(
      IF(
        t.shelf_fill_flag = 1 && ! ISNULL(d.product_id), t.max_quantity, 0
      )
    ) max_quantity, SUM(
      IF(
        f.sales_flag = 5 && (f.new_flag = 2 || f.new_flag IS NULL), IF(
          t.stock_quantity > 0, t.stock_quantity, 0
        ) * t.sale_price, 0
      )
    ) val_sto_flag5, SUM(t.stock_quantity < 0) negative_skus
  FROM
    fe.sf_shelf_product_detail t
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.detail_id = f.detail_id
      AND f.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.zs_product_dim_sserp d
      ON s.business_name = d.business_area
      AND t.product_id = d.product_id
      AND d.product_type IN (
        '新增（试运行）', '原有'
      )
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  SET @time_75 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_73--@time_75", @time_73, @time_75
  );
  DROP TEMPORARY TABLE IF EXISTS feods.order_tmp;
  SET @time_78 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.order_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, COUNT(*) orders, COUNT(DISTINCT t.user_id) users
  FROM
    fe.sf_order t
  WHERE t.order_status = 2
    AND t.order_date >= @sub6_day
    AND t.order_date < @add_day
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  SET @time_80 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_78--@time_80", @time_78, @time_80
  );
  DROP TEMPORARY TABLE IF EXISTS feods.gmv3_tmp;
  SET @time_83 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.gmv3_tmp (PRIMARY KEY (shelf_id))
  SELECT
    shelf_id, COUNT(*) weeks, SUM(IF(sdate = @week_end, gmv, 0)) gmv, SUM(IF(sdate = @week_end_last, gmv, 0)) lgmv, SUBSTRING_INDEX(
      GROUP_CONCAT(gmv
        ORDER BY gmv DESC), ',', 3
    ) gmv3
  FROM
    feods.fjr_shelf_wgmv
  GROUP BY shelf_id;
  SET @time_85 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_83--@time_85", @time_83, @time_85
  );
  DROP TEMPORARY TABLE IF EXISTS feods.wgmv3_tmp;
  SET @time_88 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.wgmv3_tmp (PRIMARY KEY (shelf_id))
  SELECT
    shelf_id, weeks, gmv, lgmv, SUBSTRING_INDEX(gmv3, ',', 1) gmv_top, IF(
      weeks < 2, 0, SUBSTRING_INDEX(
        SUBSTRING_INDEX(gmv3, ',', 2), ',', - 1
      )
    ) gmv_top2, IF(
      weeks < 3, 0, SUBSTRING_INDEX(gmv3, ',', - 1)
    ) gmv_top3, @add_user add_user
  FROM
    feods.gmv3_tmp;
  SET @time_90 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_88--@time_90", @time_88, @time_90
  );
  DROP TEMPORARY TABLE IF EXISTS feods.gmv3_tmp;
  SET @time_93 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.gmv3_tmp (PRIMARY KEY (shelf_id))
  SELECT
    shelf_id, COUNT(*) months, SUM(IF(month_id = @y_m, gmv, 0)) gmv, SUM(IF(month_id = @y_m_last, gmv, 0)) lgmv, SUBSTRING_INDEX(
      GROUP_CONCAT(gmv
        ORDER BY gmv DESC), ',', 3
    ) gmv3
  FROM
    feods.fjr_shelf_mgmv
  GROUP BY shelf_id;
  SET @time_95 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_93--@time_95", @time_93, @time_95
  );
  DROP TEMPORARY TABLE IF EXISTS feods.mgmv3_tmp;
  SET @time_98 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.mgmv3_tmp (PRIMARY KEY (shelf_id))
  SELECT
    shelf_id, months, gmv, lgmv, SUBSTRING_INDEX(gmv3, ',', 1) gmv_top, IF(
      months < 2, 0, SUBSTRING_INDEX(
        SUBSTRING_INDEX(gmv3, ',', 2), ',', - 1
      )
    ) gmv_top2, IF(
      months < 3, 0, SUBSTRING_INDEX(gmv3, ',', - 1)
    ) gmv_top3, @add_user add_user
  FROM
    feods.gmv3_tmp;
  SET @time_100 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_98--@time_100", @time_98, @time_100
  );
  DROP TEMPORARY TABLE IF EXISTS feods.dgmv_tmp;
  SET @time_103 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.dgmv_tmp (PRIMARY KEY (shelf_id))
  SELECT
    shelf_id, COUNT(*) days, SUM(gmv) gmv_total, SUM(IF(sdate = @sdate, gmv, 0)) gmv, SUM(IF(sdate = @sub_day, gmv, 0)) lgmv
  FROM
    feods.fjr_shelf_dgmv
  GROUP BY shelf_id;
  SET @time_105 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_103--@time_105", @time_103, @time_105
  );
  TRUNCATE TABLE feods.fjr_shelf_board;
  SET @time_108 := CURRENT_TIMESTAMP();
  INSERT INTO feods.fjr_shelf_board (
    shelf_id, pgmv_mmax, pgmv_lm2, pgmv_lm, shelf_level_lm, pgmv_m, afterpay_m, pgmv_mp, pgmv_d7, orders_d7, users_d7, shelf_level, val_error, month_lose_rate, val_sto_danger, last_check, check_audit_status, last_fill, onway_day, qty_sto, val_sto, skus_sto, negative_skus, skus_sto_new, skus_nsto_flag12, skus_fflag, max_quantity, val_sto_flag5, gmv_total, days, gmv, lgmv, weeks, wgmv, lwgmv, wgmv_top, wgmv_top2, wgmv_top3, months, mgmv, lmgmv, mgmv_top, mgmv_top2, mgmv_top3, add_user
  )
  SELECT
    t.shelf_id, IFNULL(m.pgmv_mmax, 0), IFNULL(m.pgmv_lm2, 0), IFNULL(m.pgmv_lm, 0), IFNULL(s.shelf_level_lm, 0), IFNULL(m.pgmv_m, 0), IFNULL(m.afterpay_m, 0), IFNULL(m.pgmv_mp, 0), IFNULL(m.pgmv_d7, 0), IFNULL(o.orders, 0) orders_d7, IFNULL(o.users, 0) users_d7, IFNULL(s.shelf_level, 0), IFNULL(ce.val_error, 0), IFNULL(lc.month_lose_rate, 0), IFNULL(t.val_sto_danger, 0), lc.last_check, IFNULL(lc.check_audit_status, 0), f.last_fill, IFNULL(f.onway_day, 0), IFNULL(t.qty_sto, 0), IFNULL(t.val_sto, 0), IFNULL(t.skus_sto, 0), IFNULL(t.negative_skus, 0), IFNULL(t.skus_sto_new, 0), IFNULL(t.skus_nsto_flag12, 0), IFNULL(t.skus_fflag, 0), IFNULL(t.max_quantity, 0), IFNULL(t.val_sto_flag5, 0), IFNULL(dg.gmv_total, 0), IFNULL(dg.days, 0), IFNULL(dg.gmv, 0), IFNULL(dg.lgmv, 0), IFNULL(w3.weeks, 0), IFNULL(w3.gmv, 0), IFNULL(w3.lgmv, 0), IFNULL(w3.gmv_top, 0), IFNULL(w3.gmv_top2, 0), IFNULL(w3.gmv_top3, 0), IFNULL(m3.months, 0), IFNULL(m3.gmv, 0), IFNULL(m3.lgmv, 0), IFNULL(m3.gmv_top, 0), IFNULL(m3.gmv_top2, 0), IFNULL(m3.gmv_top3, 0), @add_user add_user
  FROM
      -- feods.detail_tmp t -- 20200512修改：以这个为主表会导致已激活但未上架商品的货架缺失
 --   feods.shelf_tmp a -- 20200512增加
--    LEFT JOIN feods.detail_tmp t ON a.shelf_id = t.shelf_id -- 20200512增加
    
    feods.detail_tmp t -- 这是原来的写法
    LEFT JOIN feods.mgmv_tmp m
      ON t.shelf_id = m.shelf_id
    LEFT JOIN feods.shelf_level_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.check_error_tmp ce
      ON t.shelf_id = ce.shelf_id
    LEFT JOIN feods.last_check_tmp lc
      ON t.shelf_id = lc.shelf_id
    LEFT JOIN feods.fill_tmp f
      ON t.shelf_id = f.shelf_id
    LEFT JOIN feods.order_tmp o
      ON t.shelf_id = o.shelf_id
    LEFT JOIN feods.mgmv3_tmp m3
      ON t.shelf_id = m3.shelf_id
    LEFT JOIN feods.wgmv3_tmp w3
      ON t.shelf_id = w3.shelf_id
    LEFT JOIN feods.dgmv_tmp dg
      ON t.shelf_id = dg.shelf_id;
      
  DELETE
  FROM
    feods.d_op_shelf_board_month
  WHERE sdate BETWEEN @month_start
    AND @month_end;
  INSERT INTO feods.d_op_shelf_board_month (
    sdate, shelf_id, pgmv_mmax, pgmv_lm2, pgmv_lm, shelf_level_lm, pgmv_m, afterpay_m, pgmv_mp, pgmv_d7, orders_d7, users_d7, shelf_level, val_error, month_lose_rate, val_sto_danger, last_check, check_audit_status, last_fill, onway_day, qty_sto, val_sto, skus_sto, negative_skus, skus_sto_new, skus_nsto_flag12, skus_fflag, max_quantity, val_sto_flag5, gmv_total, days, gmv, lgmv, weeks, wgmv, lwgmv, wgmv_top, wgmv_top2, wgmv_top3, months, mgmv, lmgmv, mgmv_top, mgmv_top2, mgmv_top3, add_user
  )
  SELECT
    @sdate sdate, shelf_id, pgmv_mmax, pgmv_lm2, pgmv_lm, shelf_level_lm, pgmv_m, afterpay_m, pgmv_mp, pgmv_d7, orders_d7, users_d7, shelf_level, val_error, month_lose_rate, val_sto_danger, last_check, check_audit_status, last_fill, onway_day, qty_sto, val_sto, skus_sto, negative_skus, skus_sto_new, skus_nsto_flag12, skus_fflag, max_quantity, val_sto_flag5, gmv_total, days, gmv, lgmv, weeks, wgmv, lwgmv, wgmv_top, wgmv_top2, wgmv_top3, months, mgmv, lmgmv, mgmv_top, mgmv_top2, mgmv_top3, @add_user add_user
  FROM
    feods.fjr_shelf_board;
  SET @time_110 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_108--@time_110", @time_108, @time_110
  );
  DELETE
  FROM
    feods.d_op_shelfs_dstat
  WHERE sdate = @sdate;
  SET @time_113 := CURRENT_TIMESTAMP();
  INSERT INTO feods.d_op_shelfs_dstat (
    sdate, business_name, shelf_type, if_relation, shelfs_act, shelfs_rem, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_type, t.if_relation, COUNT(*) shelfs_act, SUM(flag_status) shelfs_rem, @add_user add_user
  FROM
    feods.shelf_tmp t
  WHERE t.shelf_status2
  GROUP BY t.business_name, t.shelf_type, t.if_relation;
  SET @time_115 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_113--@time_115", @time_113, @time_115
  );
  DELETE
  FROM
    feods.d_op_shelfs_area
  WHERE sdate = @sdate;
  SET @time_118 := CURRENT_TIMESTAMP();
  INSERT INTO feods.d_op_shelfs_area (
    sdate, business_name, shelfs_act13, shelfs_rem13, shelfs_act2, shelfs_rem2, shelfs_act4, shelfs_rem4, shelfs_act_relation, shelfs_rem_relation, shelfs_act, shelfs_rem, add_user
  )
  SELECT
    t.sdate, t.business_name, SUM(
      IF(
        ! t.if_relation && t.shelf_type IN (1, 3), t.shelfs_act, 0
      )
    ) shelfs_act13, SUM(
      IF(
        ! t.if_relation && t.shelf_type IN (1, 3), t.shelfs_rem, 0
      )
    ) shelfs_rem13, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 2, t.shelfs_act, 0
      )
    ) shelfs_act2, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 2, t.shelfs_rem, 0
      )
    ) shelfs_rem2, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 5, t.shelfs_act, 0
      )
    ) shelfs_act4, SUM(
      IF(
        ! t.if_relation && t.shelf_type = 5, t.shelfs_rem, 0
      )
    ) shelfs_rem4, SUM(IF(t.if_relation, t.shelfs_act, 0)) shelfs_act_relation, SUM(IF(t.if_relation, t.shelfs_rem, 0)) shelfs_rem_relation, SUM(t.shelfs_act) shelfs_act, SUM(t.shelfs_rem) shelfs_rem, @add_user add_user
  FROM
    feods.d_op_shelfs_dstat t
  WHERE t.sdate = @sdate
    AND t.shelf_type IN (1, 2, 3, 5)
  GROUP BY t.business_name;
  SET @time_120 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info (
    "sp_shelf_board", "@time_118--@time_120", @time_118, @time_120
  );
  CALL feods.sp_task_log (
    'sp_shelf_board', @sdate, CONCAT(
      'fjr_d_844a56f35a81601ddd9f76fef9eddf4a', @timestamp, @add_user
    )
  );
  COMMIT;
END