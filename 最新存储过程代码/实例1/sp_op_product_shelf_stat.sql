CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_product_shelf_stat`()
BEGIN
  #run after sh_process.sh_shelf_level_ab
   #run after sh_process.sp_area_product_dgmv
   #run after sh_process.sp_op_order_and_item
   #run after sh_process.sp_op_sp_stock_detail
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @day1m := SUBDATE(@sdate, INTERVAL 1 MONTH);
  SET @day2m := SUBDATE(@sdate, INTERVAL 2 MONTH);
  SET @ym_last := DATE_FORMAT(@day1m, '%Y%m');
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @ym := DATE_FORMAT(@sdate, '%Y%m');
  SET @y_m_last := DATE_FORMAT(@day1m, '%Y-%m');
  SET @y_m_last2 := DATE_FORMAT(@day2m, '%Y-%m');
  SET @day14 := SUBDATE(@sdate, 13);
  SET @month_start := SUBDATE(@sdate, DAY(@sdate) - 1);
  SET @add_day := ADDDATE(@sdate, 1);
  SET @d := DAY(@sdate);
  SET @d_add := DAY(@add_day);
  SET @d_lm := DAY(SUBDATE(@month_start, 1));
  SET @y_m_add := DATE_FORMAT(@add_day, '%Y-%m');
  SET @month_flag := (@sdate = LAST_DAY(@sdate));
  SET @dtable := CONCAT(
    'feods.d_op_shelf_product_detail_combine', @d_add
  );
  
  
  SET @time_1 := CURRENT_TIMESTAMP();
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id, b.business_name, t.shelf_status = 2 if2, abl.shelf_level shelf_level_last, ab.shelf_level_t, IFNULL(
      abl.shelf_level IN (
        '甲级', '甲级2', '乙级', '乙级2'
      ), 0
    ) if12, IFNULL(
      t.activate_time >= @month_start, 0
    ) ifact, IFNULL(t.revoke_time >= @month_start, 0) ifrev, t.shelf_type NOT IN (6, 7) type_flag, (
      t.shelf_status = 2 && t.revoke_status = 1 && t.whether_close = 2
    ) status_flag
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN feods.pj_shelf_level_ab abl
      ON t.shelf_id = abl.shelf_id
      AND abl.smonth = @ym_last
    LEFT JOIN feods.pj_shelf_level_ab ab
      ON t.shelf_id = ab.shelf_id
      AND ab.smonth = @ym
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id);
	
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_1--@time_2",@time_1,@time_2);
	
  DROP TEMPORARY TABLE IF EXISTS feods.lxy_shelf_tmp;
  CREATE TEMPORARY TABLE feods.lxy_shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    *
  FROM
    feods.shelf_tmp t
  WHERE t.type_flag
    AND ! ISNULL(t.shelf_id);
	
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_2--@time_3",@time_2,@time_3);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_area_tmp;
  CREATE TEMPORARY TABLE feods.shelf_area_tmp (PRIMARY KEY (business_name)) AS
  SELECT
    t.business_name, SUM(t.if2) shelfs_if2, SUM(t.if12) shelfs_ifl12
  FROM
    feods.lxy_shelf_tmp t
  WHERE t.status_flag
    AND ! ISNULL(t.business_name)
  GROUP BY t.business_name;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_3--@time_4",@time_3,@time_4);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.weeksales_tmp;
  CREATE TEMPORARY TABLE feods.weeksales_tmp (PRIMARY KEY (product_id, shelf_id))
  SELECT
    t.product_id, t.shelf_id
  FROM
    fe.sf_shelf_product_weeksales_detail t
  WHERE t.stat_date = ADDDATE(@day2m, 6- WEEKDAY(@day2m))
    AND t.sales_flag IN (4, 5)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_4--@time_5",@time_4,@time_5);
	
  DROP TEMPORARY TABLE IF EXISTS feods.detail_area_tmp;
  SET @sql_str := CONCAT(
    "CREATE TEMPORARY TABLE feods.detail_area_tmp ( PRIMARY KEY (product_id, business_name) ) AS SELECT t.product_id, s.business_name, MIN(t.first_fill_time) first_fill_time, SUM(t.shelf_fill_flag = 1) shelfs_sff, SUM( t.shelf_fill_flag = 1 && t.stock_quantity ) shelfs_sff_sto, SUM(t.stock_quantity > 0) shelfs_sto, SUM(t.stock_quantity > 0 && s.if12) shelfs_sto12, SUM( IF( t.stock_quantity > 0, t.stock_quantity, 0 ) ) stock_quantity, SUM(t.sales_flag = 5) shelfs_flag5, SUM( t.sales_flag = 5 && t.stock_quantity ) shelfs_flag5_sto, SUM( IF( t.sales_flag = 5 && t.stock_quantity > 0, t.stock_quantity, 0 ) ) stoqty_flag5 FROM ", @dtable, " t JOIN feods.lxy_shelf_tmp s ON t.shelf_id = s.shelf_id AND s.status_flag where !ISNULL(s.business_name) AND !ISNULL(t.product_id)  GROUP BY t.product_id, s.business_name"
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_5--@time_6",@time_5,@time_6);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    fi.product_id, s.business_name, SUM(
      IFNULL(
        fi.actual_send_num, fi.actual_apply_num
      )
    ) actual_num
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.lxy_shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND s.status_flag
  WHERE t.data_flag = 1
    AND t.order_status IN (1, 2)
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(fi.product_id)
  GROUP BY fi.product_id, s.business_name;
  
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_6--@time_7",@time_6,@time_7); 
  
  DROP TEMPORARY TABLE IF EXISTS feods.fill_ps_month_tmp;
  CREATE TEMPORARY TABLE feods.fill_ps_month_tmp (PRIMARY KEY (product_id, shelf_id)) AS
  SELECT
    fi.product_id, t.shelf_id, SUM(fi.actual_fill_num) actual_fill_num
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
  WHERE t.data_flag = 1
    AND t.order_status IN (3, 4)
    AND t.fill_time >= @month_start
    AND t.fill_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY fi.product_id, t.shelf_id
  HAVING SUM(fi.actual_fill_num) != 0;
  
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_7--@time_8",@time_7,@time_8);
 
  DROP TEMPORARY TABLE IF EXISTS feods.check_ps_month_tmp;
  CREATE TEMPORARY TABLE feods.check_ps_month_tmp (PRIMARY KEY (product_id, shelf_id)) AS
  SELECT
    cd.product_id, t.shelf_id, SUM(cd.stock_num) stock_num, SUM(cd.check_num) check_num, SUM(
      IF(
        cd.error_reason = 1, cd.stock_num, 0
      )
    ) stock_num1, SUM(
      IF(
        cd.error_reason = 1, cd.check_num, 0
      )
    ) check_num1, SUM(
      IF(
        cd.error_reason = 2, cd.stock_num, 0
      )
    ) stock_num2, SUM(
      IF(
        cd.error_reason = 2, cd.check_num, 0
      )
    ) check_num2, SUM(
      IF(
        cd.error_reason = 4, cd.stock_num, 0
      )
    ) stock_num4, SUM(
      IF(
        cd.error_reason = 4, cd.check_num, 0
      )
    ) check_num4
  FROM
    fe.sf_shelf_check t
    JOIN fe.sf_shelf_check_detail cd
      ON t.check_id = cd.check_id
      AND cd.data_flag = 1
      AND cd.stock_num != cd.check_num
  WHERE t.data_flag = 1
    AND t.operate_time >= @month_start
    AND t.operate_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(cd.product_id)
  GROUP BY cd.product_id, t.shelf_id
  HAVING SUM(cd.stock_num) != SUM(cd.check_num);
  
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_8--@time_9",@time_8,@time_9);
  
  DROP TEMPORARY TABLE IF EXISTS feods.dc_tmp;
  CREATE TEMPORARY TABLE feods.dc_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    p.product_id, t.business_name, t.dc_qty, t.qty_on_way
  FROM
    (SELECT
      t1.business_area business_name, t1.product_code2, SUM(t1.fbaseqty) dc_qty, IFNULL(SUM(ow.qty_on_way), 0) qty_on_way
    FROM
      (SELECT
        f.business_area, e.fnumber dc_code, d.fnumber product_code2, FLOOR(SUM(a.fbaseqty)) fbaseqty
      FROM
        sserp.T_STK_INVENTORY a
        JOIN sserp.T_BD_MATERIAL d
          ON a.fmaterialid = d.fmaterialid
        JOIN sserp.T_BD_STOCK e
          ON a.fstockid = e.fstockid
        JOIN sserp.ZS_DC_BUSINESS_AREA f
          ON e.fnumber = f.dc_code
      GROUP BY f.business_area, e.fnumber, d.fnumber) t1
      LEFT JOIN
        (SELECT
          po.dc_code, po.fnumber, SUM(
            IFNULL(po.FSALQTY, 0) - IFNULL(insto.qty, 0)
          ) qty_on_way
        FROM
          (SELECT
            s.FNUMBER DC_CODE, o.FBILLNO, m.fnumber, oe.FMATERIALID, FLOOR(SUM(oe.FSALQTY)) FSALQTY
          FROM
            sserp.T_PUR_POORDERENTRY oe
            LEFT JOIN sserp.T_PUR_POORDER o
              ON o.FID = oe.FID
            LEFT JOIN sserp.T_BD_MATERIAL m
              ON m.FMATERIALID = oe.FMATERIALID
            LEFT JOIN sserp.T_BD_STOCK s
              ON s.FSTOCKID = o.F_BGJ_FSTOREHOUSE
          WHERE 1
            AND o.FCLOSESTATUS = 'A'
            AND o.FCANCELSTATUS = 'A'
            AND oe.FMRPCLOSESTATUS = 'A'
          GROUP BY s.fnumber, o.fbillno, m.fnumber, oe.fmaterialid) po
          LEFT JOIN
            (SELECT
              a.fpoorderno, a.fmaterialid, FLOOR(SUM(a.frealqty)) AS qty
            FROM
              sserp.T_STK_INSTOCKENTRY a
              LEFT JOIN sserp.T_STK_INSTOCK b
                ON a.fid = b.fid
            WHERE 1
            GROUP BY a.fpoorderno, a.fmaterialid) insto
            ON po.fbillno = insto.fpoorderno
            AND po.fmaterialid = insto.fmaterialid
        GROUP BY po.dc_code, po.fnumber) ow
        ON t1.dc_code = ow.dc_code
        AND t1.product_code2 = ow.fnumber
    GROUP BY t1.business_area, t1.product_code2
    HAVING dc_qty > 0
      OR qty_on_way > 0) t
    JOIN fe.sf_product p
      ON t.product_code2 = p.product_code2
      AND p.data_flag = 1
  WHERE ! ISNULL(p.product_id)
    AND ! ISNULL(t.business_name);
	
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_9--@time_10",@time_9,@time_10);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.pre_tmp;
  CREATE TEMPORARY TABLE feods.pre_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    t.product_id, s.business_name, SUM(t.available_stock) available_stock
  FROM
    fe.sf_prewarehouse_stock_detail t
    JOIN feods.shelf_tmp s
      ON t.warehouse_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.available_stock > 0
    AND ! ISNULL(s.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.product_id, s.business_name;
  
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_10--@time_11",@time_10,@time_11);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.dgmv14_tmp;
  CREATE TEMPORARY TABLE feods.dgmv14_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    t.product_id, t.business_name, SUM(t.qty_sal) qty_sal
  FROM
    feods.d_op_product_area_shelftype_dgmv t
  WHERE t.sdate >= @day14
    AND t.sdate < @add_day
    AND t.shelf_type NOT IN (6, 7)
    AND ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.product_id, t.business_name;
  
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_11--@time_12",@time_11,@time_12);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.dgmv_tmp;
  CREATE TEMPORARY TABLE feods.dgmv_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    t.product_id, t.business_name, SUM(t.qty_sal) qty_sal, SUM(t.gmv) gmv, SUM(t.discount) discount
  FROM
    feods.d_op_product_area_shelftype_dgmv t
  WHERE t.sdate >= @month_start
    AND t.sdate < @add_day
    AND t.shelf_type NOT IN (6, 7)
    AND ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id)
  GROUP BY t.product_id, t.business_name;
  
SET @time_13 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_12--@time_13",@time_12,@time_13);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.sto_month_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.sto_month_tmp (PRIMARY KEY(product_id,business_name)) SELECT t.product_id, s.business_name, COUNT(*) shelfs_sto_month FROM feods.d_op_sp_stock_detail t JOIN feods.lxy_shelf_tmp s ON t.shelf_id = s.shelf_id and s.status_flag WHERE t.month_id = @y_m AND !ISNULL(s.business_name) AND !ISNULL(t.product_id)  AND (0 ", GROUP_CONCAT(
        CONCAT(" OR t.d", DAY(t.sdate), ">0 ") SEPARATOR ''
      ), ") GROUP BY t.product_id, s.business_name"
    ) INTO @sql_str
  FROM
    feods.fjr_work_days t
  WHERE t.sdate >= @month_start
    AND t.sdate < @add_day;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_14 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_13--@time_14",@time_13,@time_14);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp (
    KEY (shelf_id, product_id), KEY (product_id, business_name)
  ) AS
  SELECT
    t.order_id, DAY(t.pay_date) order_date, t.user_id, t.shelf_id, s.business_name, t.product_id,
    t.quantity_act, t.sale_price, t.ogmv, t.o_discount_amount, t.o_coupon_amount
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @month_start
    AND t.pay_date < @add_day
    AND t.quantity_act > 0;
	
SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_14--@time_15",@time_14,@time_15);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.spoi_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.spoi_tmp(PRIMARY KEY(shelf_id,product_id)) SELECT t.shelf_id,t.product_id", GROUP_CONCAT(
        CONCAT(
          ",SUM(t.order_date=", t.number, ")d", t.number
        ) SEPARATOR ' '
      ), " FROM feods.oi_tmp t GROUP BY t.shelf_id,t.product_id;"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_16 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_15--@time_16",@time_15,@time_16);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.sp_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.sp_tmp(PRIMARY KEY(shelf_id,product_id)) SELECT t.shelf_id,t.product_id,", GROUP_CONCAT(
        CONCAT(
          "(t.d", t.number, ">0||s.d", t.number, ">0)+"
        ) SEPARATOR ' '
      ), "0 days_sal_sto FROM feods.spoi_tmp t left JOIN feods.d_op_sp_stock_detail s ON t.shelf_id=s.shelf_id AND t.product_id=s.product_id AND s.month_id = @y_m"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_17 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_16--@time_17",@time_16,@time_17);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.oi_area_tmp;
  CREATE TEMPORARY TABLE feods.oi_area_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    t.product_id, t.business_name, COUNT(DISTINCT t.shelf_id) shelfs_sal_month, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.user_id) users
  FROM
    feods.oi_tmp t
    JOIN feods.lxy_shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND s.status_flag
  GROUP BY t.product_id, t.business_name;
  
 SET @time_18 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_17--@time_18",@time_17,@time_18); 
  
  DROP TEMPORARY TABLE IF EXISTS feods.oi_shelf_tmp;
  CREATE TEMPORARY TABLE feods.oi_shelf_tmp (PRIMARY KEY (product_id, shelf_id)) AS
  SELECT
    t.product_id, t.shelf_id, SUM(t.quantity_act) quantity_act, SUM(t.quantity_act * t.sale_price) gmv, SUM(
      t.o_discount_amount * t.quantity_act * t.sale_price / t.ogmv
    ) discount, SUM(
      t.o_coupon_amount * t.quantity_act * t.sale_price / t.ogmv
    ) coupon, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.order_date) days_sal, COUNT(DISTINCT t.user_id) users
  FROM
    feods.oi_tmp t
  GROUP BY t.product_id, t.shelf_id;
  
SET @time_19 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_18--@time_19",@time_18,@time_19);	  
  
  DROP TEMPORARY TABLE IF EXISTS feods.oi_single_tmp;
  CREATE TEMPORARY TABLE feods.oi_single_tmp (
    PRIMARY KEY (product_id, business_name)
  ) AS
  SELECT
    t.product_id, t.business_name, COUNT(*) users_single
  FROM
    (SELECT
      t.product_id, t.business_name, t.user_id, COUNT(*) ct
    FROM
      feods.oi_tmp t
      JOIN feods.lxy_shelf_tmp s
        ON t.shelf_id = s.shelf_id
    GROUP BY t.product_id, t.business_name, t.user_id
    HAVING ct = 1) t
  GROUP BY t.product_id, t.business_name;
  
SET @time_20 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_19--@time_20",@time_19,@time_20);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.oi_single_shelf_tmp;
  CREATE TEMPORARY TABLE feods.oi_single_shelf_tmp (PRIMARY KEY (product_id, shelf_id)) AS
  SELECT
    t.product_id, t.shelf_id, COUNT(*) users_single
  FROM
    (SELECT
      t.product_id, t.shelf_id, t.user_id, COUNT(*) ct
    FROM
      feods.oi_tmp t
    GROUP BY t.product_id, t.shelf_id, t.user_id
    HAVING ct = 1) t
  GROUP BY t.product_id, t.shelf_id;
  
  
SET @time_21 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_20--@time_21",@time_20,@time_21);  
  
  DELETE
  FROM
    feods.d_op_product_shelf_sal_month
  WHERE month_id = @y_m;
  INSERT INTO feods.d_op_product_shelf_sal_month (
    month_id, product_id, shelf_id, qty_sal, gmv, discount, coupon, days_sal, days_sal_sto, orders, users, users_single, add_user
  )
  SELECT
    @y_m month_id, t.product_id, t.shelf_id, t.quantity_act qty_sal, t.gmv, t.discount, t.coupon, t.days_sal, sp.days_sal_sto, t.orders, t.users, si.users_single, @add_user add_user
  FROM
    feods.oi_shelf_tmp t
    LEFT JOIN feods.oi_single_shelf_tmp si
      ON t.product_id = si.product_id
      AND t.shelf_id = si.shelf_id
    LEFT JOIN feods.sp_tmp sp
      ON t.product_id = sp.product_id
      AND t.shelf_id = sp.shelf_id;
	  
SET @time_22 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_21--@time_22",@time_21,@time_22);	  
	  
  DELETE
  FROM
    feods.d_op_product_area_stat_month
  WHERE month_id = @y_m;
  INSERT INTO feods.d_op_product_area_stat_month (
    month_id, product_id, business_name, first_fill_time, shelfs_sff, shelfs_sff_sto, shelfs_sto, shelfs_sto12, stock_quantity, shelfs_flag5, shelfs_flag5_sto, stoqty_flag5, shelfs_if2, shelfs_ifl12, actual_num, dc_qty, qty_on_way, available_stock, qty_sal14, qty_sal, gmv, discount, shelfs_sto_month, shelfs_sal_month, orders, users, users_single, add_user
  )
  SELECT
    @y_m month_id, t.product_id, t.business_name, t.first_fill_time, t.shelfs_sff, t.shelfs_sff_sto, t.shelfs_sto, t.shelfs_sto12, t.stock_quantity, t.shelfs_flag5, t.shelfs_flag5_sto, t.stoqty_flag5, ss.shelfs_if2, ss.shelfs_ifl12, f.actual_num, dc.dc_qty, dc.qty_on_way, pre.available_stock, dg14.qty_sal qty_sal14, dg.qty_sal, dg.gmv, dg.discount, sm.shelfs_sto_month, ois.shelfs_sal_month, ois.orders, ois.users, si.users_single, @add_user add_user
  FROM
    feods.detail_area_tmp t
    LEFT JOIN feods.shelf_area_tmp ss
      ON t.business_name = ss.business_name
    LEFT JOIN feods.fill_tmp f
      ON t.product_id = f.product_id
      AND t.business_name = f.business_name
    LEFT JOIN feods.dc_tmp dc
      ON t.product_id = dc.product_id
      AND t.business_name = dc.business_name
    LEFT JOIN feods.pre_tmp pre
      ON t.product_id = pre.product_id
      AND t.business_name = pre.business_name
    LEFT JOIN feods.dgmv14_tmp dg14
      ON t.product_id = dg14.product_id
      AND t.business_name = dg14.business_name
    LEFT JOIN feods.dgmv_tmp dg
      ON t.product_id = dg.product_id
      AND t.business_name = dg.business_name
    LEFT JOIN feods.sto_month_tmp sm
      ON t.product_id = sm.product_id
      AND t.business_name = sm.business_name
    LEFT JOIN feods.oi_area_tmp ois
      ON t.product_id = ois.product_id
      AND t.business_name = ois.business_name
    LEFT JOIN feods.oi_single_tmp si
      ON t.product_id = si.product_id
      AND t.business_name = si.business_name;
	  
SET @time_23 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_22--@time_23",@time_22,@time_23);	  
	  
  DELETE
  FROM
    feods.d_op_product_shelf_sto_month
  WHERE month_id = @y_m;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_product_shelf_sto_month(month_id, product_id, shelf_id, days_sto, qty_sto, qty_start, qty_end, add_user) SELECT @y_m month_id,product_id,shelf_id,0", GROUP_CONCAT(
        CONCAT(" +(d", DAY(t.sdate), ">0)") SEPARATOR ''
      ), "days_sto,0", GROUP_CONCAT(
        CONCAT(" +d", DAY(t.sdate)) SEPARATOR ''
      ), " qty_sto,d1 qty_start,d", LEAST(@d + 1, 31), " qty_end,@add_user add_user FROM feods.d_op_sp_stock_detail WHERE month_id=@y_m AND (0", GROUP_CONCAT(
        CONCAT(" OR d", DAY(t.sdate), ">0") SEPARATOR ' '
      ), ")"
    ) INTO @sql_str
  FROM
    feods.fjr_work_days t
  WHERE t.sdate >= @month_start
    AND t.sdate < @add_day;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
 SET @time_24 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_23--@time_24",@time_23,@time_24); 
  
  UPDATE
    feods.d_op_product_shelf_sto_month t
    JOIN feods.d_op_sp_stock_detail s
      ON t.shelf_id = s.shelf_id
      AND t.product_id = s.product_id
      AND s.month_id = @y_m_add SET t.qty_end = s.d1
  WHERE t.month_id = @y_m
    AND @month_flag = 1;
	
SET @time_25 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_24--@time_25",@time_24,@time_25);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.dim_list_tmp;
  CREATE TEMPORARY TABLE feods.dim_list_tmp (PRIMARY KEY (version_id)) AS
  SELECT
    DATE(
      IFNULL(@ddate, ADDDATE(CURRENT_DATE, 1))
    ) edate, @ddate := t.sdate sdate, t.version_id
  FROM
    (SELECT DISTINCT
      t.version version_id, DATE(t.pub_time) sdate, @ddate := NULL
    FROM
      feods.zs_product_dim_sserp_his t
    ORDER BY sdate DESC) t;
	
SET @time_26 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_25--@time_26",@time_25,@time_26);	
	
  SET @dim_start_day := DATE_FORMAT(@day1m, '%Y-%m-%01');
  SET @dim_end_day := @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.dim_2m_tmp;
  CREATE TEMPORARY TABLE feods.dim_2m_tmp (PRIMARY KEY (version_id)) AS
  SELECT
    t.version_id, t.sdate, t.edate
  FROM
    feods.dim_list_tmp t
  WHERE t.edate > @dim_start_day
    AND t.sdate < @dim_end_day;
  SELECT
    COUNT(*) INTO @dims
  FROM
    feods.dim_2m_tmp t;
  SELECT
    MIN(t.sdate) INTO @dim_min_sdate
  FROM
    feods.dim_2m_tmp t;
  SELECT
    t.version_id INTO @lversion
  FROM
    feods.dim_list_tmp t
  WHERE t.edate = @dim_min_sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.dim_2m_flag_tmp;
  CREATE TEMPORARY TABLE feods.dim_2m_flag_tmp (
    PRIMARY KEY (product_id, business_area)
  ) AS
  SELECT
    t.product_id, t.business_area
  FROM
    feods.zs_product_dim_sserp_his t
    JOIN feods.dim_2m_tmp m
      ON t.version = m.version_id
  WHERE t.product_type = '原有'
  GROUP BY t.product_id, t.business_area
  HAVING COUNT(*) = @dims;
  
SET @time_27 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_26--@time_27",@time_26,@time_27);  
  
  DROP TEMPORARY TABLE IF EXISTS feods.dim_main_tmp;
  CREATE TEMPORARY TABLE feods.dim_main_tmp (
    PRIMARY KEY (product_id, business_area)
  ) AS
  SELECT DISTINCT
    t.product_id, t.business_area
  FROM
    feods.zs_product_dim_sserp_his t
  WHERE t.version >= @lversion;
SET @time_28 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_27--@time_28",@time_27,@time_28);	  
  
  DELETE
  FROM
    feods.d_op_dim_product_area_normal
  WHERE month_id = @y_m;
  INSERT INTO feods.d_op_dim_product_area_normal (
    month_id, product_id, business_name, product_type, product_type_last2, normal_flag, fill_model_flag, add_user
  )
  SELECT
    @y_m month_id, t.product_id, t.business_area business_name, d.product_type, h.product_type product_type_last2, ! ISNULL(f.product_id) normal_flag, p.fill_model > 1 fill_model_flag, @add_user add_user
  FROM
    feods.dim_main_tmp t
    LEFT JOIN feods.zs_product_dim_sserp_his h
      ON t.product_id = h.product_id
      AND t.business_area = h.business_area
      AND h.version = @lversion
    LEFT JOIN feods.dim_2m_flag_tmp f
      ON t.product_id = f.product_id
      AND t.business_area = f.business_area
    LEFT JOIN feods.zs_product_dim_sserp d
      ON t.product_id = d.product_id
      AND t.business_area = d.business_area
    LEFT JOIN fe.sf_product p
      ON t.product_id = p.product_id;
	  
SET @time_29 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_28--@time_29",@time_28,@time_29);	
	  
  DROP TEMPORARY TABLE IF EXISTS feods.sal_tmp;
  CREATE TEMPORARY TABLE feods.sal_tmp (PRIMARY KEY (product_id, shelf_id)) AS
  SELECT
    t.product_id, t.shelf_id, SUM(
      IF(t.month_id = @y_m_last2, t.gmv, 0)
    ) gmv_last2, SUM(IF(t.month_id = @y_m_last, t.gmv, 0)) gmv_last, SUM(IF(t.month_id = @y_m, t.gmv, 0)) gmv, SUM(
      IF(
        t.month_id = @y_m_last, t.qty_sal, 0
      )
    ) qty_sal_last, SUM(IF(t.month_id = @y_m, t.qty_sal, 0)) qty_sal, SUM(
      IF(
        t.month_id = @y_m_last, t.discount, 0
      )
    ) discount_last, SUM(IF(t.month_id = @y_m, t.discount, 0)) discount, SUM(
      IF(t.month_id = @y_m_last, t.coupon, 0)
    ) coupon_last, SUM(IF(t.month_id = @y_m, t.coupon, 0)) coupon, SUM(
      IF(t.month_id = @y_m_last, t.users, 0)
    ) users_last, SUM(IF(t.month_id = @y_m, t.users, 0)) users, SUM(
      IF(t.month_id = @y_m_last, t.orders, 0)
    ) orders_last, SUM(IF(t.month_id = @y_m, t.orders, 0)) orders, SUM(
      IF(
        t.month_id = @y_m_last, t.users_single, 0
      )
    ) users_single_last, SUM(
      IF(
        t.month_id = @y_m, t.users_single, 0
      )
    ) users_single
  FROM
    feods.d_op_product_shelf_sal_month t
  WHERE t.month_id IN (@y_m, @y_m_last, @y_m_last2)
  GROUP BY t.product_id, t.shelf_id;
  
SET @time_30 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_29--@time_30",@time_29,@time_30);	  
  
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  CREATE TEMPORARY TABLE feods.sto_tmp (PRIMARY KEY (product_id, shelf_id)) AS
  SELECT
    t.product_id, t.shelf_id, SUM(IF(t.month_id = @y_m, 0, t.days_sto)) days_sto_last, SUM(IF(t.month_id = @y_m, t.days_sto, 0)) days_sto, SUM(IF(t.month_id = @y_m, 0, t.qty_sto)) qty_sto_last, SUM(IF(t.month_id = @y_m, t.qty_sto, 0)) qty_sto
  FROM
    feods.d_op_product_shelf_sto_month t
  WHERE t.month_id IN (@y_m, @y_m_last)
  GROUP BY t.product_id, t.shelf_id;
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_product_shelf_stat TRUNCATE PARTITION p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_31 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_30--@time_31",@time_30,@time_31);	   
  
  SET @sql_str := CONCAT(
    " INSERT INTO feods.d_op_product_shelf_stat ( month_id, product_id, shelf_id, ifact, ifrev, shelf_level_t, shelf_level_last, first_fill_time, fill_model_flag, product_type, product_type_last2, normal_flag, sales_flag, sales_flag_last2, gmv_last2, gmv_last, gmv, qty_sal_last, qty_sal, discount_last, discount, coupon_last, coupon, days_sto_last, days_sto, qty_sto_last, qty_sto, stock_quantity, users_last, users, orders_last, orders, users_single_last, users_single, add_user ) SELECT @y_m month_id, t.product_id, t.shelf_id, s.ifact, s.ifrev, s.shelf_level_t, s.shelf_level_last, t.first_fill_time, d.fill_model_flag, d.product_type, d.product_type_last2, d.normal_flag, t.sales_flag, ! ISNULL(ws.shelf_id)sales_flag_last2, sm.gmv_last2, sm.gmv_last, sm.gmv, sm.qty_sal_last, sm.qty_sal, sm.discount_last, sm.discount, sm.coupon_last, sm.coupon, stm.days_sto_last, stm.days_sto, stm.qty_sto_last, stm.qty_sto, t.stock_quantity, sm.users_last, sm.users, sm.orders_last, sm.orders, sm.users_single_last, sm.users_single, @add_user add_user FROM ", @dtable, " t LEFT JOIN feods.shelf_tmp s ON t.shelf_id = s.shelf_id LEFT JOIN feods.d_op_dim_product_area_normal d ON t.product_id = d.product_id AND s.business_name = d.business_name AND d.month_id = @y_m LEFT JOIN feods.sal_tmp sm ON t.shelf_id = sm.shelf_id AND t.product_id = sm.product_id LEFT JOIN feods.sto_tmp stm ON t.shelf_id = stm.shelf_id AND t.product_id = stm.product_id LEFT JOIN feods.weeksales_tmp ws ON t.shelf_id = ws.shelf_id AND t.product_id = ws.product_id WHERE t.stock_quantity > 0 OR sm.gmv > 0 OR sm.gmv_last > 0 OR sm.gmv_last2 > 0 OR stm.qty_sto > 0 OR stm.qty_sto_last > 0"
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_32 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_31--@time_32",@time_31,@time_32);	  
  
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_product_shelf_dam_month TRUNCATE PARTITION p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(
    "INSERT INTO feods.d_op_product_shelf_dam_month ( month_id, product_id, shelf_id, qty_sal, gmv, qty_start, qty_end, qty_fill, qty_check_error, qty_check_error1, qty_check_error2, qty_check_error4, add_user ) SELECT @y_m month_id, t.product_id, t.shelf_id, sal.qty_sal, sal.gmv, sto.qty_start, sto.qty_end, fil.actual_fill_num qty_fill, che.check_num - che.stock_num qty_check_error, che.check_num1 - che.stock_num1 qty_check_error1, che.check_num2 - che.stock_num2 qty_check_error2, che.check_num4 - che.stock_num4 qty_check_error4, @add_user add_user FROM ", @dtable, " t LEFT JOIN feods.d_op_product_shelf_sal_month sal ON t.product_id = sal.product_id AND t.shelf_id = sal.shelf_id AND sal.month_id = @y_m LEFT JOIN feods.d_op_product_shelf_sto_month sto ON t.product_id = sto.product_id AND t.shelf_id = sto.shelf_id AND sto.month_id = @y_m LEFT JOIN feods.fill_ps_month_tmp fil ON t.product_id = fil.product_id AND t.shelf_id = fil.shelf_id LEFT JOIN feods.check_ps_month_tmp che ON t.product_id = che.product_id AND t.shelf_id = che.shelf_id WHERE sal.qty_sal > 0 OR sto.qty_start != sto.qty_end OR fil.actual_fill_num != 0 OR che.check_num - che.stock_num != 0"
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_33 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_32--@time_33",@time_32,@time_33);	   
  
  DELETE
  FROM
    feods.d_op_product_shelf_sal_month_large
  WHERE month_id = @y_m;
  INSERT INTO feods.d_op_product_shelf_sal_month_large (
    month_id, shelf_id, product_id, qty_large, gmv_large, add_user
  )
  SELECT
    @y_m month_id, t.shelf_id, t.product_id, SUM(t.quantity_act) qty_large, 
    SUM(t.quantity_act * t.sale_price) gmv_large, @add_user add_user
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
  WHERE t.quantity_act * t.sale_price >= 100
    AND t.pay_date >= @month_start
    AND t.pay_date < @add_day
  GROUP BY t.shelf_id, t.product_id;
  
SET @time_34 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_33--@time_34",@time_33,@time_34);	     
  
  DELETE
  FROM
    feods.d_op_product_area_sal_month_large
  WHERE month_id = @y_m;
  INSERT INTO feods.d_op_product_area_sal_month_large (
    month_id, product_id, business_name, qty_large, gmv_large, add_user
  )
  SELECT
    @y_m month_id, t.product_id, s.business_name, SUM(t.qty_large) qty_large, SUM(t.gmv_large) gmv_large, @add_user add_user
  FROM
    feods.d_op_product_shelf_sal_month_large t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.month_id = @y_m
  GROUP BY t.product_id, s.business_name;
  
 SET @time_35 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_product_shelf_stat","@time_34--@time_35",@time_34,@time_35);	  
  
  CALL feods.sp_task_log (
    'sp_op_product_shelf_stat', @sdate, CONCAT(
      'yingnansong_d_3307c69e6baacde78733c87461db943d', @timestamp, @add_user
    )
  );
  COMMIT;
END