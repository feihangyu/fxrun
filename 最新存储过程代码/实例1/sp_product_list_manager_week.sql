CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_product_list_manager_week`()
BEGIN
  #run after sh_process.sp_order_and_item_lastxx_week
#run after sh_process.sh_shelf_flag
#run after sh_process.sh_area_product_sale_flag
#run after sh_process.sp_erp_stock_daily
#run after sh_process.sh_outstock_day
   SET @week_end := SUBDATE(
    CURRENT_DATE, WEEKDAY(CURRENT_DATE) + 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
 SET 
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @week_start := SUBDATE(@week_end, 6),
  @add_date := ADDDATE(@week_end, 1),
  @start30 := SUBDATE(@week_end, 29);  
 
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_area_tmp, feods.base_tmp, feods.dc_tmp, feods.dc_days_tmp, feods.sal_30_tmp, feods.sal_7_tmp, feods.sal_7all_tmp, feods.sal_7re_tmp, feods.sal_14_tmp, feods.sal_tmp;
  CREATE TEMPORARY TABLE feods.shelf_area_tmp AS
  SELECT
    s.shelf_id, s.shelf_status, b.business_name, b.region_name
  FROM
    fe.sf_shelf s
    JOIN fe_dwd.`dwd_city_business` b
      ON s.city = b.city
  WHERE s.data_flag = 1;
  CREATE INDEX idx_shelf_area_tmp_shelf_id_shelf_status
  ON feods.shelf_area_tmp (shelf_id, shelf_status);
  CREATE INDEX idx_shelf_area_tmp_shelf_status
  ON feods.shelf_area_tmp (shelf_status);
  CREATE TEMPORARY TABLE feods.base_tmp AS
  SELECT
    b.business_name, d.product_id, b.region_name, SUM(d.stock_quantity > 0) shelfs_stock, SUM(
      d.stock_quantity > 0
      AND (
        sf.GMV_level = 1
        OR sf.GMV_level IS NULL
      )
    ) shelfs_stock_lowsal, SUM(
      d.stock_quantity > 0
      OR (
        d.stock_quantity = 0
        AND d.shelf_fill_flag = 1
      )
    ) shelfs_normal, SUM(d.stock_quantity) stock_quantity, SUM(f.sales_flag = 5) shelfs_5, COUNT(*) shelfs, SUM(
      CASE
        f.sales_flag
        WHEN 5
        THEN d.stock_quantity
      END
    ) stock_quantity_5, MIN(f.first_fill_time) first_fill_time, COUNT(f.first_fill_time) if_fill
  FROM
    fe.sf_shelf_product_detail d
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON d.shelf_id = f.shelf_id
      AND d.product_id = f.product_id
      AND f.data_flag = 1
    JOIN feods.shelf_area_tmp b
      ON b.shelf_id = d.shelf_id
      AND b.shelf_status = 2
    LEFT JOIN feods.zs_shelf_flag sf
      ON d.shelf_id = sf.shelf_id
  WHERE d.data_flag = 1
  GROUP BY b.business_name, d.product_id;
  CREATE INDEX idx_base_tmp_business_name_product_id
  ON feods.base_tmp (business_name, product_id);
  CREATE TEMPORARY TABLE feods.dc_tmp AS
  SELECT
    t1.business_area business_name, t1.product_code2, SUM(t1.fbaseqty) fbaseqty, SUM(t2.fqty) fqty, SUM(ow.qty_on_way) qty_on_way
  FROM
    (SELECT
      f.business_area, e.fnumber dc_code, d.fnumber product_code2, SUM(a.fbaseqty) fbaseqty
    FROM
      sserp.T_STK_INVENTORY a
      LEFT JOIN sserp.T_BD_MATERIAL d
        ON a.fmaterialid = d.fmaterialid
      LEFT JOIN sserp.T_BD_STOCK e
        ON a.fstockid = e.fstockid
      LEFT JOIN sserp.ZS_DC_BUSINESS_AREA f
        ON e.fnumber = f.dc_code
    WHERE 1
    GROUP BY f.business_area, e.fnumber, d.fnumber) t1
    LEFT JOIN
      (SELECT
        b.f_bgj_fstorehouset dc_code, d.fnumber product_code2, SUM(fqty) fqty
      FROM
        sserp.T_STK_OUTSTOCKAPPLYENTRY a
        JOIN sserp.T_STK_OUTSTOCKAPPLY b
          ON a.fid = b.fid
        LEFT JOIN sserp.T_BD_MATERIAL d
          ON a.fmaterialid = d.fmaterialid
      WHERE 1
        AND b.fbilltypeid = '5b0e65b177a7e1'
        AND b.fcloseflag = 0
        AND b.fclosestatus = 'B'
        AND b.fcancelstatus = 'A'
        AND b.fdate >= SUBDATE(@week_end, 6)
        AND b.fdate < ADDDATE(@week_end, 1)
      GROUP BY b.f_bgj_fstorehouset, d.fnumber) t2
      ON t1.dc_code = t2.dc_code
      AND t1.product_code2 = t2.product_code2
    LEFT JOIN
      (SELECT
        po.dc_code, po.fnumber, SUM(
          IFNULL(po.FSALQTY, 0) - IFNULL(insto.qty, 0)
        ) qty_on_way
      FROM
        (SELECT
          s.FNUMBER DC_CODE, o.FBILLNO, m.fnumber, oe.FMATERIALID, SUM(oe.FSALQTY) FSALQTY
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
            a.fpoorderno, a.fmaterialid, SUM(a.frealqty) AS qty
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
  GROUP BY t1.business_area, t1.product_code2;
  CREATE INDEX idx_dc_tmp_business_name_product_code2
  ON feods.dc_tmp (business_name, product_code2);
  CREATE TEMPORARY TABLE feods.dc_days_tmp AS
  SELECT
    t.BUSINESS_AREA business_name, t.PRODUCT_BAR product_code2, COUNT(*) ct
  FROM
    feods.PJ_OUTSTOCK2_DAY t
  WHERE t.FPRODUCEDATE >= SUBDATE(@week_end, 6)
    AND t.FPRODUCEDATE < ADDDATE(@week_end, 1)
    AND t.FBASEQTY >= 200
    AND t.BUSINESS_AREA IS NOT NULL
  GROUP BY t.BUSINESS_AREA, t.PRODUCT_BAR;
  
  CREATE TEMPORARY TABLE feods.sal_30_tmp AS
  SELECT
    b.business_name, oi.product_id, SUM(oi.quantity) quantity, SUM(oi.quantity * oi.sale_price) gmv
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month oi
    JOIN feods.shelf_area_tmp b
      ON b.shelf_id = oi.shelf_id
      WHERE oi.pay_date >=@start30
      AND oi.pay_date <@add_date
  GROUP BY b.business_name, oi.product_id;
--   
--   CREATE TEMPORARY TABLE feods.sal_30_tmp AS
--   SELECT
--     b.business_name, oi.product_id, SUM(oi.quantity) quantity, SUM(oi.quantity * oi.sale_price) gmv
--   FROM
--     feods.fjr_order_and_item_last30 oi
--     JOIN feods.shelf_area_tmp b
--       ON b.shelf_id = oi.shelf_id
--   GROUP BY b.business_name, oi.product_id;  
  
  
--   CREATE TEMPORARY TABLE feods.sal_7_tmp AS
--   SELECT
--     b.business_name, oi.product_id, COUNT(DISTINCT oi.shelf_id) shelfs_sal, COUNT(DISTINCT oi.user_id) users, COUNT(DISTINCT oi.order_id) orders, SUM(oi.quantity) quantity
--   FROM
--     feods.fjr_order_and_item_last_week oi
--     JOIN feods.shelf_area_tmp b
--       ON b.shelf_id = oi.shelf_id
--   GROUP BY b.business_name, oi.product_id;
--   CREATE TEMPORARY TABLE feods.sal_7all_tmp AS
--   SELECT
--     b.business_name, COUNT(DISTINCT oi.user_id) users
--   FROM
--     feods.fjr_order_and_item_last_week oi
--     JOIN feods.shelf_area_tmp b
--       ON b.shelf_id = oi.shelf_id
--   GROUP BY b.business_name;
--   CREATE TEMPORARY TABLE feods.sal_7re_tmp AS
--   SELECT
--     t.business_name, t.product_id, COUNT(1) users
--   FROM
--     (SELECT
--       b.business_name, oi.product_id, oi.user_id, COUNT(1) purchases
--     FROM
--       feods.fjr_order_and_item_last_week oi
--       JOIN feods.shelf_area_tmp b
--         ON b.shelf_id = oi.shelf_id
--     GROUP BY b.business_name, oi.product_id, oi.user_id
--     HAVING purchases > 1) t
--   GROUP BY t.business_name, t.product_id;
-- 替换一下
CREATE TEMPORARY TABLE feods.sal_7_tmp AS
  SELECT
    b.business_name, oi.product_id, COUNT(DISTINCT oi.shelf_id) shelfs_sal, 
	COUNT(DISTINCT oi.user_id) users, COUNT(DISTINCT oi.order_id) orders, SUM(oi.quantity) quantity
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month oi
    JOIN feods.shelf_area_tmp b
      ON b.shelf_id = oi.shelf_id
      AND oi.pay_date>= @week_start
    AND oi.pay_date < @add_date
  GROUP BY b.business_name, oi.product_id;
    
  
  
  CREATE TEMPORARY TABLE feods.sal_7all_tmp AS
  SELECT
    b.business_name, COUNT(DISTINCT oi.user_id) users
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month oi
    JOIN feods.shelf_area_tmp b
      ON b.shelf_id = oi.shelf_id
      AND oi.pay_date>= @week_start
    AND oi.pay_date < @add_date
  GROUP BY b.business_name;
  
  
  CREATE TEMPORARY TABLE feods.sal_7re_tmp AS
  SELECT
    t.business_name, t.product_id, COUNT(1) users
  FROM
    (SELECT
      b.business_name, oi.product_id, oi.user_id, COUNT(1) purchases
    FROM
      fe_dwd.dwd_pub_order_item_recent_one_month oi
      JOIN feods.shelf_area_tmp b
        ON b.shelf_id = oi.shelf_id
        AND oi.pay_date>= @week_start
    AND oi.pay_date < @add_date
    GROUP BY b.business_name, oi.product_id, oi.user_id
    HAVING purchases > 1) t
  GROUP BY t.business_name, t.product_id;
     
--       ---------------
  CREATE TEMPORARY TABLE feods.sal_14_tmp AS
  SELECT
    t.business_name, t.product_id, SUM(
      WEEKOFYEAR(t.min_date) < WEEKOFYEAR(t.max_date)
    ) users_both, SUM(t.min_date < SUBDATE(@week_end, 6)) users_lw
  FROM
    (SELECT
      b.business_name, oi.product_id, oi.user_id, MIN(oi.order_date) min_date, MAX(oi.order_date) max_date
    FROM
      fe_dwd.dwd_pub_order_item_recent_one_month oi
      JOIN feods.shelf_area_tmp b
        ON b.shelf_id = oi.shelf_id
    WHERE oi.order_date >= SUBDATE(@week_end, 13)
      AND oi.order_date < ADDDATE(@week_end, 1)
    GROUP BY b.business_name, oi.product_id, oi.user_id) t
  GROUP BY t.business_name, t.product_id;
  
-- 替换的代码  
--   CREATE TEMPORARY TABLE feods.sal_14_tmp AS
--   SELECT
--     t.business_name, t.product_id, SUM(
--       WEEKOFYEAR(t.min_date) < WEEKOFYEAR(t.max_date)
--     ) users_both, SUM(t.min_date < SUBDATE(@week_end, 6)) users_lw
--   FROM
--     (SELECT
--       b.business_name, oi.product_id, oi.user_id, MIN(oi.order_date) min_date, MAX(oi.order_date) max_date
--     FROM
--       feods.fjr_order_and_item_last30 oi
--       JOIN feods.shelf_area_tmp b
--         ON b.shelf_id = oi.shelf_id
--     WHERE oi.order_date >= SUBDATE(@week_end, 13)
--       AND oi.order_date < ADDDATE(@week_end, 1)
--     GROUP BY b.business_name, oi.product_id, oi.user_id) t
--   GROUP BY t.business_name, t.product_id;  
--   
  CREATE TEMPORARY TABLE feods.sal_tmp AS
  SELECT
    sal_30.business_name, sal_30.product_id, sal_30.quantity quantity_30, sal_30.gmv, sal_7.shelfs_sal, sal_7.users users_7, sal_7.orders, sal_7.quantity quantity_7, sal_7_all.users users_7all, sal_7_re.users users_7re, sal_14.users_both, sal_14.users_lw
  FROM
    feods.sal_30_tmp sal_30
    LEFT JOIN feods.sal_7_tmp sal_7
      ON sal_30.business_name = sal_7.business_name
      AND sal_30.product_id = sal_7.product_id
    LEFT JOIN feods.sal_7all_tmp sal_7_all
      ON sal_30.business_name = sal_7_all.business_name
    LEFT JOIN feods.sal_7re_tmp sal_7_re
      ON sal_30.business_name = sal_7_re.business_name
      AND sal_30.product_id = sal_7_re.product_id
    LEFT JOIN feods.sal_14_tmp sal_14
      ON sal_30.business_name = sal_14.business_name
      AND sal_30.product_id = sal_14.product_id;
  CREATE INDEX idx_sal_tmp_business_name_product_id
  ON feods.sal_tmp (business_name, product_id);
  DELETE
  FROM
    feods.fjr_product_list_manager_week
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_product_list_manager_week (
    week_end, region_name, business_area, product_id, product_fe, product_name, second_type_id, second_type_name, sub_type_id, sub_type_name, product_type, shelfs_active, shelfs_stock, shelfs_stock_lowsal, shelfs_normal, fill_rate, stock_quantity, stock_quantity_dc, sto_days_dc, onway_quantity_dc, stock_quantity_pw, duration_shelf, duration_dc, quantity_sal30, gmv30, day_avg_sale_num14, product_sale_level, sal_per_sto, shelf_rate_flag5, stock_rate_flag5, user_percent, avg_pur_times, residence_rate, mult_user, first_fill_time, add_user
  )
  SELECT
    @week_end, base.region_name, base.business_name, p.product_id, p.product_code2, p.product_name, p.second_type_id, pt1.type_name, p.sub_type_id, pt2.type_name, pd.product_type, shelf.shelfs_2, base.shelfs_stock, base.shelfs_stock_lowsal, base.shelfs_normal, base.if_fill / shelf.shelfs, base.stock_quantity, dc.fbaseqty, dcd.ct, dc.qty_on_way, pw.available_stock, base.stock_quantity / sal.quantity_7, dc.fbaseqty / dc.fqty, sal.quantity_30, sal.gmv, z.avg_qty, z.sale_level, z.yxshjzb, base.shelfs_5 / base.shelfs, base.stock_quantity_5 / base.stock_quantity, sal.users_7 / sal.users_7all, sal.orders / sal.users_7, sal.users_both / sal.users_lw, sal.users_7re / sal.users_7, base.first_fill_time, @add_user
  FROM
    feods.base_tmp base
    LEFT JOIN
      (SELECT
        b.business_name, SUM(s.shelf_status = 2) shelfs_2, COUNT(1) shelfs
      FROM
        fe.sf_shelf s, fe_dwd.`dwd_city_business` b
      WHERE 1
        AND b.city = s.city
        AND s.data_flag = 1
      GROUP BY b.business_name) shelf
      ON base.business_name = shelf.business_name
    LEFT JOIN feods.sal_tmp sal
      ON base.business_name = sal.business_name
      AND base.product_id = sal.product_id
    LEFT JOIN
      (SELECT
        b.business_name, w.product_id, SUM(w.available_stock) available_stock
      FROM
        fe.sf_prewarehouse_stock_detail w, fe.sf_shelf s, fe_dwd.`dwd_city_business` b
      WHERE 1
        AND w.warehouse_id = s.shelf_id
        AND s.city = b.city
        AND w.data_flag = 1
        AND s.data_flag = 1
      GROUP BY b.business_name, w.product_id) pw
      ON base.business_name = pw.business_name
      AND base.product_id = pw.product_id
    LEFT JOIN fe.sf_product p
      ON base.product_id = p.product_id
    LEFT JOIN feods.dc_tmp dc
      ON base.business_name = dc.business_name
      AND p.product_code2 = dc.product_code2
    LEFT JOIN feods.dc_days_tmp dcd
      ON base.business_name = dcd.business_name
      AND p.product_code2 = dcd.product_code2
    LEFT JOIN feods.zs_area_product_sale_flag z
      ON base.business_name = z.business_area
      AND base.product_id = z.product_id
      AND z.sdate = ADDDATE(@week_end, 1)
    LEFT JOIN fe.sf_product_type pt1
      ON p.second_type_id = pt1.type_id
    LEFT JOIN fe.sf_product_type pt2
      ON p.sub_type_id = pt2.type_id
    LEFT JOIN feods.zs_product_dim_sserp pd
      ON base.business_name = pd.business_area
      AND p.product_id = pd.product_id;
	  
UPDATE feods.fjr_product_list_manager_week a
LEFT JOIN
(
SELECT week_end,
       business_area,
       product_id,
       CASE WHEN product_sale_level IS NULL THEN '-'
            WHEN sal_per_sto >= 0.5 AND (gmv30 / quantity_sal30 * day_avg_sale_num14) >= 0.85 THEN '好卖'
            WHEN sal_per_sto >= 0.5 AND (gmv30 / quantity_sal30 * day_avg_sale_num14) < 0.85 THEN '一般'
            WHEN sal_per_sto >= 0.25 AND (gmv30 / quantity_sal30 * day_avg_sale_num14) >= 0.5 THEN '局部好卖'
            WHEN sal_per_sto >= 0.25 AND (gmv30 / quantity_sal30 * day_avg_sale_num14) < 0.5 THEN '非常不好卖'
            WHEN sal_per_sto < 0.25 THEN '难卖' END AS gmv_sale_level
FROM feods.fjr_product_list_manager_week
)b ON a.week_end = b.week_end AND a.business_area = b.business_area AND a.product_id = b.product_id
SET a.gmv_sale_level = b.gmv_sale_level;
  CALL feods.sp_task_log (
    'sp_product_list_manager_week', @week_end, CONCAT(
      '朱星华', @timestamp, @add_user
    )
  );
  COMMIT;
END