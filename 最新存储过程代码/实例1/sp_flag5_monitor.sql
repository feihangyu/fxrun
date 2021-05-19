CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_flag5_monitor`()
BEGIN
  #run after sh_process.sh_zs_goods_damaged
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    feods.fjr_flag5_product
  WHERE sdate = @sdate;
set @time_2 := CURRENT_TIMESTAMP();
  INSERT INTO feods.fjr_flag5_product (
    sdate, business_name, product_id, product_name, product_code2, shelfs5, shelfs5_sto, stock_val_5, shelfs_fill, shelfs_sto, stock_val
  )
 SELECT
    @sdate sdate, b.business_name, p.product_id, p.product_name, p.product_code2, IFNULL(
      SUM(
        d.sales_flag = 5 && (d.new_flag = 2 || ISNULL(d.new_flag))
      ), 0
    ) shelfs5, IFNULL(
      SUM(
        d.sales_flag = 5 && (d.new_flag = 2 || ISNULL(d.new_flag)) && d.STOCK_QUANTITY > 0
      ), 0
    ) shelfs5_sto, IFNULL(
      SUM(
        IF(
          d.STOCK_QUANTITY > 0 && d.sales_flag = 5 && (d.new_flag = 2 || ISNULL(d.new_flag)),
		  d.stock_quantity * d.sale_price, 0
        )
      ), 0
    ) stock_val_5, COUNT(d.first_fill_time) shelfs_fill, SUM(d.STOCK_QUANTITY > 0) shelfs_sto, IFNULL(
      SUM(
        IF(
          d.STOCK_QUANTITY > 0, d.stock_quantity * d.sale_price, 0
        )
      ), 0
    ) stock_val
  FROM
    fe_dwd.dwd_shelf_product_day_all d 
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON d.shelf_id = s.shelf_id
    JOIN fe_dwd.dwd_city_business b
      ON s.city = b.city
    JOIN fe_dwd.dwd_product_base_day_all p
      ON d.product_id = p.product_id
  GROUP BY b.business_name, p.product_id;
  
set @time_4 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sp_flag5_monitor","@time_2--@time_4",@time_2,@time_4);
  DELETE
  FROM
    feods.fjr_flag5_shelf
  WHERE sdate = @sdate;
set @time_7 := CURRENT_TIMESTAMP();
  INSERT INTO feods.fjr_flag5_shelf (
    sdate, business_name, shelf_id, if_pre_warehouse, if_all_time, shelf_status_name, whether_close_name, revoke_status_name, shelf_type_name, shelfs5, stock_val_5, stock_quantity, stock_val, rate_stoval5, avg_gmv90_wd, loss_rate
  )
  SELECT
    @sdate, t.business_name, t.shelf_id, t.if_pre_warehouse, t.if_all_time, di1.ITEM_NAME, di2.ITEM_NAME, di3.ITEM_NAME, di4.ITEM_NAME, IFNULL(t.stock_quantity_5, 0), IFNULL(t.stock_val_5, 0), IFNULL(t.stock_quantity, 0), IFNULL(t.stock_val, 0), IFNULL(t.stock_val_5 / t.stock_val, 0), IFNULL(sal.avg_gmv, 0), IFNULL(los.loss_rate, 0)
  FROM
    (SELECT
      b.business_name, s.shelf_id, (pwh.shelf_id IS NOT NULL) if_pre_warehouse, (sf.sf_code IS NOT NULL) if_all_time, s.shelf_status, s.whether_close, s.revoke_status, s.SHELF_TYPE, SUM(
        CASE
          WHEN f.sales_flag = 5
          AND (f.new_flag = 2
            OR f.new_flag IS NULL)
          THEN d.stock_quantity
        END
      ) stock_quantity_5, SUM(
        CASE
          WHEN f.sales_flag = 5
          AND (f.new_flag = 2
            OR f.new_flag IS NULL)
          THEN d.stock_quantity * d.sale_price
        END
      ) stock_val_5, SUM(d.stock_quantity) stock_quantity, SUM(d.stock_quantity * d.sale_price) stock_val
    FROM
      fe.sf_shelf_product_detail d, fe.sf_shelf_product_detail_flag f, fe.sf_shelf s
      LEFT JOIN
        (SELECT DISTINCT
          t.shelf_id
        FROM
          fe.sf_prewarehouse_shelf_detail t
        WHERE t.data_flag = 1) pwh
        ON pwh.shelf_id = s.shelf_id
      LEFT JOIN fe.pub_shelf_manager m
        ON m.manager_id = s.manager_id
        AND m.data_flag = 1
      LEFT JOIN feods.pj_all_time_sf_code sf
        ON sf.sf_code = m.sf_code, feods.fjr_city_business b
    WHERE d.shelf_id = f.shelf_id
      AND d.product_id = f.product_id
      AND d.shelf_id = s.shelf_id
      AND s.city = b.city
      AND d.data_flag = 1
      AND f.data_flag = 1
      AND s.data_flag = 1
      AND d.stock_quantity > 0
    GROUP BY s.shelf_id) t
    LEFT JOIN
      (SELECT
        t.shelf_id, SUM(
          t.pay_total_amount + t.coupon_total_amount
        ) / wd.ct_days avg_gmv
      FROM
        fe.sf_statistics_shelf_sale t, feods.fjr_work_days w,
        (SELECT
          COUNT(1) ct_days
        FROM
          feods.fjr_work_days w
        WHERE w.sdate >= SUBDATE(@sdate, 90)
          AND w.sdate < @sdate
          AND w.if_work_day = 1) wd
      WHERE t.create_date = w.sdate
        AND t.create_date >= SUBDATE(@sdate, 90)
        AND w.if_work_day = 1
      GROUP BY t.shelf_id) sal
      ON t.shelf_id = sal.shelf_id
    LEFT JOIN
      (SELECT
        d.SHELF_ID, (
          IFNULL(d.huosun, 0) + IFNULL(d.bk_money, 0) - IFNULL(d.total_error_value, 0)
        ) / (
          IFNULL(d.GMV, 0) + ABS(
            IFNULL(d.huosun, 0) + IFNULL(d.bk_money, 0) - IFNULL(d.total_error_value, 0)
          )
        ) loss_rate
      FROM
        feods.pj_zs_goods_damaged d
      WHERE d.smonth = DATE_FORMAT(SUBDATE(@sdate, 1), '%Y%m')) los
      ON t.shelf_id = los.shelf_id
    LEFT JOIN fe.pub_dictionary_item di1
      ON di1.ITEM_VALUE = t.shelf_status
      AND di1.DICTIONARY_ID = 9
    LEFT JOIN fe.pub_dictionary_item di2
      ON di2.ITEM_VALUE = t.whether_close
      AND di2.DICTIONARY_ID = 96
    LEFT JOIN fe.pub_dictionary_item di3
      ON di3.ITEM_VALUE = t.revoke_status
      AND di3.DICTIONARY_ID = 61
    LEFT JOIN fe.pub_dictionary_item di4
      ON di4.ITEM_VALUE = t.SHELF_TYPE
      AND di4.DICTIONARY_ID = 8;
set @time_9 := CURRENT_TIMESTAMP();
call sh_process.sql_log_info("sp_flag5_monitor","@time_7--@time_9",@time_7,@time_9);
  CALL feods.sp_task_log (
    'sp_flag5_monitor', @sdate, CONCAT(
      'yingnansong_d_d255f00f5086b76abf670a6cd0c11856', @timestamp, @add_user
    )
  );
  COMMIT;
END