CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_flag5_shelf_stat_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURDATE();
SET @cur_month_01 := DATE_FORMAT(CURDATE(),'%Y-%m-01');
  DELETE
  FROM
    fe_dm.dm_op_flag5_product_stat
  WHERE sdate = @sdate;
  
  INSERT INTO fe_dm.dm_op_flag5_product_stat (
    sdate, business_name, product_id, product_name, product_code2, shelfs5, shelfs5_sto, stock_val_5, shelfs_fill, shelfs_sto, stock_val
  )
 SELECT
    @sdate sdate, s.business_name, p.product_id, p.product_name, p.product_code2, IFNULL(
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
    JOIN fe_dwd.dwd_product_base_day_all p
      ON d.product_id = p.product_id
  GROUP BY s.business_name, p.product_id;
  
  DELETE
  FROM
    fe_dm.dm_op_flag5_shelf_stat
  WHERE sdate = @sdate OR (sdate != DATE_FORMAT(sdate,'%Y-%m-01') AND sdate < SUBDATE(@sdate,INTERVAL 1 MONTH));
  INSERT INTO fe_dm.dm_op_flag5_shelf_stat (
    sdate, business_name, shelf_id, if_pre_warehouse, if_all_time, shelf_status_name, whether_close_name, revoke_status_name, shelf_type_name, shelfs5, stock_val_5, stock_quantity, stock_val, rate_stoval5, avg_gmv90_wd, loss_rate
  )
  SELECT
    @sdate, t.business_name, t.shelf_id, t.if_pre_warehouse, t.if_all_time, di1.ITEM_NAME, di2.ITEM_NAME, di3.ITEM_NAME, di4.ITEM_NAME, IFNULL(t.stock_quantity_5, 0), IFNULL(t.stock_val_5, 0), IFNULL(t.stock_quantity, 0), IFNULL(t.stock_val, 0), IFNULL(t.stock_val_5 / t.stock_val, 0), IFNULL(sal.avg_gmv, 0), IFNULL(los.loss_rate, 0)
  FROM
    (SELECT
      s.business_name, 
      s.shelf_id, 
      s.is_prewarehouse_cover if_pre_warehouse, 
      (IF(s.manager_type='全职店主',1,0)) if_all_time, 
      s.shelf_status, 
      s.whether_close, 
      s.revoke_status, 
      s.SHELF_TYPE, 
      SUM(
        CASE
          WHEN d.sales_flag = 5
          AND (d.new_flag = 2
            OR d.new_flag IS NULL)
          THEN d.stock_quantity
        END
      ) stock_quantity_5, 
      SUM(
        CASE
          WHEN d.sales_flag = 5
          AND (d.new_flag = 2
            OR d.new_flag IS NULL)
          THEN d.stock_quantity * d.sale_price
        END
      ) stock_val_5, 
      SUM(d.stock_quantity) stock_quantity, 
      SUM(d.stock_quantity * d.sale_price) stock_val
    FROM
      fe_dwd.`dwd_shelf_product_day_all` d, fe_dwd.`dwd_shelf_base_day_all` s
    WHERE d.shelf_id = s.shelf_id
      AND d.stock_quantity > 0
    GROUP BY s.shelf_id) t
    LEFT JOIN
      (SELECT
        t.shelf_id, SUM(t.gmv) / wd.ct_days avg_gmv
      FROM
        fe_dwd.`dwd_shelf_day_his` t, fe_dwd.`dwd_pub_work_day` w,
        (SELECT
          COUNT(1) ct_days
        FROM
          fe_dwd.`dwd_pub_work_day` w
        WHERE w.sdate >= SUBDATE(@sdate, 90)
          AND w.sdate < @sdate
          AND w.if_work_day = 1) wd
      WHERE t.sdate = w.sdate
        AND t.sdate >= SUBDATE(@sdate, 90)
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
        fe_dm.dm_pj_zs_goods_damaged d
      WHERE d.smonth = DATE_FORMAT(SUBDATE(@sdate, 1), '%Y%m')) los
      ON t.shelf_id = los.shelf_id
    LEFT JOIN fe_dwd.dwd_pub_dictionary di1
      ON di1.ITEM_VALUE = t.shelf_status
      AND di1.DICTIONARY_ID = 9
    LEFT JOIN fe_dwd.dwd_pub_dictionary di2
      ON di2.ITEM_VALUE = t.whether_close
      AND di2.DICTIONARY_ID = 96
    LEFT JOIN fe_dwd.dwd_pub_dictionary di3
      ON di3.ITEM_VALUE = t.revoke_status
      AND di3.DICTIONARY_ID = 61
    LEFT JOIN fe_dwd.dwd_pub_dictionary di4
      ON di4.ITEM_VALUE = t.SHELF_TYPE
      AND di4.DICTIONARY_ID = 8;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_flag5_shelf_stat_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_flag5_product_stat','dm_op_flag5_shelf_stat_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_flag5_shelf_stat','dm_op_flag5_shelf_stat_two','宋英南');
END