CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_zs_goods_damaged`()
    SQL SECURITY INVOKER
BEGIN
  SET @run_date := CURRENT_DATE(),
  @user := CURRENT_USER(),
  @timestamp := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE;
  SET @sdate1 := SUBDATE(@sdate, 1);
  SET @d := DAY(@sdate),
  @d1 := DAY(@sdate1),
  @y_m := DATE_FORMAT(@sdate, '%Y-%m'),
  @y_m1 := DATE_FORMAT(@sdate1, '%Y-%m');
  
   DROP TEMPORARY TABLE IF EXISTS feods.tab1_tmp;
  SET @time_1 := CURRENT_TIMESTAMP();
  
-- 跨月判断，如果不跨月，就不用union
if @y_m=@y_m1 then
 SELECT
    @str := CONCAT(
      "CREATE TEMPORARY TABLE feods.tab1_tmp (PRIMARY KEY (shlef_id, product_id)) AS 
      SELECT t1.shelf_id AS shlef_id, t1.PRODUCT_ID AS product_id, t1.DAY1_QUANTITY AS qichukuc, t1.DAY30_QUANTITY AS qimokuc
      , t2.buy_qty AS sale_qty, t3.buy_qty AS buhuo_qty
      , t4.qty AS 'pandian_eorror_qty' -- 该字段可去掉
      , t1.DAY1_QUANTITY + IFNULL(t3.buy_qty, 0) - IFNULL(t2.buy_qty, 0) - 
	  t1.DAY30_QUANTITY AS 'eorror_qty' 
      FROM 
      -- 库存（期初、期末）
     ( SELECT shelf_id, PRODUCT_ID, SUM(DAY1_QUANTITY) AS DAY1_QUANTITY, SUM(DAY",@d,"_QUANTITY) AS DAY30_QUANTITY 
      FROM fe.sf_shelf_product_stock_detail WHERE  STAT_DATE = @y_m 
      GROUP BY shelf_id, PRODUCT_ID) t1 
      -- 销量
      LEFT JOIN -- 已修改
      (SELECT a.shelf_id, a.PRODUCT_ID,SUM(quantity_act) AS buy_qty 
       FROM fe_dwd.`dwd_order_item_refund_day` a
       WHERE  pay_date >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
       AND pay_date < CURRENT_DATE 
       -- AND ORDER_STATUS IN (2,6,7) 
       GROUP BY a.shelf_id, a.PRODUCT_ID) t2 
      ON t1.shelf_id = t2.shelf_id AND t1.PRODUCT_ID = t2.PRODUCT_ID 
      -- 补货
      LEFT JOIN 
      (SELECT a.shelf_id, a.PRODUCT_ID, SUM(a.ACTUAL_FILL_NUM) AS buy_qty -- 已做修改
      FROM fe_dwd.`dwd_fill_day_inc_recent_two_month` a 
 
      where  FILL_TIME >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
      AND FILL_TIME < CURRENT_DATE AND order_status IN (3, 4) 
      GROUP BY a.shelf_id, a.PRODUCT_ID) t3 
      ON t1.shelf_id = t3.shelf_id AND t1.PRODUCT_ID = t3.PRODUCT_ID 
     -- 异常数量（这段没用，可以去掉）
     LEFT JOIN 
     (SELECT a.SHELF_ID, a.product_id, SUM(a.ERROR_NUM) AS qty 
     FROM fe_dwd.dwd_check_base_day_inc a
     where  a.OPERATE_TIME >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
     AND a.OPERATE_TIME < CURRENT_DATE 
     GROUP BY a.SHELF_ID, a.product_id) t4 
     ON t1.shelf_id = t4.shelf_id AND t1.PRODUCT_ID = t4.PRODUCT_ID"
    );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  
  SET @time_2 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info("sh_zs_goods_damaged","@time_1--@time_2",@time_1,@time_2);
  
else 
   SELECT
    @str := CONCAT(
      "CREATE TEMPORARY TABLE feods.tab1_tmp (PRIMARY KEY (shlef_id, product_id)) AS 
      SELECT t1.shelf_id AS shlef_id, t1.PRODUCT_ID AS product_id, t1.DAY1_QUANTITY AS qichukuc, t1.DAY30_QUANTITY AS qimokuc
      , t2.buy_qty AS sale_qty, t3.buy_qty AS buhuo_qty
      , t4.qty AS 'pandian_eorror_qty' -- 该字段可去掉
      , t1.DAY1_QUANTITY + IFNULL(t3.buy_qty, 0) - IFNULL(t2.buy_qty, 0) - 
	  t1.DAY30_QUANTITY AS 'eorror_qty' 
      FROM 
      -- 库存（期初、期末）
     ( SELECT shelf_id, PRODUCT_ID, SUM(DAY1_QUANTITY) AS DAY1_QUANTITY, SUM(DAY30_QUANTITY) AS DAY30_QUANTITY 
     FROM 
	  (SELECT a.shelf_id, a.PRODUCT_ID, a.DAY1_QUANTITY AS DAY1_QUANTITY, 0 AS DAY30_QUANTITY FROM fe.sf_shelf_product_stock_detail AS a WHERE  STAT_DATE = @y_m1 
	  UNION all
	  SELECT a.shelf_id, a.PRODUCT_ID, 0 AS DAY1_QUANTITY, a.day",
      @d,
      "_QUANTITY AS DAY30_QUANTITY FROM fe.sf_shelf_product_stock_detail AS a WHERE  STAT_DATE = @y_m ) tx 
      GROUP BY shelf_id, PRODUCT_ID) t1 
      -- 销量
      LEFT JOIN -- 已修改
      (SELECT a.shelf_id, a.PRODUCT_ID,SUM(quantity_act) AS buy_qty 
       FROM fe_dwd.`dwd_order_item_refund_day` a
       WHERE  pay_date >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
       AND pay_date < CURRENT_DATE 
       -- AND ORDER_STATUS IN (2,6,7) 
       GROUP BY a.shelf_id, a.PRODUCT_ID) t2 
      ON t1.shelf_id = t2.shelf_id AND t1.PRODUCT_ID = t2.PRODUCT_ID 
      -- 补货
      LEFT JOIN 
      (SELECT a.shelf_id, a.PRODUCT_ID, SUM(a.ACTUAL_FILL_NUM) AS buy_qty -- 已做修改
      FROM fe_dwd.`dwd_fill_day_inc_recent_two_month` a 
 
      where  FILL_TIME >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
      AND FILL_TIME < CURRENT_DATE AND order_status IN (3, 4) 
      GROUP BY a.shelf_id, a.PRODUCT_ID) t3 
      ON t1.shelf_id = t3.shelf_id AND t1.PRODUCT_ID = t3.PRODUCT_ID 
     -- 异常数量（这段没用，可以去掉）
     LEFT JOIN 
     (SELECT a.SHELF_ID, a.product_id, SUM(a.ERROR_NUM) AS qty 
     FROM fe_dwd.dwd_check_base_day_inc a
     where  a.OPERATE_TIME >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
     AND a.OPERATE_TIME < CURRENT_DATE 
     GROUP BY a.SHELF_ID, a.product_id) t4 
     ON t1.shelf_id = t4.shelf_id AND t1.PRODUCT_ID = t4.PRODUCT_ID"
    );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  
  SET @time_2 := CURRENT_TIMESTAMP();
  CALL sh_process.sql_log_info("sh_zs_goods_damaged","@time_1--@time_2",@time_1,@time_2);
end if; 
  
  DELETE
  FROM
    feods.pj_zs_goods_damaged
  WHERE smonth = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    );
	
  SET @time_3 := CURRENT_TIMESTAMP();
  
  INSERT INTO feods.pj_zs_goods_damaged (
    smonth,
    city_name,
    SHELF_ID,
    shelf_code,
    sf_code,
    real_name,
    SHELF_STATUS,
    ACTIVATE_TIME,
    REVOKE_TIME,
    stock_value_old,
    in_value,
    sale_value,
    stock_value_now,
    huosun_qty,
    huosun,
    bk_money,
    GMV,
    user_qty,
    OPERATE_TIME,
    damaged_value,
    damaged_value_aduit,
    overdue_value,
    overdue_value_aduit,
    quality_value,
    quality_value_aduit,
    total_error_value
  )
  SELECT
    DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m'
    ) AS smonth,
    city_name,
    w.SHELF_ID,
    w.shelf_code,
    w.sf_code,
    w.real_name,
    w.SHELF_STATUS,
    w.ACTIVATE_TIME,
    w.REVOKE_TIME,
    s.qichukuc,
    s.buhuo_qty,
    s.sale_qty,
    s.qimokuc,
    s.huosun_qty,
    - s.huosun AS huosun,
    xx.bk_money,
    q.GMV,
    q.user_qty,
    n.OPERATE_TIME,
    n.damaged_value,
    n.damaged_value_aduit,
    n.overdue_value,
    n.overdue_value_aduit,
    n.quality_value,
    n.quality_value_aduit,
    n.value_after_aduit
  FROM
    (SELECT
      SHELF_ID,
      SHELF_CODE,
      a.sf_code,
      a.real_name,
      SHELF_STATUS,
      ACTIVATE_TIME,
      REVOKE_TIME,
      a.business_name AS city_name  -- 希望可以将城市改为地区，现在脚本中已做修改
    FROM
      fe_dwd.`dwd_shelf_base_day_all` a
    WHERE a.DATA_FLAG = 1) w
    LEFT JOIN
      (SELECT
        shlef_id,
        SUM(qichukuc * SALE_PRICE) AS qichukuc,
        SUM(qimokuc * SALE_PRICE) AS qimokuc,
        SUM(sale_qty * SALE_PRICE) AS sale_qty,
        SUM(buhuo_qty * SALE_PRICE) AS buhuo_qty,
        SUM(pandian_eorror_qty * SALE_PRICE) AS pandian_eorror_qty,  -- 该字段可去掉
        SUM(eorror_qty) AS huosun_qty,
        SUM(eorror_qty * SALE_PRICE) AS huosun1,
        SUM(IFNULL(qichukuc * SALE_PRICE, 0)) + SUM(IFNULL(buhuo_qty * SALE_PRICE, 0)) -
		SUM(IFNULL(sale_qty * SALE_PRICE, 0)) - SUM(IFNULL(qimokuc * SALE_PRICE, 0)) AS huosun
      FROM
        feods.tab1_tmp t1
        LEFT JOIN fe.sf_shelf_product_detail t2  
          ON t1.product_id = t2.PRODUCT_ID
          AND t1.shlef_id = t2.SHELF_ID
		  AND t2.data_flag =1
      GROUP BY shlef_id) s
      ON w.SHELF_ID = s.shlef_id
    LEFT JOIN
    -- GMV、用户数，是否可以把用户数去掉，进而把这部分取数去掉？
      (SELECT
        a.shelf_id,
        COUNT(DISTINCT a.user_id) AS user_qty,
        SUM(IFNULL(a.QUANTITY,0)*IFNULL(a.SALE_PRICE,0)) AS GMV 
       FROM fe_dwd.`dwd_order_item_refund_day` a
       WHERE  pay_date >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01') 
       AND pay_date < CURRENT_DATE 
       AND ORDER_STATUS IN (2,7) 
      GROUP BY a.SHELF_ID) q
      ON w.SHELF_ID = q.SHELF_ID
    LEFT JOIN
    -- 补款
      (SELECT
        SHELF_id,
        SUM(PAYMENT_MONEY) AS bk_money
      FROM
        fe.sf_after_payment
      WHERE PAYMENT_STATUS = 2 -- and DATE_FORMAT(PAY_DATE,'%Y%m%d') between DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY),'%Y%m01') and DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY),'%Y%m%d')
         AND PAY_DATE >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01')
        AND PAY_DATE < CURRENT_DATE
      GROUP BY SHELF_id) xx
      ON w.SHELF_id = xx.SHELF_id
    LEFT JOIN
    -- 盘点
      (SELECT
        a.shelf_id,
        MAX(a.OPERATE_TIME) AS OPERATE_TIME,
        SUM(
          CASE
            WHEN ERROR_REASON = 1
           -- AND a.AUDIT_STATUS = 2
            THEN ERROR_NUM * SALE_PRICE
          END
        ) AS damaged_value,
        SUM(
          CASE
            WHEN ERROR_REASON = 1
            AND a.AUDIT_STATUS = 2
            THEN AUDIT_ERROR_NUM * SALE_PRICE
          END
        ) AS damaged_value_aduit,
        SUM(
          CASE
            WHEN ERROR_REASON = 2
           -- AND a.AUDIT_STATUS = 2
            THEN ERROR_NUM * SALE_PRICE
          END
        ) AS overdue_value,
        SUM(
          CASE
            WHEN ERROR_REASON = 2
            AND a.AUDIT_STATUS = 2
            THEN AUDIT_ERROR_NUM * SALE_PRICE
          END
        ) AS overdue_value_aduit,
        SUM(
          CASE
            WHEN ERROR_REASON = 4
         --   AND a.AUDIT_STATUS = 2
            THEN ERROR_NUM * SALE_PRICE
          END
        ) AS quality_value,
        SUM(
          CASE
            WHEN ERROR_REASON = 4
            AND a.AUDIT_STATUS = 2
            THEN AUDIT_ERROR_NUM * SALE_PRICE
          END
        ) AS quality_value_aduit,
        SUM(
          CASE
            WHEN ERROR_REASON IN (1, 2, 4)
            AND a.AUDIT_STATUS = 2
            THEN AUDIT_ERROR_NUM * SALE_PRICE
          END
        ) AS value_after_aduit
      FROM
        fe_dwd.dwd_check_base_day_inc a
       WHERE a.OPERATE_TIME >= DATE_FORMAT(SUBDATE(CURRENT_DATE, 1), '%Y%m01')
        AND a.OPERATE_TIME < CURRENT_DATE
        AND a.DATA_FLAG = 1
      GROUP BY a.shelf_id) n
      ON w.SHELF_ID = n.SHELF_ID;
	  
      SET @time_4 := CURRENT_TIMESTAMP();
      CALL sh_process.sql_log_info("sh_zs_goods_damaged","@time_3--@time_4",@time_3,@time_4);
	  
  -- 执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sh_zs_goods_damaged',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李吹防@', @user, @timestamp)
  );
  COMMIT;
END