CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_area_sale_dashboard`()
    SQL SECURITY INVOKER
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  SELECT
    @sdate := CURRENT_DATE,
    @sdate7 := SUBDATE(@sdate, 0),
    @sdate6 := SUBDATE(@sdate, 1),
    @sdate5 := SUBDATE(@sdate, 2),
    @sdate4 := SUBDATE(@sdate, 3),
    @sdate3 := SUBDATE(@sdate, 4),
    @sdate2 := SUBDATE(@sdate, 5),
    @sdate1 := SUBDATE(@sdate, 6),
    @d7 := DAY(@sdate7),
    @d6 := DAY(@sdate6),
    @d5 := DAY(@sdate5),
    @d4 := DAY(@sdate4),
    @d3 := DAY(@sdate3),
    @d2 := DAY(@sdate2),
    @d1 := DAY(@sdate1),
    @y_m7 := DATE_FORMAT(@sdate7, '%Y-%m'),
    @y_m6 := DATE_FORMAT(@sdate6, '%Y-%m'),
    @y_m5 := DATE_FORMAT(@sdate5, '%Y-%m'),
    @y_m4 := DATE_FORMAT(@sdate4, '%Y-%m'),
    @y_m3 := DATE_FORMAT(@sdate3, '%Y-%m'),
    @y_m2 := DATE_FORMAT(@sdate2, '%Y-%m'),
    @y_m1 := DATE_FORMAT(@sdate1, '%Y-%m'),
    @timestamp := CURRENT_TIMESTAMP;
	
	
	SET @time_1 := CURRENT_TIMESTAMP();
  TRUNCATE TABLE feods.pj_area_sale_dashboard_history;
  INSERT INTO feods.pj_area_sale_dashboard_history (
    sdate,
    shelf_id,
    ACTIVATE_TIME,
    REVOKE_TIME,
    SHELF_CODE,
    SHELF_STATUS,
    SHELF_TYPE,
    WHETHER_CLOSE,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SHELF_NAME
  )
  SELECT
    DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS sdate,
    t0.shelf_id,
    t0.ACTIVATE_TIME,
    t0.REVOKE_TIME,
    t0.SHELF_CODE,
    t0.SHELF_STATUS,
    t0.SHELF_TYPE,
    t0.WHETHER_CLOSE,
    SUBSTRING_INDEX(
      SUBSTRING_INDEX(t0.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) AS city_name,
    t1.BRANCH_CODE,
    t1.BRANCH_NAME,
    t0.SHELF_NAME
  FROM
    fe.sf_shelf t0
    LEFT JOIN fe.pub_shelf_manager t1
      ON t0.MANAGER_ID = t1.MANAGER_ID;
  COMMIT;
  
  SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_1--@time_2",@time_1,@time_2);
  
UPDATE feods.pj_area_sale_dashboard_history AS b
LEFT JOIN
    (SELECT
        SHELF_ID
         ,COUNT(1) fill_order_qty
         ,COUNT( DISTINCT IF(DATEDIFF(FILL_TIME,APPLY_TIME) <= 1,order_id,NULL) )   cr_fill_order_qty
         ,COUNT( DISTINCT IF(ERROR_NUM>0,order_id,NULL)  )   yc_fill_order_qty
    FROM
        (SELECT
            a.shelf_id,a.ORDER_ID,a.FILL_TIME,a.APPLY_TIME,
            #SUM(PURCHASE_PRICE * ACTUAL_SIGN_NUM) AS real_value,
            SUM(ABS(ERROR_NUM)) AS ERROR_NUM
        FROM fe.sf_product_fill_order a
        JOIN fe.sf_product_fill_order_item b ON a.ORDER_ID=b.ORDER_ID AND b.DATA_FLAG=1
        WHERE ORDER_STATUS IN (1, 2, 3, 4)
            AND a.DATA_FLAG = 1
            AND a.FILL_TYPE IN (1, 2, 8, 9, 10)
            AND a.APPLY_TIME >= DATE_SUB(CURDATE(), INTERVAL 2 DAY) AND a.APPLY_TIME < DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        GROUP BY a.ORDER_ID
        ) t1
    GROUP BY SHELF_ID
    ) a ON a.SHELF_ID=b.shelf_id
SET  b.fill_order_qty = a.fill_order_qty,b.cr_fill_order_qty = a.cr_fill_order_qty,b.yc_fill_order_qty = a.yc_fill_order_qty;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_2--@time_3",@time_2,@time_3);
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
    LEFT JOIN
      (SELECT
        shelf_id,
        huosun,
        bk_money,
        sale_value,
        GMV AS GMV_this_month,
        total_error_value
      FROM
        feods.pj_zs_goods_damaged
      WHERE smonth = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m'
        )) AS a
      ON a.shelf_id = b.shelf_id SET b.huosun = a.huosun,
    b.bk_money = a.bk_money,
    b.sale_value = a.sale_value,
    b.GMV_this_month = a.GMV_this_month,
    b.total_error_value = a.total_error_value;
	
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_3--@time_4",@time_3,@time_4);
	
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
    LEFT JOIN
      (SELECT
        a.shelf_id,
        SUM(a.stock_quantity * a.sale_price) AS stock_value_new,
        SUM(a.stock_quantity) AS stock_qty_new,
        SUM(
          CASE
            WHEN b.sales_flag = 5
            AND b.new_flag = 2
            THEN a.stock_quantity * a.sale_price
          END
        ) AS stock_value_new52
      FROM
        fe.sf_shelf_product_detail a
        LEFT JOIN fe.sf_shelf_product_detail_flag b
          ON a.shelf_id = b.shelf_id
          AND a.product_id = b.product_id
      GROUP BY a.shelf_id) AS a
      ON a.shelf_id = b.shelf_id SET b.stock_value_new = a.stock_value_new,
    b.stock_qty_new = a.stock_qty_new,
    b.stock_value_new52 = a.stock_value_new52;
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_4--@time_5",@time_4,@time_5);
	
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
    LEFT JOIN
      (SELECT
        b.shelf_id,
        SUM(QUANTITY * SALE_PRICE) AS GMV,
        SUM(a.REAL_TOTAL_PRICE) AS goods_amount
      FROM fe.sf_order b        
      JOIN fe.sf_order_item a
          ON a.order_id = b.order_id
      WHERE  ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
        AND ORDER_STATUS = 2       
      GROUP BY b.shelf_id) AS a
      ON a.shelf_id = b.shelf_id 
  SET b.GMV = a.GMV,  b.goods_amount = a.goods_amount;
  
  SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_5--@time_6",@time_5,@time_6);
  UPDATE feods.pj_area_sale_dashboard_history AS b
  LEFT JOIN feods.shelf_sku_stock_7days_tmp a 
  ON a.shelf_id = b.shelf_id 
  SET b.stock_qty1 = a.stock_qty1,
  b.stock_qty2 = a.stock_qty2,
  b.stock_qty3 = a.stock_qty3,
  b.stock_qty4 = a.stock_qty4,
  b.stock_qty5 = a.stock_qty5,
  b.stock_qty6 = a.stock_qty6,
  b.stock_qty7 = a.stock_qty7,
  b.sku1 = a.sku1, 
  b.sku2 = a.sku2, 
  b.sku3 = a.sku3, 
  b.sku4 = a.sku4, 
  b.sku5 = a.sku5, 
  b.sku6 = a.sku6, 
  b.sku7 = a.sku7; 
  
  SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_6--@time_7",@time_6,@time_7);
  
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
  JOIN
      (SELECT DISTINCT
        shelf_id,
        warehouse_id
      FROM
        fe.sf_prewarehouse_shelf_detail
      WHERE data_flag = 1) AS a
      ON a.shelf_id = b.shelf_id SET b.warehouse_id = a.warehouse_id;
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_7--@time_8",@time_7,@time_8);	  
	  
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
  JOIN
      (SELECT
        SHELF_ID,
        COUNT(DISTINCT CHECK_ID) AS CHECK_qty,
        MAX(OPERATE_TIME) AS max_OPERATE_TIME
      FROM
        fe.sf_shelf_check
      WHERE OPERATE_TIME >= DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m%01'
        )
        AND OPERATE_TIME < ADDDATE(
          DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            '%Y%m%01'
          ),
          INTERVAL 1 MONTH
        )
      GROUP BY SHELF_ID) AS a
      ON a.shelf_id = b.shelf_id SET b.CHECK_qty = a.CHECK_qty,
    b.max_OPERATE_TIME = a.max_OPERATE_TIME;
	
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_8--@time_9",@time_8,@time_9);
	
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
  JOIN
      (SELECT
        a.shelf_id,
        SUM(
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            THEN PRODUCT_TOTAL_AMOUNT
          END
        ) AS amount,
        SUM(
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            THEN COUPON_AMOUNT
          END
        ) AS COUPON_AMOUNT,
        SUM(
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            THEN DISCOUNT_AMOUNT
          END
        ) AS DISCOUNT_AMOUNT,
        SUM(
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            AND b.COUPON_ID IN (2, 448, 449, 450, 451)
            THEN COUPON_AMOUNT
          END
        ) AS COUPON_AMOUNT_new_user,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            THEN a.user_id
          END
        ) AS total_user,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) - 1
            THEN a.user_id
          END
        ) AS total_user_last_week,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            AND DATE_FORMAT(a.order_date, '%H:%i') BETWEEN '06:00'
            AND '10:00'
            THEN a.user_id
          END
        ) AS mom_user,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            AND DATE_FORMAT(a.order_date, '%H:%i') BETWEEN '11:30'
            AND '14:00'
            THEN a.user_id
          END
        ) AS lun_user,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            AND DATE_FORMAT(a.order_date, '%H:%i') BETWEEN '17:00'
            AND '19:00'
            THEN a.user_id
          END
        ) AS sup_user,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            AND c.user_type_activity IN (5, 6)
            THEN a.user_id
          END
        ) AS act_user,
        COUNT(
          DISTINCT
          CASE
            WHEN WEEKOFYEAR(ORDER_DATE) = WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            AND c.user_type_activity = 7
            THEN a.user_id
          END
        ) AS loss_act_user
      FROM
        fe.sf_order a
        LEFT JOIN fe.sf_coupon_record b
          ON a.ORDER_ID = b.order_id
        LEFT JOIN feods.zs_shelf_member_flag c
          ON a.user_id = c.user_id
      WHERE WEEKOFYEAR(ORDER_DATE) >= WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) - 1
        AND a.ORDER_DATE >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        AND ORDER_STATUS = 2
      GROUP BY a.shelf_id) AS a
      ON a.shelf_id = b.shelf_id SET b.amount = a.amount,
    b.COUPON_AMOUNT = a.COUPON_AMOUNT,
    b.total_user_last_week = a.total_user_last_week,
    b.DISCOUNT_AMOUNT = a.DISCOUNT_AMOUNT,
    b.COUPON_AMOUNT_new_user = a.COUPON_AMOUNT_new_user,
    b.total_user = a.total_user,
    b.mom_user = a.mom_user,
    b.lun_user = a.lun_user,
    b.sup_user = a.sup_user,
    b.act_user = a.act_user,
    b.loss_act_user = a.loss_act_user;
	
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_9--@time_10",@time_9,@time_10);
	
  UPDATE
    feods.pj_area_sale_dashboard_history AS b
   JOIN
      (SELECT
        shelf_id,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 1
            THEN user_id
          END
        ) AS act_user_qty1,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 2
            THEN user_id
          END
        ) AS act_user_qty2,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 3
            THEN user_id
          END
        ) AS act_user_qty3,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 4
            THEN user_id
          END
        ) AS act_user_qty4,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 5
            THEN user_id
          END
        ) AS act_user_qty5,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 6
            THEN user_id
          END
        ) AS act_user_qty6,
        COUNT(
          DISTINCT
          CASE
            WHEN user_type_activity = 7
            THEN user_id
          END
        ) AS act_user_qty7
      FROM
        feods.zs_shelf_member_flag
      GROUP BY shelf_id) AS a
      ON a.shelf_id = b.shelf_id SET b.act_user_qty1 = a.act_user_qty1,
    b.act_user_qty2 = a.act_user_qty2,
    b.act_user_qty3 = a.act_user_qty3,
    b.act_user_qty4 = a.act_user_qty4,
    b.act_user_qty5 = a.act_user_qty5,
    b.act_user_qty6 = a.act_user_qty6,
    b.act_user_qty7 = a.act_user_qty7;
	
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_10--@time_11",@time_10,@time_11);	
	
  DELETE
  FROM
    feods.pj_area_sale_dashboard
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 1 DAY)  OR sdate<SUBDATE(CURDATE(),90)  ;
  INSERT INTO feods.pj_area_sale_dashboard (
    sdate,
    shelf_id,
    ACTIVATE_TIME,
    REVOKE_TIME,
    SHELF_CODE,
    SHELF_STATUS,
    SHELF_TYPE,
    WHETHER_CLOSE,
    fill_order_qty,
    cr_fill_order_qty,
    yc_fill_order_qty,
    huosun,
    bk_money,
    sale_value,
    GMV_this_month,
    total_error_value,
    stock_value_new,
    stock_qty_new,
    stock_value_new52,
    GMV,
    goods_amount,
    stock_qty1,
    stock_qty2,
    stock_qty3,
    stock_qty4,
    stock_qty5,
    stock_qty6,
    stock_qty7,
    sku1,
    sku2,
    sku3,
    sku4,
    sku5,
    sku6,
    sku7,
    warehouse_id,
    amount,
    COUPON_AMOUNT,
    DISCOUNT_AMOUNT,
    COUPON_AMOUNT_new_user,
    total_user,
    mom_user,
    lun_user,
    sup_user,
    act_user,
    loss_act_user,
    CHECK_qty,
    max_OPERATE_TIME,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SHELF_NAME,
    total_user_last_week,
    act_user_qty1,
    act_user_qty2,
    act_user_qty3,
    act_user_qty4,
    act_user_qty5,
    act_user_qty6,
    act_user_qty7
  )
  SELECT
    sdate,
    shelf_id,
    ACTIVATE_TIME,
    REVOKE_TIME,
    SHELF_CODE,
    SHELF_STATUS,
    SHELF_TYPE,
    WHETHER_CLOSE,
    fill_order_qty,
    cr_fill_order_qty,
    yc_fill_order_qty,
    huosun,
    bk_money,
    sale_value,
    GMV_this_month,
    total_error_value,
    stock_value_new,
    stock_qty_new,
    stock_value_new52,
    GMV,
    goods_amount,
    stock_qty1,
    stock_qty2,
    stock_qty3,
    stock_qty4,
    stock_qty5,
    stock_qty6,
    stock_qty7,
    sku1,
    sku2,
    sku3,
    sku4,
    sku5,
    sku6,
    sku7,
    warehouse_id,
    amount,
    COUPON_AMOUNT,
    DISCOUNT_AMOUNT,
    COUPON_AMOUNT_new_user,
    total_user,
    mom_user,
    lun_user,
    sup_user,
    act_user,
    loss_act_user,
    CHECK_qty,
    max_OPERATE_TIME,
    city_name,
    BRANCH_CODE,
    BRANCH_NAME,
    SHELF_NAME,
    total_user_last_week,
    act_user_qty1,
    act_user_qty2,
    act_user_qty3,
    act_user_qty4,
    act_user_qty5,
    act_user_qty6,
    act_user_qty7
  FROM
    feods.pj_area_sale_dashboard_history;
	
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_area_sale_dashboard","@time_11--@time_12",@time_11,@time_12);	
	
  CALL feods.sp_task_log (
    'sh_area_sale_dashboard',
    SUBDATE(CURRENT_DATE, 1),
    @timestamp
  );
  
  COMMIT;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_area_sale_dashboard',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
END