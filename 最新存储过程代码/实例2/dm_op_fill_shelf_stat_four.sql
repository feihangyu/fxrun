CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_fill_shelf_stat_four`()
BEGIN
  SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_date := ADDDATE(@sdate, 1);
  SET @pre_2day := SUBDATE(@sdate,1);
  SET @pre_2month_01 := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 1 MONTH),'%Y-%m-01');
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.business_name, s.shelf_id, s.shelf_type
  FROM
    fe_dwd.`dwd_shelf_base_day_all` s
  WHERE ! ISNULL(s.shelf_id);
  
  DELETE
  FROM
    fe_dm.dm_shelftype_order_stat
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_shelftype_order_stat (
    sdate, shelf_type, orders, quantity, gmv, product_total_amount, discount_amount, coupon_amount, add_user
  )
  SELECT
    @sdate sdate, s.shelf_type, COUNT(*) orders, SUM(t.quantity_act) quantity, SUM(gmv) AS gmv,SUM(t.product_total_amount) product_total_amount, SUM(t.discount_amount) discount_amount, SUM(t.coupon_amount) coupon_amount, @add_user add_user
  FROM
    (SELECT
      t.order_id, t.shelf_id, SUM(pay_amount_product) AS product_total_amount, t.o_discount_amount AS  discount_amount, t.coupon_amount, SUM(t.quantity_act) quantity_act,SUM(t.quantity_act * t.sale_price) AS gmv
    FROM
      fe_dwd.`dwd_order_item_refund_day` t
    WHERE  t.PAY_DATE >= @sdate
      AND t.PAY_DATE < @add_date
    GROUP BY t.order_id) t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  GROUP BY s.shelf_type;
DELETE FROM fe_dm.dm_pub_shelf_dgmv WHERE sdate = @sdate OR (sdate < @pre_2month_01 AND ADDDATE(sdate,1) != DATE_FORMAT(ADDDATE(sdate,1),'%Y-%m-01'));
INSERT INTO fe_dm.dm_pub_shelf_dgmv
(
    sdate, 
    shelf_id, 
    qty_sal_cum, 
    gmv_cum,
    discount_cum, 
    add_user
)
SELECT
    @sdate sdate, 
    t.shelf_id, 
    SUM(t.qty_sal_cum) qty_sal_cum, 
    SUM(t.gmv_cum) AS gmv_cum,
    SUM(t.discount_cum) discount_cum, 
    @add_user add_user
FROM
        (
                SELECT
                    t.shelf_id, 
                    SUM(t.quantity_act) qty_sal_cum, 
                    SUM(gmv) AS gmv_cum,
                    SUM(t.discount_amount) discount_cum
                FROM
                    (
                        SELECT
                                t.order_id, t.shelf_id, SUM(pay_amount_product) AS product_total_amount, t.o_discount_amount AS discount_amount,  t.coupon_amount, SUM(t.quantity_act) quantity_act,SUM(t.quantity_act*t.`SALE_PRICE`) AS gmv
                        FROM
                                fe_dwd.`dwd_order_item_refund_day` t  
                        WHERE  t.PAY_DATE < @add_date
                                AND t.PAY_DATE >= @sdate
                        GROUP BY t.order_id
                    ) t
                GROUP BY t.shelf_id
                UNION ALL
                SELECT
                        shelf_id, 
                        qty_sal_cum, 
                        gmv_cum,
                        discount_cum
                FROM
                        fe_dm.dm_pub_shelf_dgmv
                WHERE sdate = @pre_2day
        ) t
GROUP BY t.shelf_id
;   
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_order_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_order_tmp (KEY idx_shelf_id(shelf_id)) AS
SELECT
    @sdate sdate, 
    t.shelf_id, 
    SUM(t.quantity_act) qty_sal, 
    SUM(gmv) AS gmv,
    SUM(t.discount_amount) discount
FROM
    (
        SELECT
                t.order_id, t.shelf_id, SUM(pay_amount_product) AS product_total_amount, t.o_discount_amount AS discount_amount,  t.coupon_amount, SUM(t.quantity_act) quantity_act,SUM(t.quantity_act*t.`SALE_PRICE`) AS gmv
        FROM
                fe_dwd.`dwd_order_item_refund_day` t  
        WHERE  t.PAY_DATE < @add_date
                AND  t.PAY_DATE >= @sdate
        GROUP BY t.order_id
    ) t
GROUP BY t.shelf_id
;   
  
UPDATE
        fe_dm.dm_pub_shelf_dgmv a
        JOIN fe_dwd.shelf_order_tmp b
                ON a.`shelf_id` = b.shelf_id
                AND a.`sdate` = b.sdate
SET a.qty_sal = b.qty_sal,
        a.gmv = b.gmv,
        a.discount = b.discount
;
  
 DELETE FROM fe_dm.dm_op_area_product_dgmv_cum WHERE sdate = @sdate OR (sdate < @pre_2month_01 AND ADDDATE(sdate,1) != DATE_FORMAT(ADDDATE(sdate,1),'%Y-%m-01'));
 INSERT INTO fe_dm.dm_op_area_product_dgmv_cum
(
    sdate, 
    business_name, 
    product_id,
    qty_sal_cum, 
    gmv_cum,
    discount_cum, 
    add_user
)
SELECT
    @sdate sdate, 
    t.business_name,
    t.product_id, 
    SUM(t.qty_sal_cum) qty_sal_cum, 
    SUM(t.gmv_cum) AS gmv_cum,
    SUM(t.discount_cum) discount_cum, 
    @add_user add_user
FROM
(
        SELECT
            s.business_name,
            t.product_id, 
            SUM(t.quantity_act) qty_sal_cum, 
            SUM(t.quantity_act*t.`SALE_PRICE`) AS gmv_cum,
            SUM(t.discount_amount) discount_cum
        FROM
                fe_dwd.`dwd_order_item_refund_day` t  
                JOIN fe_dwd.shelf_tmp s
                        ON t.`shelf_id` = s.shelf_id
        WHERE t.PAY_DATE < @add_date
                      AND t.PAY_DATE >= @sdate
        GROUP BY s.business_name,t.product_id
        UNION ALL
        SELECT
                business_name, 
                product_id,
                qty_sal_cum, 
                gmv_cum,
                discount_cum
        FROM
                fe_dm.dm_op_area_product_dgmv_cum
        WHERE sdate = @pre_2day
) t
GROUP BY t.business_name,t.product_id
; 
 
 DROP TEMPORARY TABLE IF EXISTS fe_dwd.area_product_order_tmp;
CREATE TEMPORARY TABLE fe_dwd.area_product_order_tmp (KEY (business_name,product_id)) AS
SELECT
    @sdate sdate, 
    business_name,
    product_id, 
    SUM(t.quantity_act) qty_sal, 
    SUM(t.quantity_act*t.`SALE_PRICE`) AS gmv,
    SUM(t.discount_amount) discount
FROM
        fe_dwd.`dwd_order_item_refund_day` t  
        JOIN fe_dwd.shelf_tmp s
                ON t.`shelf_id` = s.shelf_id
WHERE  t.PAY_DATE < @add_date
        AND  t.PAY_DATE >= @sdate
GROUP BY s.business_name,t.product_id
;
 
 UPDATE
        fe_dm.dm_op_area_product_dgmv_cum a
        JOIN fe_dwd.area_product_order_tmp b
                ON a.sdate = b.sdate
                AND a.business_name = b.business_name
                AND a.product_id = b.product_id
SET a.qty_sal = b.qty_sal,
        a.gmv = b.gmv,
        a.discount = b.discount
;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fill_tmp (
    KEY idx_shelf_id (shelf_id), KEY idx_business_name_product_id (business_name, product_id)
  ) AS
SELECT
s.business_name, f.order_id, f.fill_type, f.shelf_id, s.shelf_type, f.product_id, f.SALE_PRICE, f.ACTUAL_FILL_NUM
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` f
        JOIN fe_dwd.shelf_tmp s
                ON f.shelf_id = s.shelf_id
WHERE f.order_status = 4
        AND f.fill_time < @add_date
        AND f.fill_time >= @sdate
        AND f.fill_type IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12)
        AND s.shelf_type != 9;    
    
    
  DELETE
  FROM
    fe_dm.dm_shelftype_fill_stat
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_shelftype_fill_stat (
    sdate, shelf_type, fill_type, orders, actual_apply_num, actual_apply_val, add_user
  )
  SELECT
    @sdate sdate, t.shelf_type, t.fill_type, COUNT(DISTINCT t.order_id) orders, SUM(t.ACTUAL_FILL_NUM) actual_apply_num, SUM(
      t.SALE_PRICE * t.ACTUAL_FILL_NUM
    ) actual_apply_val, @add_user add_user
  FROM
    fe_dwd.fill_tmp t
  WHERE t.fill_type IN (1, 2, 8, 9)
  GROUP BY t.fill_type, t.shelf_type
  ;  
  
  DELETE FROM fe_dm.dm_op_fill_area_product_stat WHERE sdate = @sdate OR (sdate < @pre_2month_01 AND ADDDATE(sdate,1) != DATE_FORMAT(ADDDATE(sdate,1),'%Y-%m-01'));
  INSERT INTO fe_dm.dm_op_fill_area_product_stat (
    sdate, business_name, product_id, orders_cum, actual_apply_num_cum,actual_apply_val_cum, add_user
  )
    SELECT
    @sdate sdate, 
      t.business_name, t.product_id, SUM(orders_cum) orders_cum, SUM(t.actual_apply_num_cum) actual_apply_num_cum, SUM(t.actual_apply_val_cum) actual_apply_val_cum,
      @add_user add_user
    FROM
(
        SELECT
                business_name,
                product_id,
                orders_cum,
                actual_apply_num_cum,
                actual_apply_val_cum
        FROM
                fe_dm.dm_op_fill_area_product_stat
        WHERE sdate = @pre_2day
        UNION ALL
        SELECT
                business_name, 
                product_id,
                COUNT(DISTINCT order_id) orders_cum,
                SUM(ACTUAL_FILL_NUM) actual_apply_num_cum,
                SUM(SALE_PRICE * ACTUAL_FILL_NUM) actual_apply_val_cum
        FROM
                fe_dwd.fill_tmp
        GROUP BY business_name, product_id
) t
GROUP BY t.business_name,t.product_id
;
   
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sdate_area_product_fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.sdate_area_product_fill_tmp (
 KEY idx_business_name_product_id (business_name, product_id)
) AS
SELECT
        @sdate sdate,
        business_name,
        product_id,
        COUNT(DISTINCT t.order_id) AS orders,
        SUM(t.ACTUAL_FILL_NUM) AS actual_apply_num,
        SUM(t.SALE_PRICE * t.ACTUAL_FILL_NUM) AS actual_apply_val
FROM
        fe_dwd.fill_tmp t
GROUP BY business_name,product_id
;
   
UPDATE
        fe_dm.dm_op_fill_area_product_stat a
        JOIN fe_dwd.sdate_area_product_fill_tmp b
                ON a.sdate = b.sdate
                AND a.business_name = b.business_name 
                AND a.product_id = b.product_id
SET a.orders = b.orders,
        a.actual_apply_num = b.actual_apply_num,
        a.actual_apply_val = b.actual_apply_val
;
DELETE FROM fe_dm.dm_op_fill_shelf_stat WHERE sdate >= @sdate OR (sdate < @pre_2month_01 AND ADDDATE(sdate,1) != DATE_FORMAT(ADDDATE(sdate,1),'%Y-%m-01'));  
  INSERT INTO fe_dm.dm_op_fill_shelf_stat (
    sdate, shelf_id,  orders_cum,  actual_apply_num_cum,  actual_apply_val_cum, add_user
  )  
    SELECT
    @sdate sdate, 
      t.shelf_id, SUM(orders_cum) orders_cum, SUM(t.actual_apply_num_cum) actual_apply_num_cum, SUM(t.actual_apply_val_cum) actual_apply_val_cum,
      @add_user add_user
    FROM
(
        SELECT
                shelf_id,
                orders_cum,
                actual_apply_num_cum,
                actual_apply_val_cum
        FROM
                fe_dm.dm_op_fill_shelf_stat
        WHERE sdate = @pre_2day
        UNION ALL
        SELECT
                shelf_id,
                COUNT(DISTINCT order_id) orders_cum,
                SUM(ACTUAL_FILL_NUM) actual_apply_num_cum,
                SUM(SALE_PRICE * ACTUAL_FILL_NUM) actual_apply_val_cum
        FROM
                fe_dwd.fill_tmp
        GROUP BY shelf_id
) t
GROUP BY t.shelf_id
;
      
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sdate_shelf_fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.sdate_shelf_fill_tmp (
 KEY idx_shelf_id (shelf_id)
) AS
SELECT
        @sdate sdate,
        shelf_id,
        COUNT(DISTINCT t.order_id) AS orders,
        SUM(t.ACTUAL_FILL_NUM) AS actual_apply_num,
        SUM(t.SALE_PRICE * t.ACTUAL_FILL_NUM) AS actual_apply_val
FROM
        fe_dwd.fill_tmp t
GROUP BY shelf_id
;
UPDATE
        fe_dm.dm_op_fill_shelf_stat a
        JOIN fe_dwd.sdate_shelf_fill_tmp b
                ON a.sdate = b.sdate
                AND a.shelf_id = b.shelf_id 
SET a.orders = b.orders,
        a.actual_apply_num = b.actual_apply_num,
        a.actual_apply_val = b.actual_apply_val
;
      
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_fill_shelf_stat_four',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('宋英南@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_area_product_stat','dm_op_fill_shelf_stat_four','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_shelf_stat','dm_op_fill_shelf_stat_four','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelftype_fill_stat','dm_op_fill_shelf_stat_four','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelftype_order_stat','dm_op_fill_shelf_stat_four','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_pub_shelf_dgmv','dm_op_fill_shelf_stat_four','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_dgmv_cum','dm_op_fill_shelf_stat_four','宋英南');
END