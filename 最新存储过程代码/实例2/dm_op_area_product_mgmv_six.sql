CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_mgmv_six`()
BEGIN
  SET @run_date := SUBDATE(CURRENT_DATE, 1), @user := CURRENT_USER, @stime := CURRENT_TIMESTAMP;
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_end_last := SUBDATE(@month_start, 1);
  SET @w := WEEKDAY(@sdate);
  SET @week_start := SUBDATE(@sdate, @w);
  SET @week_end := ADDDATE(@week_start, 6);
  SET @pre_sdate := SUBDATE(@sdate,1);
	
  DELETE
  FROM
    fe_dm.dm_op_product_area_shelftype_dgmv
  WHERE sdate >= @sdate;
  INSERT INTO fe_dm.dm_op_product_area_shelftype_dgmv (
    sdate, product_id, business_name, shelf_type, qty_sal, gmv, discount, coupon, add_user
  )
    SELECT
@sdate sdate,t.product_id, s.business_name, s.shelf_type, 
	SUM(t.quantity_act) qty_sal, SUM(IF(t.refund_amount>0,t.quantity_act,t.`QUANTITY`) * t.`SALE_PRICE`) gmv,
	 SUM(
      t.discount_amount * t.quantity_act / t.quantity
    ) discount,
    SUM(
      t.o_coupon_amount * t.quantity_act * t.sale_price / t.ogmv
    ) coupon,  @add_user
  FROM
    `fe_dwd`.`dwd_pub_order_item_recent_one_month`  t
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON t.shelf_id = s.shelf_id
   WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
  GROUP BY t.product_id, s.business_name, s.shelf_type;
  
  DELETE
  FROM
    fe_dm.dm_area_product_dgmv
  WHERE sdate >= @sdate;
  
--   DROP TEMPORARY TABLE IF EXISTS fe_dm.cum_tmp;
--   CREATE TEMPORARY TABLE fe_dm.cum_tmp (
--     PRIMARY KEY (product_id, business_name)
--   ) AS
--   SELECT
--     t.product_id, t.business_name, t.qty_sal_cum, t.gmv_cum, t.discount_cum
--   FROM
--     fe_dm.dm_area_product_dgmv t
--     JOIN
--       (SELECT
--         t.product_id, t.business_name, MAX(t.sdate) sdate
--       FROM
--         fe_dm.dm_area_product_dgmv t
--       GROUP BY t.product_id, t.business_name) l  -- 取出前一天的商品地区信息
--       ON t.sdate = l.sdate
--       AND t.product_id = l.product_id
--       AND t.business_name = l.business_name
--   WHERE ! ISNULL(t.product_id)
--     AND ! ISNULL(t.business_name);
	
	-- 取累计值
--   INSERT INTO fe_dm.dm_area_product_dgmv (
--     sdate, product_id, business_name, qty_sal, qty_sal_cum, gmv, gmv_cum, discount, discount_cum, add_user
--   )
--   SELECT
--     @sdate sdate, t.product_id, t.business_name, t.qty_sal, t.qty_sal + 
-- 	IFNULL(c.qty_sal_cum, 0) qty_sal_cum, t.gmv, t.gmv + IFNULL(c.gmv_cum, 0) gmv_cum, 
-- 	t.discount, t.discount + IFNULL(c.discount_cum, 0) discount_cum, @add_user
--   FROM
--     (SELECT
--       t.product_id, t.business_name, SUM(t.qty_sal) qty_sal, SUM(t.gmv) gmv, SUM(t.discount) discount
--     FROM
--       fe_dm.dm_op_product_area_shelftype_dgmv t
--     WHERE t.sdate = @sdate
--     GROUP BY t.product_id, t.business_name) t
--     LEFT JOIN fe_dm.cum_tmp c
--       ON t.business_name = c.business_name
--       AND t.product_id = c.product_id;
  INSERT INTO fe_dm.dm_area_product_dgmv (
    sdate, product_id, business_name, qty_sal, qty_sal_cum, gmv, gmv_cum, discount, discount_cum, add_user
  )
SELECT
        @sdate AS sdate, 
        t.product_id, 
        t.business_name, 
        t.qty_sal, 
        t.qty_sal + IFNULL(t2.qty_sal_cum, 0) qty_sal_cum, 
	t.gmv, 
	t.gmv + IFNULL(t2.gmv_cum, 0) gmv_cum, 
	t.discount, 
	t.discount + IFNULL(t2.discount_cum, 0) discount_cum, 
	@add_user
FROM
        (
                SELECT
                        t.product_id, 
                        t.business_name, 
                        SUM(t.qty_sal) AS qty_sal, 
                        SUM(t.gmv) AS gmv, 
                        SUM(t.discount) AS discount
                FROM
                        fe_dm.dm_op_product_area_shelftype_dgmv t
                WHERE t.sdate = @sdate
                GROUP BY t.product_id, t.business_name
        ) t
        LEFT JOIN fe_dm.dm_area_product_dgmv t2
                ON t.business_name = t2.business_name
                AND t.product_id = t2.product_id
                AND t2.sdate = @pre_sdate
UNION ALL 
SELECT
        @sdate AS sdate, 
        t.product_id, 
        t.business_name, 
        NULL AS qty_sal, 
        t.qty_sal_cum AS qty_sal_cum, 
	NULL AS gmv, 
	t.gmv_cum AS gmv_cum, 
	NULL AS discount, 
	t.discount_cum AS  discount_cum, 
	@add_user
FROM
        fe_dm.dm_area_product_dgmv t
        LEFT JOIN 
                (
                        SELECT
                                DISTINCT 
                                t.product_id, 
                                t.business_name
                        FROM
                                fe_dm.dm_op_product_area_shelftype_dgmv t
                        WHERE t.sdate = @sdate
                ) t2
        ON t.business_name = t2.business_name
        AND t.product_id = t2.product_id
WHERE t.sdate = @pre_sdate
        AND t2.business_name IS NULL
;
-- 取周表	
  DELETE
  FROM
    fe_dm.dm_op_product_area_shelftype_wgmv
  WHERE sdate = @week_end;
  INSERT INTO fe_dm.dm_op_product_area_shelftype_wgmv (
    sdate, product_id, business_name, shelf_type, qty_sal, gmv, discount, coupon, add_user
  )
  SELECT
    @week_end sdate, product_id, business_name, shelf_type, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, SUM(coupon) coupon, CURRENT_USER add_user
  FROM
    fe_dm.dm_op_product_area_shelftype_dgmv
  WHERE sdate BETWEEN @week_start
    AND @week_end
  GROUP BY product_id, business_name, shelf_type;
  
  DELETE
  FROM
    fe_dm.dm_op_area_product_wgmv
  WHERE sdate = @week_end;
  INSERT INTO fe_dm.dm_op_area_product_wgmv (
    sdate, product_id, business_name, qty_sal, gmv, discount, add_user
  )
  SELECT
    sdate, product_id, business_name, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, CURRENT_USER add_user
  FROM
    fe_dm.dm_op_product_area_shelftype_wgmv
  WHERE sdate = @week_end
  GROUP BY product_id, business_name;  
    
 
  
  -- 取月表
  DELETE
  FROM
    fe_dm.dm_op_product_area_shelftype_mgmv
  WHERE month_id = @y_m;
  INSERT INTO fe_dm.dm_op_product_area_shelftype_mgmv (
    month_id, product_id, business_name, shelf_type, qty_sal, gmv, discount, coupon, add_user
  )
  SELECT
    @y_m month_id, product_id, business_name, shelf_type, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, SUM(coupon) coupon, @add_user add_user
  FROM
    fe_dm.dm_op_product_area_shelftype_dgmv
  WHERE sdate BETWEEN @month_start
    AND @sdate
  GROUP BY product_id, business_name, shelf_type;
  
  
  DELETE
  FROM
    fe_dm.dm_op_area_product_mgmv
  WHERE month_id = @y_m;
  INSERT INTO fe_dm.dm_op_area_product_mgmv (
    month_id, product_id, business_name, qty_sal, gmv, discount, add_user
  )
  SELECT
    month_id, product_id, business_name, SUM(qty_sal) qty_sal, SUM(gmv) gmv, SUM(discount) discount, @add_user add_user
  FROM
    fe_dm.dm_op_product_area_shelftype_mgmv
  WHERE month_id = @y_m
  GROUP BY product_id, business_name;
  
  
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_area_product_mgmv_six',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('宋英南@', @user), @stime);
-- 更新任务的执行状态
UPDATE fe_dwd.dwd_project_excute_status SET execute_status=1,load_time=CURRENT_TIMESTAMP WHERE process_name='dm_op_area_product_mgmv_six' AND sdate=CURRENT_DATE;
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_area_shelftype_dgmv','dm_op_area_product_mgmv_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_area_product_dgmv','dm_op_area_product_mgmv_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_area_shelftype_wgmv','dm_op_area_product_mgmv_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_wgmv','dm_op_area_product_mgmv_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_area_shelftype_mgmv','dm_op_area_product_mgmv_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_mgmv','dm_op_area_product_mgmv_six','宋英南');
COMMIT;
END