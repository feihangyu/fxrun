CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_price_sensitive_stat_month`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @month_id := DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m'), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := CONCAT(@month_id, '-01'), @ym := REPLACE(@month_id, '-', '');
  SET @month_end := LAST_DAY(@month_start), @add_day := ADDDATE(@month_start, INTERVAL 1 MONTH);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    s.business_name, s.shelf_id
  FROM
    fe_dwd.dwd_shelf_base_day_all s
  WHERE   ! ISNULL(s.shelf_id);
	
  DELETE
  FROM
    fe_dm.dm_op_price_sensitive_stat_month
  WHERE month_id = @month_id;
  INSERT INTO fe_dm.dm_op_price_sensitive_stat_month (
    month_id, business_name, product_id, salqty, gmv, total_discount, profit, orders_related, gmv_related, add_user
  )
  SELECT
    @month_id month_id, s.business_name, t.product_id, SUM(t.quantity_act) salqty, SUM(t.quantity_act * t.sale_price) gmv, ROUND(
      SUM(
        t.quantity_act * t.sale_price * (
          t.o_discount_amount + t.o_coupon_amount
        ) / t.ogmv
      ), 2
    ) total_discount, ROUND(
      SUM(
        t.quantity_act * (
          t.sale_price * (
            t.ogmv - t.o_discount_amount - t.o_coupon_amount
          ) / t.ogmv - IFNULL(purchase_price, 0)
        )
      ), 2
    ) profit, SUM(
      t.ogmv > (t.quantity_act * t.sale_price + .1)
    ) orders_related, SUM(
      t.ogmv - t.quantity_act * t.sale_price
    ) gmv_related, @add_user
  FROM
    fe_dwd.dwd_pub_order_item_recent_two_month t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @month_start
    AND t.pay_date < @add_day
  GROUP BY s.business_name, t.product_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.area_count_tmp;
  CREATE TEMPORARY TABLE fe_dm.area_count_tmp AS
  SELECT
    t.business_name, ROUND(AVG(t.gmv), 6) agmv, ROUND(AVG(t.profit), 6) aprofit, ROUND(AVG(t.gmv_related), 6) agmv_related, STDDEV_POP(t.gmv) sgmv, STDDEV_POP(t.profit) sprofit, STDDEV_POP(t.gmv_related) sgmv_related
  FROM
    fe_dm.dm_op_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  GROUP BY t.business_name;
  
  
  UPDATE
    fe_dm.dm_op_price_sensitive_stat_month t
    JOIN fe_dm.area_count_tmp a
      ON t.business_name = a.business_name SET t.gmv_normal = ROUND((t.gmv - a.agmv) / a.sgmv, 2), t.profit_normal = ROUND((t.profit - a.aprofit) / a.sprofit, 2), t.gmv_related_normal = ROUND(
      (t.gmv_related - a.agmv_related) / a.sgmv_related, 2
    )
  WHERE t.month_id = @month_id;
  SET @delta := 0.4;
  
  UPDATE
    fe_dm.dm_op_price_sensitive_stat_month t
  SET
    t.product_contribution_mark = ROUND(
      .4 * t.gmv_normal + .3 * t.profit_normal + .3 * t.gmv_related_normal, 2
    ), t.product_contribution_tag = (t.profit >= 0) * (
      4 * (t.gmv_normal > @delta) + 2 * (t.profit_normal > @delta) + (t.gmv_related_normal > @delta) + 1
    ) - 1- (t.profit < 0) * (t.gmv_normal < 0) * (t.gmv_related_normal < 0)
  WHERE t.month_id = @month_id;
  SET @order_num := 0, @order_area := '';
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.gmv DESC, t.profit DESC, t.gmv_related DESC;
  SET @order_num := 0, @order_area := '';
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.profit_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.profit_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.profit DESC, t.gmv DESC, t.gmv_related DESC;
  
  SET @order_num := 0, @order_area := '';
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.related_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.related_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.gmv_related DESC, t.gmv DESC, t.profit DESC;
  
  SET @order_num := 0, @order_area := '';
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.contribution_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.contribution_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.product_contribution_mark DESC, t.gmv DESC, t.profit DESC, t.gmv_related DESC;
  
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.gmv_order_tmp (business_name, product_id);
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.profit_order_tmp (business_name, product_id);
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.related_order_tmp (business_name, product_id);
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.contribution_order_tmp (business_name, product_id);
  
  UPDATE
    fe_dm.dm_op_price_sensitive_stat_month t
    JOIN fe_dm.gmv_order_tmp g
      ON t.business_name = g.business_name
      AND t.product_id = g.product_id
    JOIN fe_dm.profit_order_tmp p
      ON t.business_name = p.business_name
      AND t.product_id = p.product_id
    JOIN fe_dm.related_order_tmp r
      ON t.business_name = r.business_name
      AND t.product_id = r.product_id
    JOIN fe_dm.contribution_order_tmp c
      ON t.business_name = c.business_name
      AND t.product_id = c.product_id SET t.gmv_order = g.order_num, t.profit_order = p.order_num, t.gmv_related_order = r.order_num, t.product_contribution_order = c.order_num
  WHERE t.month_id = @month_id;
  
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_price_sensitive_stat_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_price_sensitive_stat_month','dm_op_price_sensitive_stat_month','李世龙');
COMMIT;
    END