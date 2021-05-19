CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_price_sensitive_stat_month`(in_month_id CHAR(7))
BEGIN
  #run after sh_process.dwd_order_item_refund_day_inc
   SET @month_id := in_month_id, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := CONCAT(@month_id, '-01'), @ym := REPLACE(@month_id, '-', '');
  SET @month_end := LAST_DAY(@month_start), @add_day := ADDDATE(@month_start, INTERVAL 1 MONTH);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    b.business_name, s.shelf_id
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1
    AND ! ISNULL(s.shelf_id);
  DELETE
  FROM
    feods.fjr_price_sensitive_stat_month
  WHERE month_id = @month_id;
  INSERT INTO feods.fjr_price_sensitive_stat_month (
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
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @month_start
    AND t.pay_date < @add_day
  GROUP BY s.business_name, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.area_count_tmp;
  CREATE TEMPORARY TABLE feods.area_count_tmp AS
  SELECT
    t.business_name, ROUND(AVG(t.gmv), 6) agmv, ROUND(AVG(t.profit), 6) aprofit, ROUND(AVG(t.gmv_related), 6) agmv_related, STDDEV_POP(t.gmv) sgmv, STDDEV_POP(t.profit) sprofit, STDDEV_POP(t.gmv_related) sgmv_related
  FROM
    feods.fjr_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  GROUP BY t.business_name;
  UPDATE
    feods.fjr_price_sensitive_stat_month t
    JOIN feods.area_count_tmp a
      ON t.business_name = a.business_name SET t.gmv_normal = ROUND((t.gmv - a.agmv) / a.sgmv, 2), t.profit_normal = ROUND((t.profit - a.aprofit) / a.sprofit, 2), t.gmv_related_normal = ROUND(
      (t.gmv_related - a.agmv_related) / a.sgmv_related, 2
    )
  WHERE t.month_id = @month_id;
  SET @delta := 0.4;
  UPDATE
    feods.fjr_price_sensitive_stat_month t
  SET
    t.product_contribution_mark = ROUND(
      .4 * t.gmv_normal + .3 * t.profit_normal + .3 * t.gmv_related_normal, 2
    ), t.product_contribution_tag = (t.profit >= 0) * (
      4 * (t.gmv_normal > @delta) + 2 * (t.profit_normal > @delta) + (t.gmv_related_normal > @delta) + 1
    ) - 1- (t.profit < 0) * (t.gmv_normal < 0) * (t.gmv_related_normal < 0)
  WHERE t.month_id = @month_id;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_order_tmp;
  CREATE TEMPORARY TABLE feods.gmv_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    feods.fjr_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.gmv DESC, t.profit DESC, t.gmv_related DESC;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.profit_order_tmp;
  CREATE TEMPORARY TABLE feods.profit_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    feods.fjr_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.profit DESC, t.gmv DESC, t.gmv_related DESC;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.related_order_tmp;
  CREATE TEMPORARY TABLE feods.related_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    feods.fjr_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.gmv_related DESC, t.gmv DESC, t.profit DESC;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.contribution_order_tmp;
  CREATE TEMPORARY TABLE feods.contribution_order_tmp AS
  SELECT
    @order_num := IF(
      @order_area = t.business_name, @order_num + 1, 1
    ) order_num, @order_area := t.business_name business_name, t.product_id
  FROM
    feods.fjr_price_sensitive_stat_month t
  WHERE t.month_id = @month_id
  ORDER BY t.business_name, t.product_contribution_mark DESC, t.gmv DESC, t.profit DESC, t.gmv_related DESC;
  CREATE INDEX idx_business_name_product_id
  ON feods.gmv_order_tmp (business_name, product_id);
  CREATE INDEX idx_business_name_product_id
  ON feods.profit_order_tmp (business_name, product_id);
  CREATE INDEX idx_business_name_product_id
  ON feods.related_order_tmp (business_name, product_id);
  CREATE INDEX idx_business_name_product_id
  ON feods.contribution_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat_month t
    JOIN feods.gmv_order_tmp g
      ON t.business_name = g.business_name
      AND t.product_id = g.product_id
    JOIN feods.profit_order_tmp p
      ON t.business_name = p.business_name
      AND t.product_id = p.product_id
    JOIN feods.related_order_tmp r
      ON t.business_name = r.business_name
      AND t.product_id = r.product_id
    JOIN feods.contribution_order_tmp c
      ON t.business_name = c.business_name
      AND t.product_id = c.product_id SET t.gmv_order = g.order_num, t.profit_order = p.order_num, t.gmv_related_order = r.order_num, t.product_contribution_order = c.order_num
  WHERE t.month_id = @month_id;
  CALL feods.sp_task_log (
    'sp_price_sensitive_stat_month', @month_start, CONCAT(
      'fjr_m_9d1c18656800bab2c9228b50f8093e6d', @timestamp, @add_user
    )
  );
  CALL sh_process.sp_price_sensitive_stat_month_nation (@month_id);
  COMMIT;
END