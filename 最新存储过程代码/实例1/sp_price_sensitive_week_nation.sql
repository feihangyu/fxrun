CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_price_sensitive_week_nation`(in_week_end DATE)
BEGIN
  #run after sh_process.sp_price_sensitive_week
   SET @week_end := SUBDATE(
    in_week_end,
    DAYOFWEEK(in_week_end) - 1
  ),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @week_start := SUBDATE(@week_end, 6),
  @add_date := ADDDATE(@week_end, 1);
  DELETE
  FROM
    feods.fjr_price_sensitive_stat_nation
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_price_sensitive_stat_nation (
    week_end,
    product_id,
    salqty,
    gmv,
    total_discount,
    profit,
    orders_related,
    gmv_related,
    add_user
  )
  SELECT
    t.week_end,
    t.product_id,
    SUM(t.salqty) salqty,
    SUM(t.gmv) gmv,
    SUM(t.total_discount) total_discount,
    SUM(t.profit) profit,
    SUM(t.orders_related) orders_related,
    SUM(t.gmv_related) gmv_related,
    @add_user add_user
  FROM
    feods.fjr_price_sensitive_stat t
  WHERE t.week_end = @week_end
  GROUP BY t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.count_tmp;
  CREATE TEMPORARY TABLE feods.count_tmp AS
  SELECT
    ROUND(AVG(t.gmv), 6) agmv,
    ROUND(AVG(t.profit), 6) aprofit,
    ROUND(AVG(t.gmv_related), 6) agmv_related,
    STDDEV_POP(t.gmv) sgmv,
    STDDEV_POP(t.profit) sprofit,
    STDDEV_POP(t.gmv_related) sgmv_related
  FROM
    feods.fjr_price_sensitive_stat_nation t
  WHERE t.week_end = @week_end;
  UPDATE
    feods.fjr_price_sensitive_stat_nation t
    JOIN feods.count_tmp a
      ON 1 SET t.gmv_normal = ROUND((t.gmv - a.agmv) / a.sgmv, 2),
    t.profit_normal = ROUND((t.profit - a.aprofit) / a.sprofit, 2),
    t.gmv_related_normal = ROUND(
      (t.gmv_related - a.agmv_related) / a.sgmv_related,
      2
    )
  WHERE t.week_end = @week_end;
  SET @delta := 0.4;
  UPDATE
    feods.fjr_price_sensitive_stat_nation t
  SET
    t.product_contribution_mark = ROUND(
      .4 * t.gmv_normal + .3 * t.profit_normal + .3 * t.gmv_related_normal,
      2
    ),
    t.product_contribution_tag = (t.profit >= 0) * (
      4 * (t.gmv_normal > @delta) + 2 * (t.profit_normal > @delta) + (t.gmv_related_normal > @delta) + 1
    ) - 1- (t.profit < 0) * (t.gmv_normal < 0) * (t.gmv_related_normal < 0)
  WHERE t.week_end = @week_end;
  SET @order_num := 0;
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_order_tmp;
  CREATE TEMPORARY TABLE feods.gmv_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    feods.fjr_price_sensitive_stat_nation t
  WHERE t.week_end = @week_end
  ORDER BY t.gmv DESC,
    t.profit DESC,
    t.gmv_related DESC;
  SET @order_num := 0;
  DROP TEMPORARY TABLE IF EXISTS feods.profit_order_tmp;
  CREATE TEMPORARY TABLE feods.profit_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    feods.fjr_price_sensitive_stat_nation t
  WHERE t.week_end = @week_end
  ORDER BY t.profit DESC,
    t.gmv DESC,
    t.gmv_related DESC;
  SET @order_num := 0;
  DROP TEMPORARY TABLE IF EXISTS feods.related_order_tmp;
  CREATE TEMPORARY TABLE feods.related_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    feods.fjr_price_sensitive_stat_nation t
  WHERE t.week_end = @week_end
  ORDER BY t.gmv_related DESC,
    t.gmv DESC,
    t.profit DESC;
  SET @order_num := 0;
  DROP TEMPORARY TABLE IF EXISTS feods.contribution_order_tmp;
  CREATE TEMPORARY TABLE feods.contribution_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    feods.fjr_price_sensitive_stat_nation t
  WHERE t.week_end = @week_end
  ORDER BY t.product_contribution_mark DESC,
    t.gmv DESC,
    t.profit DESC,
    t.gmv_related DESC;
  UPDATE
    feods.fjr_price_sensitive_stat_nation t
    JOIN feods.gmv_order_tmp g
      ON t.product_id = g.product_id
    JOIN feods.profit_order_tmp p
      ON t.product_id = p.product_id
    JOIN feods.related_order_tmp r
      ON t.product_id = r.product_id
    JOIN feods.contribution_order_tmp c
      ON t.product_id = c.product_id SET t.gmv_order = g.order_num,
    t.profit_order = p.order_num,
    t.gmv_related_order = r.order_num,
    t.product_contribution_order = c.order_num
  WHERE t.week_end = @week_end;
  CALL feods.sp_task_log (
    'sp_price_sensitive_week_nation',
    @week_end,
    CONCAT(
      'fjr_w_9a0a85d7a1a8a3d18ce6705c250103ec',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END