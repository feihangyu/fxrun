CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_price_sensitive_stat_month_nation`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @month_id := DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m'),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := CONCAT(@month_id, '-01');
  
  DELETE
  FROM
    fe_dm.dm_op_price_sensitive_stat_month_nation
  WHERE month_id = @month_id;
  INSERT INTO fe_dm.dm_op_price_sensitive_stat_month_nation (
    month_id,
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
    t.month_id,
    t.product_id,
    SUM(t.salqty) salqty,
    SUM(t.gmv) gmv,
    SUM(t.total_discount) total_discount,
    SUM(t.profit) profit,
    SUM(t.orders_related) orders_related,
    SUM(t.gmv_related) gmv_related,
    @add_user add_user
  FROM
    fe_dm.dm_op_price_sensitive_stat_month t  
  WHERE t.month_id = @month_id
  GROUP BY t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.count_tmp;
  CREATE TEMPORARY TABLE fe_dm.count_tmp AS
  SELECT
    ROUND(AVG(t.gmv), 6) agmv,
    ROUND(AVG(t.profit), 6) aprofit,
    ROUND(AVG(t.gmv_related), 6) agmv_related,
    STDDEV_POP(t.gmv) sgmv,
    STDDEV_POP(t.profit) sprofit,
    STDDEV_POP(t.gmv_related) sgmv_related
  FROM
    fe_dm.dm_op_price_sensitive_stat_month_nation t
  WHERE t.month_id = @month_id;
  
  UPDATE
    fe_dm.dm_op_price_sensitive_stat_month_nation t
    JOIN fe_dm.count_tmp a
      ON 1 SET t.gmv_normal = ROUND((t.gmv - a.agmv) / a.sgmv, 2),
    t.profit_normal = ROUND((t.profit - a.aprofit) / a.sprofit, 2),
    t.gmv_related_normal = ROUND(
      (t.gmv_related - a.agmv_related) / a.sgmv_related,
      2
    )
  WHERE t.month_id = @month_id;
  SET @delta := 0.4;
  
  
  UPDATE
    fe_dm.dm_op_price_sensitive_stat_month_nation t
  SET
    t.product_contribution_mark = ROUND(
      .4 * t.gmv_normal + .3 * t.profit_normal + .3 * t.gmv_related_normal,
      2
    ),
    t.product_contribution_tag = (t.profit >= 0) * (
      4 * (t.gmv_normal > @delta) + 2 * (t.profit_normal > @delta) + (t.gmv_related_normal > @delta) + 1
    ) - 1- (t.profit < 0) * (t.gmv_normal < 0) * (t.gmv_related_normal < 0)
  WHERE t.month_id = @month_id;
  SET @order_num := 0;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month_nation t
  WHERE t.month_id = @month_id
  ORDER BY t.gmv DESC,
    t.profit DESC,
    t.gmv_related DESC;
  SET @order_num := 0;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.profit_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.profit_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month_nation t
  WHERE t.month_id = @month_id
  ORDER BY t.profit DESC,
    t.gmv DESC,
    t.gmv_related DESC;
  SET @order_num := 0;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.related_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.related_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month_nation t
  WHERE t.month_id = @month_id
  ORDER BY t.gmv_related DESC,
    t.gmv DESC,
    t.profit DESC;
  SET @order_num := 0;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.contribution_order_tmp;
  CREATE TEMPORARY TABLE fe_dm.contribution_order_tmp AS
  SELECT
    @order_num := @order_num + 1 order_num,
    t.product_id
  FROM
    fe_dm.dm_op_price_sensitive_stat_month_nation t
  WHERE t.month_id = @month_id
  ORDER BY t.product_contribution_mark DESC,
    t.gmv DESC,
    t.profit DESC,
    t.gmv_related DESC;
	
	
  UPDATE
    fe_dm.dm_op_price_sensitive_stat_month_nation t
    JOIN fe_dm.gmv_order_tmp g
      ON t.product_id = g.product_id
    JOIN fe_dm.profit_order_tmp p
      ON t.product_id = p.product_id
    JOIN fe_dm.related_order_tmp r
      ON t.product_id = r.product_id
    JOIN fe_dm.contribution_order_tmp c
      ON t.product_id = c.product_id SET t.gmv_order = g.order_num,
    t.profit_order = p.order_num,
    t.gmv_related_order = r.order_num,
    t.product_contribution_order = c.order_num
  WHERE t.month_id = @month_id;
 
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_price_sensitive_stat_month_nation',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_price_sensitive_stat_month_nation','dm_op_price_sensitive_stat_month_nation','李世龙');
COMMIT;
    END