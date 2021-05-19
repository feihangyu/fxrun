CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_outlet_rate`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   SET @sdate := CURRENT_DATE,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP,
  @outlet_w := 0,
  @outlet_m := 0,
  @version_id = '';
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1),
  @last_month_end := SUBDATE(@sdate, DAY(@sdate));
  SET @last_month_start := SUBDATE(
    @last_month_end,
    DAY(@last_month_end) - 1
  );
  SELECT
    @version_id := MAX(t.version)
  FROM
	fe_dwd.dwd_pub_product_dim_sserp t;
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.dim_tmp AS
  SELECT
    t.business_area business_name,
    t.product_id
  FROM
    fe_dwd.dwd_pub_product_dim_sserp t
  WHERE t.product_type IN (
      '停补',
      '停补（替补）',
      '淘汰',
      '淘汰（替补）',
      '退出'
    );
	
	
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.dim_tmp (business_name, product_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dm.sal_tmp;
  CREATE TEMPORARY TABLE fe_dm.sal_tmp AS
  SELECT
    t.business_name,
    t.product_id,
    SUM(t.qty_sal) cum_qty_sal,
    SUM(t.gmv) cum_gmv,
    SUM(t.discount) cum_discount
  FROM
    fe_dm.dm_area_product_dgmv t
    JOIN fe_dm.dim_tmp d
      ON t.business_name = d.business_name
      AND t.product_id = d.product_id
  WHERE t.sdate < @sdate
  GROUP BY t.business_name,
    t.product_id;
	
	
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.sal_tmp (business_name, product_id);
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.fil_tmp;
  CREATE TEMPORARY TABLE fe_dm.fil_tmp AS
  SELECT
    t.business_name,
    t.product_id,
    SUM(t.qty_fill) cum_qty_fil
  FROM
    fe_dm.dm_op_area_product_dfill t
    JOIN fe_dm.dim_tmp d
      ON t.business_name = d.business_name
      AND t.product_id = d.product_id
  WHERE t.sdate < @sdate
  GROUP BY t.business_name,
    t.product_id;
	
	
  CREATE INDEX idx_business_name_product_id
  ON fe_dm.fil_tmp (business_name, product_id);
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_outlet_rate
  WHERE sdate = @sdate;
  
  INSERT INTO fe_dm.dm_op_kpi2_outlet_rate (
    sdate,
    version_id,
    business_name,
    product_id,
    cum_qty_sal,
    cum_gmv,
    cum_discount,
    cum_qty_fil,
    add_user
  )
  SELECT
    @sdate sdate,
    @version_id version_id,
    t.business_name,
    t.product_id,
    IFNULL(s.cum_qty_sal, 0) cum_qty_sal,
    IFNULL(s.cum_gmv, 0) cum_gmv,
    IFNULL(s.cum_discount, 0) cum_discount,
    IFNULL(f.cum_qty_fil, 0) cum_qty_fil,
    @add_user
  FROM
    fe_dm.dim_tmp t
    LEFT JOIN fe_dm.sal_tmp s
      ON t.business_name = s.business_name
      AND t.product_id = s.product_id
    LEFT JOIN fe_dm.fil_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id;
	  
	  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE indicate_id = 109
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate,
    business_name,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @last_week_end sdate,
    t.business_name,
    'w' indicate_type,
    109 indicate_id,
    'dm_op_kpi2_outlet_rate' indicate_name,
    ROUND(
      SUM(t.cum_gmv) / SUM(
        t.cum_qty_fil * t.cum_gmv / t.cum_qty_sal
      ),
      6
    ) indicate_value,
    @add_user
  FROM
    (SELECT
      t.business_name,
      t.product_id,
      SUM(t.cum_qty_sal) cum_qty_sal,
      SUM(t.cum_gmv) cum_gmv,
      SUM(t.cum_qty_fil) cum_qty_fil
    FROM
      fe_dm.dm_op_kpi2_outlet_rate t
    WHERE t.sdate = @last_week_end
    GROUP BY t.business_name,
      t.product_id) t
  GROUP BY t.business_name;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate,
    business_name,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  SELECT
    @last_month_start sdate,
    t.business_name,
    'm' indicate_type,
    109 indicate_id,
    'dm_op_kpi2_outlet_rate' indicate_name,
    ROUND(
      SUM(t.cum_gmv) / SUM(
        t.cum_qty_fil * t.cum_gmv / t.cum_qty_sal
      ),
      6
    ) indicate_value,
    @add_user
  FROM
    (SELECT
      t.business_name,
      t.product_id,
      SUM(t.cum_qty_sal) cum_qty_sal,
      SUM(t.cum_gmv) cum_gmv,
      SUM(t.cum_qty_fil) cum_qty_fil
    FROM
      fe_dm.dm_op_kpi2_outlet_rate t
    WHERE t.sdate = @last_month_end
    GROUP BY t.business_name,
      t.product_id) t
  GROUP BY t.business_name;
  SELECT
    @outlet_w := ROUND(
      SUM(t.cum_gmv) / SUM(
        t.cum_qty_fil * t.cum_gmv / t.cum_qty_sal
      ),
      6
    )
  FROM
    (SELECT
      t.product_id,
      SUM(t.cum_qty_sal) cum_qty_sal,
      SUM(t.cum_gmv) cum_gmv,
      SUM(t.cum_qty_fil) cum_qty_fil
    FROM
      fe_dm.dm_op_kpi2_outlet_rate t
    WHERE t.sdate = @last_week_end
    GROUP BY t.product_id) t;
  SELECT
    @outlet_m := ROUND(
      SUM(t.cum_gmv) / SUM(
        t.cum_qty_fil * t.cum_gmv / t.cum_qty_sal
      ),
      6
    )
  FROM
    (SELECT
      t.product_id,
      SUM(t.cum_qty_sal) cum_qty_sal,
      SUM(t.cum_gmv) cum_gmv,
      SUM(t.cum_qty_fil) cum_qty_fil
    FROM
      fe_dm.dm_op_kpi2_outlet_rate t
    WHERE t.sdate = @last_month_end
    GROUP BY t.product_id) t;
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE indicate_id = 109
    AND (
      sdate = @last_week_end
      OR sdate = @last_month_start
    );
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate,
    indicate_type,
    indicate_id,
    indicate_name,
    indicate_value,
    add_user
  )
  VALUES
    (
      @last_week_end,
      'w',
      109,
      'dm_op_kpi2_outlet_rate',
      @outlet_w,
      @add_user
    ),
    (
      @last_month_start,
      'm',
      109,
      'dm_op_kpi2_outlet_rate',
      @outlet_m,
      @add_user
    );
  
  
  
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_outlet_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_outlet_rate','dm_op_kpi2_outlet_rate','李世龙');
COMMIT;
    END