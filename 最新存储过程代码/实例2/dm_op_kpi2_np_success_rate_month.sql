CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_np_success_rate_month`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
   SET @month_id := DATE_FORMAT(SUBDATE(DATE_FORMAT(current_date,'%Y-%m-01'),INTERVAL 1 DAY),'%Y-%m'), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := CONCAT(@month_id, '-01');
  SET @month_end := LAST_DAY(@month_start);
  SET @add_day := ADDDATE(@month_end, 1);
  SET @last_month_end := SUBDATE(@month_start, 1);
 
  DROP TEMPORARY TABLE IF EXISTS fe_dm.vv_dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.vv_dim_tmp AS
  SELECT
    t.version_id version_id, DATE(t.sdate) min_date, DATE(t.edate) max_date     --  t.version version_id, DATE(t.min_date) min_date, DATE(t.max_date) max_date
  FROM
    fe_dwd.dwd_op_dim_date t;                         -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.month_last_date_tmp;
  CREATE TEMPORARY TABLE fe_dm.month_last_date_tmp AS
  SELECT
    MAX(t.sdate) ldate
  FROM
    fe_dwd.dwd_pub_work_day t
  GROUP BY DATE_FORMAT(t.sdate, '%y%m');
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.month_dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.month_dim_tmp AS
  SELECT
    t.ldate, v.version_id
  FROM
    fe_dm.month_last_date_tmp t
    JOIN fe_dm.vv_dim_tmp v
      ON t.ldate >= v.min_date
      AND t.ldate < v.max_date;
	  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.last_dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.last_dim_tmp AS
  SELECT
    LAST_DAY(ADDDATE(t.ldate, 1)) ldate, dh.business_area business_name, dh.product_id
  FROM
    fe_dm.month_dim_tmp t
    JOIN fe_dwd.dwd_pub_product_dim_sserp_his dh
      ON t.version_id = dh.version
      AND dh.product_type IN (
        '原有', '新增（正式运行）'
      );
  DROP TEMPORARY TABLE IF EXISTS fe_dm.out_dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.out_dim_tmp AS
  SELECT
    ld.business_name, ld.product_id, MAX(t.ldate) ldate
  FROM
    fe_dm.month_dim_tmp t
    JOIN fe_dwd.dwd_pub_product_dim_sserp_his dh
      ON t.version_id = dh.version
      AND dh.product_type IN (
        '停补', '停补（替补）', '淘汰', '淘汰（替补）', '退出'
      )
    JOIN fe_dm.last_dim_tmp ld
      ON t.ldate = ld.ldate
      AND dh.business_area = ld.business_name
      AND dh.product_id = ld.product_id
  GROUP BY ld.business_name, ld.product_id;
  
  SELECT
    @version_id := t.version_id      -- @version_id := t.version
  FROM
    fe_dwd.dwd_op_dim_date t            -- 用 d_op_dim_date 取代 vv_fjr_product_dim_sserp_period3
  WHERE t.sdate <= @month_end        -- t.min_date <= @month_end
    AND t.edate > @month_end;        -- t.max_date > @month_end
  DROP TEMPORARY TABLE IF EXISTS fe_dm.np_dim_tmp;
  CREATE TEMPORARY TABLE fe_dm.np_dim_tmp AS
  SELECT
    t.business_area business_name, t.product_id, p.product_id replace_product_id
  FROM
    fe_dwd.dwd_pub_product_dim_sserp_his t
    LEFT JOIN fe_dwd.dwd_product_base_day_all p
      ON t.replace_product_fe = p.product_code2
      AND p.product_code2 != ''
  WHERE t.version = @version_id
    AND t.product_type IN (
      '新增（试运行）', '新增（免费货）'
    );
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id)) AS
	SELECT
    t.shelf_id, t.business_name
  FROM
    fe_dwd.dwd_shelf_base_day_all  t
  WHERE   ! ISNULL(t.shelf_id);
	
	
  
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.np_tmp;
  CREATE TEMPORARY TABLE fe_dm.np_tmp AS
  SELECT DISTINCT
    d.business_name, d.product_id, d.replace_product_id
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN fe_dm.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN fe_dm.np_dim_tmp d
      ON s.business_name = d.business_name
      AND t.product_id = d.product_id
  WHERE t.first_fill_time < @month_start;
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.np_replace_tmp;
  CREATE TEMPORARY TABLE fe_dm.np_replace_tmp AS
  SELECT
    t.business_name, t.replace_product_id, ADDDATE(IFNULL(o.ldate, @month_end), 1) ldate
  FROM
    fe_dm.np_tmp t
    LEFT JOIN fe_dm.out_dim_tmp o
      ON t.business_name = o.business_name
      AND t.replace_product_id = o.product_id
  WHERE t.replace_product_id IS NOT NULL;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_replace_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_replace_tmp AS
  SELECT
    t.business_name, t.product_id, AVG(t.gmv) gmv
  FROM
    (SELECT
      MONTH(t.sdate) smonth, t.business_name, t.product_id, SUM(t.gmv) gmv
    FROM
      fe_dm.dm_area_product_dgmv t
      JOIN fe_dm.np_replace_tmp r
        ON t.business_name = r.business_name
        AND t.product_id = r.replace_product_id
        AND t.sdate >= SUBDATE(r.ldate, INTERVAL 3 MONTH)
        AND t.sdate < r.ldate
    GROUP BY smonth, t.business_name, t.product_id) t
  GROUP BY t.business_name, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_np_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_np_tmp AS
  SELECT
    t.business_name, t.product_id, SUM(t.gmv) gmv
  FROM
    fe_dm.dm_area_product_dgmv t
    JOIN fe_dm.np_tmp np
      ON t.business_name = np.business_name
      AND t.product_id = np.product_id
      AND t.sdate >= @month_start
      AND t.sdate < @add_day
  GROUP BY t.business_name, t.product_id;
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.gmv_area_tmp;
  CREATE TEMPORARY TABLE fe_dm.gmv_area_tmp AS
  SELECT
    t.business_name, SUM(t.gmv) gmv
  FROM
    fe_dm.dm_area_product_dgmv t
  WHERE t.sdate >= @month_start
    AND t.sdate < @add_day
  GROUP BY t.business_name;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_np_success_rate_month
  WHERE month_id = @month_id;
  INSERT INTO fe_dm.dm_op_kpi2_np_success_rate_month (
    month_id, version_id, business_name, product_id, replace_product_id, gmv, gmv_aim, add_user
  )
  SELECT
    @month_id month_id, @version_id version_id, t.business_name, t.product_id, t.replace_product_id, IFNULL(gn.gmv, 0) gmv, ROUND(IFNULL(gr.gmv, ga.gmv * .015), 2) gmv_aim, @add_user
  FROM
    fe_dm.np_tmp t
    LEFT JOIN fe_dm.gmv_np_tmp gn
      ON t.business_name = gn.business_name
      AND t.product_id = gn.product_id
    LEFT JOIN fe_dm.gmv_replace_tmp gr
      ON t.business_name = gr.business_name
      AND t.replace_product_id = gr.product_id
    LEFT JOIN fe_dm.gmv_area_tmp ga
      ON t.business_name = ga.business_name;
	  
	  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor_area
  WHERE indicate_id = 106
    AND sdate = @month_start;
  INSERT INTO fe_dm.dm_op_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, t.business_name, 'm' indicate_type, 106 indicate_id, 'dm_op_kpi2_np_success_rate_month' indicate_name, ROUND(SUM(t.gmv >= t.gmv_aim) / COUNT(*), 6) indicate_value, @add_user
  FROM
    fe_dm.dm_op_kpi2_np_success_rate_month t
  WHERE t.month_id = @month_id
  GROUP BY t.business_name;
  
  SELECT
    @npr := ROUND(SUM(t.gmv >= t.gmv_aim) / COUNT(*), 6)
  FROM
    fe_dm.dm_op_kpi2_np_success_rate_month t
  WHERE t.month_id = @month_id;
  
  
  DELETE
  FROM
    fe_dm.dm_op_kpi2_monitor
  WHERE indicate_id = 106
    AND sdate = @month_start;
  INSERT INTO fe_dm.dm_op_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @month_start, 'm', 106, 'dm_op_kpi2_np_success_rate_month', @npr, @add_user
    );
  
  
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi2_np_success_rate_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_np_success_rate_month','dm_op_kpi2_np_success_rate_month','李世龙');
COMMIT;
    END