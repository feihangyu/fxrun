CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_effective_stock_two`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP, @high_tot := 6;
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_start_last := SUBDATE(@month_start, INTERVAL 1 MONTH), @month_start_last2 := SUBDATE(@month_start, INTERVAL 2 MONTH), @month_start_last3 := SUBDATE(@month_start, INTERVAL 3 MONTH);
  SET @y_m := DATE_FORMAT(@month_start, '%Y-%m'), @y_m_last := DATE_FORMAT(@month_start_last, '%Y-%m'), @y_m_last2 := DATE_FORMAT(@month_start_last2, '%Y-%m'), @y_m_last3 := DATE_FORMAT(@month_start_last3, '%Y-%m');
  SET @ym := DATE_FORMAT(@month_start, '%Y%m'), @ym_last := DATE_FORMAT(@month_start_last, '%Y%m'), @ym_last2 := DATE_FORMAT(@month_start_last2, '%Y%m'), @ym_last3 := DATE_FORMAT(@month_start_last3, '%Y%m');
SET @cur_month_01 := DATE_FORMAT(@sdate,'%Y-%m-01');  
  SET @sql_str := CONCAT(
    "ALTER TABLE fe_dm.dm_op_tot_stat TRUNCATE PARTITION p", @ym_last3, ",p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
SET @time_1 := CURRENT_TIMESTAMP();	
INSERT INTO fe_dm.dm_op_tot_stat (
        month_id,
        shelf_id,
        product_id,
        tot,
        qty_sal,
        days_sal,
        days_tot,
        add_user
) 
SELECT 
        @y_m month_id,
        shelf_id,
        product_id,
        tot,
        IFNULL(SUM(sal),0) qty_sal,
        IFNULL(SUM(sal > 0),0) days_sal,
        COUNT(*) days_tot,
        @add_user add_user 
FROM
(
        SELECT 
                shelf_id,
                product_id,
                sal_qty sal,
                stock_quantity tot 
        FROM
                fe_dwd.`dwd_shelf_product_sto_sal_30_days` 
        WHERE sdate >= @cur_month_01
                AND stock_quantity > 0 
) t 
GROUP BY shelf_id,
        product_id,
        tot 
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_effective_stock_two","@time_1--@time_2",@time_1,@time_2);	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.dst_tmp;
  CREATE TEMPORARY TABLE fe_dwd.dst_tmp (KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id, tot, SUM(days_tot) days_tot, SUM(days_sal) days_sal, SUM(qty_sal) qty_sal
  FROM
        fe_dm.dm_op_tot_stat
  GROUP BY shelf_id, product_id, tot;
  
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_effective_stock_two","@time_2--@time_3",@time_2,@time_3);	
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.dst_ct_tmp;
  CREATE TEMPORARY TABLE fe_dwd.dst_ct_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id, COUNT(*) ct, CEILING(COUNT(*) / 3) ct3, SUM(days_tot) days_tott, SUM(days_sal) days_salt, SUM(qty_sal) qty_salt, SUM(tot * days_tot) qty_tott, SUM(IF(tot >= @high_tot, days_tot, 0)) days_toth, SUM(IF(tot >= @high_tot, days_sal, 0)) days_salh, SUM(IF(tot >= @high_tot, qty_sal, 0)) qty_salh, SUM(
      IF(tot >= @high_tot, tot * days_tot, 0)
    ) qty_toth
  FROM
    fe_dwd.dst_tmp
  GROUP BY shelf_id, product_id;
  
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_effective_stock_two","@time_3--@time_4",@time_3,@time_4);	
  
  SET @shelf_id := NULL, @product_id := NULL, @order_num := NULL;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.dst_re_tmp;
  CREATE TEMPORARY TABLE fe_dwd.dst_re_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, MIN(t.tot) effective_stock
  FROM
    (SELECT
      @order_num := IF(
        @shelf_id = t.shelf_id && @product_id = t.product_id, @order_num, 0
      ) + 1 order_num, @shelf_id := t.shelf_id shelf_id, @product_id := t.product_id product_id, t.tot, t.avgsal
    FROM
      (SELECT
        shelf_id, product_id, tot, qty_sal / days_tot avgsal
      FROM
        fe_dwd.dst_tmp
      ORDER BY shelf_id, product_id, avgsal DESC) t) t
    JOIN fe_dwd.dst_ct_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
      AND t.order_num <= d.ct3
  GROUP BY t.shelf_id, t.product_id;
  
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_effective_stock_two","@time_4--@time_5",@time_4,@time_5);	
  
  TRUNCATE fe_dm.dm_op_effective_stock;
  INSERT INTO fe_dm.dm_op_effective_stock (
    shelf_id, product_id, effective_stock, qty_sal, qty_tot, days_sal, days_tot, qty_salh, qty_toth, days_salh, days_toth, tots, qty_salt, qty_tott, days_salt, days_tott, add_user
  )
  SELECT
    t.shelf_id, t.product_id, r.effective_stock, d.qty_sal, d.tot * d.days_tot qty_tot, d.days_sal, d.days_tot, t.qty_salh, t.qty_toth, t.days_salh, t.days_toth days_toth, t.ct tots, t.qty_salt, t.qty_tott, t.days_salt, t.days_tott days_tott, @add_user add_user
  FROM
    fe_dwd.dst_ct_tmp t
    JOIN fe_dwd.dst_re_tmp r
      ON t.shelf_id = r.shelf_id
      AND t.product_id = r.product_id
    JOIN fe_dwd.dst_tmp d
      ON r.shelf_id = d.shelf_id
      AND r.product_id = d.product_id
      AND r.effective_stock = d.tot;
	  
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_effective_stock_two","@time_5--@time_6",@time_5,@time_6);	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_effective_stock_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_tot_stat','dm_op_effective_stock_two','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_effective_stock','dm_op_effective_stock_two','宋英南');
END