CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_shelf_add_mgmv`()
BEGIN 
  SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @sdate_lm := SUBDATE(@sdate, INTERVAL 1 MONTH);
  SET @y_m_lm := DATE_FORMAT(@sdate_lm, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@add_day, @d);
  SET @month_start_lm := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @last_day := LAST_DAY(@sdate);
  SET @d_m := DAY(@last_day);
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_shelf_add_mgmv_1;
  CREATE TEMPORARY TABLE fe_dm.dm_shelf_add_mgmv_1 (KEY (month_id, shelf_id))
	SELECT DATE_FORMAT(sdate, '%Y-%m') month_id,
	shelf_id,
	SUM(AFTER_PAYMENT_MONEY) gmv
	FROM fe_dwd.dwd_shelf_day_his
	WHERE DATE_FORMAT(sdate, '%Y-%m') IN (@y_m, @y_m_lm)
	GROUP BY DATE_FORMAT(sdate, '%Y-%m') ,
	shelf_id
	HAVING SUM(AFTER_PAYMENT_MONEY) >0;
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_shelf_add_mgmv_2;
  CREATE TEMPORARY TABLE fe_dm.dm_shelf_add_mgmv_2 (KEY (month_id, shelf_id))
  SELECT
    DATE_FORMAT(apply_time, '%Y-%m') month_id, supplier_id shelf_id, SUM(total_price) gmv
  FROM
    fe.sf_product_fill_order
  WHERE apply_time >= @month_start_lm
    AND apply_time < @add_day
    AND order_status = 11
    AND sales_bussniess_channel = 1
    AND sales_order_status = 3
    AND sales_audit_status = 2
    AND fill_type = 13
    AND total_price > 0
    AND ! ISNULL(supplier_id)
  GROUP BY month_id, supplier_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_shelf_add_mgmv_3;
  CREATE TEMPORARY TABLE fe_dm.dm_shelf_add_mgmv_3 (KEY (month_id, shelf_id))
  SELECT
    DATE_FORMAT(t.paytime, '%Y-%m') month_id, t.shelf_id, SUM(oi.price * oi.product_count) gmv
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
  WHERE t.data_flag = 1
    AND t.paytime >= @month_start_lm
    AND t.paytime < @add_day
    AND t.pay_status = 1
    AND ! ISNULL(t.shelf_id)
  GROUP BY month_id, t.shelf_id
  HAVING gmv > 0;
  
    DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_shelf_add_mgmv_4;
  CREATE TEMPORARY TABLE fe_dm.dm_shelf_add_mgmv_4 (KEY (month_id, shelf_id))
  SELECT a.month_id,a.shelf_id,
  SUM(gmv) gmv
  FROM 
  (SELECT * FROM fe_dm.dm_shelf_add_mgmv_1
   UNION ALL 
   SELECT * FROM fe_dm.dm_shelf_add_mgmv_2
   UNION ALL 
   SELECT * FROM fe_dm.dm_shelf_add_mgmv_3) a
GROUP BY a.month_id,a.shelf_id;
 TRUNCATE TABLE fe_dm.dm_shelf_add_mgmv;	
 INSERT INTO fe_dm.dm_shelf_add_mgmv
 (
 month_id,
 shelf_id,
 gmv
 )
 SELECT  a.month_id,a.shelf_id,
  a.gmv
  FROM fe_dm.dm_shelf_add_mgmv_4 a;
  
 --   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_shelf_add_mgmv',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
  COMMIT;
END