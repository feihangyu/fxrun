CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_newshelf_quality`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @sdate := SUBDATE(CURRENT_DATE, 1),
  @add_user := current_user,
  @timestamp := CURRENT_TIMESTAMP;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dm.oi_tmp,
  fe_dm.ap_tmp;
  CREATE TEMPORARY TABLE fe_dm.oi_tmp AS
  SELECT
    o.shelf_id,
    s.exploit_type,
    s.revoke_status,
    COUNT(DISTINCT o.user_id) users,
    SUM(o.quantity * o.sale_price) gmv
  FROM
  fe_dwd.dwd_pub_order_item_recent_one_month o
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON o.shelf_id = s.shelf_id
      AND s.shelf_type IN (1, 3)
      AND s.shelf_status IN (2, 3)
      AND s.activate_time >= SUBDATE(@sdate, 14-1)
      AND s.activate_time < SUBDATE(@sdate, 14-2)
	  AND o.pay_date >= SUBDATE(@sdate, 14-1)
      AND o.pay_date < ADDDATE(@sdate, 1)
  GROUP BY o.shelf_id;
  
  
  
  CREATE INDEX idx_oi_tmp_shelf_id
  ON fe_dm.oi_tmp (shelf_id);
  CREATE TEMPORARY TABLE fe_dm.ap_tmp AS
  SELECT
    t.shelf_id,
    s.exploit_type,
    s.revoke_status,
    SUM(t.payment_money) payment_money
  FROM
    fe_dwd.dwd_sf_after_payment t
    JOIN fe_dwd.dwd_shelf_base_day_all s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 1
      AND s.shelf_type IN (1, 3)
      AND s.shelf_status IN (2, 3)
      AND s.activate_time >= SUBDATE(@sdate, 14-1)
      AND s.activate_time < SUBDATE(@sdate, 14-2)
  WHERE t.payment_status = 2
    AND t.payment_date >= SUBDATE(@sdate, 14-1)
    AND t.payment_date < ADDDATE(@sdate, 1)
  GROUP BY t.shelf_id;
  
  
  CREATE INDEX idx_ap_tmp_shelf_id
  ON fe_dm.ap_tmp (shelf_id);
  
    DELETE
  FROM
    fe_dm.`dm_op_newshelf_quality`
  WHERE sdate = @sdate;
  insert into fe_dm.`dm_op_newshelf_quality` (
    sdate,
    shelf_id,
    exploit_type,
    revoke_status,
    users,
    gmv,
    payment_money,
    add_user
  )
  SELECT
    @sdate,
    t.shelf_id,
    t.exploit_type,
    t.revoke_status,
    SUM(t.users) users,
    SUM(t.gmv) gmv,
    SUM(t.payment_money) payment_money,
    @add_user
  FROM
    (SELECT
      t.shelf_id,
      t.exploit_type,
      t.revoke_status,
      t.users,
      t.gmv,
      0 payment_money
    FROM
      fe_dm.oi_tmp t
    UNION
    ALL
    SELECT
      t.shelf_id,
      t.exploit_type,
      t.revoke_status,
      0 users,
      0 gmv,
      t.payment_money
    FROM
      fe_dm.ap_tmp t
    union
    all
    select
      s.shelf_id,
      s.exploit_type,
      s.revoke_status,
      0 users,
      0 gmv,
      0 payment_money
    from
      fe_dwd.dwd_shelf_base_day_all s
    where s.data_flag = 1
      AND s.shelf_type IN (1, 3)
      AND s.shelf_status IN (2, 3)
      AND s.activate_time >= SUBDATE(@sdate, 14-1)
      AND s.activate_time < SUBDATE(@sdate, 14-2)) t
  GROUP BY t.shelf_id;
  
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_newshelf_quality',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_newshelf_quality','dm_op_newshelf_quality','李世龙');
COMMIT;
    END