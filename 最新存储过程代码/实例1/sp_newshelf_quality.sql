CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_newshelf_quality`()
BEGIN
  SET @sdate := SUBDATE(CURRENT_DATE, 1),
  @add_user := current_user,
  @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_tmp,
  feods.ap_tmp;
  CREATE TEMPORARY TABLE feods.oi_tmp AS
  SELECT
    o.shelf_id,
    s.exploit_type,
    s.revoke_status,
    COUNT(DISTINCT o.user_id) users,
    SUM(oi.quantity * oi.sale_price) gmv
  FROM
    fe.sf_order_item oi
    JOIN fe.sf_order o
      ON oi.order_id = o.order_id
      AND o.order_status = 2
      AND o.order_date >= SUBDATE(@sdate, 14-1)
      AND o.order_date < ADDDATE(@sdate, 1)
    JOIN fe.sf_shelf s
      ON o.shelf_id = s.shelf_id
      AND s.data_flag = 1
      AND s.shelf_type IN (1, 3)
      AND s.shelf_status IN (2, 3)
      AND s.activate_time >= SUBDATE(@sdate, 14-1)
      AND s.activate_time < SUBDATE(@sdate, 14-2)
  GROUP BY o.shelf_id;
  CREATE INDEX idx_oi_tmp_shelf_id
  ON feods.oi_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.ap_tmp AS
  SELECT
    t.shelf_id,
    s.exploit_type,
    s.revoke_status,
    SUM(t.payment_money) payment_money
  FROM
    fe.sf_after_payment t
    JOIN fe.sf_shelf s
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
  ON feods.ap_tmp (shelf_id);
  delete
  from
    feods.fjr_newshelf_quality
  where sdate = @sdate;
  insert into feods.fjr_newshelf_quality (
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
      feods.oi_tmp t
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
      feods.ap_tmp t
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
      fe.sf_shelf s
    where s.data_flag = 1
      AND s.shelf_type IN (1, 3)
      AND s.shelf_status IN (2, 3)
      AND s.activate_time >= SUBDATE(@sdate, 14-1)
      AND s.activate_time < SUBDATE(@sdate, 14-2)) t
  GROUP BY t.shelf_id;
  CALL feods.sp_task_log (
    'sp_newshelf_quality',
    @sdate,
    CONCAT(
      'fjr_d_ee72adb9975e9306fe77aa3e3dc03db5',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END