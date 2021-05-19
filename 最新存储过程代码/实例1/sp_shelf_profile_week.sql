CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_profile_week`(in_week_end date)
begin
  SET @week_end := SUBDATE(
    in_week_end, DAYOFWEEK(in_week_end) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @week_start := SUBDATE(@week_end, 6), @add_day := ADDDATE(@week_end, 1), @week_start_add := SUBDATE(@week_end, 5), @first7_start := SUBDATE(@week_end, 12), @second7_start := SUBDATE(@week_end, 19);
  DROP TEMPORARY TABLE IF EXISTS feods.order_tmp;
  CREATE TEMPORARY TABLE feods.order_tmp AS
  SELECT
    o.order_date, o.shelf_id, o.user_id, IFNULL(o.product_total_amount, 0) + IFNULL(o.discount_amount, 0) + IFNULL(o.coupon_amount, 0) gmv
  FROM
    fe.sf_order o
  WHERE o.order_status IN (2, 6, 7)
    AND o.order_date >= @first7_start
    AND o.order_date < @add_day;
  CREATE INDEX idx_shelf_id_order_date
  ON feods.order_tmp (shelf_id, order_date);
  DROP TEMPORARY TABLE IF EXISTS feods.order_week_tmp;
  CREATE TEMPORARY TABLE feods.order_week_tmp AS
  SELECT
    o.order_date, o.shelf_id, o.user_id, o.gmv
  FROM
    feods.order_tmp o
  WHERE o.order_date >= @week_start;
  CREATE INDEX idx_shelf_id
  ON feods.order_week_tmp (shelf_id);
  CREATE INDEX idx_shelf_id_user_id
  ON feods.order_week_tmp (shelf_id, user_id);
  DROP TEMPORARY TABLE IF EXISTS feods.order_stat_tmp;
  CREATE TEMPORARY TABLE feods.order_stat_tmp AS
  SELECT
    o.shelf_id, COUNT(DISTINCT o.user_id) users, SUM(o.gmv) gmv
  FROM
    feods.order_week_tmp o
  GROUP BY o.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.order_stat_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.afpay_tmp;
  CREATE TEMPORARY TABLE feods.afpay_tmp AS
  SELECT
    t.shelf_id, SUM(t.payment_money) after_pay_val
  FROM
    fe.sf_after_payment t
  WHERE t.payment_status = 2
    AND t.payment_date >= @week_start
    AND t.payment_date < @add_day
  GROUP BY t.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.afpay_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.check_tmp;
  CREATE TEMPORARY TABLE feods.check_tmp AS
  SELECT
    t.shelf_id, SUM(cd.error_num * cd.sale_price) check_diff_val
  FROM
    fe.sf_shelf_check t
    JOIN fe.sf_shelf_check_detail cd
      ON t.check_id = cd.check_id
      AND cd.data_flag = 1
      AND cd.error_num != 0
  WHERE t.data_flag = 1
    AND t.operate_time >= @week_start
    AND t.operate_time < @add_day
  GROUP BY t.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.check_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.base_tmp;
  CREATE TEMPORARY TABLE feods.base_tmp AS
  SELECT
    s.shelf_id, oi.gmv, af.after_pay_val, oi.users, ch.check_diff_val
  FROM
    fe.sf_shelf s
    LEFT JOIN feods.order_stat_tmp oi
      ON s.shelf_id = oi.shelf_id
    LEFT JOIN feods.afpay_tmp af
      ON s.shelf_id = af.shelf_id
    LEFT JOIN feods.check_tmp ch
      ON s.shelf_id = ch.shelf_id
  WHERE s.data_flag = 1
    AND (
      oi.shelf_id IS NOT NULL
      OR af.shelf_id IS NOT NULL
      OR ch.shelf_id IS NOT NULL
    );
  CREATE INDEX idx_shelf_id
  ON feods.base_tmp (shelf_id);
  DELETE
  FROM
    feods.fjr_shelf_sal_base
  WHERE week_end >= @week_end;
  INSERT INTO feods.fjr_shelf_sal_base (
    week_end, shelf_id, gmv, after_pay_val, users, check_diff_val, add_user
  )
  SELECT
    @week_end week_end, t.shelf_id, IFNULL(t.gmv, 0) gmv, IFNULL(t.after_pay_val, 0) after_pay_val, IFNULL(t.users, 0) users, IFNULL(t.check_diff_val, 0) check_diff_val, @add_user add_user
  FROM
    feods.base_tmp t;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_user_tmp;
  CREATE TEMPORARY TABLE feods.oi_user_tmp AS
  SELECT
    o.shelf_id, o.user_id, MIN(o.order_date) first_pur_time, MAX(o.order_date) last_pur_time
  FROM
    feods.order_week_tmp o
  WHERE o.order_date >= @week_start
  GROUP BY o.shelf_id, o.user_id;
  -- 这个表可以被 d_op_su_stat 替换。故停止
 --  DROP TEMPORARY TABLE IF EXISTS feods.user_tmp;
--   CREATE TEMPORARY TABLE feods.user_tmp AS
--   SELECT
--     t.shelf_id, t.user_id, MIN(t.first_pur_time) first_pur_time, MAX(t.last_pur_time) last_pur_time
--   FROM
--     (SELECT
--       *
--     FROM
--       feods.oi_user_tmp
--     UNION
--     ALL
--     SELECT
--       t.shelf_id, t.user_id, t.first_pur_time, t.last_pur_time
--     FROM
--       feods.fjr_shelf_user_duration t) t
--   GROUP BY t.shelf_id, t.user_id;
--   TRUNCATE TABLE feods.fjr_shelf_user_duration;
--   INSERT INTO feods.fjr_shelf_user_duration (
--     shelf_id, user_id, first_pur_time, last_pur_time, add_user
--   )
--   SELECT
--     t.shelf_id, t.user_id, t.first_pur_time, t.last_pur_time, @add_user add_user
--   FROM
--     feods.user_tmp t;
    

  DROP TEMPORARY TABLE IF EXISTS feods.shelf_users_tmp;
  CREATE TEMPORARY TABLE feods.shelf_users_tmp AS
  SELECT
    t.shelf_id, COUNT(*) cum_users, SUM(
      t.min_order_date = t.max_order_date
    ) once_users, SUM(
      DATEDIFF(
        t.min_order_date, t.max_order_date
      ) >= 30
    ) long_users
  FROM
    feods.d_op_su_stat t
  GROUP BY t.shelf_id;
  
  
  CREATE INDEX idx_shelf_id
  ON feods.shelf_users_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.shelf7_tmp;
  CREATE TEMPORARY TABLE feods.shelf7_tmp AS
  SELECT
    s.shelf_id, s.activate_time < @first7_start if_second7, IF(
      s.activate_time < @first7_start, DATE(ADDDATE(s.activate_time, 7)), s.activate_time
    ) start_time, IF(
      s.activate_time < @first7_start, DATE(ADDDATE(s.activate_time, 15)), DATE(ADDDATE(s.activate_time, 7))
    ) end_time
  FROM
    fe.sf_shelf s
  WHERE s.data_flag = 1
    AND s.activate_time >= @second7_start
    AND s.activate_time < @week_start_add;
  DROP TEMPORARY TABLE IF EXISTS feods.oi_first7_tmp;
  CREATE TEMPORARY TABLE feods.oi_first7_tmp AS
  SELECT
    o.shelf_id, SUM(o.gmv) first7_gmv
  FROM
    feods.order_tmp o
    JOIN feods.shelf7_tmp s
      ON o.shelf_id = s.shelf_id
      AND o.order_date >= s.start_time
      AND o.order_date < s.end_time
      AND s.if_second7 = 0
  GROUP BY o.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.oi_first7_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.oi_second7_tmp;
  CREATE TEMPORARY TABLE feods.oi_second7_tmp AS
  SELECT
    o.shelf_id, SUM(o.gmv) second7_gmv
  FROM
    feods.order_tmp o
    JOIN feods.shelf7_tmp s
      ON o.shelf_id = s.shelf_id
      AND o.order_date >= s.start_time
      AND o.order_date < s.end_time
      AND s.if_second7 = 1
  GROUP BY o.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.oi_second7_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp AS
  SELECT
    t.shelf_id, MAX(t.apply_time) last_fill_apply
  FROM
    fe.sf_product_fill_order t
  WHERE t.data_flag = 1
    AND t.apply_time < @add_day
    AND t.order_status != 9
  GROUP BY t.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.fill_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.profile_tmp;
  CREATE TEMPORARY TABLE feods.profile_tmp AS
  SELECT
    s.shelf_id, IFNULL(sp.cum_gmv, 0) + IFNULL(ba.gmv, 0) cum_gmv, IFNULL(sp.cum_afpay, 0) + IFNULL(ba.after_pay_val, 0) cum_afpay, IFNULL(sp.cum_check_diff, 0) + IFNULL(ba.check_diff_val, 0) cum_check_diff, IFNULL(
      fi.last_fill_apply, '0000-00-00 00:00:00'
    ) last_fill_apply, IFNULL(su.cum_users, 0) cum_users, IFNULL(su.once_users, 0) once_users, IFNULL(su.long_users, 0) long_users, IF(
      IFNULL(sp.max_week_gmv, 0) >= IFNULL(ba.gmv, 0), IFNULL(sp.max_week_gmv, 0), IFNULL(ba.gmv, 0)
    ) max_week_gmv, IF(
      f7.shelf_id IS NULL, IFNULL(sp.first7_gmv, 0), f7.first7_gmv
    ) first7_gmv, IF(
      s7.shelf_id IS NULL, IFNULL(sp.second7_gmv, 0), s7.second7_gmv
    ) second7_gmv,
    CASE
      WHEN IFNULL(ba.gmv, 0) = 0
      THEN IFNULL(sp.life_tract, '')
      WHEN ba.gmv < 10
      THEN CONCAT(IFNULL(sp.life_tract, ''), '0')
      WHEN ba.gmv < 20
      THEN CONCAT(IFNULL(sp.life_tract, ''), '1')
      WHEN ba.gmv < 40
      THEN CONCAT(IFNULL(sp.life_tract, ''), '2')
      WHEN ba.gmv < 70
      THEN CONCAT(IFNULL(sp.life_tract, ''), '3')
      WHEN ba.gmv < 110
      THEN CONCAT(IFNULL(sp.life_tract, ''), '4')
      WHEN ba.gmv < 160
      THEN CONCAT(IFNULL(sp.life_tract, ''), '5')
      WHEN ba.gmv < 220
      THEN CONCAT(IFNULL(sp.life_tract, ''), '6')
      WHEN ba.gmv < 290
      THEN CONCAT(IFNULL(sp.life_tract, ''), '7')
      WHEN ba.gmv < 370
      THEN CONCAT(IFNULL(sp.life_tract, ''), '8')
      WHEN ba.gmv < 460
      THEN CONCAT(IFNULL(sp.life_tract, ''), '9')
      WHEN ba.gmv < 560
      THEN CONCAT(IFNULL(sp.life_tract, ''), 'a')
      WHEN ba.gmv < 670
      THEN CONCAT(IFNULL(sp.life_tract, ''), 'b')
      WHEN ba.gmv < 790
      THEN CONCAT(IFNULL(sp.life_tract, ''), 'c')
      WHEN ba.gmv < 920
      THEN CONCAT(IFNULL(sp.life_tract, ''), 'd')
      WHEN ba.gmv < 1060
      THEN CONCAT(IFNULL(sp.life_tract, ''), 'e')
      ELSE CONCAT(IFNULL(sp.life_tract, ''), 'f')
    END life_tract
  FROM
    fe.sf_shelf s
    LEFT JOIN feods.fjr_shelf_profile sp
      ON s.shelf_id = sp.shelf_id
    LEFT JOIN feods.base_tmp ba
      ON s.shelf_id = ba.shelf_id
    LEFT JOIN feods.fill_tmp fi
      ON s.shelf_id = fi.shelf_id
    LEFT JOIN feods.shelf_users_tmp su
      ON s.shelf_id = su.shelf_id
    LEFT JOIN feods.oi_first7_tmp f7
      ON s.shelf_id = f7.shelf_id
    LEFT JOIN feods.oi_second7_tmp s7
      ON s.shelf_id = s7.shelf_id
  WHERE s.data_flag = 1
  HAVING life_tract != '';
  TRUNCATE TABLE feods.fjr_shelf_profile;
  INSERT INTO feods.fjr_shelf_profile (
    shelf_id, cum_gmv, cum_afpay, cum_check_diff, last_fill_apply, cum_users, once_users, long_users, max_week_gmv, first7_gmv, second7_gmv, life_tract, add_user
  )
  SELECT
    t.shelf_id, t.cum_gmv, t.cum_afpay, t.cum_check_diff, t.last_fill_apply, t.cum_users, t.once_users, t.long_users, t.max_week_gmv, t.first7_gmv, t.second7_gmv, t.life_tract, @add_user add_user
  FROM
    feods.profile_tmp t;
  update
    feods.fjr_shelf_profile t
  SET
    t.profile_tag = sh_process.fjr_shelf_tag (
      t.max_week_gmv, CONV(SUBSTR(t.life_tract, - 3, 1), 16, 10), CONV(SUBSTR(t.life_tract, - 2, 1), 16, 10), CONV(SUBSTR(t.life_tract, - 1, 1), 16, 10)
    );
  CALL feods.sp_task_log (
    'sp_shelf_profile_week', @week_end, CONCAT(
      'fjr_w_10d5a14c16a053a19c0a9659ecfa1552', @timestamp, @add_user
    )
  );
  #call sh_process.sp_unstock_detail_week (@week_end);
  commit;
end