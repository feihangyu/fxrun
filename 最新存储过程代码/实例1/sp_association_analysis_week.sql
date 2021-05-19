CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_association_analysis_week`()
BEGIN
  #run after sh_process.sp_order_and_item_lastxx_week
   SET @week_end := SUBDATE(
    CURRENT_DATE, WEEKDAY(CURRENT_DATE) + 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_area_tmp, feods.area_users_tmp, feods.area_product_association_tmp;
  CREATE TEMPORARY TABLE feods.shelf_area_tmp AS
  SELECT
    s.shelf_id, b.business_code, b.business_name business_area
  FROM
    fe.sf_shelf s
    JOIN feods.fjr_city_business b
      ON s.city = b.city
  WHERE s.data_flag = 1;
  CREATE INDEX idx_shelf_area_tmp_shelf_id
  ON feods.shelf_area_tmp (shelf_id);
  CREATE TEMPORARY TABLE feods.area_users_tmp AS
  SELECT
    a.business_code, COUNT(DISTINCT t.user_id) users
  FROM
    fe_dwd.`dwd_pub_order_item_recent_one_month` t
    JOIN feods.shelf_area_tmp a
      ON t.shelf_id = a.shelf_id
  GROUP BY a.business_code;
  DELETE
  FROM
    feods.area_product_user;
  INSERT INTO feods.area_product_user (
    business_code, product_id, user_id, add_user
  )
  SELECT DISTINCT
    a.business_code, t.product_id, t.user_id, @add_user
  FROM
    fe_dwd.`dwd_pub_order_item_recent_one_month` t
    JOIN feods.shelf_area_tmp a
      ON t.shelf_id = a.shelf_id;
  DELETE
  FROM
    feods.area_product_countuser;
  INSERT INTO feods.area_product_countuser (
    business_code, product_id, users, add_user
  )
  SELECT
    t.business_code, t.product_id, COUNT(*) users, @add_user
  FROM
    feods.area_product_user t
  GROUP BY t.business_code, t.product_id;
  CREATE TEMPORARY TABLE feods.area_product_association_tmp AS
  SELECT
    t1.business_code, t1.product_id product_id_a, t2.product_id product_id_b, COUNT(*) users_ab
  FROM
    feods.area_product_user t1
    JOIN feods.area_product_user t2
      ON t1.business_code = t2.business_code
      AND t1.user_id = t2.user_id
      AND t1.product_id > t2.product_id
  GROUP BY t1.business_code, t1.product_id, t2.product_id;
  DELETE
  FROM
    feods.fjr_association_analysis;
  INSERT INTO feods.fjr_association_analysis (
    week_end, business_code, business_area, product_id_a, product_id_b, users_all, users_a, users_b, users_ab, add_user
  )
  SELECT
    @week_end, t.business_code, b.business_area, t.product_id_a, t.product_id_b, u.users users_all, cu1.users users_a, cu2.users users_b, t.users_ab, @add_user
  FROM
    feods.area_product_association_tmp t
    JOIN feods.area_users_tmp u
      ON t.business_code = u.business_code
    JOIN feods.area_product_countuser cu1
      ON t.business_code = cu1.business_code
      AND t.product_id_a = cu1.product_id
    JOIN feods.area_product_countuser cu2
      ON t.business_code = cu2.business_code
      AND t.product_id_b = cu2.product_id
    JOIN
      (SELECT DISTINCT
        b.business_code, b.business_name business_area
      FROM
        feods.fjr_city_business b) b
      ON t.business_code = b.business_code;
  CALL feods.sp_task_log (
    'sp_association_analysis_week', @week_end, CONCAT(
      'fjr_w_bd314e4ee151b4a73e20d49a0f2c9589', @timestamp, @add_user
    )
  );
  COMMIT;
END