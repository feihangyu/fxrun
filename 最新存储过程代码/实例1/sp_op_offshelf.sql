CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_offshelf`()
BEGIN
  #run after sh_process.sp_shelf_dgmv
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @sdate7 := SUBDATE(@sdate, 7);
  DROP TEMPORARY TABLE IF EXISTS feods.material_tmp;
  CREATE TEMPORARY TABLE feods.material_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUBSTRING_INDEX(
      GROUP_CONCAT(
        m.material_name
        ORDER BY t.add_time DESC
      ), ',', 1
    ) material_name
  FROM
    fe.sf_material_shelf_relation t
    LEFT JOIN fe.sf_material_detail md
      ON t.material_detail_id = md.material_detail_id
      AND md.data_flag = 1
    LEFT JOIN fe.sf_material m
      ON md.material_id = m.material_id
      AND m.data_flag = 1
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  
  
   DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp_1;
  CREATE TEMPORARY TABLE feods.shelf_tmp_1 (PRIMARY KEY (secondary_shelf_id)) 
  SELECT DISTINCT main_shelf_id,secondary_shelf_id
  FROM fe.sf_shelf_relation_record r
        WHERE r.data_flag = 1
      AND r.shelf_handle_status = 9;
  
  
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, b.business_name, m.material_name, r.main_shelf_id
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN feods.material_tmp m
      ON t.shelf_id = m.shelf_id
    LEFT JOIN feods.shelf_tmp_1 r
      ON t.shelf_id = r.secondary_shelf_id
  WHERE t.data_flag = 1
    AND t.activate_time <= @sdate7
    AND t.shelf_status IN (2, 5)
    AND t.shelf_code != ''
    AND ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.gmv_tmp;
  CREATE TEMPORARY TABLE feods.gmv_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, MAX(t.sdate) gmv_date
  FROM
    feods.fjr_shelf_dgmv t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  where ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id
  HAVING gmv_date < @sdate7;
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, DATE(MAX(t.fill_time)) fill_date
  FROM
    fe.sf_product_fill_order t
    JOIN feods.gmv_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.check_tmp;
  CREATE TEMPORARY TABLE feods.check_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, DATE(MAX(t.operate_time)) check_date
  FROM
    fe.sf_shelf_check t
    JOIN feods.gmv_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.res_tmp;
  CREATE TEMPORARY TABLE feods.res_tmp (PRIMARY KEY (shelf_id))
  SELECT
    s.shelf_id, s.business_name, s.material_name, s.main_shelf_id, DATEDIFF(@sdate, t.gmv_date) days_ngmv, DATEDIFF(@sdate, f.fill_date) days_nfill, DATEDIFF(@sdate, c.check_date) days_ncheck
  FROM
    feods.gmv_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.fill_tmp f
      ON t.shelf_id = f.shelf_id
    LEFT JOIN feods.check_tmp c
      ON t.shelf_id = c.shelf_id
  where ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.nsecond_tmp;
  CREATE TEMPORARY TABLE feods.nsecond_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, t.days_ngmv, t.days_nfill, t.days_ncheck
  FROM
    feods.res_tmp t
  WHERE ISNULL(t.main_shelf_id)
    AND ! ISNULL(t.shelf_id);
  DELETE
    t
  FROM
    feods.res_tmp t
    LEFT JOIN feods.nsecond_tmp ns
      ON t.main_shelf_id = ns.shelf_id
  WHERE t.main_shelf_id
    AND ISNULL(ns.shelf_id);
  UPDATE
    feods.res_tmp t
    JOIN feods.nsecond_tmp ns
      ON t.main_shelf_id = ns.shelf_id SET t.days_ngmv = ns.days_ngmv, t.days_nfill = ns.days_nfill, t.days_ncheck = ns.days_ncheck;
  DELETE
  FROM
    feods.d_op_offshelf
  WHERE sdate = @sdate;
  INSERT INTO feods.d_op_offshelf (
    sdate, business_name, shelf_id, main_shelf_id, material_name, days_ngmv, days_nfill, days_ncheck, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_id, t.main_shelf_id, t.material_name, t.days_ngmv, t.days_nfill, t.days_ncheck, @add_user add_user
  FROM
    feods.res_tmp t;
  CALL feods.sp_task_log (
    'sp_op_offshelf', @sdate, CONCAT(
      'fjr_d_7333069f8396e205cee57dca0c228bd1', @timestamp, @add_user
    )
  );
  COMMIT;
END