CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_info`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP ;
  SET @sub_day := SUBDATE(@sdate, 1) ;
  SET @d := DAY(@sdate) ;
  SET @month_start := SUBDATE(@sdate, @d - 1) ;
  SET @month_end := LAST_DAY(@sdate) ;
  DELETE 
  FROM
    feods.d_op_fill3_detail 
  WHERE fill_time >= @sub_day 
    AND fill_time < @sdate ;
  INSERT INTO feods.d_op_fill3_detail (
    order_id, product_id, shelf_id, fill_time, apply_time, purchase_price, actual_apply_num, actual_send_num, actual_sign_num, actual_fill_num, add_user
  ) 
  SELECT 
    t.order_id, fi.product_id, t.shelf_id, t.fill_time, t.apply_time, MAX(fi.purchase_price) purchase_price, SUM(fi.actual_apply_num) actual_apply_num, SUM(fi.actual_send_num) actual_send_num, SUM(fi.actual_sign_num) actual_sign_num, SUM(fi.actual_fill_num) actual_fill_num, @add_user add_user 
  FROM
    fe.sf_product_fill_order t 
    JOIN fe.sf_product_fill_order_item fi 
      ON t.order_id = fi.order_id 
      AND fi.data_flag = 1 
      AND fi.actual_sign_num > 0 
      AND ! ISNULL(fi.product_id) 
  WHERE t.data_flag = 1 
    AND t.fill_type = 3 
    AND t.order_status IN (3, 4) 
    AND ! ISNULL(t.shelf_id) 
    AND t.fill_time >= @sub_day 
    AND t.fill_time < @sdate 
    AND ! ISNULL(t.apply_time) 
  GROUP BY t.order_id, fi.product_id ;
  TRUNCATE feods.d_op_shelf_firstfill ;
  INSERT INTO feods.d_op_shelf_firstfill (
    shelf_id, firstfill_all, firstfill, order_id, sku, actual_apply_num, actual_send_num, actual_sign_num, actual_fill_num, actual_apply_val, actual_send_val, actual_sign_val, actual_fill_val, add_user
  ) 
  SELECT 
    t.shelf_id, t.fill_time firstfill_all, f.firstfill, f.order_id, f.sku, f.actual_apply_num, f.actual_send_num, f.actual_sign_num, f.actual_fill_num, f.actual_apply_val, f.actual_send_val, f.actual_sign_val, f.actual_fill_val, @add_user add_user 
  FROM
    (SELECT 
      shelf_id, DATE(MIN(fill_time)) fill_time 
    FROM
      fe.sf_product_fill_order 
    WHERE data_flag = 1 
      AND order_status IN (3, 4) 
      AND ! ISNULL(shelf_id) 
      AND ! ISNULL(fill_time) 
    GROUP BY shelf_id) t 
    LEFT JOIN 
      (SELECT 
        shelf_id, DATE(MIN(fill_time)) firstfill, MIN(order_id) order_id, COUNT(DISTINCT product_id) sku, SUM(actual_apply_num) actual_apply_num, SUM(actual_send_num) actual_send_num, SUM(actual_sign_num) actual_sign_num, SUM(actual_fill_num) actual_fill_num, SUM(
          actual_apply_num * purchase_price
        ) actual_apply_val, SUM(actual_send_num * purchase_price) actual_send_val, SUM(actual_sign_num * purchase_price) actual_sign_val, SUM(actual_fill_num * purchase_price) actual_fill_val 
      FROM
        feods.d_op_fill3_detail 
      GROUP BY shelf_id) f 
      ON t.shelf_id = f.shelf_id ;
  TRUNCATE feods.d_op_shelf_info ;
  INSERT INTO feods.d_op_shelf_info (
    region_name, business_name, city_name, address, shelf_id, shelf_code, shelf_name, shelf_type, shelf_status, revoke_status, whether_close, activate_time, revoke_time, shlef_add_time, mobile_phone, sf_code, real_name, shelfs, shelfs6, shelfs7, branch_name, branch_code, fulltime_falg, sf_code_bd, real_name_bd, bdtype, company_name, prewh_falg, warehouse_id, warehouse_name, rel_flag, main_shelf_id, loss_pro_flag, last_revoke_time, lastrevoke_status, inner_flag, machine_type, product_template_id, template_name, online_status, firstfill, add_user
  ) 
  SELECT 
    b.region_name, b.business_name, b.city_name, t.address, t.shelf_id, t.shelf_code, t.shelf_name, di8.item_name shelf_type, di9.item_name shelf_status, di50.item_name revoke_status, IF(t.whether_close = 1, '是', '否') whether_close, t.activate_time, t.revoke_time, t.add_time, CONCAT(
      SUBSTRING(m.mobile_phone, 1, 3), SUBSTRING(m.mobile_phone, 8, 1), 
      CASE
        SUBSTRING(m.mobile_phone, 5, 1) 
        WHEN '0' 
        THEN '9' 
        WHEN '1' 
        THEN '5' 
        WHEN '2' 
        THEN '4' 
        WHEN '3' 
        THEN '0' 
        WHEN '4' 
        THEN '3' 
        WHEN '5' 
        THEN '8' 
        WHEN '6' 
        THEN '1' 
        WHEN '7' 
        THEN '7' 
        WHEN '8' 
        THEN '2' 
        ELSE '6' 
      END, SUBSTRING(m.mobile_phone, 10, 1), SUBSTRING(m.mobile_phone, 7, 1), SUBSTRING(m.mobile_phone, 4, 1), 
      CASE
        SUBSTRING(m.mobile_phone, 9, 1) 
        WHEN '0' 
        THEN '9' 
        WHEN '1' 
        THEN '5' 
        WHEN '2' 
        THEN '4' 
        WHEN '3' 
        THEN '0' 
        WHEN '4' 
        THEN '3' 
        WHEN '5' 
        THEN '8' 
        WHEN '6' 
        THEN '1' 
        WHEN '7' 
        THEN '7' 
        WHEN '8' 
        THEN '2' 
        ELSE '6' 
      END, SUBSTRING(m.mobile_phone, 6, 1), SUBSTRING(m.mobile_phone, 11, 1)
    ) mobile_phone, m.sf_code, m.real_name, ms.shelfs, ms.shelfs6, ms.shelfs7, m.branch_name, m.branch_code, IF(
      m.second_user_type = 1, '是', '否'
    ) fulltime_falg, mbd.sf_code sf_code_bd, mbd.real_name real_name_bd, di17.item_name bdtype, c.company_name, IF(! ISNULL(ps.shelf_id), '是', '否') prewh_falg, ws.shelf_id warehouse_id, ws.shelf_name warehouse_name, IF(! ISNULL(sr.shelf_id), '是', '否') rel_flag, sr.main_shelf_id, IF(
      ! ISNULL(pa.final_company_id), '是', '否'
    ) loss_pro_flag, rev.last_revoke_time, di61.item_name lastrevoke_status, IF(
      t.shelf_name LIKE '%顺丰%' || ! ISNULL(ap.shelf_id) || c.company_name LIKE '%顺丰%' || c.company_name LIKE '%速运%' || c.company_name LIKE '%重货%', '是', '否'
    ) inner_flag, smt.type_name machine_type, mt.product_template_id, mt.template_name, sm.online_status, ff.firstfill, @add_user add_user 
  FROM
    fe.sf_shelf t 
    JOIN feods.fjr_city_business b 
      ON t.city = b.city 
    LEFT JOIN fe.pub_shelf_manager m 
      ON t.manager_id = m.manager_id 
      AND m.data_flag = 1 
    LEFT JOIN 
      (SELECT 
        t.manager_id, COUNT(*) shelfs, SUM(t.shelf_type = 6) shelfs6, SUM(t.shelf_type = 7) shelfs7 
      FROM
        fe.sf_shelf t 
      WHERE t.data_flag = 1 
        AND ! ISNULL(t.manager_id) 
      GROUP BY t.manager_id) ms 
      ON m.manager_id = ms.manager_id 
    LEFT JOIN fe.pub_shelf_manager mbd 
      ON t.bd_id = mbd.manager_id 
      AND mbd.data_flag = 1 
    LEFT JOIN fe.sf_company c 
      ON t.company_id = c.company_id 
      AND c.data_flag = 1 
    LEFT JOIN 
      (SELECT 
        t.shelf_id, MAX(warehouse_id) warehouse_id 
      FROM
        fe.sf_prewarehouse_shelf_detail t 
      WHERE t.data_flag = 1 
      GROUP BY t.shelf_id) ps 
      ON t.shelf_id = ps.shelf_id 
    LEFT JOIN fe.sf_shelf ws 
      ON ps.warehouse_id = ws.shelf_id 
      AND ws.data_flag = 1 
    LEFT JOIN 
      (SELECT 
        t.secondary_shelf_id shelf_id, t.main_shelf_id 
      FROM
        fe.sf_shelf_relation_record t 
      WHERE t.data_flag = 1 
        AND t.shelf_handle_status = 9 
      UNION
      #ALL 
      SELECT DISTINCT 
        t.main_shelf_id shelf_id, t.main_shelf_id 
      FROM
        fe.sf_shelf_relation_record t 
      WHERE t.data_flag = 1 
        AND t.shelf_handle_status = 9) sr 
      ON t.shelf_id = sr.shelf_id 
    LEFT JOIN 
      (SELECT DISTINCT 
        t.final_company_id 
      FROM
        fe.sf_company_protocol_apply t 
      WHERE t.data_flag = 1 
        AND t.apply_status = 2) pa 
      ON t.company_id = pa.final_company_id 
    LEFT JOIN 
      (SELECT 
        t.shelf_id, MAX(t.add_time) last_revoke_time, CAST(
          SUBSTRING_INDEX(
            GROUP_CONCAT(
              t.audit_status 
              ORDER BY t.add_time DESC
            ), ',', 1
          ) AS UNSIGNED
        ) audit_status 
      FROM
        fe.sf_shelf_revoke t 
      WHERE t.data_flag = 1 
      GROUP BY t.shelf_id) rev 
      ON t.shelf_id = rev.shelf_id 
    LEFT JOIN 
      (SELECT 
        a.shelf_id 
      FROM
        fe.sf_shelf_apply_addition_info t 
        JOIN fe.sf_shelf_apply a 
          ON t.record_id = a.record_id 
          AND a.data_flag = 1 
          AND a.shelf_id > 0 
      WHERE t.data_flag = 1 
        AND t.is_inner_shelf = 1) ap 
      ON t.shelf_id = ap.shelf_id 
    LEFT JOIN fe.sf_shelf_machine sm 
      ON t.shelf_id = sm.shelf_id 
      AND sm.data_flag = 1 
    LEFT JOIN fe.sf_shelf_machine_type smt 
      ON sm.machine_type_id = smt.machine_type_id 
      AND smt.data_flag = 1 
    LEFT JOIN fe.sf_shelf_machine_product_template mt 
      ON sm.product_template_id = mt.product_template_id 
      AND mt.data_flag = 1 
    LEFT JOIN feods.d_op_shelf_firstfill ff 
      ON t.shelf_id = ff.shelf_id 
    LEFT JOIN fe.pub_dictionary_item di17 
      ON mbd.user_type = di17.item_value 
      AND di17.dictionary_id = 17 
    LEFT JOIN fe.pub_dictionary_item di8 
      ON t.shelf_type = di8.item_value 
      AND di8.dictionary_id = 8 
    LEFT JOIN fe.pub_dictionary_item di9 
      ON t.shelf_status = di9.item_value 
      AND di9.dictionary_id = 9 
    LEFT JOIN fe.pub_dictionary_item di50 
      ON t.revoke_status = di50.item_value 
      AND di50.dictionary_id = 50 
    LEFT JOIN fe.pub_dictionary_item di61 
      ON rev.audit_status = di61.item_value 
      AND di61.dictionary_id = 61 
  WHERE t.data_flag = 1 ;
  delete 
  from
    feods.d_op_shelf_info_month 
  where sdate = @sdate 
    or (
      sdate > @month_start 
      and sdate < @month_end
    ) ;
  insert into feods.d_op_shelf_info_month (
    sdate, region_name, business_name, city_name, address, shelf_id, shelf_code, shelf_name, shelf_type, shelf_status, revoke_status, whether_close, activate_time, revoke_time, shlef_add_time, mobile_phone, sf_code, real_name, shelfs, shelfs6, shelfs7, branch_name, branch_code, fulltime_falg, sf_code_bd, real_name_bd, bdtype, company_name, prewh_falg, warehouse_id, warehouse_name, rel_flag, main_shelf_id, loss_pro_flag, last_revoke_time, lastrevoke_status, inner_flag, machine_type, product_template_id, template_name, online_status, firstfill, add_user
  ) 
  select 
    @sdate sdate, region_name, business_name, city_name, address, shelf_id, shelf_code, shelf_name, shelf_type, shelf_status, revoke_status, whether_close, activate_time, revoke_time, shlef_add_time, mobile_phone, sf_code, real_name, shelfs, shelfs6, shelfs7, branch_name, branch_code, fulltime_falg, sf_code_bd, real_name_bd, bdtype, company_name, prewh_falg, warehouse_id, warehouse_name, rel_flag, main_shelf_id, loss_pro_flag, last_revoke_time, lastrevoke_status, inner_flag, machine_type, product_template_id, template_name, online_status, firstfill, @add_user add_user 
  from
    feods.d_op_shelf_info ;
  CALL feods.sp_task_log (
    'sp_op_shelf_info', @sdate, CONCAT(
      'fjr_d_1a604ae2140416cc7672e11074ff738b', @timestamp, @add_user
    )
  ) ;
  COMMIT ;
END