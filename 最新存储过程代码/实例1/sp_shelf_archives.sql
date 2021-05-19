CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_archives`()
begin
  #run after sh_process.sh_shelf_level_ab
   SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @ym := YEAR(@sdate) * 100+ MONTH(@sdate), @sdate7 := SUBDATE(@sdate, 7);
  DROP TEMPORARY TABLE IF EXISTS feods.disa_tmp;
  CREATE TEMPORARY TABLE feods.disa_tmp AS
  SELECT
    t.staff_age_stages, GROUP_CONCAT(di.item_name) item_name
  FROM
    (SELECT DISTINCT
      t.staff_age_stages
    FROM
      fe.sf_shelf_apply_record t
    WHERE t.data_flag = 1) t
    JOIN feods.fjr_number n
      ON n.number <= LENGTH(t.staff_age_stages) - LENGTH(
        REPLACE(t.staff_age_stages, ',', '')
      )
    JOIN fe.pub_dictionary_item di
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(
          t.staff_age_stages, ',', n.number + 1
        ), ',', - 1
      ) = di.item_value
      AND di.dictionary_id = 123
  GROUP BY t.staff_age_stages;
  DROP TEMPORARY TABLE IF EXISTS feods.diew_tmp;
  CREATE TEMPORARY TABLE feods.diew_tmp AS
  SELECT
    t.employee_welfare, GROUP_CONCAT(di.item_name) item_name
  FROM
    (SELECT DISTINCT
      t.employee_welfare
    FROM
      fe.sf_shelf_apply_record t
    WHERE t.data_flag = 1) t
    JOIN feods.fjr_number n
      ON n.number <= LENGTH(t.employee_welfare) - LENGTH(
        REPLACE(t.employee_welfare, ',', '')
      )
    JOIN fe.pub_dictionary_item di
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(
          t.employee_welfare, ',', n.number + 1
        ), ',', - 1
      ) = di.item_value
      AND di.dictionary_id = 129
  GROUP BY t.employee_welfare;
  DROP TEMPORARY TABLE IF EXISTS feods.apply_tmp;
  CREATE TEMPORARY TABLE feods.apply_tmp AS
  SELECT
    a.shelf_id, a.floor_staff_num, dila.item_name launch_address, ar.business_characteristics, ar.company_property, ar.belong_industry, ar.female_ratio, ar.work_overtime_freq, ar.overtime_meal, ar.contact_name, ar.contact_phone, ar.competing_status, ar.staff_age_stages, ar.employee_welfare, ai.have_camera, ai.company_employees_num, ai.is_package_damage
  FROM
    fe.sf_shelf_apply a
    LEFT JOIN fe.pub_dictionary_item dila
      ON a.launch_address = dila.item_value
      AND dila.dictionary_id = 59
    LEFT JOIN
      (SELECT
        ar.record_id, dibc.item_name business_characteristics, dicp.item_name company_property, dibi.item_name belong_industry, difr.item_name female_ratio, diwo.item_name work_overtime_freq, IF(ar.overtime_meal = 1, '是', '否') overtime_meal, IF(
          LENGTH(ar.contact_name) = CHAR_LENGTH(ar.contact_name), NULL, ar.contact_name
        ) contact_name, IF(
          LENGTH(ar.contact_phone) = 11, sh_process.fjr_decode_phone (ar.contact_phone), NULL
        ) contact_phone, dics.item_name competing_status, disa.item_name staff_age_stages, diew.item_name employee_welfare
      FROM
        fe.sf_shelf_apply_record ar
        LEFT JOIN fe.pub_dictionary_item dibc
          ON ar.business_characteristics = dibc.item_value
          AND dibc.dictionary_id = 120
        LEFT JOIN fe.pub_dictionary_item dicp
          ON ar.company_property = dicp.item_value
          AND dicp.dictionary_id = 121
        LEFT JOIN fe.pub_dictionary_item dibi
          ON ar.belong_industry = dibi.item_value
          AND dibi.dictionary_id = 6
        LEFT JOIN fe.pub_dictionary_item difr
          ON ar.female_ratio = difr.item_value
          AND difr.dictionary_id = 124
        LEFT JOIN fe.pub_dictionary_item diwo
          ON ar.work_overtime_freq = diwo.item_value
          AND diwo.dictionary_id = 125
        LEFT JOIN fe.pub_dictionary_item dics
          ON ar.competing_status = dics.item_value
          AND dics.dictionary_id = 76
        LEFT JOIN feods.disa_tmp disa
          ON ar.staff_age_stages = disa.staff_age_stages
        LEFT JOIN feods.diew_tmp diew
          ON ar.employee_welfare = diew.employee_welfare
      WHERE ar.data_flag = 1) ar
      ON a.record_id = ar.record_id
    LEFT JOIN
      (SELECT
        ai.record_id, dihc.item_name have_camera, dice.item_name company_employees_num, IF(
          ai.is_package_damage = 1, '是', '否'
        ) is_package_damage
      FROM
        fe.sf_shelf_apply_addition_info ai
        LEFT JOIN fe.pub_dictionary_item dihc
          ON ai.have_camera = dihc.item_value
          AND dihc.dictionary_id = 193
        LEFT JOIN fe.pub_dictionary_item dice
          ON ai.company_employees_num = dice.item_value
          AND dice.dictionary_id = 194
      WHERE ai.data_flag = 1) ai
      ON a.record_id = ai.record_id
  WHERE a.data_flag = 1
    AND a.shelf_id > 0;
  CREATE INDEX idx_shelf_id
  ON feods.apply_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.staff_num_check_tmp;
  CREATE TEMPORARY TABLE feods.staff_num_check_tmp AS
  SELECT
    tm.shelf_id, isa.content staff_num_check
  FROM
    fe.sf_shelf_inspection_survey_answer isa
    JOIN
      (SELECT
        it.shelf_id, MAX(isa.id) max_id
      FROM
        fe.sf_shelf_inspection_survey_answer isa, fe.sf_shelf_inspection_task it
      WHERE isa.task_id = it.id
        AND isa.question_id IN (6, 24, 44, 62)
      GROUP BY it.shelf_id) tm
      ON isa.id = tm.max_id;
  CREATE INDEX idx_shelf_id
  ON feods.staff_num_check_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.dipn_tmp;
  CREATE TEMPORARY TABLE feods.dipn_tmp AS
  SELECT
    t.photo_nopass_reason, GROUP_CONCAT(di.item_name) item_name
  FROM
    (SELECT DISTINCT
      t.photo_nopass_reason
    FROM
      fe.sf_shelf_check t
    WHERE t.data_flag = 1
      AND t.photo_nopass_reason REGEXP '[6-8]') t
    JOIN feods.fjr_number n
      ON n.number < LENGTH(t.photo_nopass_reason) - LENGTH(
        REPLACE(t.photo_nopass_reason, ',', '')
      )
    JOIN fe.pub_dictionary_item di
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(
          t.photo_nopass_reason, ',', n.number + 1
        ), ',', - 1
      ) = di.item_value
      AND di.dictionary_id = 161
      AND di.item_value > 5
  GROUP BY t.photo_nopass_reason;
  DROP TEMPORARY TABLE IF EXISTS feods.photo_nopass_reason_tmp;
  CREATE TEMPORARY TABLE feods.photo_nopass_reason_tmp AS
  SELECT
    t.shelf_id, dipn.item_name photo_nopass_reason
  FROM
    (SELECT
      t.shelf_id, t.photo_nopass_reason
    FROM
      fe.sf_shelf_check t
      JOIN
        (SELECT
          t.shelf_id, MAX(t.check_id) check_id
        FROM
          fe.sf_shelf_check t
        WHERE t.data_flag = 1
        GROUP BY t.shelf_id) m
        ON t.check_id = m.check_id
    WHERE t.photo_nopass_reason REGEXP '[6-8]') t
    LEFT JOIN feods.dipn_tmp dipn
      ON t.photo_nopass_reason = dipn.photo_nopass_reason;
  CREATE INDEX idx_shelf_id
  ON feods.photo_nopass_reason_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.fills_tmp;
  CREATE TEMPORARY TABLE feods.fills_tmp AS
  SELECT
    t.shelf_id, COUNT(*) fills
  FROM
    fe.sf_product_fill_order t
  WHERE t.data_flag = 1
    AND t.order_status IN (3, 4)
    AND t.fill_time >= @sdate7
  GROUP BY t.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.fills_tmp (shelf_id);
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  CREATE TEMPORARY TABLE feods.sto_tmp AS
  SELECT
    t.shelf_id, COUNT(*) skus, SUM(
      IF(
        f.sales_flag = 5, t.sale_price * t.stock_quantity, 0
      )
    ) val_stock_flag5, SUM(
      IF(
        f.danger_flag > 3, t.sale_price * t.stock_quantity, 0
      )
    ) val_stock_danger, SUM(t.sale_price * t.stock_quantity) val_stock
  FROM
    fe.sf_shelf_product_detail t
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id
      AND f.data_flag = 1
  WHERE t.data_flag = 1
    AND t.stock_quantity > 0
  GROUP BY t.shelf_id;
  CREATE INDEX idx_shelf_id
  ON feods.sto_tmp (shelf_id);
  TRUNCATE TABLE feods.fjr_shelf_archives;
  INSERT INTO feods.fjr_shelf_archives (
    shelf_id, shelf_code, city_name, business_name, business_characteristics, company_property, belong_industry, address, staff_age_stages, female_ratio, work_overtime_freq, overtime_meal, employee_welfare, shelf_type, is_package_damage, launch_address, contact_name, contact_phone, shelf_manager_type, manager_level, staff_num_check, floor_staff_num, company_employees_num, competing_status, shelf_grade, photo_nopass_reason, have_camera, fills, skus, val_stock_flag5, val_stock_danger, val_stock, shelf_level_t, add_user
  )
  SELECT
    t.shelf_id, t.shelf_code, b.city_name, b.business_name, ap.business_characteristics, ap.company_property, ap.belong_industry, t.address, ap.staff_age_stages, ap.female_ratio, ap.work_overtime_freq, ap.overtime_meal, ap.employee_welfare, t.shelf_type, ap.is_package_damage, ap.launch_address, ap.contact_name, ap.contact_phone, IF(
      m.second_user_type = 1, '全职店主', '兼职店主'
    ) shelf_manager_type, mf.manager_level, c.staff_num_check, ap.floor_staff_num, ap.company_employees_num, ap.competing_status, g.grade shelf_grade, ck.photo_nopass_reason, ap.have_camera, fil.fills, sto.skus, sto.val_stock_flag5, sto.val_stock_danger, sto.val_stock, ab.shelf_level_t, @add_user
  FROM
    fe.sf_shelf t
    LEFT JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN fe.pub_shelf_manager m
      ON t.manager_id = m.manager_id
      AND m.data_flag = 1
    LEFT JOIN feods.zs_shelf_manager_flag mf
      ON m.sf_code = mf.sf_code
    LEFT JOIN feods.apply_tmp ap
      ON t.shelf_id = ap.shelf_id
    LEFT JOIN feods.staff_num_check_tmp c
      ON t.shelf_id = c.shelf_id
    LEFT JOIN feods.zs_shelf_grade g
      ON t.shelf_id = g.shelf_id
    LEFT JOIN feods.photo_nopass_reason_tmp ck
      ON t.shelf_id = ck.shelf_id
    LEFT JOIN feods.fills_tmp fil
      ON t.shelf_id = fil.shelf_id
    LEFT JOIN feods.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
    LEFT JOIN feods.pj_shelf_level_ab ab
      ON t.shelf_id = ab.shelf_id
      AND ab.smonth = @ym
    LEFT JOIN fe.pub_dictionary_item dist
      ON t.shelf_type = dist.item_value
      AND dist.dictionary_id = 8
  WHERE t.data_flag = 1;
  call feods.sp_task_log (
    'sp_shelf_archives', @sdate, concat(
      'fjr_d_0d0235af1fdb196a21f86f3fa9b65164', @timestamp, @add_user
    )
  );
  commit;
end