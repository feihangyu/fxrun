CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi2_shelf_level_stat`()
BEGIN
  #run after sh_process.sh_shelf_level_ab
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @last_week_end := SUBDATE(@sdate, DAYOFWEEK(@sdate) - 1), @month_end := LAST_DAY(@sdate);
  SET @month_start := SUBDATE(@month_end, DAY(@month_end) - 1);
  SET @ym := DATE_FORMAT(@sdate, '%Y%m'), @ym_lm := DATE_FORMAT(SUBDATE(@month_start, 1), '%Y%m');
  DROP TEMPORARY TABLE IF EXISTS feods.ab_tmp;
  CREATE TEMPORARY TABLE feods.ab_tmp AS
  SELECT
    b.business_name, COUNT(*) shelfs32_lm, SUM(
      IF(
        t.shelf_level IN ('乙级', '乙级2'), 2, 3
      ) > IFNULL(s.shelf_level_num, 0)
    ) shelfs32_lm_loss
  FROM
    feods.pj_shelf_level_ab t
    LEFT JOIN
      (SELECT
        t.shelf_id,
        CASE
          WHEN t.shelf_level_t IN ('甲级', '甲级2')
          THEN 3
          WHEN t.shelf_level_t IN ('乙级', '乙级2')
          THEN 2
          WHEN t.shelf_level_t IN ('丙级', '丙级2')
          THEN 1
          ELSE 0
        END shelf_level_num
      FROM
        feods.pj_shelf_level_ab t
      WHERE t.smonth = @ym) s
      ON t.shelf_id = s.shelf_id
    JOIN feods.fjr_city_business b
      ON t.city_name = b.city_name
  WHERE t.smonth = @ym_lm
    AND t.shelf_level IN (
      '乙级', '乙级2', '甲级', '甲级2'
    )
  GROUP BY b.business_name;
  DELETE
  FROM
    feods.fjr_kpi2_shelf_level_stat
  WHERE sdate = @sdate;
  INSERT INTO feods.fjr_kpi2_shelf_level_stat (
    sdate, business_name, shelfs32, shelfs0, shelfs, shelfs32_lm, shelfs32_lm_loss, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelfs32, t.shelfs0, t.shelfs, IFNULL(ab.shelfs32_lm, 0) shelfs32_lm, IFNULL(ab.shelfs32_lm_loss, 0) shelfs32_lm_loss, @add_user add_user
  FROM
    (SELECT
      b.business_name, SUM(
        t.shelf_level_t IN (
          '甲级', '甲级2', '乙级', '乙级2'
        )
      ) shelfs32, SUM(
        t.shelf_level_t IN ('丁级', '丁级2')
      ) shelfs0, COUNT(*) shelfs
    FROM
      feods.pj_shelf_level_ab t
      JOIN feods.fjr_city_business b
        ON t.city_name = b.city_name
    WHERE t.smonth = @ym
    GROUP BY b.business_name) t
    LEFT JOIN feods.ab_tmp ab
      ON t.business_name = ab.business_name;
  DELETE
  FROM
    feods.fjr_kpi2_monitor_area
  WHERE indicate_id IN (110, 111, 112)
    AND (
      sdate = @last_week_end
      OR sdate = @month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 110 indicate_id, 'fjr_kpi2_shelf_level_stat' indicate_name, t.shelfs32 indicate_value, @add_user
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @last_week_end;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 111 indicate_id, 'fjr_kpi2_shelf_level_stat' indicate_name, ROUND(
      t.shelfs32_lm_loss / t.shelfs32_lm, 6
    ) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @last_week_end;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @last_week_end sdate, t.business_name, 'w' indicate_type, 112 indicate_id, 'fjr_kpi2_shelf_level_stat' indicate_name, ROUND(t.shelfs0 / t.shelfs, 6) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @last_week_end;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, t.business_name, 'm' indicate_type, 110 indicate_id, 'fjr_kpi2_shelf_level_stat' indicate_name, t.shelfs32 indicate_value, @add_user
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @month_end;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, t.business_name, 'm' indicate_type, 111 indicate_id, 'fjr_kpi2_shelf_level_stat' indicate_name, ROUND(
      t.shelfs32_lm_loss / t.shelfs32_lm, 6
    ) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @month_end;
  INSERT INTO feods.fjr_kpi2_monitor_area (
    sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  SELECT
    @month_start sdate, t.business_name, 'm' indicate_type, 112 indicate_id, 'fjr_kpi2_shelf_level_stat' indicate_name, ROUND(t.shelfs0 / t.shelfs, 6) indicate_value, @add_user
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @month_end;
  SELECT
    @shelfs32_w := SUM(t.shelfs32), @loss_rate_w := SUM(t.shelfs32_lm_loss) / SUM(t.shelfs32_lm), @shelf0_rate_w := SUM(t.shelfs0) / SUM(t.shelfs)
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @last_week_end;
  SELECT
    @shelfs32_m := SUM(t.shelfs32), @loss_rate_m := SUM(t.shelfs32_lm_loss) / SUM(t.shelfs32_lm), @shelf0_rate_m := SUM(t.shelfs0) / SUM(t.shelfs)
  FROM
    feods.fjr_kpi2_shelf_level_stat t
  WHERE t.sdate = @month_end;
  DELETE
  FROM
    feods.fjr_kpi2_monitor
  WHERE indicate_id IN (110, 111, 112)
    AND (
      sdate = @last_week_end
      OR sdate = @month_start
    );
  INSERT INTO feods.fjr_kpi2_monitor (
    sdate, indicate_type, indicate_id, indicate_name, indicate_value, add_user
  )
  VALUES
    (
      @last_week_end, 'w', 110, 'fjr_kpi2_shelf_level_stat', @shelfs32_w, @add_user
    ), (
      @month_start, 'm', 110, 'fjr_kpi2_shelf_level_stat', @shelfs32_m, @add_user
    ), (
      @last_week_end, 'w', 111, 'fjr_kpi2_shelf_level_stat', @loss_rate_w, @add_user
    ), (
      @month_start, 'm', 111, 'fjr_kpi2_shelf_level_stat', @loss_rate_m, @add_user
    ), (
      @last_week_end, 'w', 112, 'fjr_kpi2_shelf_level_stat', @shelf0_rate_w, @add_user
    ), (
      @month_start, 'm', 112, 'fjr_kpi2_shelf_level_stat', @shelf0_rate_m, @add_user
    );
  CALL feods.sp_task_log (
    'sp_kpi2_shelf_level_stat', @sdate, CONCAT(
      'fjr_d_c322575913535b6a14b14e14ee60419d', @timestamp, @add_user
    )
  );
  COMMIT;
END