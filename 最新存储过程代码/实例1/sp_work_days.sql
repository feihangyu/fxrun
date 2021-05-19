CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_work_days`()
BEGIN
  DROP TEMPORARY TABLE IF EXISTS feods.wds_tmp;
  CREATE TEMPORARY TABLE feods.wds_tmp (PRIMARY KEY (sdate))
  SELECT
    t.sdate,
    @work_day_seq := @work_day_seq + t.if_work_day work_day_seq
  FROM
    feods.fjr_work_days t,
    (SELECT
      @work_day_seq := 0) w;
  UPDATE
    feods.fjr_work_days t
    JOIN feods.wds_tmp w
      ON t.sdate = w.sdate SET t.work_day_seq = IF(t.if_work_day, w.work_day_seq, 0),
    t.bussiness_month = 1;
  DROP TEMPORARY TABLE IF EXISTS feods.bm_tmp;
  CREATE TEMPORARY TABLE feods.bm_tmp (PRIMARY KEY (sdate))
  SELECT
    DATE_FORMAT(t.sdate, '%y%m') ym,
    @weeks := @weeks + (WEEKDAY(t.sdate) = 0) weeks,
    t.sdate,
    t.if_work_day
  FROM
    feods.fjr_work_days t,
    (SELECT
      @weeks := 0) w;
  DROP TEMPORARY TABLE IF EXISTS feods.ym_tmp;
  CREATE TEMPORARY TABLE feods.ym_tmp (PRIMARY KEY (weeks))
  SELECT
    t.weeks,
    CAST(
      SUBSTRING_INDEX(
        GROUP_CONCAT(
          t.ym
          ORDER BY ct_if_work_day DESC,
          t.ym
        ),
        ',',
        1
      ) AS CHAR(4)
    ) ym
  FROM
    (SELECT
      t.weeks,
      t.ym,
      COUNT(*) ct,
      SUM(t.if_work_day) ct_if_work_day
    FROM
      feods.bm_tmp t
    GROUP BY t.weeks,
      t.ym
    HAVING ct < 7) t
  GROUP BY t.weeks;
  UPDATE
    feods.fjr_work_days t
    JOIN feods.bm_tmp bm
      ON t.sdate = bm.sdate
    JOIN feods.ym_tmp ym
      ON bm.weeks = ym.weeks
      AND bm.ym != ym.ym SET t.bussiness_month = 0;
  COMMIT;
END