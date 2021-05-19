CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_shelf_nps`(IN in_sdate DATE)
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime:= CURRENT_TIMESTAMP();  
  SET @sdate := in_sdate,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    fe_dm.dm_op_kpi_shelf_nps  
  WHERE sdate >= CONCAT(
      DATE_FORMAT(
        SUBDATE(@sdate, INTERVAL 2 MONTH),
        '%Y-%m'
      ),
      '-01'
    )
    AND sdate < LAST_DAY(SUBDATE(@sdate, INTERVAL 2 MONTH));
  DELETE
  FROM
    fe_dm.dm_op_kpi_shelf_nps
  WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_kpi_shelf_nps (
    sdate,
    region,
    business_area,
    shelf_id,
    new_products,
    add_user
  )
  SELECT
    t.sdate,
    t.REGION_NAME,
    t.BUSINESS_NAME,
    t.SHELF_ID,
    IFNULL(SUM(t.new_products), 0) new_products,
    @add_user
  FROM
    (SELECT
      @sdate sdate,
      s.REGION_NAME,
      s.BUSINESS_NAME,
      s.SHELF_ID,
      COUNT(1) new_products
    FROM
      fe_dwd.dwd_shelf_product_day_all f,
      fe_dwd.dwd_shelf_base_day_all s
    WHERE s.SHELF_ID = f.SHELF_ID
      AND s.SHELF_type IN (1, 2, 3, 5)
      AND s.ACTIVATE_TIME < SUBDATE(LAST_DAY(@sdate), 13)
      AND (
        s.REVOKE_TIME IS NULL
        OR s.REVOKE_TIME >= CONCAT(
          DATE_FORMAT(@sdate, '%Y-%m'),
          '-01'
        )
      )
      AND f.FIRST_FILL_TIME >= ADDDATE(s.ACTIVATE_TIME, 14)
      AND f.FIRST_FILL_TIME >= SUBDATE(
        CONCAT(
          DATE_FORMAT(@sdate, '%Y-%m'),
          '-01'
        ),
        14-1
      )
      AND f.FIRST_FILL_TIME < ADDDATE(@sdate, 1)
    GROUP BY s.SHELF_ID
    UNION
    ALL
    SELECT
      @sdate sdate,
      s.REGION_NAME,
      s.BUSINESS_NAME,
      s.SHELF_ID,
      0 new_products
    FROM
     fe_dwd.dwd_shelf_base_day_all s
    WHERE  s.SHELF_type IN (1, 2, 3, 5)
      AND s.ACTIVATE_TIME < SUBDATE(LAST_DAY(@sdate), 13)
      AND (
        s.REVOKE_TIME IS NULL
        OR s.REVOKE_TIME >= CONCAT(
          DATE_FORMAT(@sdate, '%Y-%m'),
          '-01'
        )
      )) t
  GROUP BY t.SHELF_ID;
  
  
  
  #执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_kpi_shelf_nps',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('李世龙@', @user), @stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_shelf_nps','dm_op_kpi_shelf_nps','李世龙');
END