CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_days_sto30`(in_sdate DATE)
BEGIN
  #run after sh_process.sp_op_sp_stock_detail_after
#run after sh_process.sp_op_sp_stock_detail
   SET @sdate := CURRENT_DATE;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @add_user := CURRENT_USER;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @d := DAY(@sdate);
  SET @month_start := SUBDATE(@sdate, @d - 1);
  SET @month_start1 := SUBDATE(@month_start, INTERVAL 1 MONTH);
  SET @month_start2 := SUBDATE(@month_start, INTERVAL 2 MONTH);
  SET @y_m1 := DATE_FORMAT(@month_start1, '%Y-%m');
  SET @y_m2 := DATE_FORMAT(@month_start2, '%Y-%m');
  SET @sdate30 := SUBDATE(@sdate, 29);
  SET @month_end1 := LAST_DAY(@month_start1);
  SET @month_end2 := LAST_DAY(@month_start2);
  SET @mflag1 := @month_end1 >= @sdate30;
  SET @mflag2 := @month_end2 >= @sdate30;
  SELECT
    CONCAT(
      "SELECT t.shelf_id,t.product_id,0", GROUP_CONCAT(
        CONCAT("+(t.d", DAY(t.sdate), ">0)")
        ORDER BY t.sdate SEPARATOR ' '
      ), "days_sto FROM feods.d_op_sp_stock_detail t WHERE t.month_id=@y_m and(0 ", GROUP_CONCAT(
        CONCAT(" OR t.d", DAY(t.sdate), "> 0 ")
        ORDER BY t.sdate SEPARATOR ' '
      ), ")"
    ) INTO @sql_str
  FROM
    feods.fjr_work_days t
  WHERE t.sdate BETWEEN GREATEST(@sdate30, @month_start)
    AND @sdate;
  SELECT
    CONCAT(
      @sql_str, IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0", GROUP_CONCAT(
            CONCAT("+(t.d", DAY(t.sdate), ">0)")
            ORDER BY t.sdate SEPARATOR ' '
          ), "days_sto FROM feods.d_op_sp_stock_detail t WHERE t.month_id=@y_m1 and @mflag1=1 and(0 ", GROUP_CONCAT(
            CONCAT(" OR t.d", DAY(t.sdate), "> 0 ")
            ORDER BY t.sdate SEPARATOR ' '
          ), ")"
        ), ""
      )
    ) INTO @sql_str
  FROM
    feods.fjr_work_days t
  WHERE t.sdate BETWEEN GREATEST(@sdate30, @month_start1)
    AND @month_end1
    AND @mflag1 = 1;
  SELECT
    CONCAT(
      @sql_str, IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0", GROUP_CONCAT(
            CONCAT("+(t.d", DAY(t.sdate), ">0)")
            ORDER BY t.sdate SEPARATOR ' '
          ), "days_sto FROM feods.d_op_sp_stock_detail t WHERE t.month_id=@y_m2 and @mflag2=1 and(0 ", GROUP_CONCAT(
            CONCAT(" OR t.d", DAY(t.sdate), "> 0 ")
            ORDER BY t.sdate SEPARATOR ' '
          ), ")"
        ), ""
      )
    ) INTO @sql_str
  FROM
    feods.fjr_work_days t
  WHERE t.sdate BETWEEN @sdate30
    AND @month_end2
    AND @mflag2 = 1;
  SET @sql_str := CONCAT(
    "insert into feods.d_op_sp_days_sto30(shelf_id,product_id,days_sto30) select t.shelf_id,t.product_id,sum(t.days_sto)days_sto from (", @sql_str, ")t group by t.shelf_id,t.product_id;"
  );
  PREPARE sql_exe FROM @sql_str;
  TRUNCATE feods.d_op_sp_days_sto30;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_days_sto30', @sdate, CONCAT(
      'yingnansong_d_38b7b09a10b4bf1067eeff296de0f77f', @timestamp, @add_user
    )
  );
  COMMIT;
END