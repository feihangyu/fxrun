CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sp_stock_detail`(in_month_id char(7))
BEGIN
  SET @month_id := in_month_id, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := DATE(CONCAT(@month_id, '-01'));
  SET @sdate := LEAST(
    CURRENT_DATE, LAST_DAY(@month_start)
  );
  SET @d := DAY(@sdate);
  SET @ym := REPLACE(@month_id, '-', '');
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_stock_detail TRUNCATE PARTITION p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_stock_detail(month_id,shelf_id,product_id", GROUP_CONCAT(
        CONCAT(",d", t.number) SEPARATOR ' '
      ), ",add_user)select @month_id month_id,shelf_id,product_id", GROUP_CONCAT(
        CONCAT(
          ",SUM(day", t.number, "_quantity) d", t.number
        ) SEPARATOR ' '
      ), ",@add_user add_user FROM fe.sf_shelf_product_stock_detail WHERE stat_date=@month_id AND (", GROUP_CONCAT(
        CONCAT("day", t.number, "_quantity!=0") SEPARATOR ' OR '
      ), ") GROUP BY shelf_id,product_id;"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_op_sp_stock_detail', @sdate, CONCAT(
      'fjr_d_de646feda1143f5d41aae0e179296b0b', @timestamp, @add_user
    )
  );
  COMMIT;
END