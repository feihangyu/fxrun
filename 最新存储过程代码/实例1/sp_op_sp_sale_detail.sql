CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sp_sale_detail`(in_y_m CHAR(7))
BEGIN
  #run after sh_process.sp_op_order_and_item
   SET @y_m := in_y_m;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := DATE(CONCAT(@y_m, '-01'));
  SET @month_end := LEAST(
    LAST_DAY(@month_start), SUBDATE(CURRENT_DATE, 1)
  );
  SET @add_day := ADDDATE(@month_end, 1);
  SET @d := DAY(@month_end);
  SET @ym := REPLACE(@y_m, '-', '');
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_sale_detail TRUNCATE PARTITION p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_sale_detail(month_id, shelf_id, product_id", GROUP_CONCAT(
        CONCAT(",d", t.number) SEPARATOR ' '
      ), ",add_user)SELECT @y_m month_id,shelf_id, product_id", GROUP_CONCAT(
        CONCAT(
          ",SUM(IF(DAY(pay_date)=", t.number, ",quantity_act,0))"
        ) SEPARATOR ' '
      ), ",@add_user add_user FROM fe_dwd.dwd_order_item_refund_day WHERE pay_date>=@month_start AND pay_date<@add_day GROUP BY shelf_id, product_id"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_op_sp_sale_detail', @month_end, CONCAT(
      'fjr_d_bd7bf2fe31e3a6f39151261fbe8c0662', @timestamp, @add_user
    )
  );
  COMMIT;
END