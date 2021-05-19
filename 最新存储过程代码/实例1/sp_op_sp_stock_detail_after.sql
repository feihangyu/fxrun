CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sp_stock_detail_after`(in_y_m CHAR(7))
BEGIN
  #run after sh_process.sp_op_sp_stock_detail
   SET @y_m := in_y_m,
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := DATE(CONCAT(@y_m, '-01'));
  SET @month_end := LAST_DAY(@month_start);
  SET @d := DAY(@month_end);
  SET @ym := REPLACE(@y_m, '-', '');
  SET @y_m_next := DATE_FORMAT(
    ADDDATE(@month_start, INTERVAL 1 MONTH),
    '%Y-%m'
  );
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_stock_detail_after TRUNCATE PARTITION p",
    @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_stock_detail_after ( month_id, shelf_id, product_id",
      GROUP_CONCAT(
        CONCAT(",d", t.number) SEPARATOR ' '
      ),
      ",d",
      @d,
      ", add_user ) SELECT @y_m month_id, t.shelf_id, t.product_id",
      GROUP_CONCAT(
        CONCAT(",t.d", t.number + 1) SEPARATOR ' '
      ),
      ",IFNULL(tn.d1,0),@add_user add_user FROM feods.d_op_sp_stock_detail t LEFT JOIN feods.d_op_sp_stock_detail tn ON t.shelf_id = tn.shelf_id AND t.product_id = tn.product_id AND tn.month_id = @y_m_next AND tn.d1 != 0 WHERE t.month_id = @y_m;"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d - 1;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_stock_detail_after ( month_id, shelf_id, product_id,d",
      @d,
      ", add_user ) SELECT @y_m month_id, t.shelf_id, t.product_id,t.d1,@add_user add_user FROM feods.d_op_sp_stock_detail t LEFT JOIN feods.d_op_sp_stock_detail tl ON t.shelf_id = tl.shelf_id AND t.product_id = tl.product_id AND tl.month_id = @y_m WHERE t.month_id = @y_m_next AND t.d1 != 0 AND ISNULL(tl.month_id);"
    ) INTO @sql_str;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_op_sp_stock_detail_after',
    @month_start,
    CONCAT(
      'fjr_d_926bf3fef796406f206ab9437adf9081',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END