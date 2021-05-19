CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sp_sal_sto_detail`(in_y_m CHAR(7))
BEGIN
  #run after sh_process.sp_op_sp_sale_detail
#run after sh_process.sp_op_sp_stock_detail_after
   SET @y_m := in_y_m;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @month_start := DATE(CONCAT(@y_m, '-01'));
  SET @month_end := LEAST(
    LAST_DAY(@month_start), SUBDATE(CURRENT_DATE, 1)
  );
SET @month_start1 := SUBDATE(@month_start, INTERVAL 1 MONTH);
SET @month_start5 := SUBDATE(@month_start, INTERVAL 5 MONTH);
 SET @ym5 := REPLACE(LEFT(@month_start5,7), '-', '');
  SET @d := DAY(@month_end);
  SET @ym := REPLACE(@y_m, '-', '');
  
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_sal_sto_detail TRUNCATE PARTITION p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.sp_tmp;
  CREATE TEMPORARY TABLE feods.sp_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id, product_id
  FROM
    feods.d_op_sp_sale_detail
  WHERE month_id = @y_m
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id)
  UNION
  SELECT
    shelf_id, product_id
  FROM
    feods.d_op_sp_stock_detail_after
  WHERE month_id = @y_m
    AND ! ISNULL(shelf_id)
    AND ! ISNULL(product_id);
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_sal_sto_detail(month_id, shelf_id, product_id", GROUP_CONCAT(
        CONCAT(",s", t.number, ",t", t.number) SEPARATOR ' '
      ), ",add_user)SELECT @y_m month_id,t.shelf_id, t.product_id", GROUP_CONCAT(
        CONCAT(
          ",IFNULL(sal.d", t.number, ",0),IFNULL(sal.d", t.number, ",0)+IFNULL(sto.d", t.number, ",0)"
        ) SEPARATOR ' '
      ), ",@add_user add_user FROM feods.sp_tmp t LEFT JOIN feods.d_op_sp_sale_detail sal ON t.shelf_id=sal.shelf_id AND t.product_id =sal.product_id AND sal.month_id=@y_m LEFT JOIN feods.d_op_sp_stock_detail_after sto ON t.shelf_id=sto.shelf_id AND t.product_id =sto.product_id AND sto.month_id=@y_m"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
  -- 动态保留5个月的数据
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_sal_sto_detail TRUNCATE PARTITION p", @ym5
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;  
  
  CALL feods.sp_task_log (
    'sp_op_sp_sal_sto_detail', @month_start, CONCAT(
      'fjr_d_311999192f5c54944f3ba5ce075b53b4', @add_user, @timestamp
    )
  );
  COMMIT;
END