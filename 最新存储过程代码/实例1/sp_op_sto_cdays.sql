CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sto_cdays`(in_sdate DATE)
BEGIN
  #run after sh_process.sp_op_sp_stock_detail
   SET @sdate := in_sdate;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @d := DAY(@sdate);
  DROP TEMPORARY TABLE IF EXISTS feods.tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.tmp(PRIMARY KEY(shelf_id,product_id)) SELECT shelf_id,product_id,d", @d, " sto_qty,date(concat(@y_m,'-',CASE ", GROUP_CONCAT(
        CONCAT(
          " WHEN d", t.number, ">0 THEN ", t.number
        )
        ORDER BY t.number DESC SEPARATOR ''
      ), " ELSE null END))ld_sto,date(concat(@y_m,'-',CASE ", GROUP_CONCAT(
        CONCAT(
          " WHEN d", t.number, "<=0 THEN ", t.number
        )
        ORDER BY t.number DESC SEPARATOR ''
      ), " ELSE null END))ld_nsto FROM feods.d_op_sp_stock_detail WHERE month_id = @y_m AND !ISNULL(shelf_id) AND !ISNULL(product_id)  and(0", GROUP_CONCAT(
        CONCAT(" or d", t.number, ">0") SEPARATOR ''
      ), ") "
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @d;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  INSERT INTO feods.d_op_sto_cdays (
    shelf_id, product_id, sto_qty, ld_sto, ld_nsto, add_user
  )
  SELECT
    t.shelf_id, t.product_id, t.sto_qty, t.ld_sto, t.ld_nsto, @add_user add_user
  FROM
    feods.tmp t
    LEFT JOIN feods.d_op_sto_cdays s
      ON t.shelf_id = s.shelf_id
      AND t.product_id = s.product_id
  WHERE ISNULL(s.shelf_id);
  UPDATE
    feods.d_op_sto_cdays t
    LEFT JOIN feods.tmp s
      ON t.shelf_id = s.shelf_id
      AND t.product_id = s.product_id SET t.sto_qty = IFNULL(s.sto_qty, 0), t.ld_sto = IFNULL(s.ld_sto, t.ld_sto), t.ld_nsto = IFNULL(s.ld_nsto, @sdate);
  CALL feods.sp_task_log (
    'sp_op_sto_cdays', @sdate, CONCAT(
      'yingnansong_d_728f542e3178f95e1db0029a6f5750cb', @timestamp, @add_user
    )
  );
  COMMIT;
END