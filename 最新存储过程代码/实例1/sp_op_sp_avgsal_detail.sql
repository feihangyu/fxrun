CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_sp_avgsal_detail`(in_sdate DATE)
BEGIN
  #run after sh_process.sp_op_sp_sal_sto_detail
   SET @sdate := in_sdate;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @ym := DATE_FORMAT(@sdate, '%Y%m');
  SET @d := DAY(@sdate);
  SET @avgdays := 30;
  SET @alldays := @d + @avgdays;
  SET @month_end_last := SUBDATE(@sdate, @d);
  SET @y_m_last := DATE_FORMAT(@month_end_last, '%Y-%m');
  DROP TEMPORARY TABLE IF EXISTS feods.charws_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.charws_tmp (PRIMARY KEY (shelf_id, product_id))SELECT t.shelf_id, t.product_id,@dct:=CONCAT_WS(',',", GROUP_CONCAT(
        "if(", IF(number > @d, 'a', 't'), ".t", IF(number > @d, @avgdays, 0) + @d + 1- number, ">0,", IF(number > @d, 'a', 't'), ".t", IF(number > @d, @avgdays, 0) + @d + 1- number, ",null)"
      ), ")tt,length(@dct)-length(replace(@dct,',',''))ltt,@dct!='' tlflag,@dct:=CONCAT_WS(',',", GROUP_CONCAT(
        "if(", IF(number > @d, 'a', 't'), ".s", IF(number > @d, @avgdays, 0) + @d + 1- number, ">0,", IF(number > @d, 'a', 't'), ".s", IF(number > @d, @avgdays, 0) + @d + 1- number, ",null)"
      ), ")ss,length(@dct)-length(replace(@dct,',',''))lss,@dct!='' slflag FROM feods.d_op_sp_sal_sto_detail t LEFT JOIN feods.d_op_sp_avgsal_detail a ON t.shelf_id=a.shelf_id AND t.product_id=a.product_id AND a.month_id = @y_m_last where t.month_id = @y_m AND !ISNULL(t.shelf_id) AND !ISNULL(t.product_id) "
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 1
    AND @alldays;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.split_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.split_tmp (PRIMARY KEY (shelf_id, product_id))SELECT shelf_id,product_id,IF(ltt>@avgdays-1,@avgdays,ltt+1)days_tot,IF(lss>@avgdays-1,@avgdays,lss+1)days_sal,", GROUP_CONCAT(
        "cast(IF(ltt>=", number - 1, "&&tlflag,SUBSTRING_INDEX(SUBSTRING_INDEX(tt,',',", number, "),',',-1),0)as unsigned)t", number, ",cast(IF(lss>=", number - 1, "&&slflag,SUBSTRING_INDEX(SUBSTRING_INDEX(ss,',',", number, "),',',-1),0)as unsigned)s", number
      ), " from feods.charws_tmp  where !ISNULL(shelf_id) AND !ISNULL(product_id) "
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 1
    AND @avgdays;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_avgsal_detail TRUNCATE PARTITION p", @ym
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_avgsal_detail(month_id,shelf_id,product_id,qty_sal,qty_tot,days_sal,days_tot,", GROUP_CONCAT("s", number, ",t", number), ",add_user)SELECT @y_m month_id,shelf_id,product_id,", GROUP_CONCAT("s", number SEPARATOR '+'), " qty_sal,", GROUP_CONCAT("t", number SEPARATOR '+'), " qty_tot,days_sal,days_tot,", GROUP_CONCAT("s", number, ",t", number), ",@add_user add_user FROM feods.split_tmp"
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 1
    AND @avgdays;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_sp_avgsal_detail(month_id,shelf_id,product_id,qty_sal,qty_tot,days_sal,days_tot,", GROUP_CONCAT("s", number, ",t", number), ",add_user)SELECT @y_m month_id,t.shelf_id,t.product_id,t.qty_sal,t.qty_tot,t.days_sal,t.days_tot,", GROUP_CONCAT("t.s", number, ",t.t", number), ",@add_user add_user FROM feods.d_op_sp_avgsal_detail t LEFT JOIN feods.d_op_sp_avgsal_detail a ON t.shelf_id=a.shelf_id AND t.product_id=a.product_id AND a.month_id = @y_m where t.month_id = @y_m_last and isnull(a.month_id)"
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 1
    AND @avgdays;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_op_sp_avgsal_detail', @sdate, CONCAT(
      'yingnansong_d_78009a35181f8491580cfaf190cad967', @timestamp, @add_user
    )
  );
  COMMIT;
END