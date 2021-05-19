CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_tsp_routine_relation`()
BEGIN
  SET @sdate := CURRENT_DATE;
  SET @add_user := CURRENT_USER;
  SET @timestamp := CURRENT_TIMESTAMP;
  SET @str_concat := 'into';
  SET @len_concat := LENGTH(@str_concat);
  SET @level := 11;
  SET @lefts := 6;
  SET @row_id := 0;
  TRUNCATE feods.d_op_tsp_tables;
  INSERT INTO feods.d_op_tsp_tables (
    table_schema, table_name, len_table
  )
  SELECT
    LOWER(t.table_schema) table_schema, LOWER(t.table_name) table_name, LENGTH(t.table_schema) + LENGTH(t.table_name) len_table
  FROM
    information_schema.tables t;
	
  TRUNCATE feods.d_op_tsp_table_left;
  INSERT INTO feods.d_op_tsp_table_left (table_long, table_short)
  SELECT
    t.table_id table_long, ts.table_id table_long
  FROM
    feods.d_op_tsp_tables t
    JOIN feods.d_op_tsp_tables ts
      ON t.table_schema = ts.table_schema
      AND INSTR(t.table_name, ts.table_name) = 1
      AND t.len_table > ts.len_table;
	  
  DROP TEMPORARY TABLE IF EXISTS feods.lefts_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.lefts_tmp(primary key(row_id)) SELECT @row_id:=@row_id+1 row_id,t1.table_short table_id, t1.table_long table_id1,", GROUP_CONCAT(
        "t", number, ".table_long table_id", number
      ), " FROM feods.d_op_tsp_table_left t1 LEFT JOIN ", GROUP_CONCAT(
        "feods.d_op_tsp_table_left t", number, " ON t", number - 1, ".table_long = t", number, ".table_short" SEPARATOR ' LEFT JOIN '
      )
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 2
    AND @lefts;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
  
  DROP TABLE IF EXISTS test.lefts;
  CREATE TABLE test.lefts LIKE feods.lefts_tmp;
  INSERT INTO test.lefts
  SELECT
    *
  FROM
    feods.lefts_tmp;
  DROP TEMPORARY TABLE IF EXISTS feods.tmax_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.tmax_tmp(primary key(table_id)) SELECT table_id,MAX(ni)ni FROM(SELECT DISTINCT 0 ni,table_id FROM test.lefts where !isnull(table_id) UNION ALL ", GROUP_CONCAT(
        "SELECT DISTINCT ", number, " ni,table_id", number, " table_id FROM test.lefts where !isnull(table_id", number, ")" SEPARATOR ' UNION ALL '
      ), ") t GROUP BY table_id"
    ) INTO @sql_str
  FROM
    feods.fjr_number
  WHERE number BETWEEN 1
    AND @lefts;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
  
  DROP TABLE IF EXISTS test.lefts;
  TRUNCATE feods.d_op_tsp_routines;
  INSERT INTO feods.d_op_tsp_routines (
    routine_schema, routine_name, routine_definition, len_routine
  )
  SELECT
    LOWER(t.routine_schema) routine_schema, LOWER(t.routine_name) routine_name, @str_tmp := LOWER(
      REPLACE(
        REPLACE(
          REPLACE(t.routine_definition, ' ', ''), '`', ''
        ), '.', ''
      )
    ) routine_definition, LENGTH(@str_tmp) len_routine
  FROM
    information_schema.routines t;
  TRUNCATE feods.d_op_tsp_stat;
  INSERT INTO feods.d_op_tsp_stat (table_id, routine_id, ct, ct_into)
  SELECT
    t.table_id, r.routine_id, ROUND(
      (
        r.len_routine - LENGTH(
          REPLACE(
            r.routine_definition, CONCAT(t.table_schema, t.table_name), ''
          )
        )
      ) / t.len_table
    ) ct, IFNULL(
      ROUND(
        (
          r.len_routine - LENGTH(
            REPLACE(
              r.routine_definition, CONCAT(
                @str_concat, t.table_schema, t.table_name
              ), ''
            )
          )
        ) / (t.len_table + @len_concat)
      ), 0
    ) ct_into
  FROM
    feods.d_op_tsp_tables t
    JOIN feods.d_op_tsp_routines r
      ON INSTR(
        r.routine_definition, CONCAT(t.table_schema, t.table_name)
      ) > 0;
  SET @sql_str1 := "UPDATE feods.d_op_tsp_stat t JOIN (SELECT t.table_id,t.routine_id,SUM(t2.ct)ct,SUM(t2.ct_into)ct_into FROM feods.d_op_tsp_stat t JOIN  feods.tmax_tmp tm ON t.table_id=tm.table_id AND tm.ni=";
  SET @sql_str2 := " JOIN feods.d_op_tsp_table_left le ON t.table_id=le.table_short JOIN feods.d_op_tsp_stat t2 ON le.table_long=t2.table_id AND t.routine_id=t2.routine_id GROUP BY t.table_id,t.routine_id )u ON t.table_id=u.table_id AND t.routine_id=u.routine_id SET t.ct=t.ct-u.ct,t.ct_into=t.ct_into-u.ct_into";
  SET @sql_str := CONCAT(@sql_str1, 6, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(@sql_str1, 5, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(@sql_str1, 4, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(@sql_str1, 3, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(@sql_str1, 2, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(@sql_str1, 1, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SET @sql_str := CONCAT(@sql_str1, 0, @sql_str2);
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DELETE
  FROM
    feods.d_op_tsp_stat
  WHERE ct = 0;
  TRUNCATE feods.d_op_tsp_routine_relation;
  INSERT INTO feods.d_op_tsp_routine_relation (routine_id, routine_head)
  SELECT DISTINCT
    t1.routine_id, t.routine_id routine_head
  FROM
    feods.d_op_tsp_stat t
    JOIN feods.d_op_tsp_stat t1
      ON t.table_id = t1.table_id
      AND t.routine_id != t1.routine_id
      AND t1.ct_into = 0
    JOIN feods.d_op_tsp_routines r
      ON t.routine_id = r.routine_id
      AND r.routine_name NOT IN (
        'sp_sf_dw_task_log', 'sp_task_log'
      )
  WHERE t.ct_into > 0;
  DROP TEMPORARY TABLE IF EXISTS feods.long_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.long_tmp SELECT t1.routine_head routine_id1,t1.routine_id routine_id2,", GROUP_CONCAT(
        "t", t.number, ".routine_id routine_id", t.number + 1
      ), " FROM feods.d_op_tsp_routine_relation t1", GROUP_CONCAT(
        " LEFT JOIN feods.d_op_tsp_routine_relation t", t.number, " ON t", t.number - 1, ".routine_id=t", t.number, ".routine_head" SEPARATOR ''
      )
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 2
    AND @level;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "SELECT ", GROUP_CONCAT(
        "(COUNT(routine_id", t.number, ")>0)" SEPARATOR '+'
      ), "into @level FROM feods.long_tmp"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @level + 1;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TABLE IF EXISTS test.d_op_tsp_routine_long_relation;
  SELECT
    CONCAT(
      "CREATE TABLE test.d_op_tsp_routine_long_relation (row_id INT AUTO_INCREMENT COMMENT '行号',", GROUP_CONCAT(
        "routine_id", t.number, " INT COMMENT '过程", t.number, "'"
      ), ",PRIMARY KEY(row_id),UNIQUE KEY(", GROUP_CONCAT("routine_id", t.number), "))COMMENT='过程长依赖'"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @level;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  SELECT
    CONCAT(
      "INSERT INTO test.d_op_tsp_routine_long_relation(", GROUP_CONCAT("routine_id", t.number), ")SELECT ", GROUP_CONCAT("routine_id", t.number), " FROM feods.long_tmp"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @level;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  TRUNCATE feods.d_op_tsp_routine_vertical_relation;
  SELECT
    CONCAT(
      "INSERT INTO feods.d_op_tsp_routine_vertical_relation(routine_id,routine_head,routine_tail)", GROUP_CONCAT(
        "SELECT routine_id", t.number, ",routine_id", t1.number, ",routine_id", t2.number, " FROM test.d_op_tsp_routine_long_relation WHERE !ISNULL(routine_id", t2.number, ") AND !ISNULL(routine_id", t.number, ")" SEPARATOR 'UNION '
      )
    ) INTO @sql_str
  FROM
    feods.fjr_number t
    JOIN feods.fjr_number t1
      ON t1.number BETWEEN 1
      AND @level
    JOIN feods.fjr_number t2
      ON t2.number BETWEEN 1
      AND @level
      AND t1.number = t2.number - 1
  WHERE t.number BETWEEN 1
    AND @level;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  DROP TEMPORARY TABLE IF EXISTS feods.run_tmp;
  SELECT
    CONCAT(
      "CREATE TEMPORARY TABLE feods.run_tmp", GROUP_CONCAT(
        " select routine_id", t.number, " routine_id,", t1.number - t.number, " level_flag,routine_id", t1.number, " routine_run from test.d_op_tsp_routine_long_relation where !isnull(routine_id", t.number, ") and !isnull(routine_id", t1.number, ")" SEPARATOR 'union'
      )
    ) INTO @sql_str
  FROM
    feods.fjr_number t
    JOIN feods.fjr_number t1
      ON t1.number BETWEEN 1
      AND @level
      AND t.number != t1.number
  WHERE t.number BETWEEN 1
    AND @level;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  TRUNCATE feods.d_op_tsp_routine_run_relation;
  INSERT INTO feods.d_op_tsp_routine_run_relation (
    routine_id, level_flag, routine_run
  )
  SELECT
    t.routine_id, MAX(ABS(t.level_flag)) * SIGN(t.level_flag) level_flag, t.routine_run
  FROM
    feods.run_tmp t
  GROUP BY t.routine_id, t.routine_run;
  TRUNCATE feods.d_op_tsp_routine_level;
  SELECT
    CONCAT(
      "insert into feods.d_op_tsp_routine_level(routine_id,level_flag)SELECT routine_id,MAX(level_flag)level_flag FROM(", GROUP_CONCAT(
        "SELECT DISTINCT routine_id", t.number, " routine_id,", t.number, " level_flag FROM test.d_op_tsp_routine_long_relation WHERE !ISNULL(routine_id", t.number, ")"
        ORDER BY t.number DESC SEPARATOR 'UNION ALL '
      ), ")t GROUP BY routine_id"
    ) INTO @sql_str
  FROM
    feods.fjr_number t
  WHERE t.number BETWEEN 1
    AND @level;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  CALL feods.sp_task_log (
    'sp_op_tsp_routine_relation', @sdate, CONCAT(
      'fjr_d_22eb57cff8ac934a111d7c2ccaadb6ab', @timestamp, @add_user
    )
  );
  COMMIT;
END