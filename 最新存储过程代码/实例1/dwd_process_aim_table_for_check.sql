CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_process_aim_table_for_check`(IN table_name VARCHAR(128),IN column1 VARCHAR(64),IN column2 VARCHAR(64),IN column3 VARCHAR(64),IN column4 VARCHAR(64),IN where_condition VARCHAR(256),IN maintainer VARCHAR(32))
BEGIN
  SET @table_name := table_name;
  SET @where_condition := where_condition;
  
  #查询对应的字段注释
  SELECT  IFNULL(t.`COLUMN_COMMENT`,'该字段无注释') INTO @column_one_name FROM `information_schema`.`COLUMNS` t WHERE t.`TABLE_SCHEMA` =SUBSTRING_INDEX(table_name,'.',1) AND  t.`TABLE_NAME`=SUBSTRING_INDEX(table_name,'.',-1) AND t.`COLUMN_NAME`= column1;
  
  SELECT  IFNULL(t.`COLUMN_COMMENT`,'该字段无注释') INTO @column_two_name FROM `information_schema`.`COLUMNS` t WHERE t.`TABLE_SCHEMA` =SUBSTRING_INDEX(table_name,'.',1) AND  t.`TABLE_NAME`=SUBSTRING_INDEX(table_name,'.',-1) AND t.`COLUMN_NAME`= column2;
	
  SELECT  IFNULL(t.`COLUMN_COMMENT`,'该字段无注释') INTO @column_three_name FROM `information_schema`.`COLUMNS` t WHERE t.`TABLE_SCHEMA` =SUBSTRING_INDEX(table_name,'.',1) AND  t.`TABLE_NAME`=SUBSTRING_INDEX(table_name,'.',-1) AND t.`COLUMN_NAME`= column3;
  
  SELECT  IFNULL(t.`COLUMN_COMMENT`,'该字段无注释') INTO @column_four_name FROM `information_schema`.`COLUMNS` t WHERE t.`TABLE_SCHEMA` =SUBSTRING_INDEX(table_name,'.',1) AND  t.`TABLE_NAME`=SUBSTRING_INDEX(table_name,'.',-1) AND t.`COLUMN_NAME`= column4;
  
  #计算对应的字段信息
  SELECT CONCAT("SELECT GROUP_CONCAT(val) into @column_one_value FROM (SELECT CONCAT_WS(':',names,nums) AS val FROM (SELECT ",column1," as names,COUNT(1) AS nums FROM ", @table_name, " where ",@where_condition," GROUP BY ",column1," ) t) q") INTO @sql_str1;
  PREPARE sql_exe1 FROM @sql_str1;
  EXECUTE sql_exe1;
  deallocate prepare sql_exe1;
  
  SELECT CONCAT("SELECT GROUP_CONCAT(val) into @column_two_value FROM (SELECT CONCAT_WS(':',names,nums) AS val FROM (SELECT ",column2," as names,COUNT(1) AS nums FROM ", @table_name, " where ",@where_condition," GROUP BY ",column2," ) t) q") INTO @sql_str2;
  PREPARE sql_exe2 FROM @sql_str2;
  EXECUTE sql_exe2;
  deallocate prepare sql_exe2;
  
  
  SELECT CONCAT("select sum(",column3,") into @column_three_value from ", @table_name, " where ",@where_condition) INTO @sql_str3;
  PREPARE sql_exe3 FROM @sql_str3;
  EXECUTE sql_exe3;
  deallocate prepare sql_exe3;
  
  SELECT CONCAT("select sum(",column4,") into @column_four_value from ", @table_name, " where ",@where_condition) INTO @sql_str4;
  PREPARE sql_exe4 FROM @sql_str4;
  EXECUTE sql_exe4;
  deallocate prepare sql_exe4;
  
  SELECT CONCAT("select count(1) into @total_rows from ", @table_name, " where ",@where_condition) INTO @sql_str5;
  PREPARE sql_exe5 FROM @sql_str5;
  EXECUTE sql_exe5;
  deallocate prepare sql_exe5;
  
  
 REPLACE INTO fe_dwd.dwd_process_aim_table_for_check(sdate,table_name,column_one,column_one_name,group_by_column_one_value,column_two,column_two_name,group_by_column_two_value,column_three,column_three_name,sum_column_three_value,column_four,column_four_name,sum_column_four_value,total_rows,where_condition,maintainer) 
 VALUES(CURRENT_DATE,@table_name,column1,@column_one_name,@column_one_value,column2,@column_two_name,@column_two_value,column3,@column_three_name,@column_three_value,column4,@column_four_name,@column_four_value,@total_rows,@where_condition,maintainer);
 
COMMIT;
END