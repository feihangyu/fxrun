CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_count_process_aim_table_size`(IN table_name VARCHAR(128),IN process_name VARCHAR(128),IN maintainer VARCHAR(32))
BEGIN
  SET @table_name := table_name;
  SET @process_name := process_name;
  SET @maintainer := maintainer;
  SELECT CONCAT("select count(1) into @nums from ", @table_name) INTO @sql_str;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
REPLACE INTO fe_dwd.dwd_count_process_aim_table_size(sdate,table_name,process_name,nums,maintainer) VALUES(CURRENT_DATE,@table_name,@process_name,@nums,@maintainer);
	
COMMIT;
END