CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_count_process_aim_table_size`(in table_name varchar(128),in process_name varchar(128),in maintainer varchar(32))
BEGIN
  set @table_name := table_name;
  set @process_name := process_name;
  set @maintainer := maintainer;
  SELECT CONCAT("select count(1) into @nums from ", @table_name) INTO @sql_str;
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
replace into fe_dwd.dwd_count_process_aim_table_size(sdate,table_name,process_name,nums,maintainer) values(current_date,@table_name,@process_name,@nums,@maintainer);
	
COMMIT;
END