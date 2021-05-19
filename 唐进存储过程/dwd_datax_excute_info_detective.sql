CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_datax_excute_info_detective`(IN datax_project_name VARCHAR(128) ,IN datax_table_name VARCHAR(128),IN channel INT, IN start_time DATETIME,IN end_time DATETIME, IN run_time VARCHAR(32), IN avg_flows VARCHAR(32), IN write_speed VARCHAR(32),IN read_out_num BIGINT,IN read_fail_num BIGINT,IN excute_status VARCHAR(32),IN fail_reason TEXT, IN erp_type TINYINT)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET NAMES utf8;
INSERT INTO fe_dwd.dwd_datax_excute_info_detective (datax_project_name,sdate,datax_table_name,channel,start_time,end_time,run_time,avg_flows,write_speed,read_out_num,read_fail_num,excute_status,fail_reason,erp_type)
VALUES(datax_project_name,CURRENT_DATE,datax_table_name,channel,start_time,end_time,run_time,avg_flows,write_speed,read_out_num,read_fail_num,excute_status,fail_reason,erp_type);
-- 更新存储过程的执行状态
REPLACE INTO fe_dwd.dwd_project_excute_status(sdate,process_name,execute_status)
VALUES(CURRENT_DATE,datax_project_name,1);
-- 更新表级的数据更新信息
INSERT INTO fe_dwd.dwd_table_update_data_status(sdate,table_name,update_status)
SELECT CURRENT_DATE,(SELECT table_name_two FROM fe_dwd.dwd_datax_table_mapping_info WHERE SUBSTRING_INDEX(table_name_one,'.',-1)=CASE WHEN datax_table_name LIKE 'dwd_shelf_product_day_all%' THEN 'dwd_shelf_product_day_all'
WHEN datax_table_name LIKE 'd_op_sp_avgsal30_part%' THEN 'd_op_sp_avgsal30'
WHEN datax_table_name LIKE 'd_sc_shelf_packages_part%' THEN 'd_sc_shelf_packages'
ELSE datax_table_name END) AS table_name,1 AS update_status;
DELETE FROM fe_dwd.dwd_table_update_data_status WHERE table_name IS NULL;
COMMIT;
END