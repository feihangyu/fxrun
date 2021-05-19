CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_azkaban_kettle_erp_detective`(IN kettle_project_name VARCHAR(128) ,IN kettle_table_name VARCHAR(128), IN start_time DATETIME,IN end_time DATETIME,IN in_nums INT,IN out_nums INT,IN excute_status VARCHAR(32),IN fail_reason TEXT, IN erp_type TINYINT)
BEGIN
SET @run_time := ROUND(TIMESTAMPDIFF(SECOND,start_time,end_time)/60,1);
INSERT INTO fe_dwd.dwd_azkaban_kettle_erp_detective (kettle_project_name,sdate,kettle_table_name,start_time,end_time,run_time,in_nums,out_nums,excute_status,fail_reason,erp_type)
VALUES(kettle_project_name,CURRENT_DATE,kettle_table_name,start_time,end_time,@run_time,in_nums,out_nums,excute_status,fail_reason,erp_type);
COMMIT;
END