CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_sf_dw_task_log`(in pi_task_name varchar(256),in pi_statedate varchar(64),in pi_loginfo varchar(8000))
BEGIN
          DECLARE l_error_flag INT;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET l_error_flag=1; 
    INSERT INTO feods.sf_dw_task_log
      (task_name, 
       statedate,
       loginfo,
       remark
       )
    VALUES
      (pi_task_name,
       pi_statedate,
       pi_loginfo,
       NULL);
       
-- 更新表级的数据更新状态
INSERT INTO fe_dwd.dwd_table_update_data_status(sdate,table_name,update_status)
SELECT CURRENT_DATE,
CONCAT(aim_base,'.',aim_table) AS table_name,
1 AS update_status
FROM feods.prc_project_process_source_aim_table_info 
WHERE PROCESS=pi_task_name   
AND LENGTH(aim_table)>1;
    COMMIT;
    END