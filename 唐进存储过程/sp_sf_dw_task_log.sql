CREATE DEFINER=`shprocess`@`%` PROCEDURE `sp_sf_dw_task_log`(IN pi_task_name VARCHAR(256),IN pi_statedate VARCHAR(64),IN pi_loginfo VARCHAR(1024),IN stime DATETIME)
BEGIN
          DECLARE l_error_flag INT;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET l_error_flag=1; 
    INSERT INTO fe_dwd.dwd_sf_dw_task_log
      (task_name, 
       statedate,
       loginfo,
       start_time,
       run_time,
       run_time_second
       )
    VALUES
      (pi_task_name,
       pi_statedate,
       pi_loginfo,
       stime,
       ROUND(TIMESTAMPDIFF(SECOND,stime,CURRENT_TIMESTAMP)/60,1),
       TIMESTAMPDIFF(SECOND,stime,CURRENT_TIMESTAMP) );
       
-- 更新存储过程的执行状态
REPLACE INTO fe_dwd.dwd_project_excute_status(sdate,process_name,execute_status,load_time)
VALUES(CURRENT_DATE,pi_task_name,1,CURRENT_TIMESTAMP);
-- 判断该任务的前置任务今天是否已经执行失败了，查到记录说明前置任务已失败
SELECT COUNT(1) INTO @exists_flag FROM fe_dwd.`dwd_project_fail_delay_again_execute` WHERE dependent_project=pi_task_name  AND sdate=CURRENT_DATE;
-- 如果已经执行失败了，将更新相关的依赖此任务的执行时间信息
SET @rankk :=0;
UPDATE fe_dwd.`dwd_project_fail_delay_again_execute` a
JOIN (
      SELECT 
      IF(@dependent_project = dependent_project  , @rankk := @rankk + 1,@rankk := 0) AS rank,
      @project := project AS project,
      @dependent_project := dependent_project AS dependent_project
      FROM fe_dwd.`dwd_project_fail_delay_again_execute` WHERE dependent_project=pi_task_name AND sdate=CURRENT_DATE and project_repair_time is null
      ORDER BY d_start_time
      ) b
ON a.project=b.project AND a.dependent_project=b.dependent_project
SET project_repair_time=CURRENT_TIMESTAMP,
dp_will_start_time=CASE WHEN b.rank<=2 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 30 SECOND)
                        WHEN b.rank<=5 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 90 SECOND)
                        WHEN b.rank<=8 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 150 SECOND)
                        WHEN b.rank<=11 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 210 SECOND)
                        WHEN b.rank<=14 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 270 SECOND)
                   ELSE  ADDDATE(CURRENT_TIMESTAMP,INTERVAL 330 SECOND)
                   END  #设置并发数为3
WHERE a.dependent_project=pi_task_name AND a.sdate=CURRENT_DATE
AND @exists_flag; #标识今日该任务已经执行失败  没有则不更新  
-- 更新表级的数据更新状态
INSERT INTO fe_dwd.dwd_table_update_data_status(sdate,table_name,update_status)
SELECT CURRENT_DATE,
CONCAT(aim_base,'.',aim_table) AS table_name,
1 AS update_status
FROM fe_dwd.dwd_prc_project_process_source_aim_table_info 
WHERE PROCESS=pi_task_name   
AND LENGTH(aim_table)>1;
    COMMIT;
    END