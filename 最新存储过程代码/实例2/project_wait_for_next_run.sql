CREATE DEFINER=`shprocess`@`%` PROCEDURE `project_wait_for_next_run`(in process_info varchar(128))
BEGIN
SET @run_date1 := CURRENT_DATE(),@user := CURRENT_USER(),@stime1 := CURRENT_TIMESTAMP();
SET @time_1 := CURRENT_TIMESTAMP();
set @wait_process := concat('project_wait_for_next_run@shprocess.',process_info);
set @process_name=substring_index(process_info,'(',1);
-- 查询依赖的任务执行情况 如果状态为0 继续等待
-- select status into @status from fe_dwd.dwd_project_excute_status where project='project_wait_for_next_run_test1' and sdate=current_date;
-- 查找依赖的任务数
SELECT COUNT(1) INTO @nums FROM (SELECT dependent_project FROM fe_dwd.`dwd_prc_project_relationship_detail_info` 
WHERE process= @process_name AND LENGTH(dependent_project)>4 AND dependent_project NOT LIKE '%erp') t;
-- 求依赖任务的状态和
SELECT SUM(execute_status) INTO @total_nums FROM fe_dwd.dwd_project_excute_status WHERE process_name IN 
(SELECT dependent_project FROM fe_dwd.`dwd_prc_project_relationship_detail_info` WHERE process= @process_name AND LENGTH(dependent_project)>4 AND dependent_project NOT LIKE '%erp' )
AND sdate=CURRENT_DATE;
-- 判断 ：如果任务数和状态数相等，表示前置依赖的任务都执行完了
WHILE @nums <> @total_nums DO 
		
	SELECT SLEEP(1);
    -- 等待1秒求依赖任务的状态和
    SELECT SUM(execute_status) INTO @total_nums1 FROM fe_dwd.dwd_project_excute_status WHERE process_name IN 
    (SELECT dependent_project FROM fe_dwd.`dwd_prc_project_relationship_detail_info` WHERE process= @process_name AND LENGTH(dependent_project)>4 AND dependent_project NOT LIKE '%erp' )
    AND sdate=CURRENT_DATE;
	SET @total_nums := @total_nums1;
	
END WHILE;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info(@wait_process,"@time_1--@time_2",@time_1,@time_2);
-- 执行业务存储过程
SELECT CONCAT("call sh_process.", process_info) INTO @work_process;
PREPARE sql_exe FROM @work_process;
EXECUTE sql_exe;
DEALLOCATE PREPARE sql_exe;
-- 结束时修改状态
UPDATE fe_dwd.dwd_project_excute_status SET execute_status=1,load_time=CURRENT_TIMESTAMP  WHERE process_name=@process_name AND sdate=CURRENT_DATE;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info(@wait_process,"@time_2--@time_3",@time_2,@time_3);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
@wait_process,
DATE_FORMAT(@run_date1, '%Y-%m-%d'),
CONCAT('唐进@', @user),
@stime1);
 
END