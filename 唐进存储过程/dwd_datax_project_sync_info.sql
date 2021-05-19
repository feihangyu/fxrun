CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_datax_project_sync_info`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
TRUNCATE TABLE fe_dwd.dwd_datax_project_sync_info;
INSERT INTO fe_dwd.dwd_datax_project_sync_info
SELECT  DATE(MAX(b.trigger_time)) AS sdate,
CONCAT(b.job_desc,'_erp') AS datax_project_name,a.table_name_one  ,a.table_name_two  ,
MAX(b.trigger_time) AS last_start_time,
MAX(b.handle_time) AS last_end_time,
TIMESTAMPDIFF(SECOND,MAX(b.trigger_time),MAX(b.handle_time)) AS last_run_time,
a.erp_frequency ,
a.remark,
CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.`dwd_datax_table_mapping_info` a
JOIN fe_datax.job_log b
ON SUBSTRING_INDEX(a.table_name_one,'.',-1)=b.job_desc
AND  b.handle_code=200  #b.trigger_time>=CURRENT_DATE AND
#AND a.table_name_two='fe_dwd.dwd_product_package'  -- 实例2表名
AND a.delete_flag=1 AND a.table_name_one  NOT LIKE 'feng1.%'
GROUP BY CONCAT(b.job_desc,'_erp');
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_datax_project_sync_info',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进@', @user),
@stime);
END