CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_prc_project_relationship_detail_info`()
BEGIN
/*
 Author: 唐进
 Create date: 2020/05/16
Modify date: 2020/10/16
 Description: 
 用于azkaban调度任务依赖关系维护
*/
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
TRUNCATE TABLE fe_dwd.`dwd_prc_project_relationship_detail_info` ;
INSERT INTO fe_dwd.`dwd_prc_project_relationship_detail_info` 
(project,PROCESS,maintainer,STATUS,update_frequency,start_time,last_start_time,last_end_time,
last_run_time,source_table,source_table_cname,dependent_project,d_maintainer,d_update_frequency,d_start_time,d_end_time,d_run_time)
SELECT a.project,
a.process,
k.maintainer AS maintainer,
CASE WHEN s.start_time IS NULL THEN '未部署'
     WHEN s.task_name IS NOT NULL THEN '已部署'
	 WHEN s.task_name IS NULL THEN '未部署'
	 ELSE '未部署' END AS STATUS,
s.update_frequency AS update_frequency,
s.start_time AS start_time,
s.last_start_time AS last_start_time,
s.last_end_time AS last_end_time,
ROUND(s.last_run_time/60,1) AS last_run_time,
a.source_table,
a.source_table_cname,
IFNULL(ak.project,d.datax_project_name) AS dependent_project,
IFNULL(kk.maintainer,(CASE WHEN d.datax_project_name IS NULL THEN '' ELSE '唐进' END)) AS d_maintainer,
IFNULL(ak.update_frequency,d.erp_frequency) AS d_update_frequency,
IFNULL(ak.last_start_time,d.start_time) AS d_start_time, 
IFNULL(ak.last_end_time,d.end_time) AS d_end_time,
ROUND(IFNULL(ak.last_run_time,d.run_time)/60,1) AS d_run_time
FROM (
-- azkaban调度任务主表 
SELECT project,PROCESS,CONCAT(source_base,'.',source_table) AS source_table ,source_table_cname
FROM fe_dwd.`dwd_prc_project_process_source_aim_table_info`  
WHERE LENGTH(source_table)>4
GROUP BY project,PROCESS,CONCAT(source_base,'.',source_table) ,source_table_cname
) a
LEFT JOIN (SELECT project,maintainer FROM fe_dwd.`dwd_prc_project_process_info` GROUP BY  project,maintainer) k
ON a.project=k.project
LEFT JOIN (
-- datax同步任务
SELECT 
a.datax_project_name,
#a.table_name_one,
a.table_name_two,
a.erp_frequency,
u.last_start_time AS start_time ,
u.last_end_time AS end_time,
TIMESTAMPDIFF(SECOND,u.last_start_time,u.last_end_time) AS run_time
FROM ( -- 主表为同步任务
      SELECT datax_project_name,SUBSTRING_INDEX(table_name_one,'.',-1) AS table_name_one,table_name_two,erp_frequency
      FROM fe_dwd.dwd_datax_table_mapping_info WHERE delete_flag=1 AND table_name_two NOT LIKE 'sserp.%'  -- 剔除掉表中从金蝶同步到实例1的sserp表
      ) a
LEFT JOIN (  -- 获取同步任务的耗时时间信息
            SELECT 
            CONCAT(a.job_desc,'_erp') AS datax_project_name,
            b.table_name_two,
            MAX(a.trigger_time) AS last_start_time,
            MAX(a.handle_time) AS last_end_time 
            FROM fe_datax.`job_log` a
            JOIN fe_dwd.`dwd_datax_table_mapping_info` b
            ON CONCAT(a.job_desc,'_erp')=b.datax_project_name WHERE b.delete_flag=1 AND a.handle_code=200
            GROUP BY CONCAT(job_desc,'_erp') ,b.table_name_two
			) u
		  ON a.datax_project_name=u.datax_project_name AND a.table_name_two=u.table_name_two
GROUP BY a.datax_project_name,a.table_name_two,a.erp_frequency 
) d
ON a.source_table=d.table_name_two
LEFT JOIN ( 
-- azkaban调度任务 run_time为分钟
SELECT  f.aim_table,f.project,s.update_frequency,s.last_start_time,s.last_end_time,s.last_run_time
FROM (
      SELECT aim_table,project
      FROM fe_dwd.dwd_prc_aim_table_to_process_project WHERE project<>'没有找到工程名'
      GROUP BY aim_table,project
      ) f 
 LEFT JOIN (
             SELECT 
             b.task_name,
              CASE WHEN DATE(b.last_run_date) NOT IN (CURRENT_DATE,SUBDATE(CURRENT_DATE,1)) AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 THEN '日任务已停止'
			       WHEN DATEDIFF(b.last_run_date,b.last_run_date2)=7 AND  DATEDIFF(CURRENT_DATE,b.last_run_date)>7 THEN '周任务已停止' 
			       WHEN DATEDIFF(CURRENT_DATE,b.last_run_date)>31 THEN '月任务已停止'
			       WHEN WEEKDAY(b.last_run_date) = 0 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周一' 
			       WHEN WEEKDAY(b.last_run_date) = 4 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周五' 
			       WHEN WEEKDAY(b.last_run_date) = 1 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周二' 
			       WHEN WEEKDAY(b.last_run_date) = 2 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周三' 
			       WHEN WEEKDAY(b.last_run_date) = 3 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周四' 
			       WHEN WEEKDAY(b.last_run_date) = 5 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周六' 
                   WHEN WEEKDAY(b.last_run_date) = 6 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周日'				   
                   WHEN DAY(b.last_run_date) = 1  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月1号' 
                   WHEN DAY(b.last_run_date) = 2  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月2号'  
				   WHEN DAY(b.last_run_date) = 3  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月3号' 
				   WHEN DAY(b.last_run_date) = 4  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月4号' 
				   WHEN DAY(b.last_run_date) = 5  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月5号' 
				   WHEN DAY(b.last_run_date) = 6  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月6号' 
				   WHEN DAY(b.last_run_date) = 7  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月7号' 
				   WHEN DAY(b.last_run_date) = 8  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月8号' 
				   WHEN DAY(b.last_run_date) = 9  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月9号' 
				   WHEN DAY(b.last_run_date) = 10  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月10号' 
				   WHEN DAY(b.last_run_date) = 11  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月11号'  
				   WHEN DAY(b.last_run_date) = 12  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月12号' 
				   WHEN DAY(b.last_run_date) = 13  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月13号' 
				   WHEN DAY(b.last_run_date) = 14  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月14号'  
				   WHEN DAY(b.last_run_date) = 15  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月15号' 
				   WHEN DAY(b.last_run_date) = 16  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月16号' 
				   WHEN DAY(b.last_run_date) = 17  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月17号'  
				   WHEN DAY(b.last_run_date) = 18  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月18号' 
				   WHEN DAY(b.last_run_date) = 19  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月19号' 
				   WHEN DAY(b.last_run_date) = 20  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月20号'  
				   WHEN DAY(b.last_run_date) = 21  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月21号'  
				   WHEN DAY(b.last_run_date) = 22  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月22号'  
				   WHEN DAY(b.last_run_date) = 23  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月23号'  
				   WHEN DAY(b.last_run_date) = 24  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月24号'  
				   WHEN DAY(b.last_run_date) = 25  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月25号'  
				   WHEN DAY(b.last_run_date) = 26  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月26号'  
				   WHEN DAY(b.last_run_date) = 27  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月27号'  
				   WHEN DAY(b.last_run_date) = 28  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月28号'  
				   WHEN DAY(b.last_run_date) = 29  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月29号'  
				   WHEN DAY(b.last_run_date) = 30  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月30号'  
				   WHEN DAY(b.last_run_date) = 31  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月31号'  
                   WHEN DATE(b.last_run_date) IN (CURRENT_DATE,SUBDATE(CURRENT_DATE,1)) AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 AND hour(last_start_time) =HOUR(last_start_time2) THEN '每日'
                   ELSE '根据执行日志表暂未判断出执行频率，待更新' END AS update_frequency,
             b.last_start_time,
             b.last_end_time,
             b.run_time AS last_run_time
             FROM (SELECT a.task_name,
             SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',1) AS last_run_date,
             SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',-1) AS last_run_date2,
             SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1) AS last_start_time,
			 SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',-1) AS last_start_time2,
             SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1) AS last_end_time,
             TIMESTAMPDIFF(SECOND,SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1),SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1)) AS run_time
             FROM (
                  SELECT task_name,
                  GROUP_CONCAT(statedate ORDER BY start_time DESC SEPARATOR '/') AS date_list,
                  GROUP_CONCAT(start_time ORDER BY start_time DESC SEPARATOR '/') AS start_time_list,
                  GROUP_CONCAT(end_time ORDER BY end_time DESC SEPARATOR '/') AS end_time_list
                  FROM (SELECT o.* FROM (
                                SELECT LEFT(start_time,10) AS statedate,task_name,LEFT(RIGHT(MIN(start_time),8),5) AS plan_time,
                                MIN(start_time) AS start_time,MIN(end_time) AS end_time 
                                FROM  fe_dwd.`dwd_sf_dw_task_log`   
                                WHERE loginfo LIKE '%shprocess%' AND start_time IS NOT NULL AND task_name NOT LIKE '%project_relation_delay%'
                                GROUP BY LEFT(start_time,10),task_name
                                ) o 
#                            JOIN (
#                                SELECT k.* FROM (
#                                SELECT p.task_name,p.plan_time,COUNT(1) AS ct FROM 
#                                (
#                                SELECT LEFT(start_time,10) AS statedate,task_name,LEFT(RIGHT(MIN(start_time),8),5) AS plan_time,
#                                MIN(start_time) AS start_time,MIN(end_time) AS end_time 
#                                FROM  fe_dwd.`dwd_sf_dw_task_log`   
#                                WHERE loginfo LIKE '%shprocess%' AND start_time IS NOT NULL AND task_name NOT LIKE '%project_relation_delay%'
#                                GROUP BY LEFT(start_time,10),task_name) p
#                                GROUP BY p.task_name,p.plan_time
#                                ) k WHERE k.ct>1
#                            ) q ON o.task_name=q.task_name AND o.plan_time=q.plan_time
                       ) t
             GROUP BY task_name ) a
             ) b 
          ) s
      ON f.project=s.task_name WHERE s.last_start_time IS NOT NULL
) ak
ON a.source_table=ak.aim_table
LEFT JOIN (SELECT project,maintainer FROM fe_dwd.`dwd_prc_project_process_info` GROUP BY  project,maintainer) kk
ON ak.project=kk.project
LEFT JOIN (
              -- 获取调度任务的执行时间信息
              SELECT 
              b.task_name,
			  SUBSTR(b.last_start_time,12,5) AS start_time,
              CASE WHEN DATE(b.last_run_date) NOT IN (CURRENT_DATE,SUBDATE(CURRENT_DATE,1)) AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 THEN '日任务已停止'
			       WHEN DATEDIFF(b.last_run_date,b.last_run_date2)=7 AND  DATEDIFF(CURRENT_DATE,b.last_run_date)>7 THEN '周任务已停止' 
			       WHEN DATEDIFF(CURRENT_DATE,b.last_run_date)>31 THEN '月任务已停止'
			       WHEN WEEKDAY(b.last_run_date) = 0 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周一' 
			       WHEN WEEKDAY(b.last_run_date) = 4 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周五' 
			       WHEN WEEKDAY(b.last_run_date) = 1 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周二' 
			       WHEN WEEKDAY(b.last_run_date) = 2 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周三' 
			       WHEN WEEKDAY(b.last_run_date) = 3 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周四' 
			       WHEN WEEKDAY(b.last_run_date) = 5 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周六' 
                   WHEN WEEKDAY(b.last_run_date) = 6 AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周日'				   
                   WHEN DAY(b.last_run_date) = 1  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月1号' 
                   WHEN DAY(b.last_run_date) = 2  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月2号'  
				   WHEN DAY(b.last_run_date) = 3  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月3号' 
				   WHEN DAY(b.last_run_date) = 4  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月4号' 
				   WHEN DAY(b.last_run_date) = 5  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月5号' 
				   WHEN DAY(b.last_run_date) = 6  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月6号' 
				   WHEN DAY(b.last_run_date) = 7  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月7号' 
				   WHEN DAY(b.last_run_date) = 8  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月8号' 
				   WHEN DAY(b.last_run_date) = 9  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月9号' 
				   WHEN DAY(b.last_run_date) = 10  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月10号' 
				   WHEN DAY(b.last_run_date) = 11  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月11号'  
				   WHEN DAY(b.last_run_date) = 12  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月12号' 
				   WHEN DAY(b.last_run_date) = 13  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月13号' 
				   WHEN DAY(b.last_run_date) = 14  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月14号'  
				   WHEN DAY(b.last_run_date) = 15  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月15号' 
				   WHEN DAY(b.last_run_date) = 16  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月16号' 
				   WHEN DAY(b.last_run_date) = 17  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月17号'  
				   WHEN DAY(b.last_run_date) = 18  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月18号' 
				   WHEN DAY(b.last_run_date) = 19  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月19号' 
				   WHEN DAY(b.last_run_date) = 20  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月20号'  
				   WHEN DAY(b.last_run_date) = 21  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月21号'  
				   WHEN DAY(b.last_run_date) = 22  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月22号'  
				   WHEN DAY(b.last_run_date) = 23  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月23号'  
				   WHEN DAY(b.last_run_date) = 24  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月24号'  
				   WHEN DAY(b.last_run_date) = 25  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月25号'  
				   WHEN DAY(b.last_run_date) = 26  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月26号'  
				   WHEN DAY(b.last_run_date) = 27  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月27号'  
				   WHEN DAY(b.last_run_date) = 28  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月28号'  
				   WHEN DAY(b.last_run_date) = 29  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月29号'  
				   WHEN DAY(b.last_run_date) = 30  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月30号'  
				   WHEN DAY(b.last_run_date) = 31  AND DATEDIFF(b.last_run_date,b.last_run_date2) IN (28,29,30,31) THEN '每月31号'  
                   WHEN DATE(b.last_run_date) IN (CURRENT_DATE,SUBDATE(CURRENT_DATE,1)) AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 AND HOUR(last_start_time) =HOUR(last_start_time2) THEN '每日'
                   ELSE '根据执行日志表暂未判断出执行频率，待更新' END AS update_frequency,
              b.last_start_time,
              b.last_end_time,
              b.run_time AS last_run_time
              FROM (SELECT a.task_name,
              SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',1) AS last_run_date,
              SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',-1) AS last_run_date2,
              SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1) AS last_start_time,
			  SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',-1) AS last_start_time2,
              SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1) AS last_end_time,
              TIMESTAMPDIFF(SECOND,SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1),SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1)) AS run_time
              FROM (
                   SELECT task_name,
                   GROUP_CONCAT(statedate ORDER BY start_time DESC SEPARATOR '/') AS date_list,
                   GROUP_CONCAT(start_time ORDER BY start_time DESC SEPARATOR '/') AS start_time_list,
                   GROUP_CONCAT(end_time ORDER BY end_time DESC SEPARATOR '/') AS end_time_list
                   FROM (SELECT o.* FROM (
                                SELECT LEFT(start_time,10) AS statedate,task_name,LEFT(RIGHT(MIN(start_time),8),5) AS plan_time,
                                MIN(start_time) AS start_time,MIN(end_time) AS end_time 
                                FROM  fe_dwd.`dwd_sf_dw_task_log`   
                                WHERE loginfo LIKE '%shprocess%' AND start_time IS NOT NULL AND task_name NOT LIKE '%project_relation_delay%'
                                GROUP BY LEFT(start_time,10),task_name
                                ) o 
#                            JOIN (
#                                SELECT k.* FROM (
#                                SELECT p.task_name,p.plan_time,COUNT(1) AS ct FROM 
#                                (
#                                SELECT LEFT(start_time,10) AS statedate,task_name,LEFT(RIGHT(MIN(start_time),8),5) AS plan_time,
#                                MIN(start_time) AS start_time,MIN(end_time) AS end_time 
#                                FROM  fe_dwd.`dwd_sf_dw_task_log`   
#                                WHERE loginfo LIKE '%shprocess%' AND start_time IS NOT NULL AND task_name NOT LIKE '%project_relation_delay%'
#                                GROUP BY LEFT(start_time,10),task_name) p
#                                GROUP BY p.task_name,p.plan_time
#                                ) k WHERE k.ct>1
#                            ) q ON o.task_name=q.task_name AND o.plan_time=q.plan_time
                        ) t
              GROUP BY task_name ) a
              ) b 
          ) s
ON a.project=s.task_name 
;
UPDATE fe_dwd.dwd_prc_project_relationship_detail_info a
JOIN fe_dwd.dwd_project_priority_with_table b
ON a.project=b.project AND a.source_table=b.source_table AND a.dependent_project=b.dependent_project
SET a.priority=b.priority;
UPDATE  fe_dwd.dwd_prc_project_relationship_detail_info a
SET a.priority=2
WHERE a.dependent_project IS NULL;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_prc_project_relationship_detail_info',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进@', @user),
@stime);
 
END