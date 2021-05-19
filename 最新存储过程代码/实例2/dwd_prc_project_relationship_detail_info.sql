CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_prc_project_relationship_detail_info`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
TRUNCATE TABLE fe_dwd.`dwd_prc_project_relationship_detail_info` ;
INSERT INTO fe_dwd.`dwd_prc_project_relationship_detail_info` 
(project,PROCESS,maintainer,STATUS,update_frequency,start_time,last_start_time,last_end_time,
last_run_time,source_table,source_table_cname,dependent_project,d_maintainer,d_update_frequency,d_start_time,d_end_time,d_run_time)
SELECT a.project,
a.process,
k.maintainer AS maintainer,
CASE WHEN s.start_time IS NULL THEN '未部署'
     WHEN s.start_time <='09:00' OR s.start_time >= '22:30' THEN '已部署调度'
	 WHEN s.update_frequency LIKE '每%' THEN '已部署调度'
	 ELSE '未部署或未调度' END AS STATUS,
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
IFNULL(MAX(b.start_time),IFNULL(c.start_time,d.start_time)) AS start_time ,
IFNULL(MAX(b.end_time),IFNULL(c.end_time,d.end_time)) AS end_time,
TIMESTAMPDIFF(SECOND,IFNULL(MAX(b.start_time),IFNULL(c.start_time,d.start_time)),IFNULL(MAX(b.end_time),IFNULL(c.end_time,d.end_time))) AS run_time
FROM ( -- 主表为同步任务
      SELECT datax_project_name,SUBSTRING_INDEX(table_name_one,'.',-1) AS table_name_one,table_name_two,erp_frequency
      FROM fe_dwd.dwd_datax_table_mapping_info WHERE delete_flag=1
      ) a
LEFT JOIN fe_dwd.`dwd_datax_excute_info_detective` b   -- 获取同步任务为每天的
     ON a.datax_project_name=b.datax_project_name AND a.table_name_one=b.datax_table_name AND b.sdate=CURRENT_DATE
LEFT JOIN (-- 获取拆分子同步的表
         SELECT datax_project_name,
         MIN(start_time) AS start_time,
         MAX(load_time) AS end_time,
         TIMESTAMPDIFF(SECOND,MIN(start_time),MAX(load_time)) AS run_time
         FROM fe_dwd.`dwd_datax_excute_info_detective` WHERE sdate=CURRENT_DATE AND datax_project_name IN('dwd_shelf_product_day_all_erp','datax_d_op_sp_avgsal30_erp','datax_sp_d_sc_shelf_package_erp')
         GROUP BY datax_project_name
         ) c
     ON a.datax_project_name=c.datax_project_name 
LEFT JOIN ( -- 获取每周一同步的
          SELECT 
          datax_project_name,
          CASE WHEN datax_table_name IN ('zs_shelf_member_flag_part01','zs_shelf_member_flag_part02') THEN 'zs_shelf_member_flag'
               WHEN datax_table_name IN ('zs_shelf_member_flag_history01','zs_shelf_member_flag_history02') THEN 'zs_shelf_member_flag_history'
               ELSE datax_table_name END AS datax_table_name,
          MIN(start_time) AS start_time,
          MAX(end_time) AS end_time,
          TIMESTAMPDIFF(SECOND,MIN(start_time),MAX(end_time)) AS run_time
          FROM fe_dwd.`dwd_datax_excute_info_detective` 
          WHERE datax_project_name IN ('zs_shelf_member_flag_erp','zs_area_product_sale_flag_erp','dwd_city_business_erp')
          AND sdate=SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE))
          GROUP BY datax_project_name,
          CASE WHEN datax_table_name IN ('zs_shelf_member_flag_part01','zs_shelf_member_flag_part02') THEN 'zs_shelf_member_flag' 
               WHEN datax_table_name IN ('zs_shelf_member_flag_history01','zs_shelf_member_flag_history02') THEN 'zs_shelf_member_flag_history' 
			   ELSE datax_table_name END
          ) d
       ON a.datax_project_name=d.datax_project_name AND a.table_name_one=d.datax_table_name
GROUP BY a.datax_project_name,a.table_name_one,a.table_name_two,a.erp_frequency 
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
             CASE WHEN LEFT(b.last_run_date,10) = SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周一' 
			      WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+3)
                  WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周五' 
			      WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+6)
                  WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周二' 
			      WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+5)
                  WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周三' 
			      WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+4)
                  WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周四' 
			      WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+2)
                  WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周六' 
                  WHEN LEFT(b.last_run_date,10) = SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+1) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周日' 
                  WHEN LEFT(b.last_run_date,10) = DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN '每月1号' 
                  WHEN LEFT(b.last_run_date,10) = DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-30') THEN '每月30号' 
                  WHEN LEFT(b.last_run_date,10) = DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-29') THEN '每月29号' 
                  WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND LEFT(b.last_run_date,10)=CURRENT_DATE AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 THEN '每日'
			      WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND LEFT(b.last_run_date,10)<>CURRENT_DATE AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 THEN '日任务已停止'
			      WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 AND  DATEDIFF(CURRENT_DATE,b.last_run_date)>7 THEN '周任务已停止' 
			      WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND DATEDIFF(CURRENT_DATE,b.last_run_date)>31 THEN '月任务已停止'
                  ELSE '未部署或暂时未调度' END AS update_frequency,
             b.last_start_time,
             b.last_end_time,
             b.run_time AS last_run_time
             FROM (SELECT a.task_name,
             SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',1) AS last_run_date,
             SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',-1) AS last_run_date2,
             SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1) AS last_start_time,
             SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1) AS last_end_time,
             TIMESTAMPDIFF(SECOND,SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1),SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1)) AS run_time
             FROM (
                  SELECT task_name,
                  GROUP_CONCAT(statedate ORDER BY start_time DESC SEPARATOR '/') AS date_list,
                  GROUP_CONCAT(start_time ORDER BY start_time DESC SEPARATOR '/') AS start_time_list,
                  GROUP_CONCAT(end_time ORDER BY end_time DESC SEPARATOR '/') AS end_time_list
                  FROM (SELECT LEFT(start_time,10) AS statedate,task_name,MIN(start_time) AS start_time,MIN(end_time) AS end_time 
                  FROM  fe_dwd.`dwd_sf_dw_task_log` 
                  WHERE loginfo LIKE '%shprocess%' AND start_time IS NOT NULL GROUP BY LEFT(start_time,10),task_name 
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
              CASE WHEN LEFT(b.last_run_date,10) = SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周一' 
			       WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+3)
                   WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周五' 
			       WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+6)
                   WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周二' 
			       WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+5)
                   WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周三' 
			       WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+4)
                   WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周四' 
			       WHEN LEFT(b.last_run_date,10) =(CASE WHEN CURRENT_DATE<SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+2)
                   WHEN CURRENT_DATE>=SUBDATE(b.last_run_date,-7) THEN SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)-WEEKDAY(SUBDATE(b.last_run_date,-7))) END) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周六' 
                   WHEN LEFT(b.last_run_date,10) = SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE)+1) AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 THEN '每周日'				   
                   WHEN LEFT(b.last_run_date,10) = DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN '每月1号' 
                   WHEN LEFT(b.last_run_date,10) = DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-30') THEN '每月30号' 
                   WHEN LEFT(b.last_run_date,10) = DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-29') THEN '每月29号' 
                   WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND LEFT(b.last_run_date,10)=CURRENT_DATE AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 THEN '每日'
			       WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND LEFT(b.last_run_date,10)<>CURRENT_DATE AND DATEDIFF(b.last_run_date,b.last_run_date2)=1 THEN '日任务已停止'
			       WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND DATEDIFF(b.last_run_date,b.last_run_date2)=7 AND  DATEDIFF(CURRENT_DATE,b.last_run_date)>7 THEN '周任务已停止' 
			       WHEN SUBSTR(b.last_start_time,12,5) < '10:00' AND DATEDIFF(CURRENT_DATE,b.last_run_date)>31 THEN '月任务已停止'
                   ELSE '未部署或暂时未调度' END AS update_frequency,
              b.last_start_time,
              b.last_end_time,
              b.run_time AS last_run_time
              FROM (SELECT a.task_name,
              SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',1) AS last_run_date,
              SUBSTRING_INDEX(SUBSTRING_INDEX(date_list,'/',2),'/',-1) AS last_run_date2,
              SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1) AS last_start_time,
              SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1) AS last_end_time,
              TIMESTAMPDIFF(SECOND,SUBSTRING_INDEX(SUBSTRING_INDEX(start_time_list,'/',2),'/',1),SUBSTRING_INDEX(SUBSTRING_INDEX(end_time_list,'/',2),'/',1)) AS run_time
              FROM (
                   SELECT task_name,
                   GROUP_CONCAT(statedate ORDER BY start_time DESC SEPARATOR '/') AS date_list,
                   GROUP_CONCAT(start_time ORDER BY start_time DESC SEPARATOR '/') AS start_time_list,
                   GROUP_CONCAT(end_time ORDER BY end_time DESC SEPARATOR '/') AS end_time_list
                   FROM (SELECT LEFT(start_time,10) AS statedate,task_name,MIN(start_time) AS start_time,MIN(end_time) AS end_time 
                   FROM  fe_dwd.`dwd_sf_dw_task_log` 
                   WHERE loginfo LIKE '%shprocess%' AND start_time IS NOT NULL GROUP BY LEFT(start_time,10),task_name 
                   ) t
              GROUP BY task_name ) a
              ) b 
          ) s
ON a.project=s.task_name ;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_prc_project_relationship_detail_info',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进@', @user),
@stime);
 
END