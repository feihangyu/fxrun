CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_project_relationship_info`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
truncate table feods.prc_project_relationship_info;
SET @sdate=DATE_ADD(CURDATE(),INTERVAL 1 DAY),
    @last_month=SUBDATE(DATE_FORMAT(@sdate,'%y-%m-01'),INTERVAL 3 DAY),
    @days=DATEDIFF(@sdate,@last_month);
insert into feods.prc_project_relationship_info(project,update_detail,update_frequency,dependent_project,d_update_detail,d_update_frequency)
SELECT 
a.project AS project,
dd.update_frequence AS update_detail,
dd.update_detail AS update_frequency,
b.project  AS dependent_project,
cc.update_frequence AS d_update_detail,
cc.update_detail AS d_update_frequency
FROM feods.`prc_project_process_source_aim_table_info` a 
LEFT JOIN 
       (SELECT * FROM feods.`prc_project_process_source_aim_table_info` WHERE aim_base in ('feods','fe_dwd','fe_dm') AND aim_table IN 
         (SELECT source_table FROM feods.`prc_project_process_source_aim_table_info` WHERE  source_base in ('feods','fe_dwd','fe_dm') GROUP BY source_table)
       ) b   -- 获取源表对应的工程
  ON a.source_base=b.aim_base AND a.source_table=b.aim_table 
LEFT JOIN 
      ( -- 统计近30天工程的执行信息
        SELECT 
         aa.*
        ,CASE WHEN DATEDIFF(aa.day1,aa.day2)=1 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 THEN '每日'
              WHEN DATEDIFF(aa.day1,aa.day2)=7 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 THEN '每周'
              WHEN ROUND(DATEDIFF(aa.day1,aa.day2)/30,0)=1 THEN '每月'
              WHEN DATEDIFF(aa.day1,aa.day2)=1 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs >0 THEN '已停止' 
        	  WHEN DATEDIFF(aa.day1,aa.day2)=7 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs >0 THEN '已停止' else '已停止'
         END AS update_frequence  
        ,CASE WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=1 THEN '每日' 
              WHEN ROUND(DATEDIFF(aa.day1,aa.day2)/30,0) =1 THEN CONCAT('每月',DAYOFMONTH(aa.day1),'号')
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=1 THEN '每周7'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=2 THEN '每周1'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=3 THEN '每周2'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=4 THEN '每周3'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=5 THEN '每周4'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=6 THEN '每周5'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=7 THEN '每周6'
              ELSE '已停止'
         END AS update_detail  
        FROM 
          (SELECT b.name 
          ,GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ",") AS sdays
          ,SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",1) AS day1
          ,SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",2),",",-1) AS day2
          ,DATEDIFF(
          MAX(DATE(FROM_UNIXTIME(ROUND(a.start_time/1000)))),
          SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",2),",",-1))
          AS diffs
          FROM azkaban.`execution_flows` a
          JOIN azkaban.`projects` b
          ON a.`project_id` = b.`id`
          WHERE FROM_UNIXTIME(ROUND(a.start_time/1000)) >= '2019-10-01'
          GROUP BY b.name
          ) aa
     ) dd 
  ON a.project=dd.name 
LEFT JOIN 
      (
       SELECT 
          aa.*
         ,CASE WHEN DATEDIFF(aa.day1,aa.day2)=1 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 THEN '每日'
               WHEN DATEDIFF(aa.day1,aa.day2)=7 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 THEN '每周'
               WHEN ROUND(DATEDIFF(aa.day1,aa.day2)/30,0)=1 THEN '每月'
               WHEN DATEDIFF(aa.day1,aa.day2)=1 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs >0 THEN '已停止' 
         	   WHEN DATEDIFF(aa.day1,aa.day2)=7 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs >0 THEN '已停止' else '已停止'
          END AS update_frequence  
         ,CASE WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=1 THEN '每日' 
               WHEN ROUND(DATEDIFF(aa.day1,aa.day2)/30,0) =1 THEN CONCAT('每月',DAYOFMONTH(aa.day1),'号')
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=1 THEN '每周7'
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=2 THEN '每周1'
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=3 THEN '每周2'
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=4 THEN '每周3'
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=5 THEN '每周4'
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=6 THEN '每周5'
               WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=7 THEN '每周6'
               ELSE '已停止'
          END AS update_detail  
         FROM 
           (SELECT b.name 
           ,GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ",") AS sdays
           ,SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",1) AS day1
           ,SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",2),",",-1) AS day2
           ,DATEDIFF(
           MAX(DATE(FROM_UNIXTIME(ROUND(a.start_time/1000)))),
           SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",2),",",-1))
           AS diffs
           FROM azkaban.`execution_flows` a
           JOIN azkaban.`projects` b
           ON a.`project_id` = b.`id`
           WHERE FROM_UNIXTIME(ROUND(a.start_time/1000)) >= '2019-10-01'
           GROUP BY b.name
           ) aa
      ) cc
ON b.project=cc.name 
WHERE b.project<>'没有找到工程名'  #AND a.project='pj_area_sale_dashboard'
GROUP BY a.project,b.project;
-- 插入没有依赖的工程
insert into feods.prc_project_relationship_info(project,update_detail,update_frequency,dependent_project)
select
a.project AS project,
dd.update_frequence AS update_detail,
dd.update_detail AS update_frequency,
'无' as dependent_project
FROM feods.`prc_project_process_source_aim_table_info` a 
LEFT JOIN 
       (SELECT * FROM feods.`prc_project_process_source_aim_table_info` WHERE aim_base='feods' AND aim_table IN 
         (SELECT source_table FROM feods.`prc_project_process_source_aim_table_info` WHERE  source_base='feods' GROUP BY source_table)
       ) b   -- 获取源表对应的工程
  ON a.source_base=b.aim_base AND a.source_table=b.aim_table 
LEFT JOIN 
      (
        SELECT 
         aa.*
        ,CASE WHEN DATEDIFF(aa.day1,aa.day2)=1 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 THEN '每日'
              WHEN DATEDIFF(aa.day1,aa.day2)=7 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 THEN '每周'
              WHEN ROUND(DATEDIFF(aa.day1,aa.day2)/30,0)=1 THEN '每月'
              WHEN DATEDIFF(aa.day1,aa.day2)=1 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs >0 THEN '已停止' 
        	  WHEN DATEDIFF(aa.day1,aa.day2)=7 AND DATEDIFF(CURDATE(),aa.day1)-aa.diffs >0 THEN '已停止' else '已停止'
         END AS update_frequence  
        ,CASE WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=1 THEN '每日' 
              WHEN ROUND(DATEDIFF(aa.day1,aa.day2)/30,0) =1 THEN CONCAT('每月',DAYOFMONTH(aa.day1),'号')
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=1 THEN '每周7'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=2 THEN '每周1'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=3 THEN '每周2'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=4 THEN '每周3'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=5 THEN '每周4'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=6 THEN '每周5'
              WHEN DATEDIFF(CURDATE(),aa.day1)-aa.diffs <=0 AND DATEDIFF(aa.day1,aa.day2)=7  AND DAYOFWEEK(aa.day1)=7 THEN '每周6'
              ELSE '已停止'
         END AS update_detail  
        FROM 
          (SELECT b.name 
          ,GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ",") AS sdays
          ,SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",1) AS day1
          ,SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",2),",",-1) AS day2
          ,DATEDIFF(
          MAX(DATE(FROM_UNIXTIME(ROUND(a.start_time/1000)))),
          SUBSTRING_INDEX(SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT DATE(FROM_UNIXTIME(ROUND(a.start_time/1000))) ORDER BY a.start_time DESC SEPARATOR ","),",",2),",",-1))
          AS diffs
          FROM azkaban.`execution_flows` a
          JOIN azkaban.`projects` b
          ON a.`project_id` = b.`id`
          WHERE FROM_UNIXTIME(ROUND(a.start_time/1000)) >= '2019-10-01'
          GROUP BY b.name
          ) aa
     ) dd 
  ON a.project=dd.name 
  WHERE b.project='没有找到工程名' OR b.project IS NULL
  GROUP BY a.project;
-- 删除工程 与依赖工程都是自身的记录
delete from feods.prc_project_relationship_info where project=dependent_project or project='没有找到工程名';
-- 取最近一条数据
drop  temporary table if exists feods.project_info;
create temporary table feods.project_info
SELECT 
name,
start_time,
end_time,
run_time_minute
FROM  (
      SELECT b.name ,FROM_UNIXTIME(ROUND(a.start_time/1000)) AS 'start_time',FROM_UNIXTIME(ROUND(a.end_time/1000)) AS 'end_time',ROUND((a.end_time-a.start_time)/1000/60,1) AS 'run_time_minute'
      FROM azkaban.`execution_flows` a
      JOIN azkaban.`projects` b
      ON a.`project_id` = b.`id`
      WHERE FROM_UNIXTIME(ROUND(a.start_time/1000)) >= @last_month and FROM_UNIXTIME(ROUND(a.start_time/1000)) < @sdate
      ORDER BY start_time DESC
      ) aa
GROUP BY name ;
-- 更新工程的当日运行时间
update feods.prc_project_relationship_info a 
left join feods.project_info b 
on a.project=b.name
set a.start_time_day=b.start_time,
a.end_time_day=b.end_time,
a.run_time=b.run_time_minute;
-- 更新依赖工程的运行时间
update feods.prc_project_relationship_info a 
join feods.project_info b 
on a.dependent_project=b.name
set a.d_start_time=b.start_time,
a.d_end_time=b.end_time;
-- 设置工程的开始时间
update feods.prc_project_relationship_info
set start_time=substr(start_time_day,12,5);
-- 设置是否正确的依赖关系
-- 设置工程的开始时间
update feods.prc_project_relationship_info
SET is_true=(CASE when dependent_project='无' THEN '是'
                  WHEN update_frequency=d_update_frequency and start_time_day > d_end_time OR dependent_project='无' THEN '是'   -- 针对于都是同一天更新的
                  WHEN update_frequency>d_update_frequency and update_detail<>d_update_detail and substr(start_time_day,12,8) > substr(d_end_time,12,8) THEN '是'   -- 针对于 每日> 每周 或者 每周2>每周1 
				  WHEN update_frequency<d_update_frequency and update_detail<>d_update_detail and substr(start_time_day,12,8) > substr(d_end_time,12,8) THEN '是'   -- 针对于 每日> 每周 或者 每周2>每周1 
				  WHEN update_frequency>d_update_frequency and update_detail=d_update_detail and update_detail='每周' THEN '是'   -- 针对于 每日> 每周 或者 每周2>每周1 
				  ELSE '否' END);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_project_relationship_info',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('唐进@', @user, @timestamp));
    END