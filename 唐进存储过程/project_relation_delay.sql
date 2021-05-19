CREATE DEFINER=`shprocess`@`%` PROCEDURE `project_relation_delay`(IN process_info VARCHAR(128))
BEGIN
SET @run_date1 := CURRENT_DATE(),@user := CURRENT_USER(),@stime1 := CURRENT_TIMESTAMP();
SET @wait_time1 := CURRENT_TIMESTAMP();
SET @wait_process := CONCAT('project_relation_delay@shprocess.',process_info);
SET @process_name=SUBSTRING_INDEX(process_info,'(',1);
-- 任务开始
INSERT INTO fe_dwd.dwd_project_execute_process_log(sdate,project,STATUS) VALUES(CURRENT_DATE,@process_name,'开始执行');
-- 任务等待
INSERT INTO fe_dwd.dwd_project_execute_process_log(sdate,project,STATUS) VALUES(CURRENT_DATE,@process_name,'判断状态，开始等待');
-- 查找依赖的任务数
SELECT COUNT(1) INTO @nums FROM (
    SELECT DISTINCT dependent_project,d_update_frequency,flag FROM (
        SELECT a.*,
        CASE WHEN a.d_update_frequency LIKE '%每日%' OR a.d_update_frequency LIKE '%每天%' THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周一%' AND WEEKDAY(CURRENT_DATE)=0 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周二%' AND WEEKDAY(CURRENT_DATE)=1 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周三%' AND WEEKDAY(CURRENT_DATE)=2 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周四%' AND WEEKDAY(CURRENT_DATE)=3 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周五%' AND WEEKDAY(CURRENT_DATE)=4 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周六%' AND WEEKDAY(CURRENT_DATE)=5 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周日%' AND WEEKDAY(CURRENT_DATE)=6 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月1号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月2号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-02') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月3号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-03') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月4号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-04') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月5号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-05') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月6号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-06') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月7号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-07') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月8号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-08') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月9号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-09') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月10号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-10') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月11号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-11') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月12号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-12') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月13号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-13') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月14号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-14') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月15号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-15') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月16号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-16') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月17号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-17') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月18号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-18') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月19号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-19') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月20号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-20') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月21号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-21') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月22号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-22') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月23号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-23') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月24号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-24') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月25号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-25') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月26号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-26') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月27号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-27') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月28号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-28') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月29号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-29') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月30号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-30') THEN 'true'
        ELSE 'false' END AS flag
        FROM  fe_dwd.`dwd_prc_project_relationship_detail_info` a 
        WHERE a.PROCESS= @process_name AND a.priority=1 AND a.dependent_project IS NOT NULL AND a.d_update_frequency NOT LIKE '%已停止%' AND a.dependent_project NOT IN ('dm_sc_current_dynamic_purchase_price_two','dm_op_shelf_product_fill_update2_his','dm_op_manager_product_trans_list','dwd_shelf_product_weeksales_detail_erp','dm_op_shelf_product_fill_suggest_label_erp','dm_op_shelf_product_start_fill_label_erp')
        ) w WHERE w.flag='true'
) t;
-- 求依赖任务的状态和
SELECT COUNT(1) INTO @total_nums FROM (
-- datax同步任务
SELECT 
CONCAT(b.job_desc,'_erp') AS datax_project_name,MIN(handle_time)
FROM fe_dwd.`dwd_datax_table_mapping_info` a
JOIN fe_datax.job_log b
ON SUBSTRING_INDEX(a.table_name_one,'.',-1)=b.job_desc
WHERE a.delete_flag=1
AND b.trigger_time>=CURRENT_DATE AND b.handle_code=200
AND a.datax_project_name IN (
SELECT DISTINCT dependent_project  
FROM (
        SELECT a.*,
        CASE WHEN a.d_update_frequency LIKE '%每日%' OR a.d_update_frequency LIKE '%每天%' THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周一%' AND WEEKDAY(CURRENT_DATE)=0 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周二%' AND WEEKDAY(CURRENT_DATE)=1 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周三%' AND WEEKDAY(CURRENT_DATE)=2 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周四%' AND WEEKDAY(CURRENT_DATE)=3 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周五%' AND WEEKDAY(CURRENT_DATE)=4 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周六%' AND WEEKDAY(CURRENT_DATE)=5 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周日%' AND WEEKDAY(CURRENT_DATE)=6 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月1号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月2号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-02') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月3号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-03') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月4号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-04') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月5号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-05') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月6号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-06') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月7号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-07') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月8号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-08') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月9号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-09') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月10号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-10') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月11号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-11') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月12号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-12') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月13号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-13') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月14号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-14') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月15号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-15') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月16号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-16') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月17号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-17') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月18号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-18') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月19号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-19') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月20号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-20') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月21号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-21') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月22号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-22') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月23号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-23') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月24号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-24') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月25号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-25') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月26号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-26') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月27号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-27') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月28号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-28') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月29号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-29') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月30号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-30') THEN 'true'
        ELSE 'false' END AS flag
        FROM  fe_dwd.`dwd_prc_project_relationship_detail_info` a 
        WHERE a.PROCESS= @process_name AND a.priority=1 AND a.dependent_project IS NOT NULL AND a.d_update_frequency NOT LIKE '%已停止%' AND a.dependent_project NOT IN ('dm_sc_current_dynamic_purchase_price_two','dm_op_shelf_product_fill_update2_his','dm_op_manager_product_trans_list','dwd_shelf_product_weeksales_detail_erp','dm_op_shelf_product_fill_suggest_label_erp','dm_op_shelf_product_start_fill_label_erp')
     ) w WHERE w.flag='true' AND dependent_project LIKE '%_erp%'
)
GROUP BY CONCAT(b.job_desc,'_erp')  -- 防止当天出现多次同步，只需要取最早一次同步
UNION ALL
-- azkaban调度任务
SELECT process_name,load_time FROM fe_dwd.`dwd_project_excute_status`
WHERE process_name IN (
SELECT DISTINCT dependent_project  
FROM (
        SELECT a.*,
        CASE WHEN a.d_update_frequency LIKE '%每日%' OR a.d_update_frequency LIKE '%每天%' THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周一%' AND WEEKDAY(CURRENT_DATE)=0 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周二%' AND WEEKDAY(CURRENT_DATE)=1 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周三%' AND WEEKDAY(CURRENT_DATE)=2 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周四%' AND WEEKDAY(CURRENT_DATE)=3 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周五%' AND WEEKDAY(CURRENT_DATE)=4 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周六%' AND WEEKDAY(CURRENT_DATE)=5 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周日%' AND WEEKDAY(CURRENT_DATE)=6 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月1号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月2号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-02') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月3号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-03') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月4号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-04') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月5号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-05') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月6号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-06') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月7号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-07') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月8号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-08') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月9号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-09') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月10号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-10') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月11号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-11') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月12号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-12') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月13号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-13') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月14号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-14') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月15号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-15') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月16号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-16') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月17号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-17') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月18号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-18') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月19号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-19') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月20号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-20') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月21号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-21') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月22号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-22') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月23号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-23') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月24号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-24') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月25号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-25') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月26号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-26') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月27号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-27') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月28号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-28') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月29号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-29') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月30号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-30') THEN 'true'
        ELSE 'false' END AS flag
        FROM  fe_dwd.`dwd_prc_project_relationship_detail_info` a 
        WHERE a.PROCESS= @process_name AND a.priority=1 AND a.dependent_project IS NOT NULL AND a.d_update_frequency NOT LIKE '%已停止%' AND a.dependent_project NOT IN ('dm_sc_current_dynamic_purchase_price_two','dm_op_shelf_product_fill_update2_his','dm_op_manager_product_trans_list','dwd_shelf_product_weeksales_detail_erp','dm_op_shelf_product_fill_suggest_label_erp','dm_op_shelf_product_start_fill_label_erp')
     ) w WHERE w.flag='true' AND dependent_project NOT LIKE '%_erp%'
 ) AND sdate=CURRENT_DATE AND execute_status=1  -- 表示已执行完
) u ;
SET @wait_total_time :=0;
-- 判断 ：如果任务数和状态数相等，表示前置依赖的任务都执行完了
WHILE @nums <> @total_nums AND @wait_total_time <=25200 DO   -- 设置总的等待时间不超过25200秒(即7个小时，留有充足的时间处理前置任务)
		
	#SELECT SLEEP(2);
    -- 等待1秒求依赖任务的状态和
SELECT COUNT(1) INTO @total_nums1 FROM (
-- datax同步任务
SELECT 
CONCAT(b.job_desc,'_erp') AS datax_project_name,MIN(handle_time)
FROM fe_dwd.`dwd_datax_table_mapping_info` a
JOIN fe_datax.job_log b
ON SUBSTRING_INDEX(a.table_name_one,'.',-1)=b.job_desc
WHERE a.delete_flag=1
AND b.trigger_time>=CURRENT_DATE AND b.handle_code=200
AND a.datax_project_name IN (
SELECT DISTINCT dependent_project  
FROM (
        SELECT a.*,
        CASE WHEN a.d_update_frequency LIKE '%每日%' OR a.d_update_frequency LIKE '%每天%' THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周一%' AND WEEKDAY(CURRENT_DATE)=0 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周二%' AND WEEKDAY(CURRENT_DATE)=1 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周三%' AND WEEKDAY(CURRENT_DATE)=2 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周四%' AND WEEKDAY(CURRENT_DATE)=3 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周五%' AND WEEKDAY(CURRENT_DATE)=4 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周六%' AND WEEKDAY(CURRENT_DATE)=5 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周日%' AND WEEKDAY(CURRENT_DATE)=6 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月1号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月2号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-02') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月3号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-03') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月4号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-04') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月5号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-05') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月6号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-06') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月7号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-07') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月8号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-08') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月9号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-09') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月10号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-10') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月11号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-11') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月12号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-12') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月13号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-13') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月14号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-14') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月15号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-15') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月16号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-16') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月17号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-17') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月18号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-18') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月19号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-19') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月20号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-20') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月21号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-21') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月22号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-22') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月23号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-23') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月24号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-24') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月25号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-25') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月26号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-26') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月27号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-27') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月28号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-28') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月29号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-29') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月30号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-30') THEN 'true'
        ELSE 'false' END AS flag
        FROM  fe_dwd.`dwd_prc_project_relationship_detail_info` a 
        WHERE a.PROCESS= @process_name AND a.priority=1 AND a.dependent_project IS NOT NULL AND a.d_update_frequency NOT LIKE '%已停止%' AND a.dependent_project NOT IN ('dm_sc_current_dynamic_purchase_price_two','dm_op_shelf_product_fill_update2_his','dm_op_manager_product_trans_list','dwd_shelf_product_weeksales_detail_erp','dm_op_shelf_product_fill_suggest_label_erp','dm_op_shelf_product_start_fill_label_erp')
     ) w WHERE w.flag='true' AND dependent_project LIKE '%_erp%'
)
GROUP BY CONCAT(b.job_desc,'_erp')  -- 防止当天出现多次同步，只需要取最早一次同步
UNION ALL
-- azkaban调度任务
SELECT process_name,load_time FROM fe_dwd.`dwd_project_excute_status`
WHERE process_name IN (
SELECT DISTINCT dependent_project  
FROM (
        SELECT a.*,
        CASE WHEN a.d_update_frequency LIKE '%每日%' OR a.d_update_frequency LIKE '%每天%' THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周一%' AND WEEKDAY(CURRENT_DATE)=0 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周二%' AND WEEKDAY(CURRENT_DATE)=1 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周三%' AND WEEKDAY(CURRENT_DATE)=2 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周四%' AND WEEKDAY(CURRENT_DATE)=3 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周五%' AND WEEKDAY(CURRENT_DATE)=4 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周六%' AND WEEKDAY(CURRENT_DATE)=5 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每周日%' AND WEEKDAY(CURRENT_DATE)=6 THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月1号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-01') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月2号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-02') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月3号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-03') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月4号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-04') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月5号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-05') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月6号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-06') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月7号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-07') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月8号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-08') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月9号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-09') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月10号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-10') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月11号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-11') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月12号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-12') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月13号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-13') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月14号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-14') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月15号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-15') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月16号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-16') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月17号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-17') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月18号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-18') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月19号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-19') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月20号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-20') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月21号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-21') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月22号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-22') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月23号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-23') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月24号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-24') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月25号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-25') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月26号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-26') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月27号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-27') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月28号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-28') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月29号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-29') THEN 'true'
        WHEN a.d_update_frequency LIKE '%每月30号%' AND CURRENT_DATE=DATE_FORMAT(CURRENT_DATE,'%Y-%m-30') THEN 'true'
        ELSE 'false' END AS flag
        FROM  fe_dwd.`dwd_prc_project_relationship_detail_info` a 
        WHERE a.PROCESS= @process_name AND a.priority=1 AND a.dependent_project IS NOT NULL AND a.d_update_frequency NOT LIKE '%已停止%' AND a.dependent_project NOT IN ('dm_sc_current_dynamic_purchase_price_two','dm_op_shelf_product_fill_update2_his','dm_op_manager_product_trans_list','dwd_shelf_product_weeksales_detail_erp','dm_op_shelf_product_fill_suggest_label_erp','dm_op_shelf_product_start_fill_label_erp')
     ) w WHERE w.flag='true' AND dependent_project NOT LIKE '%_erp%'
 ) AND sdate=CURRENT_DATE AND execute_status=1  -- 表示已执行完
) u ;
	  
	-- 判断该任务的前置任务今天是否已经执行失败了，查到记录说明前置任务已失败
    SELECT COUNT(1) INTO @exists_flag1 FROM fe_dwd.`dwd_project_fail_delay_again_execute` WHERE project=@process_name AND sdate=CURRENT_DATE;
	-- 如果涉及到多个依赖的任务，则根据最大的计划时间执行
	SELECT MAX(dp_will_start_time) INTO @will_start_time FROM fe_dwd.`dwd_project_fail_delay_again_execute` 
	WHERE project=@process_name AND @exists_flag1 AND sdate=CURRENT_DATE GROUP BY project;
	-- 如果前置依赖任务还没有处理完，则 @will_start_time为NULL或者没有记录，此时默认等待2秒钟，如果查到已处理完，则等待将计划执行离现在间隔的秒数
	SELECT SLEEP(TIMESTAMPDIFF(SECOND,CURRENT_TIMESTAMP,IF(IFNULL(@will_start_time,CURRENT_TIMESTAMP)>=CURRENT_TIMESTAMP,IFNULL(@will_start_time,CURRENT_TIMESTAMP),CURRENT_TIMESTAMP))+3);
	 
	SET @total_nums := @total_nums1;
	SET @wait_total_time := @wait_total_time + (TIMESTAMPDIFF(SECOND,CURRENT_TIMESTAMP,IF(IFNULL(@will_start_time,CURRENT_TIMESTAMP)>=CURRENT_TIMESTAMP,IFNULL(@will_start_time,CURRENT_TIMESTAMP),CURRENT_TIMESTAMP))+3);
	
END WHILE;
SET @wait_time2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info(@wait_process,"@wait_time1--@wait_time2",@wait_time1,@wait_time2);
-- 任务等待结束
INSERT INTO fe_dwd.dwd_project_execute_process_log(sdate,project,STATUS) VALUES(CURRENT_DATE,@process_name,'等待结束');
-- 开始执行业务存储过程
INSERT INTO fe_dwd.dwd_project_execute_process_log(sdate,project,STATUS) VALUES(CURRENT_DATE,@process_name,'开始执行业务存储过程');
-- 执行业务存储过程
SELECT CONCAT("call sh_process.", process_info) INTO @work_process;
PREPARE business_sql_exe FROM @work_process;
EXECUTE business_sql_exe;
DEALLOCATE PREPARE business_sql_exe;
-- 结束时修改状态
-- UPDATE fe_dwd.dwd_project_excute_status SET execute_status=1,load_time=CURRENT_TIMESTAMP  WHERE process_name=@process_name AND sdate=CURRENT_DATE;
-- 业务存储过程执行完成
INSERT INTO fe_dwd.dwd_project_execute_process_log(sdate,project,STATUS) VALUES(CURRENT_DATE,@process_name,'业务存储过程执行完成');
SET @wait_time3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info(@wait_process,"@wait_time2--@wait_time3",@wait_time2,@wait_time3);
-- 输出总的等待时间
-- SELECT @wait_total_time;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
@wait_process,
DATE_FORMAT(@run_date1, '%Y-%m-%d'),
CONCAT('唐进@', @user),
@stime1);
 
END