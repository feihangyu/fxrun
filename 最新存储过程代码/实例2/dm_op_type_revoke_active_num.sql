CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_type_revoke_active_num`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET  @month_id := DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m');
DELETE FROM fe_dm.dm_op_type_revoke_active_num WHERE month_id = @month_id;	   
INSERT INTO fe_dm.dm_op_type_revoke_active_num 
(month_id      
,business_name 
,zone_name
,shelf_type    
,ACTIVATE_m    
,revoke_m      
,still_m 
,change_m 
,revoke_all  
,revoke_rate
,one_revoke   
,two_revoke   
,three_revoke     
,load_time     
) 
SELECT 
   @month_id AS month_id
  ,s.business_name
  ,s.zone_name
  ,d1.item_name AS shelf_type    
  ,SUM(CASE WHEN s.ACTIVATE_TIME>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.ACTIVATE_TIME<CURRENT_DATE AND s.shelf_status NOT IN (1,10) THEN 1 ELSE 0 END) AS ACTIVATE_m
  ,SUM(CASE WHEN (s.REVOKE_TIME>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.REVOKE_TIME<CURRENT_DATE AND a.shelf_id IS NULL) THEN 1 ELSE 0 END) AS revoke_m
  ,SUM(CASE WHEN s.SHELF_STATUS IN (2,5) THEN 1 ELSE 0 END) AS still_m 
  ,COUNT(t.shelf_id) AS change_num -- 换架量
  ,SUM(CASE WHEN (s.REVOKE_TIME>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.REVOKE_TIME<CURRENT_DATE) THEN 1 ELSE 0 END) AS revoke_all
  ,ROUND(SUM(CASE WHEN (s.REVOKE_TIME>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.REVOKE_TIME<CURRENT_DATE AND a.shelf_id IS NULL) THEN 1 ELSE 0 END)/ IF(SUM(CASE WHEN s.SHELF_STATUS IN (2,5) THEN 1 ELSE 0 END) + SUM(CASE WHEN (s.REVOKE_TIME>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.REVOKE_TIME<CURRENT_DATE) THEN 1 ELSE 0 END) = 0,1,SUM(CASE WHEN s.SHELF_STATUS IN (2,5) THEN 1 ELSE 0 END) + SUM(CASE WHEN (s.REVOKE_TIME>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.REVOKE_TIME<CURRENT_DATE) THEN 1 ELSE 0 END)),3) AS revoke_rate
  ,SUM(CASE WHEN s.revoke_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.revoke_time<CURRENT_DATE 
   AND s.activate_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.activate_time<CURRENT_DATE
   THEN 1 ELSE 0 END) AS one_revoke
  ,SUM(CASE WHEN s.revoke_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.revoke_time<CURRENT_DATE 
   AND s.activate_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-01') AND s.activate_time<DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01')
   THEN 1 ELSE 0 END) AS two_revoke
  ,SUM(CASE WHEN s.revoke_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.revoke_time<CURRENT_DATE 
   AND s.activate_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 2 MONTH),'%Y-%m-01') AND s.activate_time<DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 MONTH),'%Y-%m-01')
   THEN 1 ELSE 0 END) AS three_revoke
  ,CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.dwd_shelf_base_day_all s	##替换表	
LEFT JOIN fe_dwd.`dwd_pub_dictionary` d1 ON (s.SHELF_TYPE= d1.item_value AND d1.dictionary_id=8)  -- 获取货架类型
LEFT JOIN  -- 撤架id
   (SELECT DISTINCT a.shelf_id 
    FROM fe_dwd.dwd_sf_shelf_revoke a   -- 货架撤销
    JOIN fe_dwd.dwd_shelf_base_day_all s          -- 货架信息
        ON (s.shelf_id=a.shelf_id AND s.data_flag=1)
    JOIN 
      (SELECT
         refer_id                  -- 关联功能主键
		,item_result               -- 操作结果
	   FROM fe_dwd.dwd_sf_operate_result   -- 操作结果表
       WHERE data_flag = 1
         AND func_type = 1         -- 功能类型：1-撤架管理
         AND operate_type = 1      -- 操作类型：1-审核通过,2-审核不通过
         AND item_id = 1           -- 对应操作项主键
         AND item_result = 11    -- item_id = 1 AND item_result = 11 表示 审核后撤架原因是更换自动贩卖机或智能柜
	   )r
         ON a.REVOKE_ID=r.refer_id
      WHERE a.data_flag=1 AND a.AUDIT_STATUS NOT IN (3, 4) AND s.revoke_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND s.REVOKE_TIME<CURRENT_DATE  -- 新增 a.data_flag=1
   )a
   ON s.shelf_id=a.shelf_id
    LEFT JOIN
 (SELECT tc.shelf_id
 FROM fe_dwd.`dwd_sf_shelf_logistics_task_change` tc 
 JOIN  fe_dwd.`dwd_sf_shelf_logistics_task` t ON tc.logistics_task_id=t.logistics_task_id
 WHERE tc.data_flag=1 
 AND t.task_type=3 -- 任务类型-换架
 AND t.task_status=5 -- 任务状态-已完成
 AND t.execute_result=1 -- 执行结果-成功
 AND tc.new_shelf_type=6
 AND t.execute_finish_time>=DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m-01') AND t.execute_finish_time<CURRENT_DATE)t
 ON s.shelf_id=t.shelf_id
GROUP BY s.business_name,s.zone_name,d1.item_name; 
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_type_revoke_active_num',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（李吹防）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_type_revoke_active_num','dm_op_type_revoke_active_num','李吹防');
  COMMIT;	
END