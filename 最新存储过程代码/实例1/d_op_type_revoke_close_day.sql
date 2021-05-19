CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_op_type_revoke_close_day`()
BEGIN
   
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();  	
	
SET @sdate=SUBDATE(CURRENT_DATE,1);
	
-- 删除昨天的数据
DELETE FROM feods.d_op_type_revoke_close_day WHERE sdate=@sdate;
	
#每天货架数据情况	
INSERT INTO feods.d_op_type_revoke_close_day(
     sdate        
    ,business_name
    ,shelf_type   
    ,activate_num 
    ,revoke_num   
    ,still_num 
    ,revoke_all   
    ,main_close   
    ,second_close 
    ,load_time)    
SELECT 	
   @sdate sdate, -- 日期	
   s.business_name -- 地区	
  ,d1.item_name AS shelf_type   -- 货架类型	
  ,SUM(CASE WHEN s.ACTIVATE_TIME>=SUBDATE(CURRENT_DATE,1) AND s.ACTIVATE_TIME<CURRENT_DATE AND s.shelf_status NOT IN (1,10) THEN 1 ELSE 0 END) AS activate_num -- 激活数量	
  ,SUM(CASE WHEN (s.REVOKE_TIME>=SUBDATE(CURRENT_DATE,1) AND s.REVOKE_TIME<CURRENT_DATE AND a.shelf_id IS NULL) THEN 1 ELSE 0 END) AS revoke_num -- 撤架数量（剔除更换自贩机智能柜的数据）	
  ,SUM(CASE WHEN s.SHELF_STATUS IN (2,5) THEN 1 ELSE 0 END) AS still_num   -- 留存量	
  ,SUM(CASE WHEN s.REVOKE_TIME>=SUBDATE(CURRENT_DATE,1) AND s.REVOKE_TIME<CURRENT_DATE THEN 1 ELSE 0 END) AS revoke_all -- 所有撤架数量	
  ,SUM(CASE WHEN s.SHELF_STATUS=2 AND s.whether_close=1 THEN 1 ELSE 0 END) AS main_close   -- 关闭的主货架量	
  ,SUM(CASE WHEN s.SHELF_STATUS=2 AND s.whether_close=1 THEN IFNULL(s.bind_cnt,0) ELSE 0 END) AS second_close -- 关闭的次货架量	
  ,CURRENT_TIMESTAMP AS load_time	
FROM fe_dwd.dwd_shelf_base_day_all s	##替换表	
LEFT JOIN fe_dwd.`dwd_pub_dictionary` d1 ON (s.SHELF_TYPE= d1.item_value AND d1.dictionary_id=8)  -- 获取货架类型	
LEFT JOIN  -- 撤架id	
   (SELECT DISTINCT a.shelf_id 	
    FROM fe.sf_shelf_revoke a   -- 货架撤销	
    JOIN fe.sf_shelf s          -- 货架信息	
        ON (s.shelf_id=a.shelf_id AND s.data_flag=1)	
    JOIN 	
      (SELECT	
         refer_id                  -- 关联功能主键	
         ,item_result               -- 操作结果	
	   FROM fe.sf_operate_result   -- 操作结果表
       WHERE data_flag = 1	
         AND func_type = 1         -- 功能类型：1-撤架管理	
         AND operate_type = 1      -- 操作类型：1-审核通过,2-审核不通过	
         AND item_id = 1           -- 对应操作项主键	
         AND item_result = 11    -- item_id = 1 AND item_result = 11 表示 审核后撤架原因是更换自动贩卖机或智能柜	
	   )r
         ON a.REVOKE_ID=r.refer_id	
      WHERE a.data_flag=1 AND a.AUDIT_STATUS NOT IN (3, 4) AND s.revoke_time>=@sdate AND s.REVOKE_TIME<CURRENT_DATE  -- 新增 a.data_flag=1	
   )a	
   ON s.shelf_id=a.shelf_id	
GROUP BY s.business_name,d1.item_name; 	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'd_op_type_revoke_close_day',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
COMMIT;
END