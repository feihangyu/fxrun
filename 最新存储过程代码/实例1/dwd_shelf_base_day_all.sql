CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_base_day_all`()
BEGIN 
    SET @rankk = 0;
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
      SET @end_date = CURDATE(); 
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_id := DATE_FORMAT(SUBDATE(@end_date,INTERVAL 1 MONTH),'%Y-%m');
   SET @month_id_cur := DATE_FORMAT(SUBDATE(@end_date,INTERVAL 0 MONTH),'%Y-%m');
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_base_day_all_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_base_day_all_test LIKE fe_dwd.dwd_shelf_base_day_all;
INSERT INTO fe_dwd.dwd_shelf_base_day_all_test
  (region_name
    ,business_name
	,zone_name
	,zone_code
	,PROVINCE
	,PROVINCE_NAME
	,CITY
    ,CITY_NAME
    ,branch_name  
    ,branch_code
    ,shelf_type
	,shelf_type_desc
    ,shelf_id
	,lng
	,lat
    ,MANAGER_ID
    ,SF_CODE
    ,manager_type
    ,MANAGER_NAME
	,REAL_NAME
    ,shelf_name
    ,SHELF_CODE
    ,ACTIVATE_TIME
    ,SHELF_STATUS
	,SHELF_STATUS_desc
	,invalid_cause
    ,relation_flag
    ,if_bind
    ,bind_cnt  
    ,MAIN_SHELF_ID
    ,SHELF_HANDLE_STATUS
    ,shelf_level
    ,inner_flag
    ,CLOSE_TIME
    ,CLOSE_TYPE
    ,CLOSE_REMARK
    ,REVOKE_TIME
    ,VALID_CHANCE
    ,EXPLOIT_TYPE
    ,DISTRICT
    ,AREA_ADDRESS
    ,ADDRESS
    ,BD_ID
    ,BD_NAME
    ,SHELF_MODEL
    ,operate_shelf_type
    ,REVOKE_STATUS
    ,revoke_trace    
    ,ADD_time
    ,RECORD_ID        
    ,FLOOR_STAFF_NUM
    ,APPLY_STATUS 
    ,apply_handle_time
    ,apply_ADD_TIME    
    ,ADD_USER_ID       
    ,AUDIT_TIME
    ,AUDIT_STATUS
    ,DATA_FLAG
    ,WHETHER_CLOSE
    ,is_prewarehouse_cover
    ,prewarehouse_dept_id
    ,loss_pro_flag
    ,COMPANY_ID
    ,COMPANY_NAME
    ,BELONG_INDUSTRY
    ,BELONG_INDUSTRY_desc
    ,COMPANY_ADDRESS
    ,EMPLOYEE_COUNT
    ,CONTACT_NAME
    ,CONTACT_POSITION
    ,CONTACT_PHONE
    ,CONTACT_MAIL
    ,operation_time
    ,WORK_START_TIME
    ,WORK_END_TIME
    ,WORK_WEEKEND
    ,group_id
    ,scope
    ,type_name
    ,shelf_tag
    ,cover_num
	)
     SELECT 
     ac.region_name
    ,ac.business_name 
	,cc.area_name  AS zone_name
	,a.area_id AS zone_code
	,ac.PROVINCE
	,ac.PROVINCE_NAME
	,ac.CITY
    ,ac.CITY_NAME
    ,ab.branch_name
    ,ab.branch_code
    ,a.shelf_type
	,bb2.ITEM_NAME AS shelf_type_desc
    ,a.shelf_id
	,dd.lng
	,dd.lat
    ,a.MANAGER_ID
    ,ab.SF_CODE
    ,CASE
          WHEN ab.second_user_type=1
          THEN '全职店主'
          WHEN ab.second_user_type=2
          THEN '兼职店主'
          ELSE '非兼非全'
        END AS manager_type
    ,a.MANAGER_NAME
	,ab.REAL_NAME	
    ,a.shelf_name
    ,a.SHELF_CODE
    ,a.ACTIVATE_TIME
    ,a.SHELF_STATUS
    ,bb3.ITEM_NAME AS SHELF_STATUS_desc	
	,a.invalid_cause
    ,IF(! ISNULL(sr.shelf_id), '1', '0')  relation_flag
    ,IF(d.bind_cnt IS NULL,0,1) AS if_bind
    ,IFNULL(d.bind_cnt,0) AS bind_cnt
    ,e.MAIN_SHELF_ID 
    ,e.SHELF_HANDLE_STATUS
    ,a.shelf_level
 --    ,IF(
--       a.shelf_name LIKE '%顺丰%' || ! ISNULL(ap.shelf_id) || b.company_name LIKE '%顺丰%' || b.company_name LIKE '%速运%' || 
-- 	  b.company_name LIKE '%重货%', '是', '否'
--     ) inner_flag
,IF(ap.shelf_id IS NULL,'否','是') inner_flag
    ,a.CLOSE_TIME
    ,a.CLOSE_TYPE
    ,a.CLOSE_REMARK
    ,a.REVOKE_TIME
    ,a.VALID_CHANCE
    ,a.EXPLOIT_TYPE
    ,a.DISTRICT
    ,a.AREA_ADDRESS
    ,a.ADDRESS
    ,a.BD_id
    ,a.BD_NAME
    ,a.SHELF_MODEL
    ,a.operate_shelf_type
    ,a.REVOKE_STATUS
    ,a.revoke_trace
    ,a.add_time
    ,c.RECORD_ID  
    ,c.FLOOR_STAFF_NUM
    ,c.APPLY_STATUS
    ,c.HANDLE_TIME AS apply_handle_time
    ,c.ADD_TIME  AS apply_ADD_TIME
    ,c.ADD_USER_ID
    ,c.AUDIT_TIME
    ,c.AUDIT_STATUS  
    ,a.DATA_FLAG
    ,a.WHETHER_CLOSE
     ,IF(pwh.shelf_id IS NULL,'0','1') is_prewarehouse_cover
     ,a.prewarehouse_dept_id
    ,IF(
      ! ISNULL(pa.final_company_id), '是', '否'
    ) loss_pro_flag
    ,b.COMPANY_ID 
    ,b.COMPANY_NAME
    ,b.BELONG_INDUSTRY
    ,bb.ITEM_NAME AS BELONG_INDUSTRY_desc
    ,b.COMPANY_ADDRESS
    ,b.EMPLOYEE_COUNT
    ,b.CONTACT_NAME
    ,b.CONTACT_POSITION
    ,b.CONTACT_PHONE
    ,b.CONTACT_MAIL
    ,a.operation_time
    ,b.WORK_START_TIME
    ,b.WORK_END_TIME
    ,b.WORK_WEEKEND
    ,b.group_id
    ,b.scope
    ,t.type_name
    ,a.shelf_tag
    ,c.cover_num		
     FROM
            fe.sf_shelf a
            LEFT JOIN fe.sf_company b
              ON a.COMPANY_ID = b.COMPANY_ID
    		  AND b.DATA_FLAG = 1
    LEFT JOIN fe.pub_dictionary_item bb ON (b.BELONG_INDUSTRY=bb.ITEM_VALUE AND bb.DICTIONARY_ID=6)		
    LEFT JOIN fe.pub_dictionary_item bb2 ON (a.shelf_type =bb2.ITEM_VALUE AND bb2.DICTIONARY_ID=8)		  
    LEFT JOIN fe.pub_dictionary_item bb3 ON (a.SHELF_STATUS =bb3.ITEM_VALUE AND bb3.DICTIONARY_ID=9)		  	
    LEFT JOIN fe.sf_shelf_apply c
              ON a.shelf_id = c.SHELF_ID
    		  AND c.DATA_FLAG = 1
    LEFT JOIN 
    (SELECT r.MAIN_SHELF_ID ,COUNT(r.SECONDARY_SHELF_ID) AS bind_cnt
    FROM fe.sf_shelf_relation_record r
    WHERE  r.data_flag = 1
    AND r.shelf_handle_status = 9
    GROUP BY r.MAIN_SHELF_ID
    ) d
    ON a.shelf_id = d.MAIN_SHELF_ID
    LEFT JOIN 
    (
    SELECT 
    IF(@MAIN_SHELF_ID = MAIN_SHELF_ID AND @SECONDARY_SHELF_ID = SECONDARY_SHELF_ID, @rankk := @rankk + 1,@rankk := 0) AS rank,
    @MAIN_SHELF_ID := MAIN_SHELF_ID AS MAIN_SHELF_ID ,
     @SECONDARY_SHELF_ID := SECONDARY_SHELF_ID AS SECONDARY_SHELF_ID, 
     @SHELF_HANDLE_STATUS := SHELF_HANDLE_STATUS AS SHELF_HANDLE_STATUS
    FROM
    (
    SELECT a.MAIN_SHELF_ID,a.SECONDARY_SHELF_ID,a.SHELF_HANDLE_STATUS
    FROM fe.sf_shelf_relation_record a
    WHERE  a.data_flag = 1
    AND a.shelf_handle_status = 9
    ORDER BY a.SECONDARY_SHELF_ID,a.ADD_TIME DESC
    ) m1) e
    ON a.shelf_id = e.SECONDARY_SHELF_ID
    AND e.rank = 0 
	left JOIN fe_dwd.`dwd_city_business`  ac
      ON a.city = ac.city
    LEFT JOIN fe.pub_shelf_manager ab
      ON a.manager_id = ab.manager_id
         AND ab.data_flag = 1
    LEFT JOIN fe.sf_shelf_area_info  cc
      ON a.area_id = cc.area_id
	  AND cc.data_flag =1
	LEFT JOIN fe_dwd.dwd_lo_shelf_longitude_latitude dd
	ON a.shelf_id = dd.SHELF_ID
	AND dd.data_flag = 1
    LEFT JOIN
 --     (SELECT
 --       a.shelf_id
 --     FROM
 --       fe.sf_shelf_apply_addition_info t
 --       JOIN fe.sf_shelf_apply a
 --         ON t.record_id = a.record_id
 --         AND a.data_flag = 1
 --         AND a.shelf_id > 0
 --     WHERE t.data_flag = 1
 --       AND t.is_inner_shelf = 1) ap
		fe_dwd.dwd_inner_shelf_insert ap
      ON a.shelf_id = ap.shelf_id
      LEFT JOIN fe.sf_shelf_machine m ON a.shelf_id = m.shelf_id AND m.data_flag = 1                                                      
LEFT JOIN fe.sf_shelf_machine_type t ON m.machine_type_id = t.machine_type_id AND t.data_flag = 1
    LEFT JOIN
      (SELECT DISTINCT
        t.secondary_shelf_id shelf_id, t.main_shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9
      UNION
      ALL
      SELECT DISTINCT
        t.main_shelf_id shelf_id, t.main_shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9) sr
      ON a.shelf_id = sr.shelf_id
       LEFT JOIN 
      (SELECT DISTINCT 
        tt.final_company_id 
      FROM
        fe.sf_company_protocol_apply tt 
      WHERE tt.data_flag = 1 
        AND tt.apply_status = 2) pa 
      ON a.company_id = pa.final_company_id 
              LEFT JOIN
        (SELECT DISTINCT
          tt.shelf_id
        FROM
          fe.sf_prewarehouse_shelf_detail tt
        WHERE tt.data_flag = 1) pwh
        ON pwh.shelf_id = a.shelf_id		
	WHERE a.DATA_FLAG = 1
	AND a.shelf_id NOT IN
(4568,
21658,
21662,
21663,
23404,
24349,
24822,
24825,
26009,
26324,
26785,
37595,
43562,
80326,
82702,
82703,
83450,
84943,
84944,
86283,
86284,
90575,
90605,
90606,
90607,
90608,
98184,
99721)
	;
-- 更新一下是否月结货架
UPDATE fe_dwd.dwd_shelf_base_day_all_test AS b
JOIN fe_dwd.`dwd_monthly_balance_shelf_insert` a 
ON a.shelf_id = b.shelf_id 
SET b.is_monthly_balance = 1; 
-- 更新一下星华的最新货架等级 注意两个字段不同
UPDATE fe_dwd.dwd_shelf_base_day_all_test AS b
JOIN feods.d_op_shelf_grade a 
ON a.shelf_id = b.shelf_id
AND a.month_id = @month_id_cur   
SET b.grade_cur_month = a.grade; 	
	
UPDATE fe_dwd.dwd_shelf_base_day_all_test AS b
JOIN feods.d_op_shelf_grade a 
ON a.shelf_id = b.shelf_id
AND a.month_id = @month_id   
SET b.grade = a.grade; 
-- 更新一下是否测试货架  货架编码由王丹苗提供
UPDATE fe_dwd.dwd_shelf_base_day_all_test AS b
SET b.is_test = 1
WHERE b.shelf_code IN
(
'A226461',
'A226327',
'A225918',
'A226673',
'A130044',
'A226460',
'A227129',
'A227128'
); 
	
-- 每天开始插入数据之前删掉之前的数据
TRUNCATE TABLE fe_dwd.dwd_shelf_base_day_all;
		
INSERT INTO fe_dwd.dwd_shelf_base_day_all
    (region_name
    ,business_name
	,zone_name
	,zone_code
	,PROVINCE
	,PROVINCE_NAME
	,CITY
    ,CITY_NAME
    ,branch_name  
    ,branch_code
    ,shelf_type
	,shelf_type_desc
    ,shelf_id
	,lng
	,lat
    ,MANAGER_ID
    ,SF_CODE
    ,manager_type
    ,MANAGER_NAME
	,REAL_NAME
    ,shelf_name
    ,SHELF_CODE
    ,ACTIVATE_TIME
    ,SHELF_STATUS
	,SHELF_STATUS_desc
	,invalid_cause
    ,relation_flag
    ,if_bind
    ,bind_cnt  
    ,MAIN_SHELF_ID
    ,SHELF_HANDLE_STATUS
    ,shelf_level
    ,inner_flag
    ,CLOSE_TIME
    ,CLOSE_TYPE
    ,CLOSE_REMARK
    ,REVOKE_TIME
    ,VALID_CHANCE
    ,EXPLOIT_TYPE
    ,DISTRICT
    ,AREA_ADDRESS
    ,ADDRESS
    ,BD_ID
    ,BD_NAME
    ,SHELF_MODEL
    ,operate_shelf_type
    ,REVOKE_STATUS
    ,revoke_trace    
    ,ADD_time
    ,RECORD_ID        
    ,FLOOR_STAFF_NUM
    ,APPLY_STATUS 
    ,apply_handle_time
    ,apply_ADD_TIME    
    ,ADD_USER_ID       
    ,AUDIT_TIME
    ,AUDIT_STATUS
    ,DATA_FLAG
    ,WHETHER_CLOSE
    ,is_prewarehouse_cover
    ,prewarehouse_dept_id
    ,loss_pro_flag
    ,COMPANY_ID
    ,COMPANY_NAME
    ,BELONG_INDUSTRY
    ,BELONG_INDUSTRY_desc
    ,COMPANY_ADDRESS
    ,EMPLOYEE_COUNT
    ,CONTACT_NAME
    ,CONTACT_POSITION
    ,CONTACT_PHONE
    ,CONTACT_MAIL
    ,operation_time
    ,WORK_START_TIME
    ,WORK_END_TIME
    ,WORK_WEEKEND
    ,group_id
    ,scope
    ,type_name
    ,shelf_tag
    ,cover_num
	)
   SELECT 
     ac.region_name
    ,ac.business_name 
	,cc.area_name  AS zone_name
	,a.area_id AS zone_code
	,ac.PROVINCE
	,ac.PROVINCE_NAME
	,ac.CITY
    ,ac.CITY_NAME
    ,ab.branch_name
    ,ab.branch_code
    ,a.shelf_type
	,bb2.ITEM_NAME AS shelf_type_desc
    ,a.shelf_id
	,dd.lng
	,dd.lat
    ,a.MANAGER_ID
    ,ab.SF_CODE
    ,CASE
          WHEN ab.second_user_type=1
          THEN '全职店主'
          WHEN ab.second_user_type=2
          THEN '兼职店主'
          ELSE '非兼非全'
        END AS manager_type
    ,a.MANAGER_NAME
	,ab.REAL_NAME	
    ,a.shelf_name
    ,a.SHELF_CODE
    ,a.ACTIVATE_TIME
    ,a.SHELF_STATUS
    ,bb3.ITEM_NAME AS SHELF_STATUS_desc	
	,a.invalid_cause
    ,IF(! ISNULL(sr.shelf_id), '1', '0')  relation_flag
    ,IF(d.bind_cnt IS NULL,0,1) AS if_bind
    ,IFNULL(d.bind_cnt,0) AS bind_cnt
    ,e.MAIN_SHELF_ID 
    ,e.SHELF_HANDLE_STATUS
    ,a.shelf_level
--     ,IF(
--       a.shelf_name LIKE '%顺丰%' || ! ISNULL(ap.shelf_id) || b.company_name LIKE '%顺丰%' || b.company_name LIKE '%速运%' || 
-- 	  b.company_name LIKE '%重货%', '是', '否'
--     ) inner_flag
,IF(ap.shelf_id IS NULL,'否','是') inner_flag
    ,a.CLOSE_TIME
    ,a.CLOSE_TYPE
    ,a.CLOSE_REMARK
    ,a.REVOKE_TIME
    ,a.VALID_CHANCE
    ,a.EXPLOIT_TYPE
    ,a.DISTRICT
    ,a.AREA_ADDRESS
    ,a.ADDRESS
    ,a.BD_id
    ,a.BD_NAME
    ,a.SHELF_MODEL
    ,a.operate_shelf_type
    ,a.REVOKE_STATUS
    ,a.revoke_trace
    ,a.add_time
    ,c.RECORD_ID  
    ,c.FLOOR_STAFF_NUM
    ,c.APPLY_STATUS
    ,c.HANDLE_TIME AS apply_handle_time
    ,c.ADD_TIME  AS apply_ADD_TIME
    ,c.ADD_USER_ID
    ,c.AUDIT_TIME
    ,c.AUDIT_STATUS  
    ,a.DATA_FLAG
    ,a.WHETHER_CLOSE
    ,IF(pwh.shelf_id IS NULL,'0','1') is_prewarehouse_cover
    ,a.prewarehouse_dept_id
      , IF(
      ! ISNULL(pa.final_company_id), '是', '否'
    ) loss_pro_flag
    ,b.COMPANY_ID 
    ,b.COMPANY_NAME
    ,b.BELONG_INDUSTRY
    ,bb.ITEM_NAME AS BELONG_INDUSTRY_desc
    ,b.COMPANY_ADDRESS
    ,b.EMPLOYEE_COUNT
    ,b.CONTACT_NAME
    ,b.CONTACT_POSITION
    ,b.CONTACT_PHONE
    ,b.CONTACT_MAIL
    ,a.operation_time
    ,b.WORK_START_TIME
    ,b.WORK_END_TIME
    ,b.WORK_WEEKEND
    ,b.group_id
    ,b.scope
    ,t.type_name	
    ,a.shelf_tag
    ,c.cover_num	
     FROM
            fe.sf_shelf a
            LEFT JOIN fe.sf_company b
              ON a.COMPANY_ID = b.COMPANY_ID
    		  AND b.DATA_FLAG = 1
    LEFT JOIN fe.pub_dictionary_item bb ON (b.BELONG_INDUSTRY=bb.ITEM_VALUE AND bb.DICTIONARY_ID=6)		
    LEFT JOIN fe.pub_dictionary_item bb2 ON (a.shelf_type =bb2.ITEM_VALUE AND bb2.DICTIONARY_ID=8)		  
    LEFT JOIN fe.pub_dictionary_item bb3 ON (a.SHELF_STATUS =bb3.ITEM_VALUE AND bb3.DICTIONARY_ID=9)		  	
    LEFT JOIN fe.sf_shelf_apply c
              ON a.shelf_id = c.SHELF_ID
    		  AND c.DATA_FLAG = 1
    LEFT JOIN 
    (SELECT r.MAIN_SHELF_ID ,COUNT(r.SECONDARY_SHELF_ID) AS bind_cnt
    FROM fe.sf_shelf_relation_record r
    WHERE  r.data_flag = 1
    AND r.shelf_handle_status = 9
    GROUP BY r.MAIN_SHELF_ID
    ) d
    ON a.shelf_id = d.MAIN_SHELF_ID
    LEFT JOIN 
    (
    SELECT 
    IF(@MAIN_SHELF_ID = MAIN_SHELF_ID AND @SECONDARY_SHELF_ID = SECONDARY_SHELF_ID, @rankk := @rankk + 1,@rankk := 0) AS rank,
    @MAIN_SHELF_ID := MAIN_SHELF_ID AS MAIN_SHELF_ID ,
     @SECONDARY_SHELF_ID := SECONDARY_SHELF_ID AS SECONDARY_SHELF_ID, 
     @SHELF_HANDLE_STATUS := SHELF_HANDLE_STATUS AS SHELF_HANDLE_STATUS
    FROM
    (
    SELECT a.MAIN_SHELF_ID,a.SECONDARY_SHELF_ID,a.SHELF_HANDLE_STATUS
    FROM fe.sf_shelf_relation_record a
    WHERE  a.data_flag = 1
    AND a.shelf_handle_status = 9
    ORDER BY a.SECONDARY_SHELF_ID,a.ADD_TIME DESC
    ) m1) e
    ON a.shelf_id = e.SECONDARY_SHELF_ID
    AND e.rank = 0 
	left JOIN fe_dwd.`dwd_city_business` ac
      ON a.city = ac.city
    LEFT JOIN fe.pub_shelf_manager ab
      ON a.manager_id = ab.manager_id
         AND ab.data_flag = 1
    LEFT JOIN fe.sf_shelf_area_info  cc
      ON a.area_id = cc.area_id
	  AND cc.data_flag =1
	LEFT JOIN fe_dwd.dwd_lo_shelf_longitude_latitude dd
	ON a.shelf_id = dd.SHELF_ID
	AND dd.data_flag = 1
    LEFT JOIN
 --     (SELECT
 --       a.shelf_id
 --     FROM
 --       fe.sf_shelf_apply_addition_info t
 --       JOIN fe.sf_shelf_apply a
 --         ON t.record_id = a.record_id
 --         AND a.data_flag = 1
 --         AND a.shelf_id > 0
 --     WHERE t.data_flag = 1
 --       AND t.is_inner_shelf = 1) ap
		fe_dwd.dwd_inner_shelf_insert ap   -- 吹防从风控组获取的内部货架ID号
      ON a.shelf_id = ap.shelf_id
      LEFT JOIN fe.sf_shelf_machine m ON a.shelf_id = m.shelf_id AND m.data_flag = 1                                                      
LEFT JOIN fe.sf_shelf_machine_type t ON m.machine_type_id = t.machine_type_id AND t.data_flag = 1
    LEFT JOIN
      (SELECT  DISTINCT
        t.secondary_shelf_id shelf_id, t.main_shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9
      UNION
      ALL
      SELECT DISTINCT
        t.main_shelf_id shelf_id, t.main_shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9) sr
      ON a.shelf_id = sr.shelf_id
          LEFT JOIN 
      (SELECT DISTINCT 
        tt.final_company_id 
      FROM
        fe.sf_company_protocol_apply tt 
      WHERE tt.data_flag = 1 
        AND tt.apply_status = 2) pa 
      ON a.company_id = pa.final_company_id 
        LEFT JOIN
        (SELECT DISTINCT
          tt.shelf_id
        FROM
          fe.sf_prewarehouse_shelf_detail tt
        WHERE tt.data_flag = 1) pwh
        ON pwh.shelf_id = a.shelf_id	
	WHERE a.DATA_FLAG = 1 
	and a.shelf_id not IN
(4568,
21658,
21662,
21663,
23404,
24349,
24822,
24825,
26009,
26324,
26785,
37595,
43562,
80326,
82702,
82703,
83450,
84943,
84944,
86283,
86284,
90575,
90605,
90606,
90607,
90608,
98184,
99721)
	;
-- 更新一下星华的货架等级	
uPDATE fe_dwd.dwd_shelf_base_day_all AS b
JOIN feods.d_op_shelf_grade a 
ON a.shelf_id = b.shelf_id
AND a.month_id = @month_id   
SET b.grade = a.grade; 
-- 更新一下星华的最新货架等级 注意两个字段不同
UPDATE fe_dwd.dwd_shelf_base_day_all AS b
JOIN feods.d_op_shelf_grade a 
ON a.shelf_id = b.shelf_id
AND a.month_id = @month_id_cur   
SET b.grade_cur_month = a.grade; 
-- 更新一下是否月结货架
UPDATE fe_dwd.dwd_shelf_base_day_all AS b
JOIN fe_dwd.`dwd_monthly_balance_shelf_insert` a 
ON a.shelf_id = b.shelf_id 
SET b.is_monthly_balance = 1; 
-- 更新一下是否测试货架  货架编码由王丹苗提供
UPDATE fe_dwd.dwd_shelf_base_day_all AS b
SET b.is_test = 1
WHERE b.shelf_code IN
(
'A226461',
'A226327',
'A225918',
'A226673',
'A130044',
'A226460',
'A227129',
'A227128'
); 
-- 这几个货架是测试的和没有用的。手动删除.防止新添加城市的出现
-- delete from fe_dwd.dwd_shelf_base_day_all 
-- where shelf_id in
-- (4568,
-- 21658,
-- 21662,
-- 21663,
-- 23404,
-- 24349,
-- 24822,
-- 24825,
-- 26009,
-- 26324,
-- 26785,
-- 37595,
-- 43562,
-- 80326,
-- 82702,
-- 82703,
-- 83450,
-- 84943,
-- 84944,
-- 86283,
-- 86284,
-- 90575,
-- 90605,
-- 90606,
-- 90607,
-- 90608,
-- 98184,
-- 99721);
-- 	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_base_day_all',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
end