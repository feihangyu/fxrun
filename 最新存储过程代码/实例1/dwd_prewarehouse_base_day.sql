CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_prewarehouse_base_day`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_prewarehouse_base_day_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_prewarehouse_base_day_test LIKE fe_dwd.dwd_prewarehouse_base_day;
INSERT INTO fe_dwd.dwd_prewarehouse_base_day_test
(
`business_name`
,`warehouse_id`
,`SHELF_CODE`
,`shelf_name`
,`SHELF_STATUS`
,`AREA_ADDRESS`
,`ADDRESS`
,`DEPT_CODE`
,`DEPT_NAME`
,`SF_CODE`
,`REAL_NAME`
,`MOBILE_PHONE`
,`manager_type`
,`warehouse_type_desc`
,`warehouse_area`
,`device_of_product_desc`
,`site_type_desc`
,`is_have_monitor_desc`
,`is_have_rats_desc`
)	
SELECT
  b.business_name ,
  a.`warehouse_id` ,
  b.SHELF_CODE ,
  b.shelf_name,
  b.SHELF_STATUS ,
  b.AREA_ADDRESS ,
  b.ADDRESS ,
  e.DEPT_CODE ,
  e.DEPT_NAME ,
  b.SF_CODE ,
  b.REAL_NAME ,
  CONCAT(                                                               
    SUBSTRING(c.MOBILE_PHONE, 1, 3),                                  
    SUBSTRING(c.MOBILE_PHONE, 8, 1),                                  
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '0','9',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '1', '5',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '2','4',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '3', '0',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '4','3',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '5', '8',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '6','1',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '7', '7',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '8',  '2', '6'               
  )   )   )   )    ) ) ) ) ),                                         
    SUBSTRING(c.MOBILE_PHONE, 10, 1),                                 
    SUBSTRING(c.MOBILE_PHONE, 7, 1),                                  
    SUBSTRING(c.MOBILE_PHONE, 4, 1),                                  
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '0','9',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '1', '5',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '2', '4',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '3', '0',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '4',  '3',                   
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '5', '8',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '6',  '1',                   
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '7',  '7',                   
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '8', '2',  '6'               
   )  )  )  )  )  )  )  ) ),                                          
    SUBSTRING(c.MOBILE_PHONE, 6, 1),                                  
    SUBSTRING(c.MOBILE_PHONE, 11, 1)                                  
  ) AS 'MOBILE_PHONE',     
  CASE
          WHEN c.second_user_type=1
          THEN '全职'
          WHEN c.second_user_type=2
          THEN '兼职'
          ELSE '非兼非全'
        END AS manager_type,
  CASE
    WHEN a.`warehouse_type` = 1
    THEN '内部前置站'
    WHEN a.`warehouse_type` = 2
    THEN '外部前置站'
  END AS 'warehouse_type_desc',
  CASE
    WHEN a.actual_use_area_type = 1
    THEN '5㎡以下'
    WHEN a.actual_use_area_type = 2
    THEN '5-10㎡'
    WHEN a.actual_use_area_type = 3
    THEN '10-20㎡'
    WHEN a.actual_use_area_type = 4
    THEN '20㎡以上'
  END AS 'warehouse_area',
  CASE
    WHEN a.device_of_product = 1
    THEN '丰E货架'
    WHEN a.device_of_product = 2
    THEN '物料货架'
    WHEN a.device_of_product = 3
    THEN '托盘'
    WHEN a.device_of_product = 4
    THEN '直接堆放地面'
  END AS device_of_product_desc,
  CASE
    WHEN a.site_type = 1
    THEN '有独立房间与门锁'
    WHEN a.site_type = 2
    THEN '有独立房间与门锁(与物料一起)'
    WHEN a.site_type = 3
    THEN '休息室、分部场地'
  END AS 'site_type_desc',
  CASE
    WHEN a.is_have_monitor = 1
    THEN '是'
    WHEN a.is_have_monitor = 2
    THEN '否'
  END AS is_have_monitor_desc,
  CASE
    WHEN a.is_have_rats = 1
    THEN '是'
    WHEN a.is_have_rats = 2
    THEN '否'
  END AS 'is_have_rats_desc'
FROM
  fe.`sf_prewarehouse_info` a
 JOIN fe_dwd.`dwd_shelf_base_day_all` b
  ON a.`warehouse_id` = b.shelf_id AND b.DATA_FLAG = 1
LEFT JOIN fe.`pub_shelf_manager` c
  ON b.MANAGER_ID = c.MANAGER_ID AND c.DATA_FLAG = 1
LEFT JOIN fe.`sf_department` e
  ON b.prewarehouse_dept_id = e.DEPT_ID
;
truncate table fe_dwd.`dwd_prewarehouse_base_day`;
INSERT INTO  fe_dwd.`dwd_prewarehouse_base_day`
(
`business_name`
,`warehouse_id`
,`SHELF_CODE`
,`shelf_name`
,`SHELF_STATUS`
,`AREA_ADDRESS`
,`ADDRESS`
,`DEPT_CODE`
,`DEPT_NAME`
,`SF_CODE`
,`REAL_NAME`
,`MOBILE_PHONE`
,`manager_type`
,`warehouse_type_desc`
,`warehouse_area`
,`device_of_product_desc`
,`site_type_desc`
,`is_have_monitor_desc`
,`is_have_rats_desc`
)	
SELECT
  b.business_name ,
  a.`warehouse_id` ,
  b.SHELF_CODE ,
  b.shelf_name,
  b.SHELF_STATUS ,
  b.AREA_ADDRESS ,
  b.ADDRESS ,
  e.DEPT_CODE ,
  e.DEPT_NAME ,
  b.SF_CODE ,
  b.REAL_NAME ,
  CONCAT(                                                               
    SUBSTRING(c.MOBILE_PHONE, 1, 3),                                  
    SUBSTRING(c.MOBILE_PHONE, 8, 1),                                  
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '0','9',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '1', '5',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '2','4',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '3', '0',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '4','3',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '5', '8',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '6','1',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '7', '7',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 5, 1) = '8',  '2', '6'               
  )   )   )   )    ) ) ) ) ),                                         
    SUBSTRING(c.MOBILE_PHONE, 10, 1),                                 
    SUBSTRING(c.MOBILE_PHONE, 7, 1),                                  
    SUBSTRING(c.MOBILE_PHONE, 4, 1),                                  
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '0','9',                     
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '1', '5',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '2', '4',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '3', '0',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '4',  '3',                   
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '5', '8',                    
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '6',  '1',                   
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '7',  '7',                   
    IF(SUBSTRING(c.MOBILE_PHONE, 9, 1) = '8', '2',  '6'               
   )  )  )  )  )  )  )  ) ),                                          
    SUBSTRING(c.MOBILE_PHONE, 6, 1),                                  
    SUBSTRING(c.MOBILE_PHONE, 11, 1)                                  
  ) AS 'MOBILE_PHONE',     
  CASE
          WHEN c.second_user_type=1
          THEN '全职'
          WHEN c.second_user_type=2
          THEN '兼职'
          ELSE '非兼非全'
        END AS manager_type,
  CASE
    WHEN a.`warehouse_type` = 1
    THEN '内部前置站'
    WHEN a.`warehouse_type` = 2
    THEN '外部前置站'
  END AS 'warehouse_type_desc',
  CASE
    WHEN a.actual_use_area_type = 1
    THEN '5㎡以下'
    WHEN a.actual_use_area_type = 2
    THEN '5-10㎡'
    WHEN a.actual_use_area_type = 3
    THEN '10-20㎡'
    WHEN a.actual_use_area_type = 4
    THEN '20㎡以上'
  END AS 'warehouse_area',
  CASE
    WHEN a.device_of_product = 1
    THEN '丰E货架'
    WHEN a.device_of_product = 2
    THEN '物料货架'
    WHEN a.device_of_product = 3
    THEN '托盘'
    WHEN a.device_of_product = 4
    THEN '直接堆放地面'
  END AS device_of_product_desc,
  CASE
    WHEN a.site_type = 1
    THEN '有独立房间与门锁'
    WHEN a.site_type = 2
    THEN '有独立房间与门锁(与物料一起)'
    WHEN a.site_type = 3
    THEN '休息室、分部场地'
  END AS 'site_type_desc',
  CASE
    WHEN a.is_have_monitor = 1
    THEN '是'
    WHEN a.is_have_monitor = 2
    THEN '否'
  END AS is_have_monitor_desc,
  CASE
    WHEN a.is_have_rats = 1
    THEN '是'
    WHEN a.is_have_rats = 2
    THEN '否'
  END AS 'is_have_rats_desc'
FROM
  fe.`sf_prewarehouse_info` a
 JOIN fe_dwd.`dwd_shelf_base_day_all` b
  ON a.`warehouse_id` = b.shelf_id AND b.DATA_FLAG = 1
LEFT JOIN fe.`pub_shelf_manager` c
  ON b.MANAGER_ID = c.MANAGER_ID AND c.DATA_FLAG = 1
LEFT JOIN fe.`sf_department` e
  ON b.prewarehouse_dept_id = e.DEPT_ID
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_prewarehouse_base_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('lishilong@', @user, @timestamp));
 
END