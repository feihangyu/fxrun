CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_relation_dc_prewarehouse_shelf_day_all`()
BEGIN 
   SET @sdate=CURDATE();
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
   -- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据  
  
DROP TEMPORARY TABLE IF EXISTS test.dwd_relation_dc_prewarehouse_shelf_day_all_test;
CREATE TEMPORARY TABLE test.dwd_relation_dc_prewarehouse_shelf_day_all_test like fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all;
  INSERT INTO test.dwd_relation_dc_prewarehouse_shelf_day_all_test
   SELECT 
     @sdate AS sdate,                         -- 统计日期
     t4.REGION_NAME AS region_area,                          -- 大区
     t4.business_name AS business_area,                        -- 区域
     t4.city_name,                            -- 城市名称
     w.warehouse_number AS dc_number,         -- 仓库编号
     w.warehouse_name AS dc_name,             -- 仓库名称
     t1.warehouse_id AS prewarehouse_id,      -- 前置仓id
     t2.shelf_code AS prewarehouse_code,      -- 前置仓编码
     t2.shelf_name AS prewarehouse_name,	  -- 前置仓名称
     t1.shelf_id,                             -- 货架id
     t3.shelf_code,                           -- 货架编码
     t3.shelf_name,                           -- 货架名称
     t3.shelf_status,                         -- 货架状态
     t3.shelf_type,                           -- 货架类型
     CURRENT_TIMESTAMP AS load_time           -- 数据加载时间 
   FROM
     fe.sf_prewarehouse_shelf_detail t1   -- 前置仓货架关联关系  一个前置仓对应多个货架 一对多的关系
     JOIN fe.sf_shelf t2                  -- 货架信息   获取 前置仓编码 前置仓名称 状态
       ON t1.warehouse_id = t2.shelf_id 
       AND t1.data_flag = 1 
       AND t2.data_flag = 1 
     JOIN fe.sf_shelf t3                 -- 货架信息   获取 货架编码 名称 状态
       ON t1.shelf_id = t3.shelf_id 
       AND t1.data_flag = 1 
       AND t3.data_flag = 1 
    JOIN fe_dwd.dwd_city_business  t4
      ON t2.city = t4.city
     JOIN 
       (SELECT 
         business_area,
         warehouse_number,
         warehouse_name 
       FROM
         fe_dwd.dwd_pub_warehouse_business_area    -- 区域大仓维表
       WHERE to_preware = 1) w
       ON t4.business_name = w.business_area;  
   
   
   	-- 上面跑通之后，每天开始插入数据之前删掉之前的数据
   TRUNCATE TABLE fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all;
   INSERT INTO fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all
   SELECT 
     @sdate AS sdate,                         -- 统计日期
     t4.REGION_NAME AS region_area,                          -- 大区
     t4.business_name AS business_area,                        -- 区域
     t4.city_name,                            -- 城市名称
     w.warehouse_number AS dc_number,         -- 仓库编号
     w.warehouse_name AS dc_name,             -- 仓库名称
     t1.warehouse_id AS prewarehouse_id,      -- 前置仓id
     t2.shelf_code AS prewarehouse_code,      -- 前置仓编码
     t2.shelf_name AS prewarehouse_name,	  -- 前置仓名称
     t1.shelf_id,                             -- 货架id
     t3.shelf_code,                           -- 货架编码
     t3.shelf_name,                           -- 货架名称
     t3.shelf_status,                         -- 货架状态
     t3.shelf_type,                           -- 货架类型
     CURRENT_TIMESTAMP AS load_time           -- 数据加载时间 
   FROM
     fe.sf_prewarehouse_shelf_detail t1   -- 前置仓货架关联关系  一个前置仓对应多个货架 一对多的关系
     JOIN fe.sf_shelf t2                  -- 货架信息   获取 前置仓编码 前置仓名称 状态
       ON t1.warehouse_id = t2.shelf_id 
       AND t1.data_flag = 1 
       AND t2.data_flag = 1 
     JOIN fe.sf_shelf t3                 -- 货架信息   获取 货架编码 名称 状态
       ON t1.shelf_id = t3.shelf_id 
       AND t1.data_flag = 1 
       AND t3.data_flag = 1 
    JOIN fe_dwd.dwd_city_business t4
      ON t2.city = t4.city
     JOIN 
       (SELECT 
         business_area,
         warehouse_number,
         warehouse_name 
       FROM
         fe_dwd.dwd_pub_warehouse_business_area    -- 区域大仓维表
       WHERE to_preware = 1) w
       ON t4.business_name = w.business_area; 
	   
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_relation_dc_prewarehouse_shelf_day_all',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
  COMMIT;
END