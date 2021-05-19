CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_manager_shelf_statistic_result`()
BEGIN
-- =============================================
-- Author:	物流店主组
-- Create date: 
-- Modify date: 
-- Description:	
-- 	兼职店主星级标签结果表（每天的1时32分）
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
set @month_top:= DATE_add(
        date_sub(current_date,interval 1 day),
        INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)) + 1 DAY);
set @month_end:= date_add(last_day(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),interval 1 day);
      
DELETE
FROM
  feods.`pj_manager_shelf_statistic_result`
WHERE date_category = 2
  AND statis_date >= @month_top
  AND statis_date < @month_end; 
  
  
  -- 统计货架每月的补款率以及该货架店主名下管理货架数
INSERT INTO feods.pj_manager_shelf_statistic_result (
  shelf_id,
  shelf_name,
  real_name,
  manager_id,
  sf_code,
  business_area,
  manager_category,
  date_category,
  statis_date,
  filling_amount_rate,
  shelf_qty,
  shelf_gmv
)
SELECT
  a.shelf_id AS '货架ID',
  a.shelf_name AS '货架名称',
  a.manager_name AS '店主名称',
  a.manager_id AS '店主id',
  b.sf_code AS '顺丰工号',
  d.business_area AS '地区',
  2 AS '店主类型',
  2 AS '统计类型',
  date_add(@month_top,interval 1 day) AS '统计日期',  -- 用每个月的第二天作为统计日期
   IF(
    h.huos_value >= 0,
    1,
    IFNULL(
      h.PAYMENT_MONEY / ABS(h.huos_value),
      0
    )
  ) AS '补款率',
  k.shelf_num AS '店主货架数量',
  c.GMV AS '月货架GMV'
FROM
  fe.sf_shelf a     -- 货架主表
   JOIN fe.pub_shelf_manager b    -- 货架管理员信息表
     ON a.manager_id = b.manager_id
  JOIN fe.zs_city_business d  -- 地方层级表
     ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(a.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) = d.city_name
  LEFT JOIN
    (SELECT
      a.manager_name AS manager_name,
      a.manager_id AS manager_id,
      COUNT(a.shelf_id) AS shelf_num
    FROM
      fe.sf_shelf a
    WHERE a.`DATA_FLAG` = 1
      AND a.MANAGER_ID IS NOT NULL
      AND a.SHELF_TYPE IN (1, 2, 3, 5)
      AND a.shelf_status IN (2, 5)
    GROUP BY a.manager_id) k      -- 查询到目前为止该店主名下拥有的货架数量
     ON k.manager_id = a.manager_id
  LEFT JOIN
    (SELECT
      CONCAT(
        DATE_FORMAT(n.stat_end_date, '%Y%m'),
        '02'
      ) AS smonth,
      n.shelf_id,
      IFNULL(n.OVERPAY_MONEY, 0) AS PAYMENT_MONEY,
      IFNULL(n.check_amount, 0) AS huos_value
     FROM
      fe.sf_statistics_shelf_detail n
     where n.stat_end_date is not null
       and n.stat_end_date >= @month_top
       AND n.stat_end_date < @month_end
    GROUP BY DATE_FORMAT(n.stat_end_date, '%Y%m'),
      n.shelf_id) h           -- 该子查询可获取货架层级月货损金额,不同于全职店主的货损
     ON a.shelf_id = h.shelf_id
  left join
    (SELECT
      f.shelf_id,
      SUM(e.QUANTITY * e.SALE_PRICE) AS GMV
    FROM
      fe.sf_order_item e,                          
      fe.sf_order f                                
    WHERE e.order_id = f.ORDER_ID
     AND f.ORDER_STATUS IN (2,6,7)
     and e.data_flag = 1
     and f.data_flag = 1
     AND f.`ORDER_DATE`>= @month_top
     AND f.`ORDER_DATE`< @month_end
    GROUP BY f.SHELF_ID) c    -- 该子查询可获取货架月GMV   
  on a.shelf_id = c.shelf_id 
WHERE a.DATA_FLAG = 1
  AND b.DATA_FLAG = 1
  AND a.SHELF_TYPE IN (1, 2, 3, 5) 
  AND b.second_user_type = 2
  AND (
      a.shelf_status IN (2, 3, 5)
      OR a.WHETHER_CLOSE = 1
    );
      
      
  -- 统计兼职店主名下货架每月的GMV
--    INSERT INTO feods.`pj_manager_shelf_statistic_result` (
--     shelf_id,
--     shelf_name,
--     real_name,
--     manager_id,
--     sf_code,
--     business_area,
--     manager_category,
--     date_category,
--     statis_date,
--     shelf_gmv
--   )
--    SELECT
--     a.shelf_id AS '货架ID',
--     a.shelf_name AS '货架名称',
--     a.manager_name AS '店主名称',
--     a.manager_id AS '店主id',
--     b.sf_code AS '顺丰工号',
--     d.business_area AS '地区',
--     2 AS '店主类型',
--     2 AS '统计类型',
--     DATE_ADD(@month_top,INTERVAL 1 DAY) AS '统计日期',  -- 用每个月的第二天作为统计日期
--     c.GMV AS '月货架GMV'
--   FROM
--     (SELECT
--       f.shelf_id,
--       SUM(e.QUANTITY * e.SALE_PRICE) AS GMV
--     FROM
--       fe.sf_order_item e,                          
--       fe.sf_order f                                
--     WHERE e.order_id = f.ORDER_ID
--      AND f.ORDER_STATUS in (2,6,7)
--      AND f.`ORDER_DATE`>= @month_top
--      AND f.`ORDER_DATE`< @month_end
--     GROUP BY f.SHELF_ID) c,           -- 该子查询可获取货架月GMV
--     fe.sf_shelf a,                                    
--     fe.pub_shelf_manager b,                           
--     feods.zs_city_business d                          
--   WHERE a.shelf_id = c.shelf_id
--     AND a.manager_id = b.manager_id
--     AND SUBSTRING_INDEX(
--         SUBSTRING_INDEX(a.AREA_ADDRESS, ',', 2),
--         ',',
--         - 1
--       ) = d.city_name
--     and b.second_user_type = 2
--     AND a.DATA_FLAG = 1
--     AND b.DATA_FLAG = 1
--     AND a.SHELF_TYPE IN (1, 2, 3, 5)
--     AND (
--       a.shelf_status IN (2, 3, 5)
--       OR a.WHETHER_CLOSE = 1
--     );
      
      
--   执行记录日志
 CALL sh_process.`sp_sf_dw_task_log`(
  'sp_manager_shelf_statistic_result',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);
COMMIT;
END