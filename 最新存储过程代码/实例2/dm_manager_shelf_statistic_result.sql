CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_manager_shelf_statistic_result`()
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
  SET @stime := CURRENT_TIMESTAMP();
SET @month_top:= DATE_ADD(
        DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),
        INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)) + 1 DAY);
SET @month_end:= DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),INTERVAL 1 DAY);
DELETE
FROM
  fe_dm.dm_manager_shelf_statistic_result
WHERE date_category = 2
  AND statis_date >= @month_top
  AND statis_date < @month_end; 
  
  
  -- 统计货架每月的补款率以及该货架店主名下管理货架数
INSERT INTO fe_dm.dm_manager_shelf_statistic_result (
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
  a.sf_code AS '顺丰工号',
  a.business_name AS '地区',
  2 AS '店主类型',
  2 AS '统计类型',
  DATE_ADD(@month_top,INTERVAL 1 DAY) AS '统计日期',  -- 用每个月的第二天作为统计日期
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
  fe_dwd.`dwd_shelf_base_day_all` a     -- 货架主表
  LEFT JOIN
    (SELECT
      a.manager_name AS manager_name,
      a.manager_id AS manager_id,
      COUNT(a.shelf_id) AS shelf_num
    FROM
      fe_dwd.`dwd_shelf_base_day_all` a
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
      fe_dwd.dwd_sf_statistics_shelf_detail n
     WHERE n.stat_end_date IS NOT NULL
       AND n.stat_end_date >= @month_top
       AND n.stat_end_date < @month_end
    GROUP BY DATE_FORMAT(n.stat_end_date, '%Y%m'),
      n.shelf_id) h           -- 该子查询可获取货架层级月货损金额,不同于全职店主的货损
     ON a.shelf_id = h.shelf_id
  LEFT JOIN
    (SELECT
      f.shelf_id,
      SUM(f.`quantity_act` * f.`sale_price`) AS GMV
    FROM                        
      fe_dwd.`dwd_order_item_refund_day` f                                 
    WHERE f.`PAY_DATE` >= @month_top
     AND f.`PAY_DATE` < @month_end
    GROUP BY f.SHELF_ID) c    -- 该子查询可获取货架月GMV   
  ON a.shelf_id = c.shelf_id 
WHERE a.SHELF_TYPE IN (1, 2, 3, 5) 
  AND a.manager_type = '兼职店主'
  AND (
      a.shelf_status IN (2, 3, 5)
      OR a.WHETHER_CLOSE = 1
    );
   
   
   
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_manager_shelf_statistic_result',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_manager_shelf_statistic_result','dm_manager_shelf_statistic_result','蔡松林');
COMMIT;
    END