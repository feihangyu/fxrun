CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_fill_operation_kpi_for_management`()
BEGIN
-- =============================================
-- Author:	经规、物流
-- Create date: 
-- Modify date: 
-- Description:	
-- 	DW层宽表，涉及补货上架及时率、补货前后GMV提升率，经规各模块达成看板的底层模型之一（每天2时45分跑）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  
SET @time_1 := CURRENT_TIMESTAMP();
TRUNCATE TABLE fe_dm.dm_fill_operation_kpi_for_management;
DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_order_second_day_fillRate_detail;
CREATE TEMPORARY TABLE fe_dm.fill_order_second_day_fillRate_detail AS
SELECT
  a.SUPPLIER_TYPE,
  CASE WHEN a.supplier_type=2
       THEN s.depot_code
       WHEN a.supplier_type IN (1,9)
       THEN k.shelf_code
       END AS supplier_code,
  CASE WHEN a.supplier_type=2
       THEN s.supplier_name
       WHEN a.supplier_type IN (1,9)
       THEN k.shelf_name
       END AS supplier_name,
  CAST(a.order_id AS CHAR) AS order_id,
  a.shelf_id,
  c.`SHELF_CODE`,
  c.grade AS shelf_level,
  a.APPLY_TIME,
  a.FILL_TIME,
  a.FILL_TYPE,
  a.ORDER_STATUS,
  a.error_reason,
  DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) AS day_diff,
  IF(
    a.fill_time IS NOT NULL
    AND WEEKDAY(a.`APPLY_TIME`) = 4,
    IF(
      DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) < 4,
      '及时',
      '不及时'
    ),
    IF(
      a.fill_time IS NOT NULL
      AND WEEKDAY(a.`APPLY_TIME`) = 5,
      IF(
        DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) < 3,
        '及时',
        '不及时'
      ),
      IF(
        a.fill_time IS NOT NULL
        AND DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) < 2,
        '及时',
        '不及时'
      )
    )
  ) AS two_day_fill_label
FROM
  fe_dwd.dwd_fill_day_inc a
  JOIN fe_dwd.`dwd_shelf_base_day_all` c
    ON a.SHELF_ID = c.shelf_id
  LEFT JOIN fe_dwd.dwd_sf_supplier s
    ON a.supplier_id = s.supplier_id
  LEFT JOIN fe_dwd.dwd_shelf_base_day_all k
    ON a.supplier_id = k.shelf_id
WHERE a.ORDER_STATUS IN (2, 4)
  AND a.`FILL_TYPE` IN (1, 2, 8, 9)
  AND a.`ORDER_ID` != 0
  AND a.apply_time >= DATE_SUB(CURRENT_DATE,INTERVAL 30 DAY)
  AND a.apply_time < CURRENT_DATE
GROUP BY a.`ORDER_ID`,
  a.`ERROR_REASON`;
  
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_operation_kpi_for_management","@time_1--@time_2",@time_1,@time_2);  
INSERT INTO fe_dm.dm_fill_operation_kpi_for_management(
first_grade_index           
,second_grade_index          
,date_type           
,action_date                 
,result_data)
SELECT
  '次日上架率' AS first_grade_index,
  IF(f.shelf_level IN ('甲级','乙级'),'甲乙、新安装货架','其它货架') AS second_grade_index,
  '申请时间' AS date_type,
  DATE(f.apply_time) AS apply_time,
  COUNT(DISTINCT IF(f.two_day_fill_label IN ('及时'),f.order_id,NULL))/COUNT(DISTINCT f.order_id) AS fill_intime_rate
FROM
  fe_dm.fill_order_second_day_fillRate_detail f
GROUP BY IF(f.shelf_level IN ('甲级','乙级'),'甲乙、新安装货架','其它货架'),
DATE(f.apply_time);
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_operation_kpi_for_management","@time_2--@time_3",@time_2,@time_3);
DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_order_GMV_increaseRate_detail;
CREATE TEMPORARY TABLE fe_dm.fill_order_GMV_increaseRate_detail AS
SELECT
  sub.business_area,
  CASE
    WHEN sub.supplier_type = 2
    THEN '仓库'
    WHEN sub.supplier_type = 9
    THEN '前置仓'
  END AS supplier_type,
  sub.fill_date,
  sub.shelf_id,
  sub.shelf_type,
  IF(
    SUM(
      IF(
        od.work_day_seq >= sub.before_workday_seq
        AND od.work_day_seq < sub.fill_date_seq,
        od.sale_price * od.quantity,
        0
      )
    ) = 0,
    IF(
      SUM(
        IF(
          od.work_day_seq > sub.fill_date_seq
          AND od.work_day_seq <= sub.after_workday_seq,
          od.sale_price * od.quantity,
          0
        )
      ) = 0,
      0,
      1
    ),
    SUM(
      IF(
        od.work_day_seq > sub.fill_date_seq
        AND od.work_day_seq <= sub.after_workday_seq,
        od.sale_price * od.quantity,
        0
      )
    ) / SUM(
      IF(
        od.work_day_seq >= sub.before_workday_seq
        AND od.work_day_seq < sub.fill_date_seq,
        od.sale_price * od.quantity,
        0
      )
    ) - 1
  ) AS A
FROM
  (SELECT
    DATE(a.fill_time) AS fill_date,
    d.business_name AS business_area,
    a.shelf_id,
    d.shelf_type,
    a.supplier_type,
    b.work_day_seq AS fill_date_seq,
    b.work_day_seq - 2 AS before_workday_seq,
    b.work_day_seq + 2 AS after_workday_seq
  FROM
    fe_dwd.dwd_fill_day_inc a,
    fe_dwd.dwd_shelf_base_day_all d,
    fe_dwd.dwd_pub_work_day b
  WHERE a.ORDER_STATUS IN (2, 3, 4)
    AND a.fill_type IN (1, 2, 8, 9, 10)
    AND d.shelf_type IN (1, 2, 3, 5)
    AND a.supplier_type IN (2, 9)
    AND a.FILL_TIME >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    AND a.fill_time < CURDATE()
    AND DATE(a.fill_time) = b.sdate
    AND a.shelf_id = d.shelf_id
    AND b.if_work_day = 1
    AND b.work_day_seq + 2 <= (
    SELECT MAX(t.work_day_seq) FROM fe_dwd.dwd_pub_work_day t WHERE t.sdate< CURRENT_DATE)) sub,-- 货架补货时间以及前后两个工作日的序列维表  706182
   (SELECT
    o.shelf_id,
    o.sale_price,
    o.quantity,
    w.sdate,
    w.work_day_seq
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month o,
    fe_dwd.dwd_pub_work_day w
  WHERE w.sdate = DATE(o.order_date)
    AND w.if_work_day = 1
    AND w.sdate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    AND w.sdate < CURDATE()
    ) od -- 货架订单时间相应序列以及销售订单数据   3478138
 WHERE sub.shelf_id = od.shelf_id
GROUP BY
  CASE
    WHEN sub.supplier_type = 2
    THEN '仓库'
    WHEN sub.supplier_type = 9
    THEN '前置仓'
  END,
  sub.fill_date,
  sub.shelf_id;
  
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_operation_kpi_for_management","@time_3--@time_4",@time_3,@time_4);
INSERT INTO fe_dm.dm_fill_operation_kpi_for_management(
first_grade_index           
,second_grade_index          
,date_type           
,action_date                 
,result_data)
SELECT
  '货架上架后GMV提升率' AS first_grade_index,
  g.supplier_type,
  '补货时间' AS date_type,
  g.fill_date,
  SUM(IF(g.A>0,1,0))/COUNT(DISTINCT g.shelf_id) AS GMV_increaseRate
FROM
  fe_dm.fill_order_GMV_increaseRate_detail g
GROUP BY g.supplier_type,fill_date;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_operation_kpi_for_management","@time_4--@time_5",@time_4,@time_5);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_fill_operation_kpi_for_management',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_fill_operation_kpi_for_management','dm_fill_operation_kpi_for_management','蔡松林');
END