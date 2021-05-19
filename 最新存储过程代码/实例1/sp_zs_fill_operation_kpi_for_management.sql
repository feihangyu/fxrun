CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_zs_fill_operation_kpi_for_management`()
BEGIN
-- =============================================
-- Author:	经规、物流
-- Create date: 
-- Modify date: 
-- Description:	
-- 	DW层宽表，涉及补货上架及时率、补货前后GMV提升率，经规各模块达成看板的底层模型之一（每天2时45分跑）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
truncate table feods.`zs_fill_operation_kpi_for_management`;
create temporary table fill_order_second_day_fillRate_detail as
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
--   f.`REGION_AREA`,
--   f.`BUSINESS_AREA`,
--   f.`CITY_NAME`,
--   d.BRANCH_CODE,
--   d.BRANCH_NAME,
  CAST(a.order_id AS CHAR) AS order_id,
  a.shelf_id,
  c.`SHELF_CODE`,
  LEFT(e.`shelf_level`, 2) AS shelf_level,
  a.APPLY_TIME,
  a.FILL_TIME,
  a.FILL_TYPE,
  a.ORDER_STATUS,
--   a.PRODUCT_TYPE_NUM,
--   a.PRODUCT_NUM,
--   a.TOTAL_PRICE,
  b.error_reason,
--   SUM(
--     b.PURCHASE_PRICE * b.ACTUAL_SIGN_NUM
--   ) AS real_value,
--   SUM(ABS(b.ERROR_NUM)) AS ERROR_NUM,
--   a.FILL_AUDIT_STATUS,
--   d.sf_code,
--   d.real_name,
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
--   IF(
--     a.fill_time IS NOT NULL
--     AND WEEKDAY(a.`APPLY_TIME`) = 4,
--     IF(
--       DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) < 5,
--       '及时',
--       '不及时'
--     ),
--     IF(
--       a.fill_time IS NOT NULL
--       AND WEEKDAY(a.`APPLY_TIME`) = 5,
--       IF(
--         DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) < 4,
--         '及时',
--         '不及时'
--       ),
--       IF(
--         a.fill_time IS NOT NULL
--         AND DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) < 3,
--         '及时',
--         '不及时'
--       )
--     )
--   ) AS three_day_fill_label
FROM
  fe.sf_product_fill_order a
  JOIN fe.sf_shelf c
    ON a.SHELF_ID = c.shelf_id
  JOIN fe.`zs_city_business` f
    ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(c.`AREA_ADDRESS`, ',', 2),
      ',',
      - 1
    ) = f.`CITY_NAME`
  JOIN fe.pub_shelf_manager d
    ON c.manager_id = d.manager_id
  JOIN fe.`sf_product_fill_order_item` b
    ON a.`ORDER_ID` = b.`ORDER_ID`
  LEFT JOIN fe.`sf_supplier` s
    ON a.supplier_id = s.supplier_id
  LEFT JOIN fe.`sf_shelf` k
    ON a.supplier_id = k.shelf_id
  LEFT JOIN feods.`pj_shelf_level_ab` e
    ON e.`shelf_id` = a.`SHELF_ID`
    AND str_to_date(concat(e.`smonth`,'01'),'%Y%m%d') = date_add(date(a.APPLY_TIME),interval -day(a.apply_time)+1 day)
WHERE a.ORDER_STATUS IN (2, 4)
  AND a.`FILL_TYPE` IN (1, 2, 8, 9)
  AND a.DATA_FLAG = 1
  AND a.`ORDER_ID` != 0
  AND a.apply_time >= date_sub(current_date,interval 30 day)
  and a.apply_time < current_date
GROUP BY a.`ORDER_ID`,
  b.`ERROR_REASON`;
  
insert into feods.`zs_fill_operation_kpi_for_management`(
first_grade_index           
,second_grade_index          
,date_type           
,action_date                 
,result_data)
select
  '次日上架率' as first_grade_index,
  if(f.shelf_level in ('甲级','乙级'),'甲乙、新安装货架','其它货架') as second_grade_index,
  '申请时间' as date_type,
  date(f.apply_time) as apply_time,
  count(distinct if(f.two_day_fill_label in ('及时'),f.order_id,null))/count(distinct f.order_id) as fill_intime_rate
from
  fill_order_second_day_fillRate_detail f
group by IF(f.shelf_level IN ('甲级','乙级'),'甲乙、新安装货架','其它货架'),
date(f.apply_time);
create temporary table fill_order_GMV_increaseRate_detail as
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
--   SUM(
--     IF(
--       od.work_day_seq >= sub.before_workday_seq
--       AND od.work_day_seq < sub.fill_date_seq,
--       od.sale_price * od.quantity,
--       0
--     )
--   ) AS before_workday_gmv,
--   SUM(
--     IF(
--       od.work_day_seq > sub.fill_date_seq
--       AND od.work_day_seq <= sub.after_workday_seq,
--       od.sale_price * od.quantity,
--       0
--     )
--   ) AS after_workday_gmv
--   ,
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
    c.business_area,
    a.shelf_id,
    d.shelf_type,
    a.supplier_type,
    b.work_day_seq AS fill_date_seq,
    b.work_day_seq - 2 AS before_workday_seq,
    b.work_day_seq + 2 AS after_workday_seq
  FROM
    fe.sf_product_fill_order a,
    fe.`sf_shelf` d,
    fe.`zs_city_business` c,
    feods.fjr_work_days b
  WHERE a.ORDER_STATUS IN (2, 3, 4)
    AND a.fill_type IN (1, 2, 8, 9, 10)
    AND d.shelf_type IN (1, 2, 3, 5)
    AND a.supplier_type IN (2, 9)
    AND a.FILL_TIME >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
    AND a.fill_time < CURDATE()
    AND DATE(a.fill_time) = b.sdate
    AND a.shelf_id = d.shelf_id
    AND SUBSTRING_INDEX(
      SUBSTRING_INDEX(d.area_address, ',', 2),
      ',',
      - 1
    ) = c.city_name
    AND b.if_work_day = 1
    AND b.work_day_seq + 2 <= (
    SELECT MAX(t.work_day_seq) FROM feods.`fjr_work_days` t WHERE t.sdate< CURRENT_DATE)) sub,-- 货架补货时间以及前后两个工作日的序列维表
   (SELECT
    o.shelf_id,
    o.sale_price,
    o.quantity,
    w.sdate,
    w.work_day_seq
  FROM
    feods.sf_order_item_temp o,
    feods.fjr_work_days w
  WHERE w.sdate = DATE(o.order_date)
    AND w.if_work_day = 1
    AND w.sdate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    AND w.sdate < CURDATE()
    ) od -- 货架订单时间相应序列以及销售订单数据
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
INSERT INTO feods.`zs_fill_operation_kpi_for_management`(
first_grade_index           
,second_grade_index          
,date_type           
,action_date                 
,result_data)
select
  '货架上架后GMV提升率' as first_grade_index,
  g.supplier_type,
  '补货时间' as date_type,
  g.fill_date,
  sum(if(g.A>0,1,0))/count(distinct g.shelf_id) AS GMV_increaseRate
from
  fill_order_GMV_increaseRate_detail g
group by g.supplier_type,fill_date;
drop table fill_order_second_day_fillRate_detail;
drop table fill_order_GMV_increaseRate_detail;
   
  -- 执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_zs_fill_operation_kpi_for_management',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
COMMIT;
END