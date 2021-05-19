CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_MA_area_history_sales_data_daily`()
BEGIN
-- =============================================
-- Author:	市场
-- Create date: 
-- Modify date: 
-- Description:	
-- 	DW层宽表，BI平台区域销售板块的模型宽表（每天3时35分跑）
-- 
-- =============================================
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
delete
from
  feods.D_MA_area_history_sales_data_daily
where order_date >= date_sub(current_date, interval 1 day)
  and order_date < current_date;
create temporary table area_history_sales_detail_mid as
SELECT
  o.order_id,
  o.ORDER_STATUS,
  o.SHELF_ID,
  b.shelf_name,
  b.shelf_type,
  DATE(o.ORDER_DATE) ORDER_DATE,
  c.BRANCH_CODE,
  c.BRANCH_NAME,
  c.sf_code,
  c.real_name,
  SUBSTRING_INDEX(
    SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
    ',',
    - 1
  ) AS city_name,
  o.USER_ID,
  o.PRODUCT_TOTAL_AMOUNT,
  o.DISCOUNT_AMOUNT,
  o.COUPON_AMOUNT,
  SUM(
    IF(
      o.ORDER_STATUS = 2,
      a.QUANTITY,
      a.quantity_shipped
    ) * a.SALE_PRICE
  ) AS GMV,
  SUM(
    IF(
      o.ORDER_STATUS = 2,
      a.QUANTITY,
      a.quantity_shipped
    ) * a.purchase_price
  ) AS cogs,
  IF(
    o.ORDER_STATUS = 6,
    SUM(
      a.quantity_shipped * a.SALE_PRICE
    ) / SUM(a.quantity * a.SALE_PRICE),
    1
  ) remain_rate,
  SUM(
    IF(
      o.ORDER_STATUS = 6,
      a.QUANTITY - a.quantity_shipped,
      0
    ) * a.SALE_PRICE
  ) AS refund_val
FROM
  fe.sf_order_item a,
  fe.sf_order o,
  fe.sf_shelf b,
  fe.pub_shelf_manager c
WHERE a.order_id = o.ORDER_ID
  AND o.`SHELF_ID` = b.`SHELF_ID`
  AND b.`MANAGER_ID` = c.`MANAGER_ID`
  AND o.ORDER_STATUS IN (2, 6, 7)
  AND a.`DATA_FLAG` = 1
  AND o.`DATA_FLAG` = 1
  AND b.`DATA_FLAG` = 1
  AND c.`DATA_FLAG` = 1
  AND o.`ORDER_DATE` >= date_sub(current_date, interval 1 day)
  AND o.order_date < current_date
GROUP BY o.order_id;
-- 区域销售宽表，维度：货架-订单状态-日期
insert into feods.D_MA_area_history_sales_data_daily (
  branch_code,
  branch_name,
  sf_code,
  real_name,
  city_name,
  shelf_id,
  shelf_name,
  shelf_type,
  order_status,
  order_date,
  amount,
  gmv,
  cogs,
  order_num,
  user_num,
  COUPON_AMOUNT,
  DISCOUNT_AMOUNT
)
SELECT
  a.BRANCH_CODE,
  a.BRANCH_NAME,
  a.sf_code,
  a.real_name,
  a.city_name,
  a.SHELF_ID,
  a.shelf_name,
  a.shelf_type,
  a.ORDER_STATUS,
  STR_TO_DATE(a.ORDER_DATE, '%Y-%m-%d') AS ORDER_DATE,
  ROUND(
    SUM(
      a.PRODUCT_TOTAL_AMOUNT * a.remain_rate
    ),
    2
  ) AS AMOUNT,
  SUM(a.GMV) AS gmv,
  SUM(a.cogs) AS cogs,
  COUNT(DISTINCT a.order_id) AS order_num,
  COUNT(DISTINCT a.user_id) AS user_num,
  ROUND(
    SUM(a.COUPON_AMOUNT * a.remain_rate),
    2
  ) AS COUPON_AMOUNT,
  ROUND(
    SUM(
      a.DISCOUNT_AMOUNT * a.remain_rate
    ),
    2
  ) AS DISCOUNT_AMOUNT
FROM
  area_history_sales_detail_mid a
GROUP BY a.SHELF_ID,
  a.ORDER_STATUS,
  STR_TO_DATE(a.ORDER_DATE, '%Y-%m-%d');
drop table area_history_sales_detail_mid;
###执行记录日志
 CALL sh_process.`sp_sf_dw_task_log` (
  'sp_D_MA_area_history_sales_data_daily',
  DATE_FORMAT(@run_date, '%Y-%m-%d'),
  CONCAT('caisonglin@', @user, @timestamp)
);
COMMIT;
END