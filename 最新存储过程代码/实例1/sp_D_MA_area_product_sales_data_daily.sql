CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_MA_area_product_sales_data_daily`()
BEGIN
-- =============================================
-- Author:	
-- Create date: 
-- Modify date: 
-- Description:	
-- 	DW层宽表，BI平台区域销售板块的模型宽表
-- 
-- =============================================
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
delete
from
  feods.D_MA_area_product_sales_data_daily
where sdate >= date_sub(current_date, interval 1 day)
  and sdate < current_date;
insert into feods.D_MA_area_product_sales_data_daily (
	sdate ,
  city_name ,
	TYPE_ID ,
	shelf_type ,
  product_id,
	PRODUCT_CODE2 ,
  PRODUCT_NAME ,
  PURCHASE_PRICE ,
  SALE_PRICE ,
	sale_qty,
	gmv ,
	cost_value ,
  DISCOUNT_AMOUNT ,
	REAL_TOTAL_PRICE 
)
SELECT
	date(b.order_date) AS sdate,
	SUBSTRING_INDEX( SUBSTRING_INDEX( c.AREA_ADDRESS, ',', 2 ), ',',- 1 ) AS CITY_NAME,
	e.TYPE_ID,
	c.shelf_type,
	a.product_id,
	e.PRODUCT_CODE2,
	e.product_name,
	a.PURCHASE_PRICE,
	a.SALE_PRICE,
	SUM( CASE WHEN b.order_status = 2 THEN a.QUANTITY ELSE a.quantity_shipped END ) AS sale_qty,
	SUM( CASE WHEN b.order_status = 2 THEN a.SALE_PRICE * a.QUANTITY ELSE a.SALE_PRICE * a.quantity_shipped END ) AS gmv,
	SUM( CASE WHEN b.order_status = 2 THEN a.PURCHASE_PRICE * a.QUANTITY ELSE a.PURCHASE_PRICE * a.quantity_shipped END ) AS cost_value,
	SUM( a.DISCOUNT_AMOUNT ) AS DISCOUNT_AMOUNT,
	SUM( a.REAL_TOTAL_PRICE ) AS REAL_TOTAL_PRICE 
FROM
	fe.sf_order_item a
	JOIN fe.sf_order b ON a.order_id = b.order_id
	LEFT JOIN fe.sf_shelf c ON b.SHELF_ID = c.shelf_id
	LEFT JOIN fe.sf_product e ON a.product_id = e.product_id 
WHERE
	b.order_status IN ( 2, 6, 7 ) 
	AND b.ORDER_DATE >= date_sub(current_date, interval 1 day)
  AND b.ORDER_DATE < current_date
GROUP BY
	sdate,
	CITY_NAME,
	e.TYPE_ID,
	c.shelf_type,
	a.product_id,
	a.product_name,
	a.PURCHASE_PRICE,
	a.SALE_PRICE;
-- 执行记录日志
 CALL sh_process.`sp_sf_dw_task_log` (
  'sp_D_MA_area_product_sales_data_daily',
  DATE_FORMAT(@run_date, '%Y-%m-%d'),
  CONCAT('feihangyu@', @user, @timestamp)
);
COMMIT;
END