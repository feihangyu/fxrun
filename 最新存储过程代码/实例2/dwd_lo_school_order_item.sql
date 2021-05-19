CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_lo_school_order_item`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 2019/08/28
-- Modify date: 
-- Description:	
-- 	校园货架订单商品明细表（每天的0时21分跑）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
 SET @top_date:= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY);
 SET @end_date:= DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),INTERVAL 1 DAY);
--   更新校园货架订单商品明细表
  DELETE FROM fe_dwd.dwd_lo_school_order_item WHERE smonth = DATE_FORMAT(@top_date,'%Y%m');
  INSERT INTO fe_dwd.dwd_lo_school_order_item
(smonth                             
,BUSINESS_AREA                          
,city                                   
,campus_name                            
,ORDER_ID                           
,SHELF_ID                           
,shelf_name                         
,ADDRESS                        
,USER_ID                                
,REAL_NAME                          
,ORDER_DATE                         
,ORDER_HOUR                           
,WEEK_DAY                             
,AMOUNT                           
,GMV                                        
,COUPON_AMOUNT
)
SELECT
  DATE_FORMAT(a.ORDER_DATE,'%Y%m') AS smonth,
  b.business_name,
  b.city_name AS city,
  d.campus_name,
  a.ORDER_ID,
  a.SHELF_ID,
  b.shelf_name,
  b.ADDRESS,
  a.USER_ID,
  c.REAL_NAME,
  DATE_FORMAT(a.ORDER_DATE, '%Y-%m-%d') AS ORDER_DATE,
  DATE_FORMAT(a.ORDER_DATE, '%H') AS ORDER_HOUR,
  WEEKOFYEAR(a.ORDER_DATE) AS WEEK_DAY,
  a.PRODUCT_TOTAL_AMOUNT AS AMOUNT,
  a.PRODUCT_TOTAL_AMOUNT + a.COUPON_AMOUNT + a.COMMIS_TOTAL_AMOUNT + a.DISCOUNT_AMOUNT + a.INTEGRAL_DISCOUNT AS GMV,
  a.COUPON_AMOUNT
FROM
  fe_dwd.dwd_order_item_refund_day a
  LEFT JOIN fe_dwd.dwd_shelf_base_day_all b
    ON a.shelf_id = b.shelf_id
  LEFT JOIN fe_dwd.dwd_user_day_inc c
    ON a.user_id = c.user_id
  LEFT JOIN fe_dwd.dwd_pub_school_shelf_infornation d
    ON d.shelf_id = b.shelf_id
WHERE a.ORDER_STATUS = 2
  AND b.shelf_type = 8
  AND d.campus_name IS NOT NULL
  AND a.ORDER_DATE >= @top_date
  AND a.ORDER_DATE < @end_date
GROUP BY a.ORDER_ID;
-- 每个月的10号更新上个月的订单数据，确保跨越支付的订单查取到
IF DAY(CURRENT_DATE) = 10 THEN
 DELETE FROM fe_dwd.dwd_lo_school_order_item WHERE smonth = DATE_FORMAT(DATE_SUB(@top_date,INTERVAL 1 DAY),'%Y%m');
 INSERT INTO fe_dwd.dwd_lo_school_order_item
(smonth                             
,BUSINESS_AREA                          
,city                                   
,campus_name                            
,ORDER_ID                           
,SHELF_ID                           
,shelf_name                         
,ADDRESS                        
,USER_ID                                
,REAL_NAME                          
,ORDER_DATE                         
,ORDER_HOUR                           
,WEEK_DAY                             
,AMOUNT                           
,GMV                                        
,COUPON_AMOUNT
)
SELECT
  DATE_FORMAT(a.ORDER_DATE,'%Y%m') AS smonth,
  b.business_name,
  b.city_name AS city,
  d.campus_name,
  a.ORDER_ID,
  a.SHELF_ID,
  b.shelf_name,
  b.ADDRESS,
  a.USER_ID,
  c.REAL_NAME,
  DATE_FORMAT(a.ORDER_DATE, '%Y-%m-%d') AS ORDER_DATE,
  DATE_FORMAT(a.ORDER_DATE, '%H') AS ORDER_HOUR,
  WEEKOFYEAR(a.ORDER_DATE) AS WEEK_DAY,
  a.PRODUCT_TOTAL_AMOUNT AS AMOUNT,
  a.PRODUCT_TOTAL_AMOUNT + a.COUPON_AMOUNT + a.COMMIS_TOTAL_AMOUNT + a.DISCOUNT_AMOUNT + a.INTEGRAL_DISCOUNT AS GMV,
  a.COUPON_AMOUNT
FROM
  fe_dwd.dwd_order_item_refund_day a
  LEFT JOIN fe_dwd.dwd_shelf_base_day_all b
    ON a.shelf_id = b.shelf_id
  LEFT JOIN fe_dwd.dwd_user_day_inc c
    ON a.user_id = c.user_id
  LEFT JOIN fe_dwd.dwd_pub_school_shelf_infornation d
    ON d.shelf_id = b.shelf_id
WHERE a.ORDER_STATUS = 2
  AND b.shelf_type = 8
  AND d.campus_name IS NOT NULL
  AND a.ORDER_DATE >= DATE_SUB(@top_date,INTERVAL 1 MONTH)
  AND a.ORDER_DATE < @top_date
GROUP BY a.ORDER_ID;
END IF;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_lo_school_order_item',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_lo_school_order_item','dwd_lo_school_order_item','蔡松林');
END