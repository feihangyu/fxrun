CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_school_order_item`()
begin
-- =============================================
-- Author:	物流
-- Create date: 2019/08/28
-- Modify date: 
-- Description:	
-- 	校园货架订单商品明细表（每天的0时21分跑）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
 set @top_date:= date_add(date_sub(current_date,interval 1 day),interval -day(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 day);
 set @end_date:= date_add(last_day(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),interval 1 day);
--   更新校园货架订单商品明细表
  delete from feods.D_LO_school_order_item where smonth = date_format(@top_date,'%Y%m');
  INSERT INTO feods.D_LO_school_order_item
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
  f.BUSINESS_AREA,
  SUBSTRING_INDEX(
    SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
    ',',
    - 1
  ) AS city,
  e.campus_name,
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
  fe.sf_order a
  LEFT JOIN fe.sf_shelf b
    ON a.shelf_id = b.shelf_id
  LEFT JOIN fe.pub_member c
    ON a.user_id = c.MEMBER_ID
  LEFT JOIN fe.`sf_shelf_campus` d
    ON d.shelf_id = b.shelf_id
  LEFT JOIN fe.`sf_pub_school_campus` e
    ON e.campus_id = d.campus_id
  LEFT JOIN fe.zs_city_business f
    ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) = f.city_name
WHERE a.ORDER_STATUS = 2
  AND b.shelf_type = 8
  AND e.campus_name IS NOT NULL
  AND a.ORDER_DATE >= @top_date
  AND a.ORDER_DATE < @end_date
GROUP BY a.ORDER_ID;
-- 每个月的10号更新上个月的订单数据，确保跨越支付的订单查取到
if day(current_date) = 10 then
 DELETE FROM feods.D_LO_school_order_item WHERE smonth = DATE_FORMAT(date_sub(@top_date,interval 1 day),'%Y%m');
  INSERT INTO feods.D_LO_school_order_item
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
  f.BUSINESS_AREA,
  SUBSTRING_INDEX(
    SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
    ',',
    - 1
  ) AS city,
  e.campus_name,
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
  fe.sf_order a
  LEFT JOIN fe.sf_shelf b
    ON a.shelf_id = b.shelf_id
  LEFT JOIN fe.pub_member c
    ON a.user_id = c.MEMBER_ID
  LEFT JOIN fe.`sf_shelf_campus` d
    ON d.shelf_id = b.shelf_id
  LEFT JOIN fe.`sf_pub_school_campus` e
    ON e.campus_id = d.campus_id
  LEFT JOIN fe.zs_city_business f
    ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) = f.city_name
WHERE a.ORDER_STATUS = 2
  AND b.shelf_type = 8
  AND e.campus_name IS NOT NULL
  AND a.ORDER_DATE >= date_sub(@top_date,interval 1 month)
  AND a.ORDER_DATE < @top_date
GROUP BY a.ORDER_ID;
end if;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_D_LO_school_order_item',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
commit;
end