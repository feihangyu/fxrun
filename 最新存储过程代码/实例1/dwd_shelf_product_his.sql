CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_product_his`()
BEGIN
   SET @end_date = CURDATE();
   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_3_date = SUBDATE(@end_date,INTERVAL 90 DAY);
   SET @month_1_date = SUBDATE(@end_date,INTERVAL 30 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @timestamp := CURRENT_TIMESTAMP();
 /*
 "维度:每天、货架、商品
口径:
1、货架状态：SHELF_STATUS = 2
2、未撤架：REVOKE_STATUS = 1
4、data_flag = 1"						
*/
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_product_his_tmp_1_11;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_1_11 AS
SELECT 
a.product_id
,a.shelf_id
FROM fe_dwd.dwd_shelf_product_day_all a 
JOIN fe_dwd.dwd_shelf_base_day_all i
ON a.shelf_id = i.shelf_id
AND i.SHELF_STATUS = 2
AND i.REVOKE_STATUS = 1;
-- AND i.WHETHER_CLOSE = 2;  -- 0413 调整逻辑
CREATE INDEX idx_dwd_lsl_shelf_product_his_1_11
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_1_11 (shelf_id,product_id);
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_1--@time_2",@time_1,@time_2);
/*添加有库存的*/
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_product_his_tmp_1_12;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_1_12 AS
SELECT 
a.product_id
,a.shelf_id
FROM fe_dwd.dwd_shelf_product_day_all a 
WHERE a.STOCK_QUANTITY > 0;
CREATE INDEX idx_dwd_lsl_shelf_product_his_1_12
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_1_12 (shelf_id,product_id);
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_2--@time_3",@time_2,@time_3);
-- 每天的GMV、销量  
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_his_tmp_3`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_3  AS 
SELECT 
        a.shelf_id,
        a.product_id,
        SUM(a.sale_price * a.quantity) AS gmv,  -- GMV
        SUM(a.quantity) AS sal_qty,   -- 销量
		SUM(a.REAL_TOTAL_PRICE) REAL_TOTAL_PRICE,  -- 实收
		SUM(a.quantity_act) sal_qty_shipped,  -- 实际出货量
		SUM(a.sale_price * a.quantity_act) AS gmv_shipped ,  -- gmv_shipped
		SUM(a.discount_amount)  discount_amount
FROM 
        `fe_dwd`.`dwd_order_item_refund_day` a
WHERE a.PAY_DATE >= @start_date
  AND a.PAY_DATE < @end_date	
GROUP BY a.shelf_id,a.product_id
;
CREATE INDEX idx_dwd_lsl_shelf_product_his_3
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_3 (shelf_id,product_id);
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_3--@time_4",@time_3,@time_4);
-- 找出撤架了仍然销售的货架信息  -- 39
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_product_his_tmp_1_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_1_2 AS
SELECT shelf_id,product_id
FROM  fe_dwd.dwd_lsl_shelf_product_his_tmp_3 a
;
CREATE INDEX idx_dwd_lsl_shelf_his_tmp_1_2 
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_1_2 (shelf_id,product_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_product_his_tmp_1_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_1_3 AS
SELECT a.shelf_id,a.product_id
FROM  fe_dwd.dwd_lsl_shelf_product_his_tmp_1_11 a
UNION 
SELECT b.shelf_id,b.product_id
FROM  fe_dwd.dwd_lsl_shelf_product_his_tmp_1_12 b
UNION 
SELECT c.shelf_id,c.product_id
FROM  fe_dwd.dwd_lsl_shelf_product_his_tmp_1_2 c
;
CREATE INDEX idx_dwd_lsl_shelf_his_tmp_1_3 
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_1_3 (shelf_id,product_id);
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_4--@time_5",@time_4,@time_5);
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_1 AS
SELECT 
a.detail_id
,i.business_name
,i.region_name
,a.item_id
,a.product_id
,a.shelf_id
,a.max_quantity
,a.alarm_quantity
,a.shelf_fill_flag
,a.near_date
,a.production_date
,a.risk_source
,a.danger_flag
,a.first_fill_time
,a.sales_flag
,a.new_flag
,a.sales_status
,a.near_date_source_flag
,a.operate_sale_reason
,a.business_status
,a.operate_fill_reason
,a.allow_sale_status
,a.sale_price
,a.purchase_price
,a.stock_quantity
FROM fe_dwd.dwd_shelf_product_day_all a 
LEFT JOIN fe_dwd.dwd_shelf_base_day_all i
ON a.shelf_id = i.shelf_id
JOIN fe_dwd.dwd_lsl_shelf_product_his_tmp_1_3 c
ON a.shelf_id = c.shelf_id 
AND a.product_id = c.product_id;
  
SET @time_31 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_5--@time_31",@time_5,@time_31);
--  上架数量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_his_tmp_2_1`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_2_1 AS 
SELECT 
        a.shelf_id,
        a.product_id,
		SUM(IFNULL(a.ACTUAL_APPLY_NUM,0))  ACTUAL_FILL_NUM
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.ORDER_STATUS =4  -- 已上架
        AND a.FILL_TIME >= @start_date
		AND a.FILL_TIME < @end_date
GROUP BY a.shelf_id,a.product_id
;
SET @time_36 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_31--@time_36",@time_31,@time_36);
CREATE INDEX idx_dwd_lsl_shelf_product_his_2_1
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_2_1 (shelf_id,product_id);
-- 在途数量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_his_tmp_2_2`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_2_2 AS 
SELECT 
        a.shelf_id,
        a.product_id,
        SUM(IFNULL(a.ACTUAL_APPLY_NUM,0)) AS ONWAY_NUM
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.ORDER_STATUS IN (1,2)
      AND a.APPLY_TIME >= @month_1_date
GROUP BY a.shelf_id,a.product_id
;
SET @time_42 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_36--@time_42",@time_36,@time_42);
CREATE INDEX idx_dwd_lsl_shelf_product_his_2_2
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_2_2 (shelf_id,product_id);
-- 当天是否参与促销活动 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_his_tmp_4`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_4  AS 
SELECT DISTINCT 
        a.shelf_id,
        a.product_id,
		a.stat_date
FROM fe.sf_product_activity_item a
WHERE  a.data_flag = 1
AND a.stat_date >= @start_date
AND a.stat_date < @end_date
;
SET @time_53 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_42--@time_53",@time_42,@time_53);
CREATE INDEX idx_dwd_lsl_shelf_product_his_4
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_4 (shelf_id,product_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_his_tmp_5`;
SET @time_57 := CURRENT_TIMESTAMP();
 
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_his_tmp_5 AS 
SELECT 
a.detail_id
,a.item_id
,a.region_name
,a.product_id
,a.shelf_id
,a.max_quantity
,a.alarm_quantity
,a.shelf_fill_flag
,a.near_date
,a.production_date
,a.risk_source
,a.danger_flag
,a.first_fill_time
,a.sales_flag
,a.new_flag
,a.sales_status
,a.near_date_source_flag
,a.operate_sale_reason
,a.business_status
,a.operate_fill_reason
,a.allow_sale_status
,b.sal_qty
,b.sal_qty_shipped
,b.gmv
,b.gmv_shipped
,a.sale_price
,a.purchase_price
,b.discount_amount
,b.REAL_TOTAL_PRICE
,CASE WHEN c.stat_date IS NOT NULL THEN '1' ELSE '0' END is_activity
,a.stock_quantity 
,d.onway_num
,e.ACTUAL_FILL_NUM 
FROM fe_dwd.dwd_lsl_shelf_product_his_tmp_1 a
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_product_his_tmp_3  b
ON a.product_id = b.product_id
AND a.shelf_id = b.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_product_his_tmp_4  c
ON a.product_id = c.product_id
AND a.shelf_id = c.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_product_his_tmp_2_2  d
ON a.product_id = d.product_id
AND a.shelf_id = d.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_product_his_tmp_2_1  e
ON a.product_id = e.product_id
AND a.shelf_id = e.shelf_id
;
SET @time_59 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_57--@time_59",@time_57,@time_59);
CREATE INDEX idx_dwd_lsl_shelf_product_his_5
ON fe_dwd.dwd_lsl_shelf_product_his_tmp_5 (region_name);


-- 删除当天跑数的数据,避免重复的数据

DELETE FROM fe_dwd.dwd_shelf_product_day_all_recent_32 WHERE sdate >= DATE_SUB(CURDATE(),INTERVAL 1 DAY);

INSERT INTO fe_dwd.dwd_shelf_product_day_all_recent_32

(

sdate

,detail_id  

,item_id  

,product_id  

,shelf_id

,max_quantity

,alarm_quantity

,shelf_fill_flag

,near_date

,production_date 

,risk_source

,danger_flag

,first_fill_time 

,sales_flag  

,new_flag

,sales_status

,near_date_source_flag

,operate_sale_reason

,business_status

,operate_fill_reason

,allow_sale_status

,sal_qty

,sal_qty_act

,gmv

,gmv_shipped

,sale_price

,purchase_price

,discount_amount

,REAL_TOTAL_PRICE

,is_activity

,stock_quantity

,onway_num

,ACTUAL_FILL_NUM 

)

SELECT 

DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate

,a.detail_id  

,a.item_id  

,a.product_id  

,a.shelf_id

,a.max_quantity

,a.alarm_quantity

,a.shelf_fill_flag

,a.near_date

,a.production_date 

,a.risk_source

,a.danger_flag

,a.first_fill_time 

,a.sales_flag  

,a.new_flag

,a.sales_status

,a.near_date_source_flag

,a.operate_sale_reason

,a.business_status

,a.operate_fill_reason

,a.allow_sale_status

,a.sal_qty

,a.sal_qty_shipped  AS sal_qty_act

,a.gmv

,a.gmv_shipped

,a.sale_price

,a.purchase_price

,a.discount_amount

,a.REAL_TOTAL_PRICE

,a.is_activity

,a.stock_quantity

,a.onway_num

,a.ACTUAL_FILL_NUM 

FROM fe_dwd.`dwd_lsl_shelf_product_his_tmp_5` a 

;
-- 
-- -- 缓冲期间，需要同时跑一下 ，预计0717后删除
-- 
-- -- 删除当天跑数的数据,避免重复的数据
-- DELETE FROM fe_dwd.dwd_shelf_product_day_east_his WHERE sdate >= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
-- SET @time_68 := CURRENT_TIMESTAMP();
-- INSERT INTO fe_dwd.dwd_shelf_product_day_east_his
-- (
-- sdate
-- ,detail_id  
-- ,item_id  
-- ,product_id  
-- ,shelf_id
-- ,max_quantity
-- ,alarm_quantity
-- ,shelf_fill_flag
-- ,near_date
-- ,production_date 
-- ,risk_source
-- ,danger_flag
-- ,first_fill_time 
-- ,sales_flag  
-- ,new_flag
-- ,sales_status
-- ,near_date_source_flag
-- ,operate_sale_reason
-- ,business_status
-- ,operate_fill_reason
-- ,allow_sale_status
-- ,sal_qty
-- ,sal_qty_act
-- ,gmv
-- ,gmv_shipped
-- ,sale_price
-- ,purchase_price
-- ,discount_amount
-- ,REAL_TOTAL_PRICE
-- ,is_activity
-- ,stock_quantity
-- ,onway_num
-- ,ACTUAL_FILL_NUM 
-- )
-- SELECT 
-- DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate
-- ,a.detail_id  
-- ,a.item_id  
-- ,a.product_id  
-- ,a.shelf_id
-- ,a.max_quantity
-- ,a.alarm_quantity
-- ,a.shelf_fill_flag
-- ,a.near_date
-- ,a.production_date 
-- ,a.risk_source
-- ,a.danger_flag
-- ,a.first_fill_time 
-- ,a.sales_flag  
-- ,a.new_flag
-- ,a.sales_status
-- ,a.near_date_source_flag
-- ,a.operate_sale_reason
-- ,a.business_status
-- ,a.operate_fill_reason
-- ,a.allow_sale_status
-- ,a.sal_qty
-- ,a.sal_qty_shipped  AS sal_qty_act
-- ,a.gmv
-- ,a.gmv_shipped
-- ,a.sale_price
-- ,a.purchase_price
-- ,a.discount_amount
-- ,a.REAL_TOTAL_PRICE
-- ,a.is_activity
-- ,a.stock_quantity
-- ,a.onway_num
-- ,a.ACTUAL_FILL_NUM 
-- FROM fe_dwd.`dwd_lsl_shelf_product_his_tmp_5` a 
-- WHERE a.region_name = '华东大区'
-- ;
-- SET @time_70 := CURRENT_TIMESTAMP();
-- CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_68--@time_70",@time_68,@time_70);
-- DELETE FROM fe_dwd.dwd_shelf_product_day_west_his WHERE sdate >= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
-- SET @time_73 := CURRENT_TIMESTAMP();
-- INSERT INTO fe_dwd.dwd_shelf_product_day_west_his
-- (
-- sdate
-- ,detail_id  
-- ,item_id  
-- ,product_id  
-- ,shelf_id
-- ,max_quantity
-- ,alarm_quantity
-- ,shelf_fill_flag
-- ,near_date
-- ,production_date 
-- ,risk_source
-- ,danger_flag
-- ,first_fill_time 
-- ,sales_flag  
-- ,new_flag
-- ,sales_status
-- ,near_date_source_flag
-- ,operate_sale_reason
-- ,business_status
-- ,operate_fill_reason
-- ,allow_sale_status
-- ,sal_qty
-- ,sal_qty_act
-- ,gmv
-- ,gmv_shipped
-- ,sale_price
-- ,purchase_price
-- ,discount_amount
-- ,REAL_TOTAL_PRICE
-- ,is_activity
-- ,stock_quantity
-- ,onway_num
-- ,ACTUAL_FILL_NUM 
-- )
-- SELECT 
-- DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate
-- ,a.detail_id  
-- ,a.item_id  
-- ,a.product_id  
-- ,a.shelf_id
-- ,a.max_quantity
-- ,a.alarm_quantity
-- ,a.shelf_fill_flag
-- ,a.near_date
-- ,a.production_date 
-- ,a.risk_source
-- ,a.danger_flag
-- ,a.first_fill_time 
-- ,a.sales_flag  
-- ,a.new_flag
-- ,a.sales_status
-- ,a.near_date_source_flag
-- ,a.operate_sale_reason
-- ,a.business_status
-- ,a.operate_fill_reason
-- ,a.allow_sale_status
-- ,a.sal_qty
-- ,a.sal_qty_shipped AS sal_qty_act
-- ,a.gmv
-- ,a.gmv_shipped
-- ,a.sale_price
-- ,a.purchase_price
-- ,a.discount_amount
-- ,a.REAL_TOTAL_PRICE
-- ,a.is_activity
-- ,a.stock_quantity
-- ,a.onway_num
-- ,a.ACTUAL_FILL_NUM 
-- FROM fe_dwd.`dwd_lsl_shelf_product_his_tmp_5` a 
-- WHERE a.region_name = '中西大区'
-- ;
-- SET @time_75 := CURRENT_TIMESTAMP();
-- CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_73--@time_75",@time_73,@time_75);
-- DELETE FROM fe_dwd.dwd_shelf_product_day_north_his WHERE sdate >= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
-- SET @time_78 := CURRENT_TIMESTAMP();
-- INSERT INTO fe_dwd.dwd_shelf_product_day_north_his
-- (
-- sdate
-- ,detail_id  
-- ,item_id  
-- ,product_id  
-- ,shelf_id
-- ,max_quantity
-- ,alarm_quantity
-- ,shelf_fill_flag
-- ,near_date
-- ,production_date 
-- ,risk_source
-- ,danger_flag
-- ,first_fill_time 
-- ,sales_flag  
-- ,new_flag
-- ,sales_status
-- ,near_date_source_flag
-- ,operate_sale_reason
-- ,business_status
-- ,operate_fill_reason
-- ,allow_sale_status
-- ,sal_qty
-- ,sal_qty_act
-- ,gmv
-- ,gmv_shipped
-- ,sale_price
-- ,purchase_price
-- ,discount_amount
-- ,REAL_TOTAL_PRICE
-- ,is_activity
-- ,stock_quantity
-- ,onway_num
-- ,ACTUAL_FILL_NUM 
-- )
-- SELECT 
-- DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate
-- ,a.detail_id  
-- ,a.item_id  
-- ,a.product_id  
-- ,a.shelf_id
-- ,a.max_quantity
-- ,a.alarm_quantity
-- ,a.shelf_fill_flag
-- ,a.near_date
-- ,a.production_date 
-- ,a.risk_source
-- ,a.danger_flag
-- ,a.first_fill_time 
-- ,a.sales_flag  
-- ,a.new_flag
-- ,a.sales_status
-- ,a.near_date_source_flag
-- ,a.operate_sale_reason
-- ,a.business_status
-- ,a.operate_fill_reason
-- ,a.allow_sale_status
-- ,a.sal_qty
-- ,a.sal_qty_shipped AS sal_qty_act
-- ,a.gmv
-- ,a.gmv_shipped
-- ,a.sale_price
-- ,a.purchase_price
-- ,a.discount_amount
-- ,a.REAL_TOTAL_PRICE
-- ,a.is_activity
-- ,a.stock_quantity
-- ,a.onway_num
-- ,a.ACTUAL_FILL_NUM 
-- FROM fe_dwd.`dwd_lsl_shelf_product_his_tmp_5` a 
-- WHERE a.region_name = '华北大区'
-- ;
-- SET @time_80 := CURRENT_TIMESTAMP();
-- CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_78--@time_80",@time_78,@time_80);
-- DELETE FROM fe_dwd.dwd_shelf_product_day_south_his WHERE sdate >= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
-- SET @time_83 := CURRENT_TIMESTAMP();
-- INSERT INTO fe_dwd.dwd_shelf_product_day_south_his
-- (
-- sdate
-- ,detail_id  
-- ,item_id  
-- ,product_id  
-- ,shelf_id
-- ,max_quantity
-- ,alarm_quantity
-- ,shelf_fill_flag
-- ,near_date
-- ,production_date 
-- ,risk_source
-- ,danger_flag
-- ,first_fill_time 
-- ,sales_flag  
-- ,new_flag
-- ,sales_status
-- ,near_date_source_flag
-- ,operate_sale_reason
-- ,business_status
-- ,operate_fill_reason
-- ,allow_sale_status
-- ,sal_qty
-- ,sal_qty_act
-- ,gmv
-- ,gmv_shipped
-- ,sale_price
-- ,purchase_price
-- ,discount_amount
-- ,REAL_TOTAL_PRICE
-- ,is_activity
-- ,stock_quantity
-- ,onway_num
-- ,ACTUAL_FILL_NUM 
-- )
-- SELECT 
-- DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate
-- ,a.detail_id  
-- ,a.item_id  
-- ,a.product_id  
-- ,a.shelf_id
-- ,a.max_quantity
-- ,a.alarm_quantity
-- ,a.shelf_fill_flag
-- ,a.near_date
-- ,a.production_date 
-- ,a.risk_source
-- ,a.danger_flag
-- ,a.first_fill_time 
-- ,a.sales_flag  
-- ,a.new_flag
-- ,a.sales_status
-- ,a.near_date_source_flag
-- ,a.operate_sale_reason
-- ,a.business_status
-- ,a.operate_fill_reason
-- ,a.allow_sale_status
-- ,a.sal_qty
-- ,a.sal_qty_shipped AS sal_qty_act
-- ,a.gmv
-- ,a.gmv_shipped
-- ,a.sale_price
-- ,a.purchase_price
-- ,a.discount_amount
-- ,a.REAL_TOTAL_PRICE
-- ,a.is_activity
-- ,a.stock_quantity
-- ,a.onway_num
-- ,a.ACTUAL_FILL_NUM 
-- FROM fe_dwd.`dwd_lsl_shelf_product_his_tmp_5` a 
-- WHERE a.region_name = '华南大区'
-- ;
SET @time_85 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_his","@time_83--@time_85",@time_83,@time_85);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_product_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('lishilong@', @user, @timestamp));
END