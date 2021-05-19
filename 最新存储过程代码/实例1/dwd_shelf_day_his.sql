CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_day_his`()
BEGIN
  SET @end_date = CURDATE(); 
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_3_date = SUBDATE(@end_date,INTERVAL 90 DAY);
   SET @month_1_date = SUBDATE(@end_date,INTERVAL 30 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @month_id := DATE_FORMAT(SUBDATE(@end_date,INTERVAL 1 MONTH),'%Y-%m');
   SET @timestamp := CURRENT_TIMESTAMP();
 
-- 删除当天跑数的数据,避免重复的数据
DELETE FROM fe_dwd.dwd_shelf_day_his WHERE sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
-- 货架等级 用星华的表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_1_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_1_1  AS 
SELECT 
a.shelf_id
,a.zone_code
,a.shelf_code
,a.ACTIVATE_TIME
,a.shelf_type
,a.shelf_status
,a.revoke_status
,a.whether_close
,c.prewarehouse_id
,a.manager_id
,CASE
          WHEN a.manager_type='全职店主'
          THEN 1
          WHEN a.manager_type='兼职店主'
          THEN 2
          ELSE 0
        END AS manager_type
,a.main_shelf_id
,a.shelf_level
,a.grade
FROM fe_dwd.dwd_shelf_base_day_all a 
LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` c
ON a.shelf_id = c.shelf_id
WHERE a.SHELF_STATUS IN (2,5)  -- 0506 调整逻辑，添加2,5状态，删除撤架状态
-- AND a.REVOKE_STATUS = 1;
-- AND a.WHETHER_CLOSE = 2;  -- 0413 调整逻辑
;
CREATE INDEX idx_dwd_lsl_shelf_1_1
ON fe_dwd.dwd_lsl_shelf_1_1 (shelf_id);
  -- 每天的销量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_2`; 
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2  AS 
SELECT 
        t.shelf_id,
      SUM(t.quantity) AS sal_qty,   -- 销量
		SUM(t.quantity_act) sal_qty_act, -- 实际出货量
       SUM(
      t.o_product_total_amount * t.sale_price * t.quantity_act / t.ogmv
    ) o_product_total_amount,   -- 取折算后的
	SUM(
      t.o_discount_amount  * t.sale_price * t.quantity_act / t.ogmv
    ) o_discount_amount,  -- 取折算后的
	SUM(
      t.o_coupon_amount * t.sale_price * t.quantity_act / t.ogmv
    ) o_coupon_amount,   -- 取折算后的
	SUM(
      t.o_third_discount_amount * t.sale_price * t.quantity_act / t.ogmv
    ) o_third_discount_amount    -- 取折算后的 
FROM 
        `fe_dwd`.`dwd_pub_order_item_recent_two_month` t
WHERE t.PAY_DATE >= @start_date
  AND t.PAY_DATE < @end_date	
GROUP BY t.shelf_id
;
 
  
 
    
  
CREATE INDEX idx_dwd_lsl_shelf_2
ON fe_dwd.dwd_lsl_shelf_2 (shelf_id);
-- 每天的订单数和人数（如果一个订单虽然已经支付了，但是出货全部失败的话，不计订单数和人数）
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_2_3`; 
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_3  AS 
SELECT 
        a.shelf_id,
        COUNT(DISTINCT a.order_id) AS orders,
        COUNT(DISTINCT a.product_id) skus,  -- sku数  
	COUNT(DISTINCT a.user_id)  users
FROM 
        `fe_dwd`.`dwd_order_item_refund_day` a
WHERE a.PAY_DATE >= @start_date
  AND a.PAY_DATE < @end_date	
  AND a.quantity_act >0
GROUP BY a.shelf_id
;
  
  
  
CREATE INDEX idx_dwd_lsl_shelf_2_3
ON fe_dwd.dwd_lsl_shelf_2_3 (shelf_id);

  
-- GMV、支付金额需要另外取，有补付款的问题
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_1 AS
SELECT 
order_id ,
shelf_id,
a.`PAY_AMOUNT`* COUNT(DISTINCT a.pay_id) pay_amount_act,
a.`PAY_AMOUNT`* COUNT(DISTINCT a.pay_id)-SUM(IFNULL(a.refund_amount,0)) AS pay_amount_shipped, -- 重复支付的数据也要算进来
 SUM(IFNULL(a.quantity_act * a.`SALE_PRICE`,0)) AS GMV ,
   SUM(IF(IFNULL(a.refund_amount,0)>0 AND a.`refund_finish_time`< @end_date, 0 ,
  (a.`QUANTITY`-a.quantity_act) * a.`SALE_PRICE`)) AS refunding_GMV  -- 当日应退未退GMV
FROM fe_dwd.`dwd_order_item_refund_day` a
WHERE a.PAY_DATE >= @start_date
  AND a.PAY_DATE < @end_date	
-- WHERE a.order_id = 23140821900760000
GROUP BY order_id,shelf_id;


DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_2(KEY idx_order(shelf_id)) AS
SELECT 
shelf_id,
SUM(IFNULL(pay_amount_shipped,0)) pay_amount,
SUM(IFNULL(pay_amount_act,0))  pay_amount_act,
SUM(IFNULL(refunding_GMV,0)) AS refunding_GMV,
SUM(IFNULL(GMV,0)) AS GMV
FROM fe_dwd.dwd_lsl_shelf_2_1
GROUP BY shelf_id;

-- 0709 添加

DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_2_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_2_1 (KEY idx_order(order_id)) AS 
SELECT
 b.`order_id`,
 b.`shelf_id`,
 SUM(b.quantity * f.sale_price) AS before_refund_gmv,-- 之前月应退当日已退GMV
 SUM(b.`refund_amount`) AS before_refund_amount   -- 之前月应退当日已退金额
FROM
fe_dwd.dwd_order_refund_item b
JOIN fe_dwd.`dwd_order_item_refund_day` f
ON b.`order_item_id` = f.`ORDER_ITEM_ID`
AND b.`order_id` = f.`order_id`
AND f.`PAY_DATE` < @month_start
WHERE b.`refund_finish_time` >= @start_date
AND b.`refund_finish_time` < @end_date
GROUP BY b.order_id,b.shelf_id
;

DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_4;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_4(KEY idx_order(shelf_id)) AS
SELECT 
shelf_id,
SUM(IFNULL(before_refund_gmv,0)) before_refund_gmv,
SUM(IFNULL(before_refund_amount,0)) before_refund_amount
FROM fe_dwd.dwd_lsl_shelf_2_2_1
GROUP BY shelf_id
;


DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_3_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_3_1 (KEY idx_order(order_id)) AS 
SELECT
 b.`order_id`,
 b.`shelf_id`,
 SUM(b.`refund_amount`) AS refund_finish_amount   -- 当月应退当日已退金额
FROM
fe_dwd.dwd_order_refund_item b
JOIN fe_dwd.`dwd_order_item_refund_day` f
ON b.`order_item_id` = f.`ORDER_ITEM_ID`
AND b.`order_id` = f.`order_id`
AND f.`PAY_DATE` >= @month_start
WHERE b.`refund_finish_time` >= @start_date
AND b.`refund_finish_time` < @end_date
GROUP BY b.order_id,b.shelf_id
;



DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_2_5;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_2_5(KEY idx_order(shelf_id)) AS
SELECT 
shelf_id,
SUM(IFNULL(refund_finish_amount,0))  refund_finish_amount
FROM fe_dwd.dwd_lsl_shelf_2_3_1   -- 当月应退当日已退金额
GROUP BY shelf_id;





-- 找出撤架了仍然销售的货架信息  -- 39   
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_1_2 AS
SELECT t1.shelf_id
FROM
(SELECT shelf_id
FROM  fe_dwd.dwd_lsl_shelf_2 
)t1
LEFT JOIN 
(
SELECT shelf_id
FROM `fe_dwd`.`dwd_lsl_shelf_1_1`
)t2 
ON t1.shelf_id = t2.shelf_id
WHERE t2.shelf_id IS NULL
;
CREATE INDEX idx_dwd_lsl_shelf_1_2
ON fe_dwd.dwd_lsl_shelf_1_2 (shelf_id);
-- 库存数量  库存金额 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_6`; -- 1 min 32 sec
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_6 AS 
SELECT 
        a.shelf_id,
          COUNT(IF(a.STOCK_QUANTITY > 0 ,a.product_id,NULL)) stock_skus,
		SUM(IF(a.STOCK_QUANTITY > 0 ,a.STOCK_QUANTITY,0)) stock_quantity,
		SUM(IF(a.STOCK_QUANTITY > 0 ,a.STOCK_QUANTITY,0) * a.SALE_PRICE) stock_sum
FROM
fe.sf_shelf_product_detail a
WHERE  a.data_flag =1
GROUP BY a.shelf_id;
CREATE INDEX idx_dwd_lsl_shelf_6
ON fe_dwd.dwd_lsl_shelf_6 (shelf_id);
-- 找出异常货架，仍有有库存的货架。如已撤架仍有库存的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_3;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_1_3 AS
SELECT t1.shelf_id
FROM
(SELECT shelf_id
FROM  fe_dwd.dwd_lsl_shelf_6 
WHERE stock_quantity > 0
)t1
LEFT JOIN 
(
SELECT shelf_id
FROM `fe_dwd`.`dwd_lsl_shelf_1_1`
)t2 
ON t1.shelf_id = t2.shelf_id
WHERE t2.shelf_id IS NULL
;
CREATE INDEX idx_dwd_lsl_shelf_1_3
ON fe_dwd.dwd_lsl_shelf_1_3 (shelf_id);
-- 补付款金额
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_3;
  CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_3 (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.payment_money) after_payment_money
  FROM
    fe.sf_after_payment t
  WHERE t.payment_status = 2
    AND t.payment_date >= @start_date
    AND t.payment_date < @end_date	
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  
 CREATE INDEX idx_dwd_lsl_shelf_3
ON fe_dwd.dwd_lsl_shelf_3 (shelf_id);
 
-- 找出有补付款金额，但已撤架的
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_4;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_1_4 AS
SELECT t1.shelf_id
FROM
(SELECT shelf_id
FROM  fe_dwd.dwd_lsl_shelf_3 
)t1
LEFT JOIN 
(
SELECT shelf_id
FROM `fe_dwd`.`dwd_lsl_shelf_1_1`
)t2 
ON t1.shelf_id = t2.shelf_id
WHERE t2.shelf_id IS NULL
;
CREATE INDEX idx_dwd_lsl_shelf_1_4
ON fe_dwd.dwd_lsl_shelf_1_4 (shelf_id);
-- 重新提取一下涉及到库存、金额的货架的基表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_1  AS 
SELECT * FROM `fe_dwd`.`dwd_lsl_shelf_1_1`
UNION 
SELECT 
a.shelf_id
,a.zone_code
,a.shelf_code
,a.ACTIVATE_TIME
,a.shelf_type
,a.shelf_status
,a.revoke_status
,a.whether_close
,c.prewarehouse_id
,a.manager_id
,CASE
    WHEN a.manager_type = '全职店主' 
    THEN 1 
    WHEN a.manager_type = '兼职店主' 
    THEN 2 
    ELSE 0 
  END AS manager_type
,a.main_shelf_id
,a.shelf_level
,a.grade
FROM fe_dwd.dwd_shelf_base_day_all a 
JOIN fe_dwd.dwd_lsl_shelf_1_2 d
ON a.shelf_id = d.shelf_id
LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` c
ON a.shelf_id = c.shelf_id
UNION 
SELECT 
a.shelf_id
,a.zone_code
,a.shelf_code
,a.ACTIVATE_TIME
,a.shelf_type
,a.shelf_status
,a.revoke_status
,a.whether_close
,c.prewarehouse_id
,a.manager_id
,CASE
    WHEN a.manager_type = '全职店主' 
    THEN 1 
    WHEN a.manager_type = '兼职店主' 
    THEN 2 
    ELSE 0 
  END AS manager_type
,a.main_shelf_id
,a.shelf_level
,a.grade
FROM fe_dwd.dwd_shelf_base_day_all a 
JOIN fe_dwd.dwd_lsl_shelf_1_3 d
ON a.shelf_id = d.shelf_id
LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` c
ON a.shelf_id = c.shelf_id
UNION 
SELECT 
a.shelf_id
,a.zone_code
,a.shelf_code
,a.ACTIVATE_TIME
,a.shelf_type
,a.shelf_status
,a.revoke_status
,a.whether_close
,c.prewarehouse_id
,a.manager_id
,CASE
    WHEN a.manager_type = '全职店主' 
    THEN 1 
    WHEN a.manager_type = '兼职店主' 
    THEN 2 
    ELSE 0 
  END AS manager_type
,a.main_shelf_id
,a.shelf_level
,a.grade
FROM fe_dwd.dwd_shelf_base_day_all a 
JOIN fe_dwd.dwd_lsl_shelf_1_4 d
ON a.shelf_id = d.shelf_id
LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` c
ON a.shelf_id = c.shelf_id
;
 CREATE INDEX idx_dwd_lsl_shelf_1
ON fe_dwd.dwd_lsl_shelf_1 (shelf_id);
 
 --  上架数量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_4`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_4 AS 
SELECT 
        a.shelf_id,
		SUM(IFNULL(a.ACTUAL_FILL_NUM,0))  ACTUAL_FILL_NUM   -- 0703 ACTUAL_FILL_NUM change
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.ORDER_STATUS =4  -- 已上架
        AND a.FILL_TIME >= @start_date
		AND a.FILL_TIME < @end_date
GROUP BY a.shelf_id
;
 
CREATE INDEX idx_dwd_lsl_shelf_4
ON fe_dwd.dwd_lsl_shelf_4 (shelf_id);
-- 在途数量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_5`; -- 12s
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_5 AS 
SELECT 
        a.shelf_id,
        SUM(IFNULL(a.ACTUAL_APPLY_NUM,0)) AS ONWAY_NUM
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.ORDER_STATUS IN (1,2)  -- 20200703 宋英南和汤云峰要求剔除3状态
      AND a.APPLY_TIME >= @month_1_date
GROUP BY a.shelf_id
;
 
 
CREATE INDEX idx_dwd_lsl_shelf_5
ON fe_dwd.dwd_lsl_shelf_5 (shelf_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_7`; 
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_7 AS 
SELECT 
a.shelf_id
,a.zone_code
,a.shelf_code
,a.ACTIVATE_TIME
,a.shelf_type
,a.shelf_status
,a.revoke_status
,a.whether_close
,a.manager_id
,a.manager_type
,a.prewarehouse_id 
,a.main_shelf_id
,a.shelf_level
,a.grade 
,b.stock_quantity
,b.stock_skus
,b.stock_sum 
,c.sal_qty
,c.sal_qty_act
,ccc.skus
,cc.gmv
,cc1.before_refund_GMV
,cc.refunding_GMV
,c.o_product_total_amount
,c.o_discount_amount
,c.o_coupon_amount
,c.o_third_discount_amount
,f.after_payment_money
,cc.pay_amount
,cc.pay_amount_act
,cc2.refund_finish_amount
,cc1.before_refund_amount
,e.onway_num
,d.ACTUAL_FILL_NUM  
,ccc.orders 
,ccc.users 
FROM fe_dwd.dwd_lsl_shelf_1 a 
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_6  b 
ON a.shelf_id = b.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_2  c 
ON a.shelf_id = c.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_2_2 cc 
ON a.shelf_id = cc.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_2_4 cc1 
ON a.shelf_id = cc1.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_2_5 cc2 
ON a.shelf_id = cc2.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_2_3 ccc 
ON a.shelf_id = ccc.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_3  f 
ON a.shelf_id = f.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_4  d 
ON a.shelf_id = d.shelf_id
LEFT JOIN 
fe_dwd.dwd_lsl_shelf_5 e
ON a.shelf_id = e.shelf_id;


 INSERT INTO fe_dwd.dwd_shelf_day_his
(
sdate
,shelf_id
,zone_code
,shelf_code
,ACTIVATE_TIME
,shelf_type
,shelf_status
,revoke_status
,whether_close
,manager_id
,manager_type
,prewarehouse_id 
,main_shelf_id
,shelf_level
,grade         
,stock_quantity
,stock_skus
,stock_sum  
,sal_qty
,sal_qty_act
,skus
,gmv
,before_refund_GMV
,refunding_GMV
,o_product_total_amount
,o_discount_amount
,o_coupon_amount
,o_third_discount_amount
,AFTER_PAYMENT_MONEY
,pay_amount
,pay_amount_act
,refund_finish_amount
,before_refund_amount
,onway_num
,ACTUAL_FILL_NUM  
,orders 
,users 
)
SELECT
DATE_SUB(CURDATE(),INTERVAL 1 DAY) sdate
,a.shelf_id
,a.zone_code
,a.shelf_code
,a.ACTIVATE_TIME
,a.shelf_type
,a.shelf_status
,a.revoke_status
,a.whether_close
,a.manager_id
,a.manager_type
,a.prewarehouse_id 
,a.main_shelf_id
,a.shelf_level
,a.grade         
,a.stock_quantity
,a.stock_skus
,a.stock_sum  
,a.sal_qty
,a.sal_qty_act
,a.skus
,a.gmv
,a.before_refund_GMV
,a.refunding_GMV
,a.o_product_total_amount
,a.o_discount_amount
,a.o_coupon_amount
,a.o_third_discount_amount
,a.AFTER_PAYMENT_MONEY
,a.pay_amount
,a.pay_amount_act
,a.refund_finish_amount
,a.before_refund_amount
,a.onway_num
,a.ACTUAL_FILL_NUM  
,a.orders 
,a.users 
FROM fe_dwd.dwd_lsl_shelf_7 a;


-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_day_his',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('lishilong@', @user, @timestamp));
 
END