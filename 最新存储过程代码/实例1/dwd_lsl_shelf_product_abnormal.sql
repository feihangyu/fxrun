CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_lsl_shelf_product_abnormal`()
BEGIN
   SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_3_date = SUBDATE(@end_date,INTERVAL 90 DAY);
   SET @month_2_date = SUBDATE(@end_date,INTERVAL 60 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @timestamp := CURRENT_TIMESTAMP();
   -- 先删除历史数据
TRUNCATE TABLE fe_dwd.dwd_lsl_shelf_product_abnormal; 
  
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_1  AS 
SELECT a.detail_id
FROM fe.sf_shelf_product_detail a
JOIN fe.sf_shelf_product_detail_flag b
ON a.detail_id = b.detail_id 
AND a.data_flag = 1
AND b.data_flag = 2;
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe.sf_shelf_product_detail.detail_id' AS table_name,
detail_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_1 ;
  
-- COMMIT;
/*
订单宽表中，订单支付了，但是订单状态没有修改为已支付的
*/
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_2`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_2  AS 
SELECT a.order_id AS detail_id FROM fe_dwd.dwd_order_item_refund_day a
WHERE a.PAY_DATE >= @month_2_date
AND a.order_status NOT IN (2,6,7);
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_order_item_refund_day.order_id.order_status' AS table_name,
detail_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_2 ;
  
-- COMMIT;


/*
订单宽表中，订单支付了，但回传失败导致pay_state ！=2 的
*/
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_2_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_2_1  AS 
SELECT a.order_id AS detail_id FROM fe_dwd.dwd_order_item_refund_day a
WHERE a.PAY_DATE >= @month_2_date
AND a.PAY_STATE != 2;
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_order_item_refund_day.order_id.PAY_STATE' AS table_name,
detail_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_2_1 ;
  
-- COMMIT;

/*
订单宽表月度表中，有重复的数据预警
*/
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_3`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_3  AS 
select order_item_id as detail_id,pay_id,count(*)
from fe_dwd.`dwd_pub_order_item_recent_two_month`
group by order_item_id,pay_id
having count(*) >1;

INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_pub_order_item_recent_two_month.order_item_id.COUNT' AS table_name,
detail_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_3 ;
  
COMMIT;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_4`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_4  AS 
select order_item_id as detail_id,count(*)
from fe_dwd.`dwd_pub_order_item_recent_one_month`
group by order_item_id
having count(*) >1;
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_pub_order_item_recent_one_month.order_item_id.COUNT' AS table_name,
detail_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_4 ;
  
-- COMMIT;
-- 订单类型
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_5`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_5  AS 
SELECT a.order_type,a.order_type_number,a.order_id FROM 
fe_dwd.dwd_group_order_refound_address_day a
WHERE a.pay_time >='2020-01-01'
AND a.order_type IS NULL 
AND a.order_type_number IS NOT NULL
union all
SELECT a.order_type,a.order_type_number,a.order_id FROM 
fe_dwd.dwd_group_order_refound_address_day a
WHERE a.pay_time >='2020-01-01'
AND a.order_type_number IS NULL 
;
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_group_order_refound_address_day.order_type' AS table_name,
order_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_5 ;
  
-- COMMIT;
-- 支付类型
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_6`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_6  AS 
SELECT a.pay_type,a.pay_type_desc,a.order_id FROM 
fe_dwd.dwd_group_order_refound_address_day a
WHERE a.pay_time >='2020-01-01'
AND a.pay_type_desc  IS NULL 
AND a.`pay_type` IS NOT NULL 
union all
SELECT a.pay_type,a.pay_type_desc,a.order_id FROM 
fe_dwd.dwd_group_order_refound_address_day a
WHERE a.pay_time >='2020-01-01'
AND a.`pay_type` IS NULL ;
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_group_order_refound_address_day.pay_type_desc' AS table_name,
order_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_6 ;
  
-- COMMIT;
-- 销售渠道
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_7`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_7  AS 
SELECT a.sale_from,a.sale_channel,a.order_from,a.order_id FROM 
fe_dwd.dwd_group_order_refound_address_day a
WHERE a.pay_time >='2020-01-01'
AND  a.sale_from   IS NULL 
AND a.sale_channel IS NOT NULL
AND  a.order_from= '用户自主下单'
union all 
SELECT a.sale_from,a.sale_channel,a.order_from,a.order_id FROM 
fe_dwd.dwd_group_order_refound_address_day a
WHERE a.pay_time >='2020-01-01'
AND a.sale_channel IS  NULL
AND  a.order_from= '用户自主下单' ;
-- 这里还要确定一下 0609
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_group_order_refound_address_day.sale_from' AS table_name,
order_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_7 ;
COMMIT;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_8`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_8  AS 
 SELECT COUNT(*) cnt FROM fe_dwd.`dwd_pub_order_item_recent_two_month` WHERE pay_date >=SUBDATE(CURDATE(),62)
 UNION
 SELECT COUNT(*) FROM fe_dwd.`dwd_order_item_refund_day` WHERE pay_date >=SUBDATE(CURDATE(),62)
  UNION
  SELECT COUNT(*) FROM fe_dwd.`dwd_pub_order_item_recent_one_month` WHERE pay_date >=SUBDATE(CURDATE(),31)
 UNION
 SELECT COUNT(*) FROM fe_dwd.`dwd_order_item_refund_day` WHERE pay_date >=SUBDATE(CURDATE(),31)
  UNION
 SELECT COUNT(*) FROM fe_dwd.`dwd_pub_order_item_recent_two_month` WHERE pay_date >=SUBDATE(CURDATE(),31)
 ;
 
 SET @cnt_num = ( SELECT COUNT(*) FROM fe_dwd.dwd_lsl_shelf_product_abnormal_8) ;
IF @cnt_num > 2 THEN
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
(
SELECT 
'fe_dwd.dwd_pub_order_item_recent_month.!=' AS table_name,
COUNT(*) cnt
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_8 
);
END IF;
COMMIT;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_abnormal_9`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_abnormal_9  AS 
SELECT order_item_id AS detail_id,COUNT(*)
FROM  fe_dwd.`dwd_group_order_refound_address_day`
WHERE order_id NOT IN (23891136100670000,23821892100670000)
GROUP BY order_item_id
HAVING COUNT(*) >1;
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe_dwd.dwd_group_order_refound_address_day.order_item_id' AS table_name,
detail_id
FROM 
fe_dwd.dwd_lsl_shelf_product_abnormal_9 ;
  
-- COMMIT;
-- 判断城市异常
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_fill_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_fill_tmp
as
select  distinct address,shelf_name, SUBSTRING_INDEX(SUBSTRING_INDEX(c.`AREA_ADDRESS`,',',2),',',-1) CITY_NAME,city
from fe.sf_shelf c
where c.`DATA_FLAG` =1
and c.invalid_reason != '系统逾期失效' ;
delete from fe_dwd.shelf_product_fill_tmp
where  shelf_name  LIKE '%测试%';
DELETE FROM fe_dwd.shelf_product_fill_tmp
WHERE  shelf_name  LIKE '%作废%';
DELETE FROM fe_dwd.shelf_product_fill_tmp
WHERE  address  LIKE '%作废%';
-- -- 几个异常值
DELETE FROM fe_dwd.shelf_product_fill_tmp
WHERE  city in   (810100,710100,710200,810100);
INSERT INTO fe_dwd.dwd_lsl_shelf_product_abnormal
(
table_name,
detail_id
)
SELECT DISTINCT
'fe.sf_shelf.city' AS table_name,
 a.city as detail_id FROM fe_dwd.shelf_product_fill_tmp a
LEFT JOIN feods.`fjr_city_business`  b
ON a.city = b.city 
WHERE b.`CITY` IS NULL;
  
-- COMMIT;
END