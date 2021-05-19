CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_pub_order_shelf_product_yht`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
       SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_order_shelf_product_yht_1`;   
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_order_shelf_product_yht_1  AS 
SELECT 
 a.`area_name`
,a.`site_name`
,a.`site_number`
,a.`line_name`
,a.`shelf_id`
,b.`locationId`
,a.`machine_type`
,a.`trade_no`
,a.`asset_id`
,a.`create_time`
,a.`recv_ts`
,a.`exec_ts`
,a.`order_no`
,a.`transaction_id`
,a.`order_id`
,a.`special_type`
,a.`pay_style`
,a.`pay_status`
,a.`payTime`
,a.`notice_ts`
,a.`deliver_status`
,a.`client_id`
,a.`open_id`
,b.goods_id AS `product_id`
,b.goods_name AS `product_name`
,b.`price`
,b.`product_type_id`
,b.`product_type_name`
,b.`product_count`
,a.`product_total_amount`
,a.`price_2`
,a.`refund_status`
,a.`refund_fee`
,a.`refund_time`
FROM
fe.sf_order_yht a
JOIN fe.sf_order_yht_item b
ON a.order_id = b.order_id
WHERE a.data_flag = 1
AND a.`pay_status` = 1 #支付成功
      AND a.payTime >= @start_date
      AND a.payTime < @end_date;
	  
-- 防止异常报错
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_pub_order_shelf_product_yht_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_order_shelf_product_yht_test LIKE fe_dwd.dwd_pub_order_shelf_product_yht;
insert into  fe_dwd.dwd_pub_order_shelf_product_yht_test
(
`area_name`
,`site_name`
,`site_number`
,`line_name`
,`shelf_id`
,`locationId`
,`machine_type`
,`trade_no`
,`asset_id`
,`create_time`
,`recv_ts`
,`exec_ts`
,`order_no`
,`transaction_id`
,`order_id`
,`special_type`
,`pay_style`
,`pay_status`
,`payTime`
,`notice_ts`
,`deliver_status`
,`client_id`
,`open_id`
,`product_id`
,`product_name`
,`price`
,`product_type_id`
,`product_type_name`
,`product_count`
,`product_total_amount`
,`price_2`
,`refund_status`
,`refund_fee`
,`refund_time`
) 
select 
a.`area_name`
,a.`site_name`
,a.`site_number`
,a.`line_name`
,a.`shelf_id`
,a.`locationId`
,a.`machine_type`
,a.`trade_no`
,a.`asset_id`
,a.`create_time`
,a.`recv_ts`
,a.`exec_ts`
,a.`order_no`
,a.`transaction_id`
,a.`order_id`
,a.`special_type`
,a.`pay_style`
,a.`pay_status`
,a.`payTime`
,a.`notice_ts`
,a.`deliver_status`
,a.`client_id`
,a.`open_id`
,a.`product_id`
,a.`product_name`
,a.`price`
,a.`product_type_id`
,a.`product_type_name`
,a.`product_count`
,a.`product_total_amount`
,a.`price_2`
,a.`refund_status`
,a.`refund_fee`
,a.`refund_time`
from fe_dwd.dwd_pub_order_shelf_product_yht_1 a;
 
	
-- 清空数据 
delete from  fe_dwd.dwd_pub_order_shelf_product_yht 
where payTime >= @start_date;
INSERT INTO fe_dwd.dwd_pub_order_shelf_product_yht
(
`area_name`
,`site_name`
,`site_number`
,`line_name`
,`shelf_id`
,`locationId`
,`machine_type`
,`trade_no`
,`asset_id`
,`create_time`
,`recv_ts`
,`exec_ts`
,`order_no`
,`transaction_id`
,`order_id`
,`special_type`
,`pay_style`
,`pay_status`
,`payTime`
,`notice_ts`
,`deliver_status`
,`client_id`
,`open_id`
,`product_id`
,`product_name`
,`price`
,`product_type_id`
,`product_type_name`
,`product_count`
,`product_total_amount`
,`price_2`
,`refund_status`
,`refund_fee`
,`refund_time`
) 
select 
a.`area_name`
,a.`site_name`
,a.`site_number`
,a.`line_name`
,a.`shelf_id`
,a.`locationId`
,a.`machine_type`
,a.`trade_no`
,a.`asset_id`
,a.`create_time`
,a.`recv_ts`
,a.`exec_ts`
,a.`order_no`
,a.`transaction_id`
,a.`order_id`
,a.`special_type`
,a.`pay_style`
,a.`pay_status`
,a.`payTime`
,a.`notice_ts`
,a.`deliver_status`
,a.`client_id`
,a.`open_id`
,a.`product_id`
,a.`product_name`
,a.`price`
,a.`product_type_id`
,a.`product_type_name`
,a.`product_count`
,a.`product_total_amount`
,a.`price_2`
,a.`refund_status`
,a.`refund_fee`
,a.`refund_time`
from fe_dwd.dwd_pub_order_shelf_product_yht_1 a;
 
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_pub_order_shelf_product_yht',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END