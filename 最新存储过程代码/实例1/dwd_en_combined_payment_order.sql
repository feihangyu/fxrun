CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_en_combined_payment_order`()
BEGIN
    SET  @sdate := SUBDATE(CURRENT_DATE,1);
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dwd.dwd_en_combined_payment_order WHERE  @sdate = DATE(pay_time_goods) ;
	   
INSERT INTO fe_dwd.`dwd_en_combined_payment_order`
(
order_pay_id_goods,
gateway_pay_id_goods,
pay_user_id_goods,
order_id_goods,
pay_amount_goods,
pay_type_goods,
pay_time_goods,
zh_pay_amount_pay,
zh_pay_type_pay,
order_pay_id_pay,
order_id_pay,
requirement_id,
order_id_re
)
SELECT 	t3.order_pay_id order_pay_id_goods,
        t3.gateway_pay_id gateway_pay_id_goods,
        t3.pay_user_id pay_user_id_goods,
	t3.`order_id`  order_id_goods,
	t3.`pay_amount`  pay_amount_goods,
	t3.pay_type pay_type_goods,
	t3.`pay_time` pay_time_goods,
	t5.PAY_AMOUNT zh_pay_amount_pay,
	CASE WHEN t5.pay_type = 4 THEN 'E币' ELSE '微信' END AS zh_pay_type_pay,
	t5.ORDER_PAY_ID order_pay_id_pay,
	t5.order_id order_id_pay,
	t4.requirement_id,
	t4.order_id order_id_re
FROM `fe_goods`.`sf_group_order_pay` t3 	
JOIN  fe_pay.sf_pay_requirement t4 ON t3.order_pay_id=t4.order_id  AND t4.pay_state IN(1,2)	
JOIN  fe_pay.sf_order_pay_2 t5 ON t4.requirement_id=t5.order_id  AND t5.pay_state=2 -- AND t5.pay_type =4 
WHERE t3.pay_type = 16	
 AND  t3.pay_state = 2
 AND t3.data_flag = 1
 AND t4.data_flag = 1
 AND t5.data_flag = 1
 AND t4.requirement_type='shopOrder'
 AND  t3.pay_time >= @sdate	
 AND  t3.pay_time < CURDATE()
UNION ALL
SELECT 	t3.order_pay_id order_pay_id_goods,
        t3.gateway_pay_id gateway_pay_id_goods,
        t3.pay_user_id pay_user_id_goods,
	t3.`order_id`  order_id_goods,
	t3.`pay_amount`  pay_amount_goods,
	t3.pay_type pay_type_goods,
	t3.`pay_time` pay_time_goods,
	t5.PAY_AMOUNT zh_pay_amount_pay,
	CASE WHEN t5.pay_type = 4 THEN 'E币' ELSE '微信' END AS zh_pay_type_pay,
	t5.ORDER_PAY_ID order_pay_id_pay,
	t5.order_id order_id_pay,
	t4.requirement_id,
	t4.order_id order_id_re
FROM `fe_goods`.`sf_group_order_pay` t3 	
JOIN  fe_pay.sf_pay_requirement t4 ON t3.order_pay_id=t4.order_id  AND t4.pay_state IN(1,2) AND t4.data_flag = 1	
JOIN  fe_pay.sf_order_pay_3 t5 ON t4.requirement_id=t5.order_id  AND t5.pay_state=2 AND t5.data_flag = 1 -- AND t5.pay_type =4 
WHERE t3.pay_type = 16 AND  t3.pay_state = 2 AND t3.data_flag = 1
 AND t4.requirement_type='shopOrder'
 AND  t3.pay_time >= @sdate	
 AND  t3.pay_time < CURDATE()
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_en_combined_payment_order',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('郑志省@', @user, @timestamp));
 
COMMIT;
END