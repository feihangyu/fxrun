CREATE DEFINER=`feprocess`@`%` PROCEDURE `sf_new_product_gmv`()
    SQL SECURITY INVOKER
BEGIN
    DECLARE l_test VARCHAR(1);
    DECLARE l_row_cnt INT;
    DECLARE CODE CHAR(5) DEFAULT '00000';
    DECLARE done INT;
    
	DECLARE l_table_owner   VARCHAR(64);
	DECLARE l_city          VARCHAR(64);
    DECLARE l_task_name     VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1
			CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
			CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
		END; 
		
    SET l_task_name = 'sf_new_product_gmv'; 
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM feods.zs_new_product_gmv WHERE order_date = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
INSERT INTO feods.zs_new_product_gmv
(order_date,
 business_area,
 product_id,
 product_code2,
 product_name,
 product_type,
 quantity,
 GMV,
 discount_amount,
 real_total_price
)
SELECT t5.order_date,t1.business_area,t1.product_id,t2.PRODUCT_CODE2,t2.PRODUCT_NAME,t1.product_type,SUM(t5.quantity) AS quantity,SUM(t5.GMV) AS GMV,
SUM(t5.discount_amount) AS discount_amount,SUM(t5.real_total_price) AS real_total_price
FROM feods.zs_product_dim_sserp t1
JOIN fe_dwd.`dwd_product_base_day_all` t2
ON t1.product_id = t2.product_id
AND t1.product_type IN ("新增（试运行）","新增（免费货）","淘汰（替补）")
JOIN fe_dwd.dwd_shelf_base_day_all t4
ON t1.business_area = t4.`business_name`
AND t4.data_flag = 1
JOIN
(SELECT DATE(a.order_date) AS order_date,a.shelf_id,a.product_id,SUM(quantity) AS quantity, SUM(quantity * sale_price) AS GMV,SUM(discount_amount) AS discount_amount, 
SUM(real_total_price) AS real_total_price
FROM feods.sf_order_item_temp a
WHERE 
a.order_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND a.order_date < CURDATE()
AND a.ORDER_STATUS = 2 # 11月05日改回去除自动贩卖机，上一次修改10月16日，包括所有货架
GROUP BY DATE(a.order_date),a.shelf_id,a.product_id) t5
ON t1.product_id = t5.product_id
AND t4.shelf_id = t5.shelf_id
-- WHERE order_date IS NOT NULL
GROUP BY t5.order_date,t1.business_area,t1.product_id,t2.PRODUCT_CODE2,t2.PRODUCT_NAME; 
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sf_new_product_gmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
  COMMIT;
END