CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_new_product_gmv`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.dm_new_product_gmv WHERE order_date = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
INSERT INTO fe_dm.dm_new_product_gmv
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
SELECT
    t5.order_date
    , t1.business_area
    , t1.product_id
    , t2.PRODUCT_CODE2
    , t2.PRODUCT_NAME
    , t1.product_type
    , SUM(t5.quantity) AS quantity
    , SUM(t5.GMV) AS GMV
    , SUM(t5.discount_amount) AS discount_amount
    , SUM(t5.real_total_price) AS real_total_price
FROM fe_dwd.`dwd_pub_product_dim_sserp` t1
JOIN fe_dwd.`dwd_product_base_day_all` t2
ON t1.product_id = t2.product_id
AND t1.product_type IN ("新增（试运行）","新增（免费货）","淘汰（替补）")
JOIN fe_dwd.dwd_shelf_base_day_all t4
ON t1.business_area = t4.`business_name`
AND t4.data_flag = 1
JOIN
(
SELECT
    DATE(a.pay_date) AS order_date
    , a.shelf_id
    , a.product_id
    , SUM(quantity_act) AS quantity
    , SUM(quantity_act * sale_price) AS GMV
    , SUM(discount_amount) AS discount_amount
    , SUM(real_total_price) AS real_total_price
FROM
    fe_dwd.`dwd_pub_order_item_recent_one_month` a
WHERE a.pay_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    AND a.pay_date < CURDATE()
    AND a.ORDER_STATUS = 2 # 11月05日改回去除自动贩卖机，上一次修改10月16日，包括所有货架
GROUP BY DATE(a.pay_date)
    , a.shelf_id
    , a.product_id) t5
ON t1.product_id = t5.product_id
AND t4.shelf_id = t5.shelf_id
-- WHERE order_date IS NOT NULL
GROUP BY t5.order_date,t1.business_area,t1.product_id,t2.PRODUCT_CODE2,t2.PRODUCT_NAME
; 
 
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_new_product_gmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_new_product_gmv','dm_new_product_gmv','吴婷');
COMMIT;
    END