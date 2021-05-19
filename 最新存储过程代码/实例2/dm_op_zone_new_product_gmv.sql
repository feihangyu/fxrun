CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_zone_new_product_gmv`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 片区新品gmv(逻辑与feods .zs_new_product_gmv保持一致，只是细分至片区维度)
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.dm_op_zone_new_product_gmv WHERE order_date = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
INSERT INTO fe_dm.dm_op_zone_new_product_gmv
(order_date,
 business_area,
 zone_code,
 zone_name,
 product_id,
 product_type,
 version,
 quantity,
 gmv,
 discount_amount,
 real_total_price,
 load_time
)
SELECT t5.order_date,
       t1.business_area,
       t4.zone_code,
       t4.zone_name,
       t1.product_id,
       t1.product_type,
       t1.version,
       SUM(t5.quantity) AS quantity,
       SUM(t5.gmv) AS gmv,
       SUM(t5.discount_amount) AS discount_amount,
       SUM(t5.real_total_price) AS real_total_price,
       @timestamp AS load_time
FROM
(SELECT business_area,
        product_id,
        product_type,
        `version`
FROM fe_dwd.dwd_pub_product_dim_sserp   -- zs_product_dim_sserp
WHERE product_type IN ('新增（试运行）','新增（免费货）')
) t1
JOIN fe_dwd.dwd_city_business t3 ON t1.business_area = t3.business_name
JOIN fe_dwd.dwd_shelf_base_day_all t4 ON t3.city_name = SUBSTRING_INDEX(SUBSTRING_INDEX(t4.AREA_ADDRESS, ',', 2),',',- 1) AND t4.data_flag = 1
JOIN
(SELECT DATE(a.PAY_DATE) AS order_date,
        a.shelf_id,
        a.product_id,
        SUM(quantity) AS quantity,
        SUM(quantity * sale_price) AS gmv,
        SUM(discount_amount) AS discount_amount,
        SUM(real_total_price) AS real_total_price
FROM fe_dwd.dwd_pub_order_item_recent_one_month  a   -- sf_order_item_temp
WHERE a.PAY_DATE >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
AND a.PAY_DATE < CURDATE()
AND a.order_status = 2 
GROUP BY DATE(a.PAY_DATE),a.shelf_id,a.product_id
) t5 ON t1.product_id = t5.product_id AND t4.shelf_id = t5.shelf_id
GROUP BY t5.order_date,t1.business_area,t4.zone_code,t4.zone_name,t1.product_id; 
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_zone_new_product_gmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_zone_new_product_gmv','dm_op_zone_new_product_gmv','朱星华');
  COMMIT;	
END