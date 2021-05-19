CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_preware_wave_cycle`(in_sdate DATE)
BEGIN
    		
SET @sdate = in_sdate;
SET @sdate1 = DATE_ADD(in_sdate,INTERVAL 1 DAY) ;
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 备货频次
DELETE FROM feods.d_sc_preware_fill_frequency ;
INSERT INTO feods.d_sc_preware_fill_frequency
(region_area
, business_area
, shelf_name
, warehouse_id
, shelf_code
, manager_id
, real_name
, manager_type
, whether_close
, create_time
, preware_type
, operate_time
, fill_frequency
, Mon
, Tue
, Wed
, Thur
, Fri
, Sat
, Sun
, avg_fill_times
, min_cycle
, max_cycle
, next_fill_date
)
SELECT w.region_area
, w.business_area
, s.shelf_name
, t.warehouse_id
, s.shelf_code
, s.manager_id
, s.real_name
, s.manager_type
, s.whether_close
, s.Add_TIME AS create_time
, CASE p.warehouse_type WHEN 1 THEN "内部前置仓" WHEN 2 THEN "外部" END AS preware_type
, IF(ISNULL(s.operation_time),s.Add_TIME,s.operation_time) AS operate_time
-- , contact
, CONCAT("1周",COUNT(*),"配")  fill_frequency
-- , 1 as fill_fixed
, SUM(IF(t.`delivery_date` =1,1,0)) Mon
, SUM(IF(t.`delivery_date` =2,1,0)) Tue
, SUM(IF(t.`delivery_date` =3,1,0)) Wed
, SUM(IF(t.`delivery_date` =4,1,0)) Thur
, SUM(IF(t.`delivery_date` =5,1,0)) Fri
, SUM(IF(t.`delivery_date` =6,1,0)) Sat
, SUM(IF(t.`delivery_date` =7,1,0)) Sun
-- , not_fixed_reason
, 7/COUNT(t.`delivery_days`) avg_fill_times 
, MIN(t.`delivery_days`) min_cycle
, MAX(t.`delivery_days`) max_cycle
-- , next_fill_date
,ADDDATE(CURDATE(), MIN(IF( WEEKDAY(CURDATE())+1 <= t.`delivery_date`,t.`delivery_date`,7+ t.`delivery_date`)) - WEEKDAY(CURDATE())-1) next_fill_date
FROM fe.`sf_prewarehouse_delivery_date_config` t
JOIN fe_dwd.`dwd_shelf_base_day_all` s
ON t.`warehouse_id` = s.`shelf_id`
AND s.data_flag = 1
JOIN fe_dwd.`dwd_sc_business_region` w
ON s.`business_name` = w.`business_area`
JOIN fe.`sf_prewarehouse_info` p
ON t.warehouse_id = p.warehouse_id
AND p.data_flag =1
WHERE t.`data_flag` = 1
GROUP BY t.`warehouse_id`
;
# 目标周转天
DELETE FROM feods.d_sc_preware_wave_cycle WHERE sdate = @sdate;
INSERT INTO feods.d_sc_preware_wave_cycle
(`sdate`,
  `region_area`,
  `business_area`,
  `warehouse_id`,
  `shelf_name`,
  `shelf_code`,
  `product_id`,
  `product_name`,
  `product_code2`,
  `product_type`,
 --  `adjust_sale_flag`,
   op_product_type,
   available_stock,
  `sales_level`,
   avg_fill_times,
   fill_frequency,
   preware_status,
   new_min_wave_cycle,
   new_max_wave_cycle,
   new_mid_wave_cycle
) 
SELECT t2.sdate
, t2.region_area
, t2.business_area
, t2.`warehouse_id`
, t2.shelf_name
, t2.shelf_code
, t2.`product_id`
, t2.product_name
, t2.product_code2
, t2.fname AS product_type
, t5.product_type AS op_product_type
, t2.available_stock 
, IFNULL(t4.sale_flag,"严重滞销") sale_flag # 前置仓畅销等级
, t1.avg_fill_times
, t1.fill_frequency
, CASE WHEN s.whether_close =1 THEN "已关闭" WHEN s.whether_close = 2 AND s.shelf_type = 11 THEN "撤架中" ELSE "未关闭"  END AS preware_status
-- , CASE 
-- WHEN ISNULL(t1.min_cycle) THEN 1
-- WHEN t5.product_type = '原有' AND t4.sale_flag IN ("爆款","畅销") THEN 2
-- WHEN t5.product_type = '原有' AND t4.sale_flag = "平销" THEN 3
-- WHEN t5.product_type = '原有' AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN 4
-- WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag IN ("爆款","畅销") THEN 5
-- WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag = "平销" THEN 6
-- WHEN t5.product_type = '新增（试运行）'  AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN 7
-- WHEN t4.sale_flag IN ("爆款","畅销") THEN 8
-- WHEN (t4.sale_flag IN ("平销","滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN 9
-- END AS sence
, CASE 
WHEN ISNULL(t1.min_cycle) THEN 6 # 没有维护的仓默认为6
WHEN t5.product_type = '原有' AND t4.sale_flag IN ("爆款","畅销") THEN min_cycle + 4
WHEN t5.product_type = '原有' AND t4.sale_flag = "平销" THEN min_cycle + 3
WHEN t5.product_type = '原有' AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN min_cycle 
WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag IN ("爆款","畅销") THEN min_cycle + 5
WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag = "平销" THEN min_cycle + 5
WHEN t5.product_type = '新增（试运行）'  AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN min_cycle + 2
WHEN t4.sale_flag IN ("爆款","畅销") THEN min_cycle 
WHEN (t4.sale_flag IN ("平销","滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN  1
END AS new_min_wave_cycle
, CASE 
WHEN ISNULL(t1.max_cycle) THEN 6 # 没有维护的仓默认为6
WHEN t5.product_type = '原有' AND t4.sale_flag IN ("爆款","畅销") THEN IF(max_cycle + 4 >=12,12,max_cycle + 4) + 2
WHEN t5.product_type = '原有' AND t4.sale_flag = "平销" THEN IF(max_cycle + 3 >=11,11,max_cycle + 3) + 2
WHEN t5.product_type = '原有' AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN IF(max_cycle >= 8,8,max_cycle  ) + 1
WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag IN ("爆款","畅销") THEN IF(max_cycle + 5 >=12,12,max_cycle + 5) + 2
WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag = "平销" THEN IF(max_cycle + 5 >=12,12,max_cycle + 5) +2
WHEN t5.product_type = '新增（试运行）'  AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN IF(max_cycle +2 >=9,9,max_cycle + 2) + 1
WHEN t4.sale_flag IN ("爆款","畅销") THEN IF(max_cycle  >=7,7,max_cycle ) + 1
WHEN (t4.sale_flag IN ("平销","滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN IF(max_cycle  >=5,5,max_cycle) + 1
END AS new_max_wave_cycle
, CASE 
WHEN ISNULL(t1.max_cycle) THEN 6 # 没有维护的仓默认为6
WHEN t5.product_type = '原有' AND t4.sale_flag IN ("爆款","畅销") THEN IF(max_cycle + 4 >=12,12,max_cycle + 4)
WHEN t5.product_type = '原有' AND t4.sale_flag = "平销" THEN IF(max_cycle + 3 >=11,11,max_cycle + 3)
WHEN t5.product_type = '原有' AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN IF(max_cycle  >= 8,8,max_cycle  )
WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag IN ("爆款","畅销") THEN IF(max_cycle + 5 >=12,12,max_cycle + 5)
WHEN t5.product_type = '新增（试运行）'  AND t4.sale_flag = "平销" THEN IF(max_cycle + 5 >=12,12,max_cycle + 5)
WHEN t5.product_type = '新增（试运行）'  AND (t4.sale_flag IN ("滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN IF(max_cycle +2 >=9,9,max_cycle + 2)
WHEN t4.sale_flag IN ("爆款","畅销") THEN IF(max_cycle  >=7,7,max_cycle )
WHEN (t4.sale_flag IN ("平销","滞销","严重滞销" ) OR ISNULL(t4.sale_flag)) THEN IF(max_cycle  >=5,5,max_cycle)
END AS new_mid_wave_cycle
FROM feods.`d_sc_preware_balance` t2
-- LEFT JOIN feods.`pj_preware_shelf_sales_thirty` t4 
# 2020-04-01日开始使用近15天日均销
LEFT JOIN feods.`pj_preware_sales_fifteen` t4
ON t2.sdate = t4.sdate
AND t2.warehouse_id = t4.warehouse_id
AND t2.product_id = t4.product_id
# 2020-04-02日修改，业务未添加的前置仓默认1周1配，最大和最小值都取6
LEFT JOIN feods.d_sc_preware_fill_frequency t1
ON t2.warehouse_id = t1.warehouse_id
LEFT JOIN feods.`zs_product_dim_sserp` t5
ON t2.business_area = t5.business_area
AND t2.product_id = t5.product_id
JOIN fe_dwd.`dwd_shelf_base_day_all` s
ON t2.warehouse_id = s.shelf_id
AND s.data_flag = 1
WHERE t2.sdate = @sdate
-- WHERE t2.sdate = subdate(curdate(),1)
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_sc_preware_wave_cycle',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
   
COMMIT;
    END