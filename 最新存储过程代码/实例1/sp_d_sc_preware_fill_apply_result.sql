CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_preware_fill_apply_result`(in_sdate DATE )
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
		
    SET l_task_name = 'sp_d_sc_preware_fill_apply_result'; 
    
SET  @sdate = in_sdate; 
SET  @sdate1 = DATE_add(@sdate1,INTERVAL 1 DAY);
SET @run_date:= CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.preware_suggest_daily_tmp;
CREATE TEMPORARY TABLE feods.preware_suggest_daily_tmp AS
SELECT a.suggest_time AS sdate,
p.region_area,
c.business_area,
b.warehouse_id,
f.shelf_code ,
f.`SHELF_NAME`,
a.`SHELF_ID`,
a.`PRODUCT_ID`,
a.`SUPPLIER_NAME`,
a.suggest_fill_num 
FROM fe.sf_prewarehouse_shelf_detail b
JOIN fe.sf_shelf f
ON b.`warehouse_id` = f.`SHELF_ID`
AND b.`data_flag` = 1
AND f.`DATA_FLAG` =1
JOIN feods.sf_product_daily_fill_requirement_temp a
ON b.shelf_id = a.shelf_id
AND suggest_time >= @sdate AND suggest_time < @sdate1
JOIN feods.`zs_city_business` c
ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(f.area_address, ",", - 2),",",1) = c.city_name
JOIN 
 fe_dwd.`dwd_pub_warehouse_business_area` p   ### 仓库编码
ON c.business_area = p.business_area
AND p.to_preware =1 ;
CREATE INDEX idx_warehouse_sdate_shelf_product
ON feods.preware_suggest_daily_tmp (sdate,warehouse_id,shelf_id, product_id);
# 当日前置仓出库
DROP TEMPORARY TABLE IF EXISTS feods.preware_fill_apply_tmp;
CREATE TEMPORARY TABLE feods.preware_fill_apply_tmp AS
SELECT DATE(fo.APPLY_TIME) AS sdate,p.region_area,c.business_area,sd.warehouse_id,f.shelf_code,f.shelf_name,fo.`shelf_id`,fi.`product_id`,fi.`actual_apply_num`
FROM fe.`sf_product_fill_order` fo
JOIN fe.`sf_product_fill_order_item` fi
ON fo.`ORDER_ID` = fi.`ORDER_ID`
JOIN fe.sf_prewarehouse_shelf_detail sd
ON fo.shelf_id = sd.shelf_id
AND sd.data_flag = 1
JOIN fe.sf_shelf f
ON sd.`warehouse_id` = f.`SHELF_ID`
AND f.`DATA_FLAG` =1
JOIN feods.`zs_city_business` c
ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(f.area_address, ",", - 2),",",1) = c.city_name
 JOIN fe_dwd.`dwd_pub_warehouse_business_area` p   ### 仓库编码
ON c.business_area = p.business_area 
and p.to_preware =1
WHERE fo.apply_time >= @sdate AND apply_time < @sdate1
AND fo.`DATA_FLAG` =1
AND fo.`FILL_TYPE` IN (1,2,8,9) # 人工、系统、要货、前置仓调货架的订单类型
AND fo.`SUPPLIER_TYPE` = 9 #供应商为前置仓
AND fo.`ORDER_STATUS` < 9;#排除已取消
CREATE INDEX idx_warehouse_sdate_shelf_product
ON feods.preware_fill_apply_tmp (sdate,warehouse_id,shelf_id, product_id);
# 当日前置仓推单和出库的商品名单
DROP TEMPORARY TABLE IF EXISTS feods.preware_require_fill_product_list;
CREATE TEMPORARY TABLE feods.preware_require_fill_product_list AS
SELECT sdate,region_area,business_area,warehouse_id,shelf_code,shelf_name ,shelf_id,product_id
FROM feods.preware_suggest_daily_tmp a
WHERE a.sdate = @sdate
UNION 
SELECT sdate,region_area,business_area,warehouse_id,shelf_code,shelf_name,shelf_id,product_id
FROM feods.preware_fill_apply_tmp a
WHERE a.sdate = @sdate;
CREATE INDEX idx_warehouse_sdate_shelf_product
ON feods.preware_require_fill_product_list (sdate,warehouse_id,shelf_id, product_id);
DROP TEMPORARY TABLE IF EXISTS feods.preware_fill_apply_tmp_result_detail;
CREATE TEMPORARY TABLE feods.preware_fill_apply_tmp_result_detail AS 
SELECT t1.sdate,t1.region_area,t1.business_area,t1.warehouse_id,t1.shelf_code,t1.shelf_name,t1.shelf_id,t1.product_id,
IFNULL(t2.suggest_fill_num ,0) AS suggest_fill_num,
t2.supplier_name,
-- t2.shelf_code,
-- t2.shelf_name,
IFNULL(t3.actual_apply_num,0) AS actual_apply_num,
CASE 
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) >3 AND IFNULL(t3.actual_apply_num,0)/ IFNULL(t2.suggest_fill_num ,0)> 2  THEN "多补>100%"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) >3 AND IFNULL(t3.actual_apply_num,0)/ IFNULL(t2.suggest_fill_num ,0)<= 2 AND IFNULL(t3.actual_apply_num,0)/ IFNULL(t2.suggest_fill_num ,0)> 1.5 THEN "多补<=100%"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) >3 AND IFNULL(t3.actual_apply_num,0)/ IFNULL(t2.suggest_fill_num ,0)<= 1.5 THEN "多补<=50%"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) <=3 AND IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) >= 1 THEN "多补数量<=3"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) = 0 THEN "一致"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) < -3 AND IFNULL(t3.actual_apply_num,0)/ IFNULL(t2.suggest_fill_num ,0) < 0.5 THEN "少补>50%"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) >= -3 AND IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) <= -1 THEN "少补<=3"
WHEN IFNULL(t3.actual_apply_num,0) - IFNULL(t2.suggest_fill_num ,0) < -3 AND IFNULL(t3.actual_apply_num,0)/ IFNULL(t2.suggest_fill_num ,0) >= 0.5 THEN "少补<=50%"
WHEN IFNULL(t3.actual_apply_num,0) > 0 AND IFNULL(t2.suggest_fill_num ,0) = 0 THEN "自主补货无推荐"
END AS same_rate
FROM feods.preware_require_fill_product_list t1
LEFT JOIN feods.preware_suggest_daily_tmp t2
ON t1.warehouse_id = t2.warehouse_id
AND t1.shelf_id = t2.shelf_id
AND t1.product_id = t2.product_id
AND t1.sdate = t2.sdate
LEFT JOIN feods.preware_fill_apply_tmp t3
ON t1.warehouse_id = t3.warehouse_id
AND t1.shelf_id = t3.shelf_id
AND t1.product_id = t3.product_id
AND t1.sdate = t3.sdate;
DELETE FROM feods.d_sc_preware_fill_apply_result WHERE sdate = @sdate;
INSERT INTO feods.d_sc_preware_fill_apply_result
(sdate,
region_area,
business_area,
warehouse_id,
shelf_code,
shelf_name,
same_rate,
cnt
)
SELECT sdate,region_area,business_area,warehouse_id,shelf_code,shelf_name,same_rate,COUNT(*) AS cnt
FROM feods.preware_fill_apply_tmp_result_detail 
GROUP BY warehouse_id,same_rate;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'sp_d_sc_preware_fill_apply_result',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('吴婷@',@user,@timestamp)
);
COMMIT;
    END