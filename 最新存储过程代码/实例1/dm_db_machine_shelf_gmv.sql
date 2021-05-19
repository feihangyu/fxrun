CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_db_machine_shelf_gmv`()
BEGIN
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
/*
## 初始化
DROP TABLE IF EXISTS test.d_db_machine_temp;
CREATE TEMPORARY TABLE test.d_db_machine_temp
(KEY(shelf_id,sdate))
AS 
SELECT sdate, shelf_id,gmv_shipped as gmv
FROM feods.fjr_kpi3_shelf7_shelf_stat_day 
WHERE gmv_shipped >0 
AND sdate < '2020-03-02'
UNION ALL 
SELECT  DATE(pay_date), a1.shelf_id, SUM(amount*sale_price) gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht a1
JOIN `fe_dwd`.`dwd_shelf_base_day_all` a2 ON a1.shelf_id = a2.shelf_id 
WHERE 
pay_date < '2020-03-02'
AND a2.shelf_type = 7 ##  只要自贩机
AND deliver_status = '已出货'
GROUP BY DATE(pay_date), a1.shelf_id
;
-- TRUNCATE TABLE `fe_dm`.`dm_db_machine_shelf_gmv` ;
INSERT INTO `fe_dm`.`dm_db_machine_shelf_gmv` (
shelf_id,
pay_date_first,
sale_day,
gmv_all
)
SELECT shelf_id,
       MIN(sdate) first_sale_date,
       TIMESTAMPDIFF(DAY,MIN(sdate), CURDATE()) sale_days,
       SUM(gmv) all_gmv
FROM test.d_db_machine_temp
GROUP BY shelf_id
;
UPDATE `fe_dm`.`dm_db_machine_shelf_gmv` a
JOIN 
(
SELECT a1.shelf_id,
       a2.SHELF_CODE,
       a2.COMPANY_NAME,
       a2.business_name,
       a2.ACTIVATE_TIME,
       a2.SHELF_STATUS,
       a2.shelf_type,
       a2.`type_name`
FROM `fe_dm`.`dm_db_machine_shelf_gmv` a1
JOIN `fe_dwd`.`dwd_shelf_base_day_all` a2 ON a1.shelf_id = a2.shelf_id 
)b ON a.`shelf_id` = b.shelf_id
SET a.SHELF_CODE = b.SHELF_CODE, 
    a.COMPANY_NAME = b.COMPANY_NAME, 
    a.business_name = b.business_name, 
    a.ACTIVATE_TIME = b.ACTIVATE_TIME, 
    a.shelf_status = b.shelf_status, 
    a.shelf_type = b.shelf_type,
    a.`machine_type_name` = b.type_name
;
UPDATE `fe_dm`.`dm_db_machine_shelf_gmv` a
JOIN 
(
SELECT  bb.shelf_id,
	SUM(CASE WHEN aa.sdate <= SUBDATE(bb.`pay_date_first`,INTERVAL -14 DAY) THEN aa.gmv END) 15_gmv,
	SUM(CASE WHEN aa.sdate <= SUBDATE(bb.`pay_date_first`,INTERVAL -29 DAY) THEN aa.gmv END) 30_gmv
FROM test.d_db_machine_temp aa
JOIN `fe_dm`.`dm_db_machine_shelf_gmv` bb ON aa.shelf_id = bb.shelf_id 
GROUP BY bb.shelf_id 
)b ON a.`shelf_id` = b.shelf_id
SET a.`gmv_15` = b.15_gmv , a.`gmv_30` = b.30_gmv
;
*/
## 2020-03-02 后增量更新
## 将中间表 存前一天的销量数据 
SET @sdate := CURDATE();
SET @start_date := SUBDATE(@sdate ,INTERVAL 1 DAY);
delete from `fe_dm`.`dm_db_machine_shelf_gmv` where add_time >= curdate();
DROP TABLE IF EXISTS feods.d_machine_one;
CREATE TEMPORARY TABLE feods.d_machine_one(KEY(shelf_id,sdate))
AS 
SELECT t.sdate, t.shelf_id, SUM(t.gmv) gmv
FROM
 (
SELECT sdate, shelf_id,gmv AS gmv 
FROM `fe_dwd`.`dwd_shelf_day_his`
WHERE gmv >0 
AND shelf_type = 7 ##  只要自贩机 
AND sdate >= @start_date
AND sdate < @sdate
UNION ALL 
SELECT  DATE(pay_date), a1.shelf_id, SUM(amount*sale_price) gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht a1
JOIN `fe_dwd`.`dwd_shelf_base_day_all` a2 ON a1.shelf_id = a2.shelf_id 
WHERE pay_date >= @start_date
AND pay_date < @sdate
AND a2.shelf_type = 7 ##  只要自贩机
AND deliver_status = '已出货'
GROUP BY DATE(pay_date), a1.shelf_id
)t 
GROUP BY t.sdate, shelf_id
;
## 更新运营天数 < 30天的  总gmv 15天 30天gmv
UPDATE `fe_dm`.`dm_db_machine_shelf_gmv` aa
JOIN  feods.d_machine_one bb ON aa.shelf_id = bb.shelf_id 
SET aa.gmv_all = bb.gmv + aa.gmv_all,
    aa.gmv_15 = IF(aa.`sale_day` < 15,aa.gmv_15 + bb.gmv,aa.gmv_15),
    aa.gmv_30 =  aa.gmv_30 + bb.gmv,
    aa.`sale_day` = aa.`sale_day` + 1
WHERE aa.sale_day < 30 
 ;
 
 ## 更新运营天数 >= 30天的  总gmv 15天 30天gmv
UPDATE `fe_dm`.`dm_db_machine_shelf_gmv` aa
JOIN  feods.d_machine_one bb ON aa.shelf_id = bb.shelf_id 
SET aa.gmv_all = bb.gmv + aa.gmv_all,
    aa.`sale_day` = aa.`sale_day` + 1
WHERE aa.sale_day >= 30 
;
## 新销售货架 
INSERT INTO `fe_dm`.`dm_db_machine_shelf_gmv` (
shelf_id,
pay_date_first,
sale_day,
gmv_15,
gmv_30,
gmv_all
)
SELECT t1.shelf_id,
       t1.sdate,
       1 AS sale_day,
       t1.gmv,
       t1.gmv,
       t1.gmv
FROM feods.d_machine_one t1
LEFT JOIN `fe_dm`.`dm_db_machine_shelf_gmv` t2 ON t1.shelf_id = t2.shelf_id 
WHERE t2.shelf_id IS NULL 
;
## 更新自贩机基础数据
UPDATE `fe_dm`.`dm_db_machine_shelf_gmv` a
JOIN 
(
SELECT a1.shelf_id,
       a2.SHELF_CODE,
       a2.COMPANY_NAME,
       a2.business_name,
       a2.ACTIVATE_TIME,
       a2.SHELF_STATUS,
       a2.shelf_type,
       a2.`type_name`
FROM `fe_dm`.`dm_db_machine_shelf_gmv` a1
JOIN `fe_dwd`.`dwd_shelf_base_day_all` a2 ON a1.shelf_id = a2.shelf_id 
)b ON a.`shelf_id` = b.shelf_id
SET a.SHELF_CODE = b.SHELF_CODE, 
    a.COMPANY_NAME = b.COMPANY_NAME, 
    a.business_name = b.business_name, 
    a.ACTIVATE_TIME = b.ACTIVATE_TIME, 
    a.shelf_status = b.shelf_status, 
    a.shelf_type = b.shelf_type,
    a.machine_type_name = b.type_name
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_db_machine_shelf_gmv',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('linihe@', @user, @timestamp)
  );
  COMMIT;
END