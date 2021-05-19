CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_prewarehouse_coverage_rate`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @all_shelf = (SELECT COUNT(*) FROM fe_dwd.`dwd_shelf_base_day_all` WHERE shelf_status =2);
SET @all_qzc_shelf =
   (SELECT COUNT(*) FROM fe_dwd.`dwd_shelf_base_day_all` a 
     JOIN fe.sf_prewarehouse_shelf_detail b 
     ON a.shelf_id = b.shelf_id
     AND a.shelf_status = 2
     AND b.data_flag =1
     AND a.data_flag =1
     ) ;
DELETE FROM feods.pj_prewarehouse_coverage_rate WHERE CHECK_DATE = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
INSERT INTO feods.pj_prewarehouse_coverage_rate (
    CHECK_DATE,
    REGION_AREA,
    WAREHOUSE_NAME,
    WAREHOUSE_NUMBER,
    BUSINESS_AREA,
    QZC_NUMBER,
    QZC_SHELF_NUMBER,
    SHELF_NUMBER,
    COVERAGE_RATE,
    shelf_cnt_a_all,
    shelf_cnt_b_all,
    shelf_cnt_a,
    shelf_cnt_b,
    QZC_NUMBER_c,
    QZC_SHELF_NUMBER_c,
    SHELF_NUMBER_c,
    COVERAGE_RATE_c,
    shelf_cnt_ac,
    shelf_cnt_bc,
    all_shelf,
    all_qzc_shelf
  )
SELECT
    SUBDATE(CURDATE(),1) AS 'CHECK_DATE',
    t1.region_area,
    t1.warehouse_name,
    t1.warehouse_number,
    t1.business_area,
    COUNT(DISTINCT t3.warehouse_id) AS 'QZC_NUMBER',
    COUNT(t3.warehouse_id) AS 'QZC_SHELF_NUMBER',
    COUNT(t2.shelf_id) AS 'SHELF_NUMBER',
    COUNT(t3.warehouse_id) / COUNT(t2.shelf_id) AS 'COVERAGE_RATE',
 
    COUNT(IF(g.grade IN ("甲","乙"),g.shelf_id,NULL)) AS 'shelf_cnt_a_all',
    COUNT(IF(g.grade IN ("丙","丁"),g.shelf_id,NULL)) AS 'shelf_cnt_b_all',
    COUNT(IF(g.grade IN ("甲","乙") AND t3.warehouse_id IS NOT NULL,g.shelf_id,NULL)) AS 'shelf_cnt_a',
    COUNT(IF(g.grade IN ("丙","丁") AND t3.warehouse_id IS NOT NULL,g.shelf_id,NULL)) AS 'shelf_cnt_b', 
     
    COUNT(DISTINCT(IF(t2.whether_close = 2,t3.warehouse_id,NULL))) AS 'QZC_NUMBER_c',
    COUNT(IF(t2.whether_close = 2,t3.warehouse_id,NULL)) AS 'QZC_SHELF_NUMBER_c',
    COUNT(IF(t2.whether_close = 2,t2.shelf_id,NULL)) AS 'SHELF_NUMBER_c',
    COUNT(IF(t2.whether_close = 2,t3.warehouse_id,NULL)) 
    / COUNT(IF(t2.whether_close = 2,t2.shelf_id,NULL)) AS 'COVERAGE_RATE_c',
    COUNT(IF(g.grade IN ("甲","乙") AND t3.warehouse_id IS NOT NULL AND t2.whether_close = 2,g.shelf_id,NULL)) AS 'shelf_cnt_ac',
    COUNT(IF(g.grade IN ("丙","丁") AND t3.warehouse_id IS NOT NULL AND t2.whether_close = 2,g.shelf_id,NULL)) AS 'shelf_cnt_bc',       
    @all_shelf,
    @all_qzc_shelf
    
FROM fe_dwd.`dwd_pub_warehouse_business_area` t1
LEFT JOIN fe_dwd.`dwd_shelf_base_day_all` t2      
      ON t1.business_area = t2.business_name
      AND t2.shelf_status = 2
      AND t2.data_flag =1
LEFT JOIN fe.sf_prewarehouse_shelf_detail t3
      ON t2.shelf_id = t3.shelf_id
      AND t3.data_flag = 1
LEFT JOIN feods.`d_op_shelf_grade` g
      ON t2.shelf_id = g.shelf_id
      AND g.shelf_status = '已激活'
      AND g.month_id = DATE_FORMAT(SUBDATE(CURDATE(),1),"%Y-%m")
WHERE t1.to_preware = 1 
GROUP BY t1.region_area,
    t1.warehouse_name,
    t1.business_area,
    t1.warehouse_number
HAVING COUNT(t2.shelf_id) > 0
;
#货架库存量   
DELETE FROM feods.d_sc_shelf_stock_daily WHERE sdate = SUBDATE(CURDATE(),1);
INSERT INTO feods.d_sc_shelf_stock_daily
  (sdate,
  shelf_id,
  stock
  )
SELECT sdate,shelf_id,s.stock_quantity
FROM  fe_dwd.`dwd_shelf_day_his` s
WHERE s.sdate = SUBDATE(CURDATE(),1)
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_prewarehouse_coverage_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
 
  COMMIT;
   
END