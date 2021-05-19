CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_prewarehouse_coverage_rate`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @all_shelf = (SELECT COUNT(*) FROM fe_dwd.dwd_shelf_base_day_all WHERE shelf_status =2);
SET @all_qzc_shelf =
   (SELECT COUNT(*) FROM fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all  t WHERE t.shelf_status =2 ) ;
DELETE FROM fe_dm.dm_prewarehouse_coverage_rate WHERE CHECK_DATE = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
INSERT INTO fe_dm.dm_prewarehouse_coverage_rate (
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
    COUNT(DISTINCT t3.prewarehouse_id) AS 'QZC_NUMBER',
    COUNT(t3.prewarehouse_id) AS 'QZC_SHELF_NUMBER',
    COUNT(t2.shelf_id) AS 'SHELF_NUMBER',
    COUNT(t3.prewarehouse_id) / COUNT(t2.shelf_id) AS 'COVERAGE_RATE',
 
    COUNT(IF(t2.grade_cur_month IN ("甲","乙"),t2.shelf_id,NULL)) AS 'shelf_cnt_a_all',
    COUNT(IF(t2.grade_cur_month IN ("丙","丁"),t2.shelf_id,NULL)) AS 'shelf_cnt_b_all',
    COUNT(IF(t2.grade_cur_month IN ("甲","乙") AND t3.prewarehouse_id IS NOT NULL,t2.shelf_id,NULL)) AS 'shelf_cnt_a',
    COUNT(IF(t2.grade_cur_month IN ("丙","丁") AND t3.prewarehouse_id IS NOT NULL,t2.shelf_id,NULL)) AS 'shelf_cnt_b', 
     
    COUNT(DISTINCT(IF(t2.whether_close = 2,t3.prewarehouse_id,NULL))) AS 'QZC_NUMBER_c',
    COUNT(IF(t2.whether_close = 2,t3.prewarehouse_id,NULL)) AS 'QZC_SHELF_NUMBER_c',
    COUNT(IF(t2.whether_close = 2,t2.shelf_id,NULL)) AS 'SHELF_NUMBER_c',
    COUNT(IF(t2.whether_close = 2,t3.prewarehouse_id,NULL)) 
    / COUNT(IF(t2.whether_close = 2,t2.shelf_id,NULL)) AS 'COVERAGE_RATE_c',
    COUNT(IF(t2.grade_cur_month IN ("甲","乙") AND t3.prewarehouse_id IS NOT NULL AND t2.whether_close = 2,t2.shelf_id,NULL)) AS 'shelf_cnt_ac',
    COUNT(IF(t2.grade_cur_month IN ("丙","丁") AND t3.prewarehouse_id IS NOT NULL AND t2.whether_close = 2,t2.shelf_id,NULL)) AS 'shelf_cnt_bc',       
    @all_shelf,
    @all_qzc_shelf
    
FROM fe_dwd.dwd_pub_warehouse_business_area t1
LEFT JOIN fe_dwd.dwd_shelf_base_day_all t2      
      ON t1.business_area = t2.business_name
      AND t2.shelf_status = 2
      AND t2.data_flag =1
LEFT JOIN fe_dwd.dwd_relation_dc_prewarehouse_shelf_day_all t3
      ON t2.shelf_id = t3.shelf_id
WHERE t1.to_preware = 1 
GROUP BY t1.region_area,
    t1.warehouse_name,
    t1.business_area,
    t1.warehouse_number
HAVING COUNT(t2.shelf_id) > 0
;
-- #货架库存量   
-- DELETE FROM feods.d_sc_shelf_stock_daily WHERE sdate = SUBDATE(CURDATE(),1);
-- INSERT INTO feods.d_sc_shelf_stock_daily
--   (sdate,
--   shelf_id,
--   stock
--   )
-- SELECT sdate,shelf_id,s.stock_quantity
-- FROM  fe_dwd.dwd_shelf_day_his s
-- WHERE s.sdate = SUBDATE(CURDATE(),1)
-- ;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_prewarehouse_coverage_rate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_prewarehouse_coverage_rate','dm_prewarehouse_coverage_rate','吴婷');
COMMIT;
    END