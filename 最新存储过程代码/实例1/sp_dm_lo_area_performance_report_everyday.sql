CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_dm_lo_area_performance_report_everyday`()
BEGIN
  -- =============================================
-- Author:	运作门店-效能
-- Create date: 2020/07/07
-- Modify date: 
-- Description:	
--    增量插入每天门店效能结果表（每天的）
-- 
-- =============================================
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();   
SET @date_top := DATE_SUB(CURDATE(),INTERVAL 1 DAY);
SET @date_end := CURDATE();
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_manager_data;
CREATE TEMPORARY TABLE fe_dm.area_manager_data
(
    KEY idx_area_id (area_id)
) AS
SELECT b.`sdate`, c.business_name, a.`area_id`, a.`area_name`, b.`if_work_day`
FROM fe.`sf_shelf_area_info` a,
     fe_dwd.`dwd_pub_work_day` b,
     fe_dwd.`dwd_city_business` c
WHERE a.`data_flag` = 1
  AND b.`sdate` >= @date_top
  AND b.`sdate` < @date_end
  AND a.`city` = c.CITY
;
DELETE FROM fe_dm.`dm_lo_area_performance_report_everyday` WHERE `sdate` >= @date_top;
INSERT INTO fe_dm.`dm_lo_area_performance_report_everyday` (
  `sdate`,
  `business_area`,
  `area_id`,
  `area_name`,
  `if_work_day`,
  `operate_qty`,
  `valid_fill_qty`,
  `valid_transfer_qty`,
  `reverse_transfer_qty`,
  `activate_machine_qty`,
  `machine_fault_qty`,
  `all_performance_qty`
    )
SELECT t0.*,
       IFNULL(t1.operate_qty, 0)          operate_qty,
       IFNULL(t2.valid_fill_qty, 0)       valid_fill_qty,
       IFNULL(t3.valid_transfer_qty, 0)   valid_transfer_qty,
       IFNULL(t3.reverse_transfer_qty, 0) reverse_transfer_qty,
       IFNULL(t4.activate_machine_qty, 0) activate_machine_qty,
       IFNULL(t5.machine_fault_qty, 0)    machine_fault_qty,
       IFNULL(t1.operate_qty, 0) +
       IFNULL(t2.valid_fill_qty, 0) +
       IFNULL(t3.valid_transfer_qty, 0) +
       IFNULL(t3.reverse_transfer_qty, 0) +
       IFNULL(t4.activate_machine_qty, 0) +
       IFNULL(t5.machine_fault_qty, 0)    all_performance_qty
FROM fe_dm.area_manager_data t0
         LEFT JOIN
     (SELECT s.`area_id`,
             DATE(b.operate_time) operate_time,
             COUNT(
                     DISTINCT CONCAT(
                     DATE_FORMAT(b.operate_time, '%Y%m%d'),
                     '-',
                     b.shelf_id
                 )
                 ) AS             operate_qty
      FROM fe.sf_shelf_check b
               JOIN fe.`sf_shelf` s
                    ON b.`SHELF_ID` = s.`SHELF_ID` AND s.`DATA_FLAG` = 1
                        AND s.shelf_type <> 7
      WHERE b.operate_time >= @date_top
        AND b.operate_time < @date_end
        AND b.check_type IN (1, 2, 3)
        AND b.DATA_FLAG = 1
      GROUP BY 1, 2) t1 -- 盘点数据
     ON t0.area_id = t1.area_id AND t0.sdate = t1.operate_time
         LEFT JOIN
     (SELECT s.`area_id`,
             DATE(f.fill_time) fill_time,
             COUNT(
                     DISTINCT
                     CASE
                         WHEN ABS(f.PRODUCT_NUM) > 10
                             THEN CONCAT(
                                 DATE_FORMAT(f.fill_time, '%Y%m%d'),
                                 '-',
                                 f.shelf_id
                             )
                         END
                 ) + COUNT(
                     DISTINCT
                     CASE
                         WHEN ABS(f.PRODUCT_NUM) > 10
                             AND f.shelf_id IN
                                 (SELECT DISTINCT h.shelf_id
                                  FROM fe.`sf_shelf` h
                                  WHERE h.data_flag = 1
                                    AND h.shelf_type = 7)
                             THEN CONCAT(
                                 DATE_FORMAT(f.fill_time, '%Y%m%d'),
                                 '-',
                                 f.shelf_id
                             )
                         END
                 ) AS          valid_fill_qty
      FROM fe.sf_product_fill_order f
               JOIN fe.`sf_shelf` s
                    ON f.`SHELF_ID` = s.`SHELF_ID` AND s.`DATA_FLAG` = 1
                        AND s.shelf_type <> 9
      WHERE f.order_status IN (3, 4)
        AND f.fill_type IN (1, 2, 8, 9, 4, 7)
        AND f.supplier_type <> 1
        AND f.fill_time >= @date_top
        AND f.fill_time < @date_end
        AND f.data_flag = 1
      GROUP BY 1, 2) t2 -- 货架补货数据
     ON t0.area_id = t2.area_id AND t0.sdate = t2.fill_time
         LEFT JOIN
     (SELECT s.`area_id`,
             DATE(g.fill_time) fill_time,
             COUNT(
                     DISTINCT
                     CASE
                         WHEN g.PRODUCT_NUM > 10
                             AND s.shelf_type <> 9
                             THEN CONCAT(
                                 DATE_FORMAT(g.FILL_TIME, '%Y%m%d'),
                                 '-',
                                 g.shelf_id
                             )
                         END
                 ) AS          valid_transfer_qty,
             COUNT(
                     DISTINCT
                     CASE
                         WHEN g.`ADD_USER_ID` = 0
                             AND g.`FILL_TYPE` = 11
                             THEN CONCAT(
                                 DATE_FORMAT(g.FILL_TIME, '%Y%m%d'),
                                 '-',
                                 g.shelf_id
                             )
                         END
                 ) AS          reverse_transfer_qty -- 逆向调货次数
      FROM fe.`sf_shelf` s
               JOIN fe.sf_product_fill_order g
                    ON g.`SHELF_ID` = s.`SHELF_ID`
                        AND s.`DATA_FLAG` = 1
                        AND g.order_status IN (3, 4) --  AND g.fill_type IN (6, 11)
                        AND g.supplier_type = 1
                        AND g.fill_time >= @date_top
                        AND g.fill_time < @date_end
                        AND g.data_flag = 1
      GROUP BY 1, 2) t3 -- 货架调货数据
     ON t0.area_id = t3.area_id AND t0.sdate = t3.fill_time
         LEFT JOIN
     (SELECT a.`area_id`,
             DATE(a.`ACTIVATE_TIME`)        activate_time,
             COUNT(DISTINCT a.shelf_id) * 2 activate_machine_qty
      FROM fe.`sf_shelf` a
      WHERE a.`SHELF_TYPE` = 7
        AND a.`DATA_FLAG` = 1
        AND a.`ACTIVATE_TIME` >= @date_top
        AND a.`ACTIVATE_TIME` < @date_end
      GROUP BY 1,
               2) t4 -- 自贩机激活数据
     ON t0.area_id = t4.area_id AND t0.sdate = t4.activate_time
         LEFT JOIN
     (SELECT b.`area_id`,
             DATE(a.solve_time)         solve_time,
             COUNT(DISTINCT a.shelf_id) machine_fault_qty
      FROM fe.sf_shelf_machine_fault a
               LEFT JOIN fe.sf_shelf b
                         ON a.shelf_id = b.shelf_id
                             AND b.data_flag = 1
      WHERE a.data_flag = 1
        AND a.solve_time IS NOT NULL
        AND a.report_time >= @date_top
        AND a.report_time < @date_end
      GROUP BY 1, 2) t5 -- 自动贩卖机故障
     ON t0.area_id = t5.area_id AND t0.sdate = t5.solve_time
ORDER BY t0.sdate,t0.area_id
;
 
    
  -- 执行日志
  CALL sh_process.`sp_sf_dw_task_log` (
    'sp_dm_lo_area_performance_report_everyday',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangyunfeng@', @user, @timestamp)
  );
  COMMIT;
END