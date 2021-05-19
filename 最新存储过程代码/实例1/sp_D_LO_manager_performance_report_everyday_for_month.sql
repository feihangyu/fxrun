CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_manager_performance_report_everyday_for_month`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 2019/09/06
-- Modify date: 
-- Description:	
-- 	全职店主每月效能的分布结果表（每天的0时54分）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
SET @time_1 := CURRENT_TIMESTAMP();
-- 当每月更新1号的数据时将货架管理员信息重新全量更新进去店主月度效能表中
IF DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)) = 1 THEN
DELETE FROM feods.D_LO_manager_performance_report_everyday_for_month WHERE STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m');
INSERT INTO feods.D_LO_manager_performance_report_everyday_for_month (
 manager_id                       
,sf_code                          
,real_name                                
,STAT_DATE                                           
  )
SELECT
  r.manager_id,
  r.sf_code AS '顺丰工号',
  r.real_name AS '店主名称',
  DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m') AS '统计月份'
FROM
  fe.`pub_shelf_manager` r
WHERE r.data_flag = 1
AND r.second_user_type = 1;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_1--@time_2",@time_1,@time_2);
    
-- 每个月更新1号的数据时，将上个月的全职名单留存进全职店主名单历史记录表中
DELETE FROM feods.`D_LO_fulltime_manager_history_record` WHERE STAT_DATE = DATE_FORMAT(DATE_SUB(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL 1 MONTH),'%Y%m');
INSERT INTO feods.`D_LO_fulltime_manager_history_record`(
 STAT_DATE           
,MANAGER_ID        
,sf_code             
,real_name)
SELECT
 t.STAT_DATE             
,t.MANAGER_ID                    
,t.sf_code                              
,t.real_name
FROM
  feods.D_LO_manager_performance_report_everyday_for_month t
WHERE t.STAT_DATE = DATE_FORMAT(DATE_SUB(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL 1 MONTH),'%Y%m');
END IF;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_2--@time_3",@time_2,@time_3);
-- 更新最新的全职店主进去
INSERT INTO feods.D_LO_manager_performance_report_everyday_for_month (
 manager_id                       
,sf_code                          
,real_name                                
,STAT_DATE                                              
  )
SELECT
  r.manager_id,
  r.sf_code AS '顺丰工号',
  r.real_name AS '店主名称',
  DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m') AS '统计月份'
FROM
  fe.`pub_shelf_manager` r
LEFT JOIN feods.D_LO_manager_performance_report_everyday_for_month m
ON r.manager_id = m.manager_id
AND m.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m')
WHERE r.data_flag = 1
AND r.second_user_type = 1
AND m.manager_id IS NULL;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_3--@time_4",@time_3,@time_4);
-- 更新累计GMV、最新货架数
UPDATE feods.D_LO_manager_performance_report_everyday_for_month m
SET m.shelf_gmv = NULL,m.`smart_shelf_gmv`= NULL,m.`auto_shelf_gmv`= NULL,
m.shelf_qty = NULL,m.`smart_shelf_qty`= NULL,m.`auto_shelf_qty`= NULL,m.low_stock_shelf_qty = NULL
WHERE m.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m');
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_4--@time_5",@time_4,@time_5);
UPDATE feods.D_LO_manager_performance_report_everyday_for_month m
LEFT JOIN
  (SELECT
  k.REGION_NAME AS REGION_NAME,
  k.business_name AS business_area,
  b.manager_id,
  SUM(IFNULL(c.shelf_gmv,0)) AS shelf_gmv,
  SUM(IFNULL(c.smart_gmv,0)) AS smart_shelf_gmv,
  SUM(IFNULL(c.auto_gmv,0)) AS auto_shelf_gmv,
  COUNT(
  IF(
      a.shelf_status != 3 AND a.shelf_type NOT IN (6,7),
      a.shelf_id,
      NULL
    )
  ) shelf_qty,
  COUNT(IF(a.`SHELF_TYPE`=6,a.`SHELF_ID`,NULL)) AS smart_shelf_qty,
  COUNT(IF(a.`SHELF_TYPE`=7,a.`SHELF_ID`,NULL)) AS auto_shelf_qty
  FROM
  fe.sf_shelf a
  LEFT JOIN fe.pub_shelf_manager b
    ON a.manager_id = b.manager_id
    AND b.data_flag = 1
  LEFT JOIN feods.`fjr_city_business` k
    ON a.city = k.city
  LEFT JOIN
    (SELECT
      s.shelf_id,
      SUM(s.shelf_gmv) AS shelf_gmv,
      SUM(s.smart_gmv) AS smart_gmv,
      SUM(s.auto_gmv) AS auto_gmv
    FROM
      (SELECT
        f.shelf_id,
        f.`ORDER_ID`,
        SUM(IF(h.`SHELF_TYPE` NOT IN (6,7) AND f.`ORDER_STATUS`= 2,e.QUANTITY,0) * e.SALE_PRICE) AS shelf_gmv,
        SUM(IF(h.`SHELF_TYPE`=6,e.`QUANTITY`,0) * e.SALE_PRICE) AS smart_gmv,
        SUM(IF(h.`SHELF_TYPE`=7,IF(f.`ORDER_STATUS`=6,e.`quantity_shipped`,e.QUANTITY),0) * e.SALE_PRICE) AS auto_gmv
      FROM
        fe.sf_order_item AS e,
        fe.sf_order AS f FORCE INDEX(idx_order_paydate),
        fe.`sf_shelf` h
      WHERE e.order_id = f.ORDER_ID
        AND f.`SHELF_ID`= h.`SHELF_ID`
        AND f.ORDER_STATUS IN (2,6,7)
        AND f.`PAY_DATE` >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY)
        AND f.`PAY_DATE` < CURRENT_DATE
        AND e.data_flag = 1
        AND f.data_flag = 1
        AND h.`DATA_FLAG`= 1
      GROUP BY f.SHELF_ID,
        f.`ORDER_ID`) s
    GROUP BY s.shelf_id) c      -- 销售数据(此处gmv不含补付款与退款,仅参考使用)
     ON a.shelf_id = c.shelf_id
WHERE a.data_flag = 1
  AND b.second_user_type = 1
  AND (
    a.shelf_status IN (2, 5)
    OR (
      a.revoke_time >= DATE_ADD(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
      )
      AND a.revoke_time < DATE_ADD(
        LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),
        INTERVAL 1 DAY
      )
    )
  )
GROUP BY a.manager_id) t
ON t.manager_id = m.manager_id
SET m.REGION_NAME = t.REGION_NAME,m.business_area = t.business_area,
m.shelf_gmv = t.shelf_gmv,m.`smart_shelf_gmv`= t.smart_shelf_gmv,m.`auto_shelf_gmv`= t.auto_shelf_gmv,
m.shelf_qty = t.shelf_qty,m.`smart_shelf_qty`= t.smart_shelf_qty,m.`auto_shelf_qty`= t.auto_shelf_qty
WHERE m.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m')
AND m.data_flag = 1;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_5--@time_6",@time_5,@time_6);
-- 更新每月截止到昨日的低库存货架数 
SET @sql_result := CONCAT(
"update feods.D_LO_manager_performance_report_everyday_for_month m
left join
(SELECT
a.manager_id,
COUNT(DISTINCT CASE WHEN j.PACKAGE_MODEL =3 AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('甲级','乙级') AND i.shelf_stock < 290 THEN a.shelf_id
WHEN j.PACKAGE_MODEL =4 AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('甲级','乙级') AND i.shelf_stock < 360 THEN a.shelf_id
WHEN j.PACKAGE_MODEL =5 AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('甲级','乙级') AND i.shelf_stock < 470 THEN a.shelf_id
WHEN a.shelf_type IN (1,3) AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('甲级','乙级') AND i.shelf_stock < 180 THEN a.shelf_id
WHEN a.shelf_type IN (2,5) AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('甲级','乙级') AND i.shelf_stock < 110 THEN a.shelf_id
WHEN j.PACKAGE_MODEL =3 AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('丙级','丁级') AND i.shelf_stock < 200 THEN a.shelf_id
WHEN j.PACKAGE_MODEL =4 AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('丙级','丁级') AND i.shelf_stock < 220 THEN a.shelf_id
WHEN j.PACKAGE_MODEL =5 AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('丙级','丁级') AND i.shelf_stock < 310 THEN a.shelf_id
WHEN a.shelf_type IN (1,3) AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('丙级','丁级') AND i.shelf_stock < 110 THEN a.shelf_id
WHEN a.shelf_type IN (2,5) AND a.shelf_status=2 AND LEFT(sf.shelf_level,2) IN ('丙级','丁级') AND i.shelf_stock < 90 THEN a.shelf_id
END
 ) AS low_stock_shelf_qty
  FROM
    fe.sf_shelf a
    LEFT JOIN fe.pub_shelf_manager b
      ON a.manager_id = b.manager_id
      AND b.data_flag =1 
    LEFT JOIN feods.`pj_shelf_level_ab` sf   -- 货架等级标签
      ON a.shelf_id = sf.shelf_id
      AND STR_TO_DATE(CONCAT(sf.smonth,'01'),'%Y%m%d')=DATE_SUB(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),INTERVAL 1 MONTH)
    LEFT JOIN
      (SELECT
        s.shelf_id,
        SUM(s.day",DAY(CURRENT_DATE),"_quantity) AS shelf_stock
      FROM
        fe.`sf_shelf_product_stock_detail` s
      WHERE s.stat_date = '", DATE_FORMAT(CURRENT_DATE,'%Y-%m') ,"'
      GROUP BY s.shelf_id) i        -- 库存数据
      ON a.shelf_id = i.shelf_id
    LEFT JOIN
        (SELECT
          a.MAIN_SHELF_ID,
          MAX(a.PACKAGE_MODEL) AS PACKAGE_MODEL
        FROM
          fe.sf_shelf_relation_record a
        WHERE a.DATA_FLAG = 1
          AND a.SHELF_HANDLE_STATUS = 9
        GROUP BY a.MAIN_SHELF_ID) j    -- 关联货架数据
      ON a.shelf_id = j.MAIN_SHELF_ID
  WHERE a.data_flag =1
    AND b.second_user_type = 1 
    AND (
      a.shelf_status IN (2, 5)
      OR (
        a.revoke_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND a.revoke_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      )
    )
    GROUP BY a.`MANAGER_ID`) t
    on m.manager_id = t.manager_id
    set m.low_stock_shelf_qty = t.low_stock_shelf_qty
    where m.stat_date = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m')");
    PREPARE stml FROM @sql_result;
    EXECUTE stml;
    DEALLOCATE PREPARE stml;
	
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_6--@time_7",@time_6,@time_7);
	
-- 更新每日对应的店主效能值
SET @sql_statement := CONCAT(
"update feods.D_LO_manager_performance_report_everyday_for_month t
left join
      (SELECT
        b.operator_id,
        b.operator_name,
        DATE(b.operate_time) operate_time,
        COUNT(DISTINCT if(b.shelf_id in (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type = 7),null,b.shelf_id)) AS operate_qty
      FROM
        fe.sf_shelf_check b
      WHERE b.operate_time >= DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)
        AND b.operate_time < CURRENT_DATE
        AND b.check_type IN (1,2,3)
        AND b.DATA_FLAG = 1
      GROUP BY b.operator_id) e     -- 盘点数据
on t.manager_id = e.operator_id
left join
      (SELECT
        f.fill_user_id,
        f.FILL_USER_NAME,
        COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10
            THEN f.shelf_id
          END ) +
          COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10 and f.shelf_id in (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type = 7)
            THEN f.shelf_id
          END 
        ) AS valid_fill_qty,
        DATE(f.fill_time) AS fill_time
      FROM
        fe.sf_product_fill_order f
      WHERE f.order_status IN (3, 4)
--         AND f.fill_type IN (1, 2, 8, 9, 4, 7)
        AND f.shelf_id IN (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type <> 9)
        AND f.supplier_type <> 1
        AND f.fill_time >= DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)
        AND f.fill_time < CURRENT_DATE
        AND f.data_flag =1 
      GROUP BY f.fill_user_id) f    -- 货架补货数据
on t.manager_id = f.fill_user_id
    LEFT JOIN
      (SELECT
  g.fill_user_id,
  g.FILL_USER_NAME,
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
  ) AS valid_transfer_qty,
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
  ) AS reverse_transfer_qty,
  MAX(g.fill_time) AS max_transfer_time
FROM
  fe.`sf_shelf` s
  JOIN fe.sf_product_fill_order g
    ON g.`SHELF_ID` = s.`SHELF_ID`
    AND s.`DATA_FLAG` = 1
    AND g.order_status IN (3, 4) 
    --  AND g.fill_type IN (6, 11)
    AND g.supplier_type = 1
    AND g.fill_time >= DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)
    AND g.fill_time < CURRENT_DATE
    AND g.data_flag = 1
GROUP BY g.fill_user_id) g    -- 货架调货数据
ON t.manager_id = g.fill_user_id
set t.DAY",DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),"_PM = 0,t.DAY",DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),"_PM = ifnull(e.operate_qty,0)+ifnull(f.valid_fill_qty,0)+ifnull(g.valid_transfer_qty,0)+IFNULL(g.reverse_transfer_qty,0)
where t.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m')");
PREPARE stml FROM @sql_statement;
EXECUTE stml;
DEALLOCATE PREPARE stml;
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_7--@time_8",@time_7,@time_8);
-- 更新当月截止到昨日的累计盘点次数、累计补货次数、累计调货次数、累计逆向调货次数
UPDATE feods.D_LO_manager_performance_report_everyday_for_month t
SET t.acc_operate_qty =0, t.acc_fill_qty =0, t.acc_transfer_qty =0, t.acc_retransfer_qty =0
WHERE t.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m');
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_8--@time_9",@time_8,@time_9);
UPDATE feods.D_LO_manager_performance_report_everyday_for_month t
LEFT JOIN
      (SELECT
        b.operator_id,
        b.operator_name,
        DATE(b.operate_time) operate_time,
        COUNT(DISTINCT CONCAT(DATE_FORMAT(b.operate_time,'%Y%m%d'),'-',b.shelf_id)) AS operate_qty
      FROM
        fe.sf_shelf_check b
      WHERE b.operate_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
        AND b.operate_time < CURRENT_DATE
        AND b.shelf_id NOT IN (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type = 7)
        AND b.check_type IN (1,2,3)
        AND b.DATA_FLAG = 1
      GROUP BY b.operator_id) e   -- 盘点数据
ON t.manager_id = e.operator_id
LEFT JOIN
      (SELECT
        f.fill_user_id,
        f.FILL_USER_NAME,
        COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10
            THEN CONCAT(DATE_FORMAT(f.fill_time,'%Y%m%d'),'-',f.shelf_id)
          END
        ) +
        COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10 AND f.shelf_id IN (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type = 7)
            THEN CONCAT(DATE_FORMAT(f.fill_time,'%Y%m%d'),'-',f.shelf_id)
          END
        ) AS valid_fill_qty,
        DATE(f.fill_time) AS fill_time
      FROM
        fe.sf_product_fill_order f
      WHERE f.order_status IN (3, 4)
--         AND f.fill_type IN (1, 2, 8, 9, 4, 7)
        AND f.shelf_id IN (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type <> 9)
        AND f.supplier_type <> 1
        AND f.fill_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
        AND f.fill_time < CURRENT_DATE
        AND f.data_flag =1 
      GROUP BY f.fill_user_id) f     -- 货架补货数据
ON t.manager_id = f.fill_user_id
    LEFT JOIN
      (SELECT
  g.fill_user_id,
  g.FILL_USER_NAME,
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
  ) AS valid_transfer_qty,
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
  ) AS reverse_transfer_qty,    -- 逆向调货次数
  MAX(g.fill_time) AS max_transfer_time
FROM
  fe.`sf_shelf` s
  JOIN fe.sf_product_fill_order g
    ON g.`SHELF_ID` = s.`SHELF_ID`
    AND s.`DATA_FLAG` = 1
    AND g.order_status IN (3, 4) 
    --  AND g.fill_type IN (6, 11)
    AND g.supplier_type = 1
    AND g.fill_time >= DATE_ADD(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
    )
    AND g.fill_time < CURRENT_DATE
    AND g.data_flag = 1
GROUP BY g.fill_user_id) g    -- 货架调货数据
      ON t.manager_id = g.fill_user_id
SET t.acc_operate_qty = IFNULL(e.operate_qty,0),t.acc_fill_qty = IFNULL(f.valid_fill_qty,0),t.acc_transfer_qty = IFNULL(g.valid_transfer_qty,0),t.acc_retransfer_qty = IFNULL(g.reverse_transfer_qty,0)
WHERE (e.operator_id IS NOT NULL OR f.fill_user_id IS NOT NULL OR g.fill_user_id IS NOT NULL) AND t.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m');
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_9--@time_10",@time_9,@time_10);
-- 计算并更新每月截止到昨日的月平均效能
DROP TEMPORARY TABLE IF EXISTS feods.manager_work_days_temp;
SET @sql_part:='';
SELECT
  @sql_part:= CONCAT(
"CREATE TEMPORARY TABLE feods.manager_work_days_temp AS 
SELECT
 t.MANAGER_ID,"
 ,IFNULL(CONCAT(GROUP_CONCAT(CONCAT('IF(t.DAY',DAY(w.sdate)) SEPARATOR '_PM IS NULL,0,1)+'),'_PM IS NULL,0,1)'),0)," as work_days
FROM
  feods.D_LO_manager_performance_report_everyday_for_month t
WHERE t.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m')"
  )
FROM
  feods.`fjr_work_days` w
WHERE w.sdate >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
AND w.sdate < CURRENT_DATE
AND w.if_work_day = 1;
PREPARE sql_work FROM @sql_part;
EXECUTE sql_work;
DEALLOCATE PREPARE sql_work;
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_10--@time_11",@time_10,@time_11);
UPDATE feods.D_LO_manager_performance_report_everyday_for_month m
LEFT JOIN feods.manager_work_days_temp w
ON m.manager_id = w.manager_id
SET m.month_mean_PM = 0 , m.month_mean_PM = IF(w.work_days = 0,0,ROUND((m.acc_operate_qty + m.acc_fill_qty + m.acc_transfer_qty + m.acc_retransfer_qty)/w.work_days,2))
WHERE m.STAT_DATE = DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m');
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_D_LO_manager_performance_report_everyday_for_month","@time_11--@time_12",@time_11,@time_12);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_D_LO_manager_performance_report_everyday_for_month',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
COMMIT;
END