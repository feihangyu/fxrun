CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_product_sto_sal_30_days`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @start_date = SUBDATE(CURDATE(),INTERVAL 1 DAY);
SET @time_1 := CURRENT_TIMESTAMP();
-- DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_sto_sal_1;
-- CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_sto_sal_1 AS
-- SELECT sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity
-- FROM fe_dwd.`dwd_shelf_product_day_all_recent_32` a
-- WHERE (a.`gmv` >0 OR a.`stock_quantity` != 0)  -- 20200414 添加负库存的数据
-- AND a.`sdate` >= @start_date
-- ;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_sto_sal_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_sto_sal_1 AS
SELECT sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity
FROM fe_dwd.`dwd_shelf_product_day_all_recent_32` a
WHERE (a.`gmv` >0 OR a.`stock_quantity` != 0)  -- 20200414 添加负库存的数据
AND a.`sdate` >= @start_date
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_30_days","@time_1--@time_2",@time_1,@time_2);
-- 动态保留30天的数据
DELETE FROM fe_dwd.dwd_shelf_product_sto_sal_30_days WHERE sdate < SUBDATE(CURDATE(),31);
DELETE FROM  fe_dwd.dwd_shelf_product_sto_sal_30_days WHERE sdate >= SUBDATE(CURDATE(),1) ;
INSERT INTO fe_dwd.`dwd_shelf_product_sto_sal_30_days`
(
sdate,
business_name,
product_id,
shelf_id,
sale_price,
purchase_price,
sal_qty,
gmv,
stock_quantity
)
SELECT
a.sdate,
b.business_name,
a.product_id,
a.shelf_id,
a.sale_price,
a.purchase_price,
a.sal_qty,
a.gmv,
a.stock_quantity
FROM fe_dwd.dwd_shelf_product_sto_sal_1 a
LEFT JOIN 
fe_dwd.dwd_shelf_base_day_all b
ON a.shelf_id = b.shelf_id;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_30_days","@time_2--@time_3",@time_2,@time_3);
-- 每月月初更新月初库存的数据
IF DAY(CURRENT_DATE) = 1 THEN
 SET @month_start := DATE_ADD(CURDATE(),INTERVAL -DAY(CURDATE())+1 DAY);
SET @last_month_start := DATE_ADD(CURDATE()-DAY(CURDATE())+1,INTERVAL -1 MONTH);
SET @last_month_end := LAST_DAY(DATE_SUB(NOW(),INTERVAL 1 MONTH));
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_sto_sal_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_sto_sal_2 AS
SELECT @month_start AS sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity
FROM fe_dwd.dwd_shelf_product_day_all_recent_32 a
WHERE (a.`gmv` >0 OR a.`stock_quantity` != 0)  -- 20200414 添加负库存的数据
AND a.`sdate`=@last_month_end
;
-- DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_shelf_product_sto_sal_2;
-- CREATE TEMPORARY TABLE fe_dwd.dwd_shelf_product_sto_sal_2 AS
-- SELECT @month_start AS sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity
-- FROM fe_dwd.`dwd_shelf_product_day_east_his` a
-- WHERE (a.`gmv` >0 OR a.`stock_quantity` != 0)  -- 20200414 添加负库存的数据
-- AND a.`sdate`=@last_month_end
-- UNION ALL
-- SELECT @month_start AS sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity 
-- FROM fe_dwd.`dwd_shelf_product_day_north_his` a
-- WHERE (a.`gmv` >0 OR a.`stock_quantity` !=0)
-- AND a.`sdate`=@last_month_end
-- UNION ALL
-- SELECT @month_start AS sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity
--  FROM fe_dwd.`dwd_shelf_product_day_south_his` a
-- WHERE (a.`gmv` >0 OR a.`stock_quantity` != 0)
-- AND a.`sdate`=@last_month_end
-- UNION ALL
--  SELECT @month_start AS sdate,product_id,shelf_id,sale_price,purchase_price,sal_qty,gmv,stock_quantity 
-- FROM fe_dwd.`dwd_shelf_product_day_west_his` a
-- WHERE (a.`gmv` >0 OR a.`stock_quantity` != 0)
-- AND a.`sdate`=@last_month_end;
CREATE INDEX idx_dwd_shelf_product_sto_sal_2
ON fe_dwd.dwd_shelf_product_sto_sal_2  (shelf_id);
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_30_days","@time_3--@time_4",@time_3,@time_4);
INSERT INTO fe_dwd.`dwd_shelf_product_sto_sal_month_start_end`
(
sdate,
business_name,
product_id,
shelf_id,
sale_price,
purchase_price,
sal_qty,
gmv,
stock_quantity
)
SELECT
a.sdate,
b.business_name,
a.product_id,
a.shelf_id,
a.sale_price,
a.purchase_price,
a.sal_qty,
a.gmv,
a.stock_quantity
FROM fe_dwd.dwd_shelf_product_sto_sal_2 a
LEFT JOIN 
fe_dwd.dwd_shelf_base_day_all b
ON a.shelf_id = b.shelf_id;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_30_days","@time_4--@time_5",@time_4,@time_5);
END IF;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_product_sto_sal_30_days',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_sto_sal_30_days','dwd_shelf_product_sto_sal_30_days','李世龙');
-- 更新任务的执行状态
UPDATE fe_dwd.dwd_project_excute_status SET execute_status=1,load_time=CURRENT_TIMESTAMP WHERE process_name='dwd_shelf_product_sto_sal_30_days' AND sdate=CURRENT_DATE;
 
END$$
