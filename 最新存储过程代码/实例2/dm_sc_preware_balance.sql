CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_preware_balance`(in_sdate DATETIME)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate = in_sdate;
SET @sdate1 = DATE_SUB(in_sdate,INTERVAL 1 DAY);
DELETE FROM fe_dm.dm_sc_preware_balance WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_balance 
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
warehouse_id,
shelf_name,
shelf_code,
product_id,
product_name,
product_code2,
fname,
total_stock,
available_stock,
purchase_price,
total_stock_yesterday,
actual_send_num,
actual_fill_num,
stock_in_theory,
actual_send_forteen,
avg_send_num,
send_amount_forteen,
avg_send_amount,
quantity,
GMV,
diff_stock
)
SELECT 
DATE(t.check_date) AS '库存日期',
t.region_area AS '大区',
t.business_area AS '区域',
t.warehouse_number AS '仓库编码',
t.warehouse_name AS '仓库名称',
t.warehouse_id AS '前置仓id',
t.shelf_name AS '前置仓名称',
t.shelf_code AS '前置仓编码',
t.product_id AS '商品ID',
t.product_name AS '商品名称',
t.product_code2 AS '商品FE码',
t.fname AS '运营商品类型',
t.total_stock AS '实际总库存',
t.AVAILABLE_STOCK AS '当日可用库存',
t.purchase_price AS '采购价',
p.total_stock AS '前一天总库存',
IFNULL(t1.actual_send_num,0) AS '出库量',
IFNULL(t2.actual_fill_num,0) AS '实际上架量',
(p.total_stock + IFNULL(t2.actual_fill_num,0) - IFNULL(t1.actual_send_num,0)) AS '理论结余量',
IFNULL(t5.actual_send_num,0) AS '前14天出库量',
IFNULL(t5.send_noholiday,0)/14 AS '前14天日均出库量',
-- IFNULL(t5.actual_send_num * t5.F_BGJ_POPRICE ,0) AS '前14天出库金额',
-- IFNULL(t5.send_noholiday * t5.F_BGJ_POPRICE ,0)/14 AS '前14天日均出库金额',
IFNULL(t5.actual_send_num * t.purchase_price ,0) AS '前14天出库金额',
IFNULL(t5.send_noholiday * t.purchase_price ,0)/14 AS '前14天日均出库金额',
IFNULL(t6.quantity,0) AS '当日销量',
IFNULL(t6.GMV,0) AS '当日GMV',
(p.total_stock + IFNULL(t2.actual_fill_num,0) - IFNULL(t1.actual_send_num,0)) - t.total_stock AS '差异量'
FROM 
(SELECT *
FROM fe_dm.dm_prewarehouse_stock_detail
WHERE check_date = @sdate) t 
LEFT JOIN 
(SELECT *
FROM fe_dm.dm_prewarehouse_stock_detail
WHERE check_date = @sdate1) p
ON t.warehouse_id = p.warehouse_id
AND t.product_id = p.product_id
AND t.check_date = DATE_ADD(p.check_date,INTERVAL 1 DAY)
LEFT JOIN 
(SELECT t.out_date,t.warehouse_id,t.shelf_name,t.shelf_code,t.product_id,SUM(IFNULL(actual_send_num,0)) AS actual_send_num
FROM fe_dwd.dwd_preware_outbound_daily t
WHERE out_date = @sdate
GROUP BY t.out_date,t.warehouse_id,t.shelf_name,t.shelf_code,t.product_id)t1 # 出库
ON t.warehouse_id = t1.warehouse_id
AND t.product_id = t1.product_id
AND t.check_date = t1.out_date
LEFT JOIN 
(SELECT t.fill_date,t.warehouse_id,t.shelf_name,t.shelf_code,t.product_id,SUM(IFNULL(actual_fill_num,0)) AS actual_fill_num
 FROM fe_dwd.dwd_preware_fill_daily t 
 WHERE fill_date = @sdate
 GROUP BY t.fill_date,t.warehouse_id,t.shelf_name,t.shelf_code,t.product_id
) t2 # 入库
ON t.check_date = t2.fill_date
AND t.warehouse_id = t2.warehouse_id
AND t.product_id = t2.product_id
LEFT JOIN fe_dwd.dwd_preware_outbound_forteen_day t5# 14天出库量
ON t.warehouse_id = t5.warehouse_id
AND t.product_id = t5.product_id
AND t.check_date = t5.sdate
AND t5.sdate = @sdate
LEFT JOIN fe_dm.dm_sc_preware_sales_daily t6
ON t.warehouse_id = t6.warehouse_id
AND t.product_id = t6.product_id
AND t.check_date = t6.sdate
AND t6.sdate = @sdate
WHERE t.total_stock >=0
OR p.total_stock >=0
OR t2.actual_fill_num > 0
OR t1.actual_send_num > 0;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_preware_balance',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_preware_balance','dm_sc_preware_balance','吴婷');
COMMIT;
    END