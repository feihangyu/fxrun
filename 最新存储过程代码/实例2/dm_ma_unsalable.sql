CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_unsalable`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := DATE_ADD(CURDATE(), INTERVAL -1 DAY),
  @user := CURRENT_USER,
  @run_date := CURRENT_DATE(),
  @timestamp := CURRENT_TIMESTAMP;
  
 DELETE FROM fe_dm.dm_ma_unsalable WHERE sdate = @sdate;
  ## 今日统计昨日严重滞销品
INSERT INTO fe_dm.dm_ma_unsalable (sdate, business_name, GMV, QTY, DISCOUNT_AMOUNT,SALE_SHELF_NUM,on_shelf_stock_qty,on_shelf_stock_shelf_num, pay_aoumnt)
SELECT DATE(a1.sdate) 日期,
       b1.business_name,
       SUM(a1.gmv) GMV,
       SUM(a1.sal_qty) QTY ,
       SUM(a1.DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,
       COUNT(DISTINCT CASE WHEN a1.sal_qty > 0 THEN a1.shelf_id ELSE NULL END)  SALE_SHELF_NUM,
       SUM(a1.stock_quantity) on_shelf_stock_qty,
       COUNT(DISTINCT CASE WHEN a1.stock_quantity > 0 THEN a1.shelf_id ELSE NULL END) on_shelf_stock_shelf_num,
       SUM(a1.REAL_TOTAL_PRICE) AMOUNT 
FROM `fe_dwd`.`dwd_shelf_product_day_all_recent_32` a1
JOIN `fe_dwd`.`dwd_shelf_base_day_all` b1 ON a1.shelf_id = b1.shelf_id
WHERE a1.sales_flag = 5
AND b1.`business_name` NOT IN ('内蒙古区','惠州区','冀北区','烟台市','台州区') 
AND a1.`sdate` >= SUBDATE(CURDATE(), INTERVAL 1 DAY)
AND a1.`sdate` < CURDATE()
GROUP BY b1.business_name
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_ma_unsalable',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_unsalable','dm_ma_unsalable','李世龙');
 
END