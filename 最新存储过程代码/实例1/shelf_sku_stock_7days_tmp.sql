CREATE DEFINER=`feprocess`@`%` PROCEDURE `shelf_sku_stock_7days_tmp`()
BEGIN
SET @sdate = CURRENT_DATE();
SET @day1 = DATE_SUB(@sdate,INTERVAL 6 DAY);
SET @day7 = @sdate;
SET @cur_m = DATE_FORMAT(@sdate,"%Y-%m");
SET @last_m1 = DATE_FORMAT(DATE_SUB(@sdate, INTERVAL 1 MONTH),"%Y-%m");
SET @last_m2 = DATE_FORMAT(DATE_SUB(@sdate, INTERVAL 2 MONTH),"%Y-%m");
SET @month_diff = IF(MONTH(@day7) < MONTH(@day1),MONTH(@day7) + 12 - MONTH(@day1),MONTH(@day7) - MONTH(@day1)); 
SET @mflag1 := IF(@month_diff -1 <0,0,1) ;  
SET @n = 8;
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SELECT
    CONCAT(
        "INSERT INTO feods.`shelf_product_stock_7days_tmp`(sdate,shelf_id,product_id,"
        , GROUP_CONCAT(
            CONCAT("day", @n := @n - 1)
            ORDER BY w.sdate DESC SEPARATOR ","
        )
        , ")
    "
        , "SELECT 
     @sdate
    ,t.shelf_id
    ,t.product_id,"
        , GROUP_CONCAT(
            CONCAT("day", DAY(w.sdate), "_quantity")
            ORDER BY w.sdate ASC SEPARATOR ","
        )
        , " FROM 
fe.`sf_shelf_product_stock_detail` t 
WHERE t.`STAT_DATE` = @cur_m
; "
    ) INTO @sql_str1
FROM feods.`fjr_work_days` w
WHERE w.sdate >= @day1
AND w.sdate <= @day7
AND LEFT(w.sdate,7) = @cur_m    
;
SELECT IFNULL(CONCAT(
"UPDATE feods.`shelf_product_stock_7days_tmp` a
JOIN fe.`sf_shelf_product_stock_detail` b
ON a.shelf_id = b.shelf_id
AND a.product_id = b.product_id"
,
" SET" 
, GROUP_CONCAT(CONCAT(" a.","day",@n := @n - 1," = ","b.","day",DAY(w.sdate),"_quantity") ORDER BY w.sdate SEPARATOR  "," )
,
"
WHERE b.`STAT_DATE` = @last_m1
; " )
, CONCAT("select  'There is not last_month';") ) INTO @sql_str2
FROM 
(SELECT sdate 
FROM feods.`fjr_work_days` 
WHERE sdate >= @day1
AND sdate <= @day7
AND LEFT(sdate,7) = @last_m1 
AND @mflag1
ORDER BY sdate DESC) w
;
TRUNCATE feods.`shelf_product_stock_7days_tmp`; 
TRUNCATE feods.`shelf_sku_stock_7days_tmp`;
PREPARE sql_exe1 FROM @sql_str1;
EXECUTE sql_exe1;
PREPARE sql_exe2 FROM @sql_str2;
EXECUTE sql_exe2;
insert into feods.shelf_sku_stock_7days_tmp(sdate,shelf_id,stock_qty1,sku1,stock_qty2,sku2,stock_qty3,sku3,stock_qty4,sku4,stock_qty5,sku5,stock_qty6,sku6,stock_qty7,sku7)
select 
@sdate as sdate,
shelf_id,
sum(DAY1) as stock_qty1,
count(distinct case when DAY1>0 then product_id end) as sku1,
sum(DAY2) as stock_qty2,
count(distinct case when DAY2>0 then product_id end) as sku2,
sum(DAY3) as stock_qty3,
count(distinct case when DAY3>0 then product_id end) as sku3,
sum(DAY4) as stock_qty4,
count(distinct case when DAY4>0 then product_id end) as sku4,
sum(DAY5) as stock_qty5,
count(distinct case when DAY5>0 then product_id end) as sku5,
sum(DAY6) as stock_qty6,
count(distinct case when DAY6>0 then product_id end) as sku6,
sum(DAY7) as stock_qty7,
count(distinct case when DAY7>0 then product_id end) as sku7
from feods.`shelf_product_stock_7days_tmp`
group by shelf_id;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'shelf_sku_stock_7days_tmp',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
  COMMIT;
   
END