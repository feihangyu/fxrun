CREATE DEFINER=`feprocess`@`%` PROCEDURE `shelf_product_stock_14days`()
BEGIN
SET @sdate = CURRENT_DATE();
SET @day1 = DATE_SUB(@sdate,INTERVAL 13 DAY);
SET @day13 = @sdate;
SET @cur_m = DATE_FORMAT(@sdate,"%Y-%m");
SET @last_m1 = DATE_FORMAT(DATE_SUB(@sdate, INTERVAL 1 MONTH),"%Y-%m");
SET @last_m2 = DATE_FORMAT(DATE_SUB(@sdate, INTERVAL 2 MONTH),"%Y-%m");
SET @month_diff = IF(MONTH(@day13) < MONTH(@day1),MONTH(@day13) + 12 - MONTH(@day1),MONTH(@day13) - MONTH(@day1)); 
SET @mflag1 := IF(@month_diff -1 <0,0,1) ;  
SET @n = 15;
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SELECT
    CONCAT(
        "INSERT INTO feods.`shelf_product_14days_stock`(sdate,shelf_id,product_id,"
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
AND w.sdate <= @day13
AND LEFT(w.sdate,7) = @cur_m    
;
SELECT IFNULL(CONCAT(
"UPDATE feods.`shelf_product_14days_stock` a
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
AND sdate <= @day13
AND LEFT(sdate,7) = @last_m1 
AND @mflag1
ORDER BY sdate DESC) w
;
TRUNCATE feods.`shelf_product_14days_stock`; 
TRUNCATE feods.`shelf_product_stock_14days`;
PREPARE sql_exe1 FROM @sql_str1;
EXECUTE sql_exe1;
PREPARE sql_exe2 FROM @sql_str2;
EXECUTE sql_exe2;
insert into  feods.`shelf_product_stock_14days`(shelf_id,product_id,days)
select shelf_id,product_id,days from (
select shelf_id,product_id,sum(DAY1+DAY2+DAY3+DAY4+DAY5+DAY6+DAY7+DAY8+DAY9+DAY10+DAY11+DAY12+DAY13+DAY14) as days
from (
select 
shelf_id,product_id,
case when DAY1 > 0 then 1 else 0 end as DAY1, 
case when DAY2 > 0 then 1 else 0 end as DAY2, 
case when DAY3 > 0 then 1 else 0 end as DAY3, 
case when DAY4 > 0 then 1 else 0 end as DAY4, 
case when DAY5 > 0 then 1 else 0 end as DAY5, 
case when DAY6 > 0 then 1 else 0 end as DAY6, 
case when DAY7 > 0 then 1 else 0 end as DAY7, 
case when DAY8 > 0 then 1 else 0 end as DAY8, 
case when DAY9 > 0 then 1 else 0 end as DAY9, 
case when DAY10 > 0 then 1 else 0 end  as DAY10, 
case when DAY11 > 0 then 1 else 0 end  as DAY11, 
case when DAY12 > 0 then 1 else 0 end  as DAY12, 
case when DAY13 > 0 then 1 else 0 end  as DAY13, 
case when DAY14 > 0 then 1 else 0 end  as DAY14
from feods.`shelf_product_14days_stock` 
) t group by shelf_id,product_id) tt where tt.days>0;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'shelf_product_stock_14days',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
COMMIT;
END