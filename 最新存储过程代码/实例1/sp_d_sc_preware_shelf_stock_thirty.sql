CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_preware_shelf_stock_thirty`(in_sdate DATETIME)
    SQL SECURITY INVOKER
BEGIN
SET @sdate = in_sdate;
SET @day1 = DATE_SUB(@sdate,INTERVAL 29 DAY);
SET @day30 = @sdate;
SET @cur_m = DATE_FORMAT(@sdate,"%Y-%m");
SET @last_m1 = DATE_FORMAT(DATE_SUB(@sdate, INTERVAL 1 MONTH),"%Y-%m");
SET @last_m2 = DATE_FORMAT(DATE_SUB(@sdate, INTERVAL 2 MONTH),"%Y-%m");
SET @month_diff = IF(MONTH(@day30) < MONTH(@day1),MONTH(@day30) + 12 - MONTH(@day1),MONTH(@day30) - MONTH(@day1));
SET @mflag1 := IF(@month_diff -1 <0,0,1) ;
SET @mflag2 := IF(@month_diff -2 <0,0,1) ;
SET @n = 31;
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SELECT
    CONCAT(
        "INSERT INTO feods.`d_sc_preware_shelf_stock_thirty`(sdate,shelf_id,product_id,"
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
feods.`d_sc_preware_shelf_sale_thirty` s
JOIN fe.`sf_shelf_product_stock_detail` t 
ON s.shelf_id = t.shelf_id
AND s.product_id = t.product_id
WHERE t.`STAT_DATE` = @cur_m
; "
    ) INTO @sql_str1
FROM feods.`fjr_work_days` w
WHERE w.sdate >= @day1
AND w.sdate <= @day30
AND LEFT(w.sdate,7) = @cur_m    
;
SELECT IFNULL(CONCAT(
"UPDATE feods.`d_sc_preware_shelf_stock_thirty` a
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
AND sdate <= @day30
AND LEFT(sdate,7) = @last_m1 
AND @mflag1
ORDER BY sdate DESC) w
;
SELECT IFNULL(CONCAT(
"UPDATE feods.`d_sc_preware_shelf_stock_thirty` a
JOIN fe.`sf_shelf_product_stock_detail` b
ON a.shelf_id = b.shelf_id
AND a.product_id = b.product_id"
,
""
," 
SET" 
, GROUP_CONCAT(CONCAT(" a.","day",@n := @n - 1," = ","b.","day",DAY(w.sdate),"_quantity") SEPARATOR  "," )
,
"
WHERE b.`STAT_DATE` = @last_m1
; "  
   ) , 
CONCAT("select 'There is not last_twomonth';")
 )INTO @sql_str3
FROM
(SELECT sdate 
FROM feods.`fjr_work_days` 
WHERE sdate >= @day1
AND sdate <= @day30
AND LEFT(sdate,7) = @last_m2 
AND @mflag2
ORDER BY sdate DESC) w
;
TRUNCATE feods.d_sc_preware_shelf_stock_thirty;
PREPARE sql_exe1 FROM @sql_str1;
EXECUTE sql_exe1;
PREPARE sql_exe2 FROM @sql_str2;
EXECUTE sql_exe2;
PREPARE sql_exe3 FROM @sql_str3;
EXECUTE sql_exe3;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_d_sc_preware_shelf_stock_thirty',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('wuting@', @user, @timestamp)
  );
  COMMIT;
   
END