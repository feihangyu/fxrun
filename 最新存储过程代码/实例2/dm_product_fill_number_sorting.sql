CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_product_fill_number_sorting`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 2019/03/14
-- Modify date: 
-- Description:	
-- 	大仓补货商品数量的频次排序结果表(每个月1号4时14分)
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @area_product:='';
SET @row:=1;
SET @freq:=0;
DELETE FROM fe_dm.dm_product_fill_number_sorting WHERE smonth = DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 MONTH),'%Y%m');
INSERT INTO fe_dm.dm_product_fill_number_sorting(
 smonth
,business_area
,product_code
,product_id
,product_name
,actual_send_num
,fill_qty
,row_seq)
 SELECT
 DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 MONTH),'%Y%m') AS smonth
,t.business_area
,t.product_code
,t.product_id
,t.product_name
,t.actual_send_num
,t.fill_freq
,t.row_freq
FROM
(SELECT
  t.*,
  @row:=
  CASE WHEN @area_product = CONCAT(t.business_area,t.product_code) 
       THEN IF(@freq = t.fill_freq,@row,@row+1)
       ELSE FLOOR(@row/@row)
       END row_freq,
  @freq:=
  t.fill_freq AS frequency,
  @area_product:=
  CONCAT(t.business_area,t.product_code) AS row_sign
FROM
(SELECT
  d.business_name AS business_area,
  e.`PRODUCT_CODE2` AS product_code,
  a.product_id,
  e.`PRODUCT_NAME`,
  a.`ACTUAL_SEND_NUM`,
  COUNT(a.`ORDER_ID`) AS fill_freq
FROM
fe_dwd.`dwd_fill_day_inc` a
INNER JOIN fe_dwd.`dwd_shelf_base_day_all` c
ON a.`SHELF_ID` = c.`SHELF_ID`
INNER JOIN fe_dwd.`dwd_city_business` d
ON c.city = d.city
INNER JOIN fe_dwd.dwd_product_base_day_all e
ON e.`PRODUCT_ID`= a.`PRODUCT_ID`
WHERE a.supplier_type=2 AND a.fill_type IN (1,2) AND a.order_status IN (2,4)
AND a.fill_time >= DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 MONTH)
AND a.fill_time <  DATE_ADD(LAST_DAY(CURRENT_DATE),INTERVAL 1 DAY)
GROUP BY d.business_name,
a.`PRODUCT_ID`,
a.`ACTUAL_SEND_NUM`) t
ORDER BY t.business_area,t.product_id,t.fill_freq DESC) t;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_product_fill_number_sorting',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_product_fill_number_sorting','dm_product_fill_number_sorting','蔡松林');
COMMIT;
    END