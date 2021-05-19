CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_zs_product_fill_number_sorting`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 2019/03/14
-- Modify date: 
-- Description:	
-- 	大仓补货商品数量的频次排序结果表(每个月1号4时14分)
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
SET @area_product:='';
SET @row:=1;
SET @freq:=0;
delete from feods.zs_product_fill_number_sorting where smonth = date_format(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 MONTH),'%Y%m');
insert into feods.zs_product_fill_number_sorting(
 smonth
,business_area
,product_code
,product_id
,product_name
,actual_send_num
,fill_qty
,row_seq)
 SELECT
 DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 MONTH),'%Y%m') as smonth
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
  d.`BUSINESS_AREA`,
  e.`PRODUCT_CODE2` AS product_code,
  b.product_id,
  e.`PRODUCT_NAME`,
  b.`ACTUAL_SEND_NUM`,
  COUNT(a.`ORDER_ID`) AS fill_freq
FROM
  fe.`sf_product_fill_order` a
inner JOIN
  fe.`sf_product_fill_order_item` b
ON a.`ORDER_ID`= b.`ORDER_ID`
inner JOIN fe.`sf_shelf` c
ON a.`SHELF_ID` = c.`SHELF_ID`
inner JOIN fe.`zs_city_business` d
ON SUBSTRING_INDEX(SUBSTRING_INDEX(c.`AREA_ADDRESS`,',',2),',',-1)= d.`CITY_NAME`
inner JOIN fe.`sf_product` e
ON e.`PRODUCT_ID`= b.`PRODUCT_ID`
WHERE a.data_flag=1 AND c.data_flag=1 AND e.data_flag=1 AND b.data_flag=1
AND a.supplier_type=2 AND a.fill_type IN (1,2) AND a.order_status IN (2,4)
and a.fill_time >= date_sub(date_add(current_date,interval -day(current_date)+1 day),interval 1 month)
and a.fill_time <  date_add(last_day(current_date),interval 1 day)
GROUP BY d.`BUSINESS_AREA`,
b.`PRODUCT_ID`,
b.`ACTUAL_SEND_NUM`) t
ORDER BY t.business_area,t.product_id,t.fill_freq DESC) t;
CALL sh_process.`sp_sf_dw_task_log`(
  'sp_zs_product_fill_number_sorting',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);
COMMIT;
-- 更新今天的历史数据
-- set @day := date(20190101);
-- while @day < date('20191001') do
-- DELETE FROM feods.zs_product_fill_number_sorting WHERE smonth = DATE_FORMAT(@day,'%Y%m');
-- 
-- INSERT INTO feods.zs_product_fill_number_sorting(
--  smonth
-- ,business_area
-- ,product_code
-- ,product_id
-- ,product_name
-- ,actual_send_num
-- ,fill_qty
-- ,row_seq)
--  SELECT
--  DATE_FORMAT(@day,'%Y%m') AS smonth
-- ,t.business_area
-- ,t.product_code
-- ,t.product_id
-- ,t.product_name
-- ,t.actual_send_num
-- ,t.fill_freq
-- ,t.row_freq
-- FROM
-- (SELECT
--   t.*,
--   @row:=
--   CASE WHEN @area_product = CONCAT(t.business_area,t.product_code) 
--        THEN IF(@freq = t.fill_freq,@row,@row+1)
--        ELSE FLOOR(@row/@row)
--        END row_freq,
--   @freq:=
--   t.fill_freq AS frequency,
--   @area_product:=
--   CONCAT(t.business_area,t.product_code) AS row_sign
-- FROM
-- (SELECT
--   d.`BUSINESS_AREA`,
--   e.`PRODUCT_CODE2` AS product_code,
--   b.product_id,
--   e.`PRODUCT_NAME`,
--   b.`ACTUAL_SEND_NUM`,
--   COUNT(a.`ORDER_ID`) AS fill_freq
-- FROM
--   fe.`sf_product_fill_order` a
-- INNER JOIN
--   fe.`sf_product_fill_order_item` b
-- ON a.`ORDER_ID`= b.`ORDER_ID`
-- INNER JOIN fe.`sf_shelf` c
-- ON a.`SHELF_ID` = c.`SHELF_ID`
-- INNER JOIN fe.`zs_city_business` d
-- ON SUBSTRING_INDEX(SUBSTRING_INDEX(c.`AREA_ADDRESS`,',',2),',',-1)= d.`CITY_NAME`
-- INNER JOIN fe.`sf_product` e
-- ON e.`PRODUCT_ID`= b.`PRODUCT_ID`
-- WHERE a.data_flag=1 AND c.data_flag=1 AND e.data_flag=1 AND b.data_flag=1
-- AND a.supplier_type=2 AND a.fill_type IN (1,2) AND a.order_status IN (2,4)
-- AND a.fill_time >= @day
-- AND a.fill_time <  date_add(@day,interval 1 month)
-- GROUP BY d.`BUSINESS_AREA`,
-- b.`PRODUCT_ID`,
-- b.`ACTUAL_SEND_NUM`) t
-- ORDER BY t.business_area,t.product_id,t.fill_freq DESC) t;
-- set @day:= date_add(@day,interval 1 month);
-- end while;
    END