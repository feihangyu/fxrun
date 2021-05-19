CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_preware_shelf_sale_thirty`(in_sdate DATETIME)
    SQL SECURITY INVOKER
BEGIN
SET @day1 = DATE_SUB(@sdate,INTERVAL 29 DAY);
SET @day2 = DATE_SUB(@sdate,INTERVAL 28 DAY);
SET @day3 = DATE_SUB(@sdate,INTERVAL 27 DAY);
SET @day4 = DATE_SUB(@sdate,INTERVAL 26 DAY);
SET @day5 = DATE_SUB(@sdate,INTERVAL 25 DAY);
SET @day6 = DATE_SUB(@sdate,INTERVAL 24 DAY);
SET @day7 = DATE_SUB(@sdate,INTERVAL 23 DAY);
SET @day8 = DATE_SUB(@sdate,INTERVAL 22 DAY);
SET @day9 = DATE_SUB(@sdate,INTERVAL 21 DAY);
SET @day10 = DATE_SUB(@sdate,INTERVAL 20 DAY);
SET @day11 = DATE_SUB(@sdate,INTERVAL 19 DAY);
SET @day12 = DATE_SUB(@sdate,INTERVAL 18 DAY);
SET @day13 = DATE_SUB(@sdate,INTERVAL 17 DAY);
SET @day14 = DATE_SUB(@sdate,INTERVAL 16 DAY);
SET @day15 = DATE_SUB(@sdate,INTERVAL 15 DAY);
SET @day16 = DATE_SUB(@sdate,INTERVAL 14 DAY);
SET @day17 = DATE_SUB(@sdate,INTERVAL 13 DAY);
SET @day18 = DATE_SUB(@sdate,INTERVAL 12 DAY);
SET @day19 = DATE_SUB(@sdate,INTERVAL 11 DAY);
SET @day20 = DATE_SUB(@sdate,INTERVAL 10 DAY);
SET @day21 = DATE_SUB(@sdate,INTERVAL 9 DAY);
SET @day22 = DATE_SUB(@sdate,INTERVAL 8 DAY);
SET @day23 = DATE_SUB(@sdate,INTERVAL 7 DAY);
SET @day24 = DATE_SUB(@sdate,INTERVAL 6 DAY);
SET @day25 = DATE_SUB(@sdate,INTERVAL 5 DAY);
SET @day26 = DATE_SUB(@sdate,INTERVAL 4 DAY);
SET @day27 = DATE_SUB(@sdate,INTERVAL 3 DAY);
SET @day28 = DATE_SUB(@sdate,INTERVAL 2 DAY);
SET @day29 = DATE_SUB(@sdate,INTERVAL 1 DAY);
SET @day30 = @sdate;
SET @run_date:= CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.preaware_sale_day30_tmp;
CREATE TEMPORARY TABLE feods.preaware_sale_day30_tmp
(KEY idx_sdate_shelf_product(sdate,shelf_id,product_id)
)
AS
SELECT
    DATE(t.`order_date`) AS sdate
    ,w.`warehouse_id`
    , t.`SHELF_ID`
    , t.`PRODUCT_ID`
    , SUM(t.`SALE_PRICE` * t.`QUANTITY`) AS GMV
    , SUM( t.`QUANTITY`) AS sale_qty
FROM 
 fe.`sf_prewarehouse_shelf_detail` w
 JOIN feods.`wt_order_item_twomonth_temp` t
    ON w.`shelf_id` = t.`SHELF_ID`
    AND t.`order_date` >= DATE_SUB(@sdate, INTERVAL 29 DAY) AND t.`order_date` < DATE_ADD(@sdate, INTERVAL 1 DAY) 
--     and t.`SHELF_ID` = 1
    AND w.`data_flag` = 1
WHERE  t.`DISCOUNT_AMOUNT` < (t.`SALE_PRICE` * t.`QUANTITY`) * 0.2
GROUP BY DATE(t.`order_date`)
    , t.`SHELF_ID`
    , t.`PRODUCT_ID`
ORDER BY DATE(t.`order_date`),t.`SHELF_ID` ASC,t.`PRODUCT_ID` ASC;
# 近30天8折以上销售的所有货架\商品list
TRUNCATE TABLE feods.`d_sc_preware_shelf_sale_thirty`;
INSERT INTO feods.`d_sc_preware_shelf_sale_thirty`
(sdate,
warehouse_id,
shelf_id,
product_id,
sdays,
gmv,
sale_qty,
day1,
day2,
day3,
day4,
day5,
day6,
day7,
day8,
day9,
day10,
day11,
day12,
day13,
day14,
day15,
day16,
day17,
day18,
day19,
day20,
day21,
day22,
day23,
day24,
day25,
day26,
day27,
day28,
day29,
day30
)
SELECT
    @day30 AS sdate
    ,t1.warehouse_id
    ,t1.shelf_id
    , t1.product_id
    , COUNT(t1.sdate) AS sdays
    ,SUM(t1.gmv ) AS gmv
    ,SUM(t1.sale_qty) AS sale_qty
--     , GROUP_CONCAT(t1.gmv ORDER BY t1.sdate ASC SEPARATOR ",") AS gmv
--     , GROUP_CONCAT(t1.sale_qty ORDER BY t1.sdate ASC SEPARATOR ",") AS sale_qty
--     , GROUP_CONCAT(t1.sdate order by t1.sdate asc SEPARATOR ",") AS sdate
    ,IF(FIND_IN_SET(@day1,GROUP_CONCAT(t1.sdate SEPARATOR ",")),1,0) day1,
IF(FIND_IN_SET(@day2,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day2,
IF(FIND_IN_SET(@day3,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day3,
IF(FIND_IN_SET(@day4,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day4,
IF(FIND_IN_SET(@day5,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day5,
IF(FIND_IN_SET(@day6,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day6,
IF(FIND_IN_SET(@day7,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day7,
IF(FIND_IN_SET(@day8,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day8,
IF(FIND_IN_SET(@day9,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day9,
IF(FIND_IN_SET(@day10,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day10,
IF(FIND_IN_SET(@day11,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day11,
IF(FIND_IN_SET(@day12,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day12,
IF(FIND_IN_SET(@day13,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day13,
IF(FIND_IN_SET(@day14,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day14,
IF(FIND_IN_SET(@day15,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day15,
IF(FIND_IN_SET(@day16,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day16,
IF(FIND_IN_SET(@day17,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day17,
IF(FIND_IN_SET(@day18,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day18,
IF(FIND_IN_SET(@day19,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day19,
IF(FIND_IN_SET(@day20,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day20,
IF(FIND_IN_SET(@day21,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day21,
IF(FIND_IN_SET(@day22,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day22,
IF(FIND_IN_SET(@day23,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day23,
IF(FIND_IN_SET(@day24,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day24,
IF(FIND_IN_SET(@day25,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day25,
IF(FIND_IN_SET(@day26,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day26,
IF(FIND_IN_SET(@day27,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day27,
IF(FIND_IN_SET(@day28,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day28,
IF(FIND_IN_SET(@day29,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day29,
IF(FIND_IN_SET(@day30,GROUP_CONCAT(t1.sdate ORDER BY t1.sdate ASC SEPARATOR ",")),1,0) day30
FROM
    feods.preaware_sale_day30_tmp t1
GROUP BY t1.shelf_id
    , t1.product_id;
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'sp_d_sc_preware_shelf_sale_thirty',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('吴婷@',@user,@timestamp)
);
   COMMIT;
   
END