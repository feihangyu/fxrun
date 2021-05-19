CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_shelf_promote_result`(in_sdate DATETIME)
    SQL SECURITY INVOKER
BEGIN
SET @sdate = in_sdate;
SET @sdate1 = DATE_ADD(in_sdate,INTERVAL 1 DAY);
SET @smonth = DATE_FORMAT(@sdate,"%Y-%m");
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 期初和期末动态SQL
SET @sql_str = 
CONCAT(
"CREATE TEMPORARY TABLE feods.shelf_stock_before_after_tmp
 (KEY idx_shelf_product(shelf_id,product_id))
 AS"
,
"
SELECT sa.month_id, sa.shelf_id, sa.product_id, sa.d"
,DAY(@sdate) 
," AS after_stock,sb.d"
,DAY(@sdate) 
," AS before_stock" 
,"  
FROM"
,"
(SELECT s.month_id, s.shelf_id, s.product_id, d"
,DAY(@sdate) 
," 
FROM feods.d_op_sp_stock_detail_after s
JOIN feods.d_sc_promote_shelf_list p
ON s.shelf_id = p.shelf_id 
WHERE s.month_id = '"
,@smonth 
,"'"
," )sa
JOIN "
,
"
(SELECT s.month_id, s.shelf_id, s.product_id, d"
,DAY(@sdate)
," 
FROM feods.d_op_sp_stock_detail s
JOIN feods.d_sc_promote_shelf_list p
ON s.shelf_id = p.shelf_id 
WHERE s.month_id = '"
,@smonth
,"'"
,
" )sb
ON sa.month_id = sb.month_id
AND sa.shelf_id = sb.shelf_id
AND sa.product_id = sb.product_id;
");
DROP TEMPORARY TABLE IF EXISTS feods.shelf_stock_before_after_tmp;
PREPARE stm_sql FROM @sql_str;
EXECUTE stm_sql;
# 当日销售
DROP TEMPORARY TABLE IF EXISTS feods.shelf_sale_promotion_tmp;
CREATE TEMPORARY TABLE feods.shelf_sale_promotion_tmp
(KEY idx_shelf_product(shelf_id,product_id))
AS
SELECT t.`sdate`
, t.region_area
, t.`business_area`
, t.shelf_id
, t.shelf_code
, t.shelf_name
, t.`product_id`
, t.`product_code2`
, t.`product_name`
-- , t.sale_price
, SUM(t.`gmv`) AS gmv
, SUM(t.`quantity`) AS quantity
, SUM(t.`gmv`) / SUM(t.`quantity`) AS avg_sale_price
, p.`purchase_price`
, SUM(t.`quantity` * p.`purchase_price`) AS cost
, SUM(t.`gmv`) - SUM(t.`quantity` * p.`purchase_price`) AS profit
, SUM(t.discount_value_active) AS discount_value
, SUM(pay_total_amount) AS pay_total_amount
FROM feods.`d_sc_active_result` t
LEFT JOIN 
-- (SELECT business_area ,product_code2,
-- SUBSTRING_INDEX(GROUP_CONCAT(stat_month ORDER BY stat_month DESC SEPARATOR "," ),",",1) AS stat_month,
-- SUBSTRING_INDEX(GROUP_CONCAT(purchase_price ORDER BY stat_month DESC SEPARATOR "," ),",",1) AS purchase_price
-- FROM feods.`wt_monthly_manual_purchase_price`
-- GROUP BY business_area ,product_code2
-- ) 
fe_dm.`dm_sc_current_dynamic_purchase_price` p
ON p.`business_area` = t.`business_area`
AND p.`product_code2` = t.`product_code2`
-- AND p.`stat_month` =  LAST_DAY(t.`sdate`)
JOIN feods.d_sc_promote_shelf_list l
ON t.shelf_id = l.shelf_id
AND t.`sdate` = @sdate
GROUP BY t.sdate
, t.`shelf_id`
, t.`product_id`
;
# 当日入库
DROP TEMPORARY TABLE IF EXISTS feods.shelf_fill_promotion_tmp;
CREATE TEMPORARY TABLE feods.shelf_fill_promotion_tmp 
AS
SELECT
DATE(f.`FILL_TIME`) AS sdate
,f.shelf_id
, f.`FILL_TIME`
    , f.`ORDER_ID`
    , fi.`PRODUCT_ID`
    ,GROUP_CONCAT(DISTINCT (CASE f.`SUPPLIER_TYPE` WHEN 1 THEN "非仓库" WHEN 2 THEN "仓库" WHEN 9 THEN "前置仓" END) SEPARATOR ",") AS SUPPLIER_TYPE 
    , GROUP_CONCAT(DISTINCT d.item_name ORDER BY fi.ACTUAL_FILL_NUM DESC SEPARATOR ",") AS fill_type
    , SUM(fi.`ACTUAL_FILL_NUM`) AS ACTUAL_FILL_NUM   
FROM fe.`sf_product_fill_order` f
JOIN fe.`sf_product_fill_order_item` fi
ON f.`ORDER_ID` = fi.`ORDER_ID`
AND f.`DATA_FLAG` =1
AND fi.`DATA_FLAG` = 1
JOIN feods.d_sc_promote_shelf_list p
ON f.`SHELF_ID` = p.shelf_id
AND f.`FILL_TIME` >= @sdate AND f.`FILL_TIME` < @sdate1 
AND f.`ORDER_STATUS` IN (3,4)
AND f.`FILL_TYPE` IN (1,2,4,7,8,9,12,14)
JOIN fe.`pub_dictionary_item` d
ON f.`FILL_TYPE` = d.item_value
AND d.dictionary_id = 24
GROUP BY DATE(f.`FILL_TIME`),f.shelf_id,fi.product_id
;
# 当日出库
# 出库_没有数据
DROP TEMPORARY TABLE IF EXISTS feods.shelf_out_promotion_tmp;
CREATE TEMPORARY TABLE feods.shelf_out_promotion_tmp  
AS 
SELECT DATE(f.`SEND_TIME`) AS sdate
, tran.`SOURCE_SHELF_ID` AS shelf_id
-- , tran.`TARGET_SHELF_ID` 
-- , fi.`SHELF_ID`
, fi.`PRODUCT_ID`
, SUM(fi.`ACTUAL_SEND_NUM`) AS ACTUAL_SEND_NUM
FROM fe.`sf_shelf_goods_transfer` tran
JOIN fe.`sf_product_fill_order_item` fi
ON tran.`SOURCE_ORDER_ID` = fi.`ORDER_ID`
AND fi.`DATA_FLAG` =1
AND tran.`DATA_FLAG` =1
JOIN fe.sf_product_fill_order f
ON f.`ORDER_ID` = fi.`ORDER_ID`
JOIN feods.d_sc_promote_shelf_list l
ON tran.`SOURCE_SHELF_ID` = l.shelf_id
WHERE f.`send_time` >= @sdate AND f.`send_time` < @sdate1
AND tran.`STATE` = 2
AND f.`ORDER_STATUS` IN (3,4)
GROUP BY DATE(f.`SEND_TIME`),tran.SOURCE_SHELF_ID,fi.`PRODUCT_ID`;
# 报损
DROP TEMPORARY TABLE IF EXISTS feods.shelf_loss_promotion_tmp;
CREATE TEMPORARY TABLE feods.shelf_loss_promotion_tmp
SELECT b.`SHELF_ID`,a.`PRODUCT_ID`,b.`OPERATE_TIME`,a.`ERROR_NUM`
FROM
    fe.sf_shelf_check_detail a -- LEFT JOIN fe.sf_shelf_check b ON a.CHECK_ID=b.CHECK_ID
     JOIN fe.sf_shelf_check b
        ON a.CHECK_ID = b.CHECK_ID
     JOIN feods.d_sc_promote_shelf_list p
        ON a.shelf_id = p.shelf_id
WHERE b.OPERATE_TIME >= @sdate
    AND b.OPERATE_TIME < DATE_ADD(@sdate,INTERVAL 1 DAY)
    AND a.DATA_FLAG = 1
    AND a.AUDIT_STATUS = 2   
   ;
DELETE FROM feods.d_sc_shelf_promote_result WHERE sdate = @sdate;
INSERT INTO feods.d_sc_shelf_promote_result
(sdate,
region_area,
business_area ,
shelf_id ,
shelf_code ,
shelf_name,
product_id ,
product_code2,
product_name ,
product_type,
gmv ,
quantity ,
avg_sale_price ,
purchase_price,
cost ,
profit,
discount_value,
pay_total_amount ,
after_stock ,
before_stock ,
supplier_type,
fill_type ,
actual_fill_num ,
actual_send_num ,
check_damage_qty ,
lost_quantity ,
stolen_qty 
)
SELECT
@sdate
, c.region_name
, c.business_name
, st.shelf_id
, s.shelf_code
, s.shelf_name
, st.product_id
, p.product_code2
, p.product_name
, pt.product_type
, IFNULL(sa.gmv,0) AS gmv
, IFNULL(sa.quantity,0) AS quantity
, IFNULL(sa.avg_sale_price,0) AS avg_sale_price
, IFNULL(sa.purchase_price,0) AS purchase_price
, IFNULL(sa.cost,0) AS cost
, IFNULL(sa.profit,0) AS profit
, IFNULL(sa.discount_value,0) AS discount_value
, IFNULL(sa.pay_total_amount,0) AS pay_total_amount
, st.after_stock
, st.before_stock 
, f.SUPPLIER_TYPE
, f.fill_type
, IFNULL(f.ACTUAL_FILL_NUM,0) AS ACTUAL_FILL_NUM
, IFNULL(ABS(ot.ACTUAL_SEND_NUM),0) AS ACTUAL_SEND_NUM
, IFNULL(l.ERROR_NUM,0) AS check_damage_qty
, st.before_stock + IFNULL(f.ACTUAL_FILL_NUM,0) - IFNULL(ABS(ot.ACTUAL_SEND_NUM),0) - IFNULL(sa.quantity,0) - st.after_stock AS lost_quantity 
, (st.before_stock + IFNULL(f.ACTUAL_FILL_NUM,0) - IFNULL(ABS(ot.ACTUAL_SEND_NUM),0) - IFNULL(sa.quantity,0)) - st.after_stock  - IFNULL(l.ERROR_NUM,0) AS stolen_qty
FROM feods.shelf_stock_before_after_tmp st
JOIN fe.`sf_shelf` s
ON st.shelf_id = s.shelf_id
AND s.data_flag =1
JOIN feods.`fjr_city_business` c
ON s.city = c.city
JOIN fe.`sf_product` p
ON st.product_id = p.product_id
AND p.data_flag =1
LEFT JOIN feods.shelf_sale_promotion_tmp sa
ON sa.shelf_id = st.shelf_id
AND sa.product_id = st.product_id
LEFT JOIN feods.shelf_fill_promotion_tmp f
ON st.shelf_id = f.shelf_id
AND st.product_id = f.product_id 
LEFT JOIN feods.shelf_out_promotion_tmp ot
ON st.shelf_id = ot.shelf_id
AND st.product_id = ot.product_id 
LEFT JOIN feods.shelf_loss_promotion_tmp l
ON st.shelf_id = l.shelf_id
AND st.product_id = l.product_id
LEFT JOIN feods.`zs_product_dim_sserp` pt
ON c.business_name = pt.business_area
AND p.product_code2 = pt.product_fe
WHERE st.after_stock >0 OR st.before_stock  >0 OR sa.quantity >0 OR f.ACTUAL_FILL_NUM > 0 OR ot.ACTUAL_SEND_NUM >0
;
  
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_sc_shelf_promote_result',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
 
   COMMIT;
   
END