CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_sectype_kpi_daily`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场  业务方(罗辉)
-- Create date: 2020-3-19
-- Modify date:
-- Description:
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @smonth=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH ),'%Y-%m-01');
#删除数据
DELETE FROM fe_dm.dm_ma_sectype_kpi_daily WHERE sdate=@sdate OR sdate<SUBDATE(@sdate,100);
#临时数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_order;
CREATE TEMPORARY TABLE fe_dm.temp_order(INDEX(PRODUCT_ID)) AS #订单数据
SELECT  a1.ORDER_ID,a2.PRODUCT_ID,DATE(ORDER_DATE) sdate, a1.SHELF_ID,a1.USER_ID
	,a2.REAL_TOTAL_PRICE-IFNULL(a4.refund_amount,0)
    +IFNULL(a2.DISCOUNT_AMOUNT*(a1.third_discount_amount/(a1.COUPON_AMOUNT+a1.DISCOUNT_AMOUNT+IFNULL(a1.third_discount_amount,0) )),0)   amount
	,IF(order_type=3,a2.quantity_shipped,a2.quantity) sale_num
	,IF(order_type=3,a2.quantity_shipped,a2.quantity)*SALE_PRICE GMV
    ,a2.DISCOUNT_AMOUNT*(a1.DISCOUNT_AMOUNT/(a1.COUPON_AMOUNT+a1.DISCOUNT_AMOUNT+IFNULL(a1.third_discount_amount,0) )) DISCOUNT_AMOUNT2
    ,a2.DISCOUNT_AMOUNT*(a1.COUPON_AMOUNT/(a1.COUPON_AMOUNT+a1.DISCOUNT_AMOUNT+IFNULL(a1.third_discount_amount,0) )) COUPON_AMOUNT2
    ,a2.DISCOUNT_AMOUNT*(a1.third_discount_amount/(a1.COUPON_AMOUNT+a1.DISCOUNT_AMOUNT+IFNULL(a1.third_discount_amount,0) )) third_discount_amount
FROM fe.sf_order a1
JOIN fe.sf_order_item a2 ON a1.ORDER_ID = a2.ORDER_ID AND a2.DATA_FLAG=1
LEFT JOIN fe.sf_order_refund_order a3 ON a1.ORDER_ID = a3.order_id AND a3.refund_status = 5 AND a3.data_flag=1
LEFT JOIN fe.sf_order_refund_item a4 ON a4.refund_order_id=a3.refund_order_id AND a4.order_item_id=a2.ORDER_ITEM_ID AND a4.data_flag=1
WHERE a1.ORDER_DATE >= @sdate AND  a1.ORDER_DATE <ADDDATE(@sdate,1)
  AND a1.DATA_FLAG = 1  AND a1.ORDER_STATUS IN (2, 6, 7)
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_stock; #库存数据
CREATE TEMPORARY TABLE fe_dm.temp_stock(INDEX(shelf_id),INDEX(product_id)) AS
    SELECT sdate,shelf_id,product_id,stock_quantity
    FROM fe_dwd.dwd_shelf_product_day_all_recent_32 WHERE sdate=@sdate AND stock_quantity>0
;
#插入数据
INSERT INTO fe_dm.dm_ma_sectype_kpi_daily
    (sdate, business_area, sec_type_name
    , stocks, skus_sale, ShelfProducts_sale, sale_num, GMV, amount, orders, users, DISCOUNT_AMOUNT, COUPON_AMOUNT
    , skus_sale_high, ShelfProducts_sale_high, sale_num_high, GMV_high, amount_high, orders_high, users_high)
SELECT @sdate, business_area, second_type_name
    , SUM(stocks) stocks, skus_sale, ShelfProducts_sale, sale_num, GMV, amount, orders, users, DISCOUNT_AMOUNT, COUPON_AMOUNT
    , skus_sale_high, ShelfProducts_sale_high, sale_num_high, GMV_high, amount_high, orders_high, users_high
FROM
    (SELECT a3.business_name business_area,a2.second_type_name
        ,0 stocks
        ,SUM(GMV) GMV,SUM(amount)  amount,SUM(sale_num) sale_num,SUM(COUPON_AMOUNT2) COUPON_AMOUNT,SUM(DISCOUNT_AMOUNT2) DISCOUNT_AMOUNT
        ,COUNT(DISTINCT ORDER_ID) orders,COUNT(DISTINCT USER_ID) users
        ,COUNT(DISTINCT a1.product_id) skus_sale
        ,COUNT(DISTINCT a1.shelf_id,a1.product_id) ShelfProducts_sale
        ,SUM(IF(a4.pid IS NOT NULL,GMV,0)) GMV_high
        ,SUM(IF(a4.pid IS NOT NULL,amount,0))  amount_high
        ,SUM(IF(a4.pid IS NOT NULL,sale_num,0)) sale_num_high
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,ORDER_ID,NULL)) orders_high
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,USER_ID,NULL)) users_high
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,a1.product_id,NULL)) skus_sale_high
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,CONCAT(a1.shelf_id,a1.product_id),NULL)) ShelfProducts_sale_high
    FROM fe_dm.temp_order a1
    JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.product_id
    JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a1.SHELF_ID
    LEFT JOIN fe_dm.dm_ma_HighProfit_list_monthly a4 ON a4.sdate=@smonth AND a4.business_area=a3.business_name AND a4.product_id=a1.PRODUCT_ID
    GROUP BY a3.business_name,a2.second_type_name
    UNION ALL
    SELECT a3.business_name,a2.second_type_name
         ,SUM(stock_quantity) stocks
        ,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    FROM fe_dm.temp_stock a1
    JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.product_id
    JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a1.shelf_id AND a3.shelf_type <>9
    GROUP BY a3.business_name,a2.second_type_name) t1
GROUP BY business_area,second_type_name
;
# 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_sectype_kpi_daily',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END