CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_sectype_kpi_monthly`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场  商品营销组(罗辉)
-- Create date: 2020-3-19
-- Modify date:
-- Description: 二级分类每日KPI数据
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
#删除数据
DELETE FROM fe_dm.dm_ma_sectype_kpi_monthly WHERE sdate=@smonth OR sdate<DATE_SUB(@smonth,INTERVAL 100 MONTH );
#临时数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_order_info;
CREATE TEMPORARY TABLE fe_dm.temp_order_info(INDEX(business_name,second_type_name)) AS #订单数据
    SELECT  a3.business_name,a2.second_type_name
         ,COUNT(DISTINCT a1.product_id) skus_sale
         ,COUNT(DISTINCT a1.shelf_id,a1.product_id) ShelfProducts_sale
         ,COUNT(DISTINCT user_id) users
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,USER_ID,NULL)) users_high
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,a1.product_id,NULL)) skus_sale_high
        ,COUNT(DISTINCT IF(a4.pid IS NOT NULL,CONCAT(a1.shelf_id,a1.product_id),NULL)) ShelfProducts_sale_high
    FROM fe_dwd.dwd_order_item_refund_day a1
    JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.product_id
    JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a1.SHELF_ID
    LEFT JOIN fe_dm.dm_ma_HighProfit_list_monthly a4 ON a4.sdate=DATE_SUB(@smonth,INTERVAL 1 MONTH) AND a4.business_area=a3.business_name AND a4.product_id=a1.PRODUCT_ID
    WHERE a1.PAY_DATE >= @smonth AND  a1.PAY_DATE <DATE_ADD(@smonth,INTERVAL 1 MONTH)
    GROUP BY a3.business_name,a2.second_type_name ;
#插入数据
INSERT INTO fe_dm.dm_ma_sectype_kpi_monthly
    (sdate, business_area, sec_type_name
    , stocks, sale_num, GMV, amount, orders, DISCOUNT_AMOUNT, COUPON_AMOUNT
    , sale_num_high, GMV_high, amount_high, orders_high)
SELECT @smonth, business_area, sec_type_name
    , MAX(stocks) stocks,SUM(sale_num) sale_num,SUM(GMV) GMV,SUM(amount) amount,SUM(orders) orders,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,SUM(COUPON_AMOUNT) COUPON_AMOUNT
    ,SUM(sale_num_high) sale_num_high,SUM(GMV_high) GMV_high,SUM(amount_high) amount_high,SUM(orders_high) orders_high
FROM fe_dm.dm_ma_sectype_kpi_daily t1
WHERE sdate>=@smonth AND sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH)
GROUP BY business_area,sec_type_name
;   #更新SKU用户数等
UPDATE fe_dm.dm_ma_sectype_kpi_monthly a1
JOIN fe_dm.temp_order_info a2 ON a2.business_name=a1.business_area AND a2.second_type_name=a1.sec_type_name
SET a1.skus_sale=a2.skus_sale, a1.ShelfProducts_sale=a2.ShelfProducts_sale,a1.users=a2.users
  , a1.skus_sale_high=a2.skus_sale_high, a1.ShelfProducts_sale_high=a2.ShelfProducts_sale_high, a1.users_high=a2.users_high
WHERE a1.sdate=@smonth;
# 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_sectype_kpi_monthly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END