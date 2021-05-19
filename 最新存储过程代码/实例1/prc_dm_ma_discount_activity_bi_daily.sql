CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_discount_activity_bi_daily`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @date0=p_sdate;
SET @date1=ADDDATE(@date0,1);
#删除数据
DELETE FROM feods.dm_ma_discount_activity_bi_daily WHERE sdate=@date0 OR sdate<SUBDATE(@date0,92);
#插入数据
DROP TEMPORARY TABLE IF EXISTS feods.temp_stock1; #库存
CREATE TEMPORARY TABLE feods.temp_stock1(INDEX(shelf_id,product_id)) AS
    SELECT shelf_id,product_id,sales_flag,stock_quantity,sale_price FROM fe_dwd.dwd_shelf_product_day_all_recent_32
    WHERE sdate=@date0
;
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_product_configure; #配置货架商品
CREATE TEMPORARY TABLE  feods.temp_shelf_product_configure(INDEX(shelf_id,product_id)) AS
SELECT shelf_id,product_id,activity_id
FROM
    (SELECT shelf_id,product_id,a1.activity_id,a2.start_time
    FROM fe.sf_product_activity a2
    JOIN fe.sf_product_activity_item a1 ON a1.activity_id=a2.activity_id
    WHERE a1.add_time>=@date0 AND a1.add_time<@date1
    ORDER BY shelf_id,product_id,a2.start_time) a1
GROUP BY shelf_id,product_id
;
DROP TEMPORARY TABLE IF EXISTS feods.temp_stock; #当日库存
CREATE TEMPORARY TABLE feods.temp_stock(INDEX(CITY_NAME,activity_id,sales_flag)) AS
    SELECT a2.CITY_NAME ,a1.activity_id,IFNULL(a3.sales_flag,0) sales_flag
        ,COUNT(1) shelf_product_configure,SUM(stock_quantity) stock,SUM(stock_quantity*sale_price) stock_value
    FROM feods.temp_shelf_product_configure a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
    JOIN feods.temp_stock1 a3 ON a3.shelf_id=a1.shelf_id AND a3.product_id=a1.product_id
    GROUP BY a2.CITY_NAME,a1.activity_id,a3.sales_flag;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_activity_order; #活动订单商品
CREATE TEMPORARY TABLE fe_dm.temp_activity_order(INDEX(ORDER_ID,product_id),INDEX(SHELF_ID,product_id))  AS # 当日优惠订单明细
    SELECT a1.ACTIVITY_ID,a1.ORDER_ID,a1.SHELF_ID,CONVERT(a1.GOODS_ID ,UNSIGNED ) product_id
         ,COMBINED_PRICE,1 sense
    FROM fe.sf_order_activity a1
    JOIN fe.sf_product_activity a2 ON a2.activity_id=a1.ACTIVITY_ID AND a2.business_type<>4
    WHERE a1.PAY_DATE>=@date0 AND a1.PAY_DATE<@date1
        AND a1.ORDER_STATUS=2
    UNION ALL
    SELECT a1.ACTIVITY_ID,a1.ORDER_ID,a1.SHELF_ID,SUBSTRING_INDEX(SUBSTRING_INDEX(a1.`GOODS_ID`, ',', a3.`number` + 1),',', - 1) AS product_id
         ,0 COMBINED_PRICE,2 sense
    FROM fe.sf_order_activity a1
    JOIN fe.sf_product_activity a2 ON a2.activity_id=a1.ACTIVITY_ID AND a2.business_type=4
    JOIN feods.fjr_number a3 ON a3.number BETWEEN 1 AND LENGTH(a1.`GOODS_ID`) - LENGTH(REPLACE(a1.`GOODS_ID`, ',', ''))
    WHERE a1.PAY_DATE>=@date0 AND a1.PAY_DATE<@date1
        AND a1.ORDER_STATUS=2
    ;
DROP TEMPORARY TABLE IF EXISTS feods.temp_sale; #订单数据
CREATE TEMPORARY TABLE feods.temp_sale(INDEX(CITY_NAME,activity_id,sales_flag),INDEX(ACTIVITY_ID))  AS # 当日优惠订单明细
    SELECT a4.CITY_NAME,ACTIVITY_ID,IFNULL(a3.sales_flag,0) sales_flag
         ,SUM(IF(a1.sense=1,COMBINED_PRICE,a2.discount_amount))  COMBINED_PRICE,SUM(quantity_act) quantity_act
         ,COUNT(DISTINCT a2.shelf_id,a1.product_id) shelf_product_sale,COUNT(DISTINCT a2.shelf_id) shelfs_sale
    FROM fe_dm.temp_activity_order a1
    JOIN fe_dwd.dwd_pub_order_item_recent_two_month a2 ON a2.order_id=a1.ORDER_ID AND a2.product_id=a1.product_id
    JOIN fe_dwd.dwd_shelf_base_day_all a4 ON a4.shelf_id=a2.shelf_id
    LEFT JOIN feods.temp_stock1 a3 ON a3.SHELF_ID=a1.SHELF_ID AND a3.PRODUCT_ID=a1.product_id
    GROUP BY a4.CITY_NAME,ACTIVITY_ID,a3.sales_flag
;
    /*日期 地区 销售等级	活动ID	活动名称  (仅记录秒杀、折扣促销、降价促销、满件折、满件减、加价购)
    业务类型	优惠方式	满件数量/金额	优惠值	适用sku	限购类型(否,定向货架)	开始时间	结束时间
    生效的配置货架-商品条数(有库存的) 库存金额	有销售货架-商品条数	销量	折扣费用*/
INSERT INTO feods.dm_ma_discount_activity_bi_daily
    (sdate, city_name, sales_flag, activity_id, activity_name,cost_dept
    , business_type, discount_type, discount_value, full_piece_number, sku_type, restrictions_type, start_time, end_time
    , shelf_product_configure,stock, stock_value, COMBINED_PRICE, quantity_act, shelf_product_sale,shelfs_sale)
SELECT @date0 sdate, t3.city_name, t3.sales_flag, t3.activity_id, t1.activity_name,cost_dept
    ,t1.business_type, discount_type, discount_value, full_piece_number, sku_type, restrictions_type, start_time, end_time
    ,shelf_product_configure,stock, stock_value, COMBINED_PRICE, quantity_act, shelf_product_sale,shelfs_sale
FROM feods.temp_sale t3
LEFT JOIN feods.temp_stock t2 ON  t2.CITY_NAME=t3.CITY_NAME AND t2.ACTIVITY_ID=t3.activity_id AND t2.sales_flag=t3.sales_flag
LEFT JOIN
    (SELECT a1.activity_id,a1.activity_name
        ,CASE a1.business_type WHEN  1 THEN'秒杀' WHEN 2 THEN '折扣促销'  WHEN 3 THEN '降价促销' WHEN 4 THEN '组合销售'
            WHEN 5 THEN '满件折' WHEN 6 THEN '满件减' WHEN 7 THEN '加价购' ELSE'其他' END business_type
        ,CASE a1.discount_type WHEN 1 THEN'打折' WHEN 2 THEN'降价' WHEN 3 THEN'优惠价' END   discount_type
        ,IFNULL(MAX(a2.discount_price),a1.discount_value ) discount_value ,MIN(a2.full_piece_number) full_piece_number
        ,CASE a1.sku_type WHEN 1 THEN'单商品' WHEN 2 THEN'多商品' WHEN 3 THEN'整单' END sku_type
        ,CASE a1.restrictions_type WHEN 1 THEN '人/活动期间' WHEN 2 THEN '人/天' ELSE IF(a3.product_scope_id IS NOT NULL,'定向货架商品',NULL) END restrictions_type
        ,CASE a1.cost_dept WHEN 1 THEN '市场组' WHEN 2 THEN '运营组' WHEN 3 THEN '采购组' WHEN 4 THEN '大客户组'
            WHEN 5 THEN 'BD组' WHEN 6 THEN '经规组' ELSE '其他' END cost_dept
        ,a1.start_time,a1.end_time
    FROM fe.sf_product_activity a1
    LEFT JOIN fe.sf_product_activity_full_piece a2 ON a2.activity_id=a1.activity_id AND a2.data_flag=1
    LEFT JOIN fe.sf_product_activity_scope a3 ON a3.activity_id=a1.activity_id
    WHERE a1.start_time<@date1 AND a1.end_time>@date0
        AND a1.data_flag=1
    GROUP BY a1.activity_id
    ) t1 ON t1.activity_id=t3.ACTIVITY_ID
;
#插入餐卡支付优惠,招行满减活动
INSERT INTO feods.dm_ma_discount_activity_bi_daily
    (sdate, city_name, sales_flag, activity_id
    , activity_name,cost_dept
    , COMBINED_PRICE, quantity_act, shelf_product_sale,shelfs_sale)
SELECT @date0 sdate,CITY_NAME,0 sales_flag,activity_id
     ,IFNULL(activity_name,'餐卡支付优惠') activity_name
     ,CASE cost_dept WHEN 1 THEN '市场组' WHEN 2 THEN '运营组' WHEN 3 THEN '采购组' WHEN 4 THEN '大客户组'
            WHEN 5 THEN 'BD组' WHEN 6 THEN '经规组' ELSE '其他' END cost_dept
    ,SUM(COMBINED_PRICE) COMBINED_PRICE,SUM(quantity_act) quantity_act
     ,SUM(shelf_product_sale) shelf_product_sale,COUNT(DISTINCT shelf_id)shelfs_sale
FROM
    (SELECT a1.ORDER_ID,a2.shelf_id,a4.CITY_NAME,IFNULL(a11.cost_dept,1) cost_dept,a11.activity_name,IFNULL(a1.ACTIVITY_ID,0) ACTIVITY_ID
        ,a1.COMBINED_PRICE,SUM(quantity_act) quantity_act
        ,COUNT(DISTINCT a2.shelf_id,a2.product_id) shelf_product_sale
    FROM fe.sf_order_activity a1
    LEFT JOIN fe.sf_product_activity a11 ON a11.activity_id=a1.ACTIVITY_ID
    JOIN fe_dwd.dwd_pub_order_item_recent_two_month a2 ON a2.order_id=a1.ORDER_ID
    JOIN fe_dwd.dwd_shelf_base_day_all a4 ON a4.shelf_id=a2.shelf_id
    WHERE a1.PAY_DATE>=@date0 AND a1.PAY_DATE<@date1
      AND (a1.ACTIVITY_TYPE=6 OR a11.activity_id IS NULL ) #6是实收满减 没有活动ID是餐卡
      AND a1.DATA_FLAG=1
    GROUP BY a1.ORDER_ID
    ) t
GROUP BY CITY_NAME,ACTIVITY_ID;
#记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_discount_activity_bi_daily',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END