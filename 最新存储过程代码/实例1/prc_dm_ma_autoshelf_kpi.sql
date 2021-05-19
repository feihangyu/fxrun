CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_autoshelf_kpi`(IN p_date DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @pdate=p_date;
SET @curmonday:= ADDDATE(@pdate, -IF(DAYOFWEEK(@pdate)=1,8,DAYOFWEEK(@pdate))+2 );

# 删除历史数据
DELETE  FROM fe_dm.dm_ma_autoshelf_kpi_daily WHERE sdate>=SUBDATE(@pdate,2)  AND sdate<ADDDATE(@pdate,1) OR sdate<SUBDATE(@curmonday,2*7);
DELETE  FROM fe_dm.dm_ma_AutoShelf_SalesFlag_kpi_daily WHERE sdate>=SUBDATE(@pdate,2)  AND sdate<ADDDATE(@pdate,1) OR sdate<SUBDATE(@curmonday,2*7);
#插入数据
DROP TEMPORARY TABLE IF EXISTS  test.temp_order_distinct; #5分钟内的订单算为一单
CREATE TEMPORARY TABLE test.temp_order_distinct (INDEX (sdate,SHELF_ID)) AS
    SELECT sdate,SHELF_ID,COUNT(1) order_num_distinct
    FROM
        (SELECT DISTINCT sdate,SHELF_ID,USER_ID,rank_num
        FROM
             (SELECT ORDER_ID,SHELF_ID, USER_ID, sdate,ORDER_DATE
                 ,CASE WHEN @rank_user_id=USER_ID AND ORDER_DATE>ADDTIME(@rank_datetime,'00:05:00') THEN @rank_num :=@rank_num+1 ELSE @rank_num:=1 END  rank_num
                 ,@rank_datetime :=ORDER_DATE rank_datetime
                 ,@rank_user_id :=USER_ID rank_user_id
            FROM
                (SELECT DISTINCT a1.ORDER_ID,a1.USER_ID,a1.SHELF_ID,DATE(a1.PAY_DATE) sdate,a1.ORDER_DATE
                FROM fe_dwd.dwd_order_item_refund_day a1
                WHERE a1.PAY_DATE>=SUBDATE(@pdate,2) AND a1.PAY_DATE<ADDDATE(@pdate,1)
                ORDER BY a1.USER_ID,a1.ORDER_DATE
                 ) a
            ) b
        ) c
    GROUP BY sdate,SHELF_ID
    ;
DROP TEMPORARY TABLE IF EXISTS  test.temp_shelf_info; #公司所在地方的商业特性
CREATE TEMPORARY TABLE test.temp_shelf_info (INDEX (SHELF_ID)) AS
    SELECT a4.SHELF_ID,t2.ITEM_NAME business_characteristics
    FROM fe.sf_machines_apply_record t
    JOIN fe.sf_machines_apply_operation ao
        ON t.record_id = ao.machine_apply_record_id AND ao.data_flag = 1 AND ao.operation_detail LIKE '货架ID%' AND ao.`operation_item` = '创建货架'
    JOIN feods.fjr_number t1
        ON t1.number > 0 AND t1.number <= LENGTH(ao.operation_detail) - LENGTH(REPLACE(ao.operation_detail, ',', ''))
    JOIN fe.pub_dictionary_item t2 ON t2.ITEM_VALUE=business_characteristics AND t2.DICTIONARY_ID=120
    JOIN fe.sf_shelf_machine a4  ON a4.SHELF_ID=SUBSTRING_INDEX(SUBSTRING_INDEX(ao.operation_detail,',',t1.number),'ID',-1) AND a4.machine_type_id=1
    ;
 # 货架商品销售标识
DROP TEMPORARY TABLE IF EXISTS test.temp_shelf_product_flag;
CREATE TEMPORARY TABLE test.temp_shelf_product_flag(INDEX(shelf_id,product_id)) AS
    SELECT a1.sdate,a1.shelf_id,a1.product_id,a1.sales_flag FROM fe_dwd.dwd_shelf_product_day_all_recent_32 a1 JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id AND shelf_type=7
    WHERE a1.sdate>=SUBDATE(@pdate,2) AND a1.sdate<ADDDATE(@pdate,1)
;
DROP TEMPORARY TABLE IF EXISTS test.temp_shelf_product_flag2;
CREATE TEMPORARY TABLE test.temp_shelf_product_flag2(INDEX(shelf_id,product_id)) AS
    SELECT * FROM test.temp_shelf_product_flag;
    #货架日报
INSERT INTO fe_dm.dm_ma_autoshelf_kpi_daily
    (sdate, shelf_id, shelf_code, business_characteristics, gmv, amount, user_num, order_num, order_num_real, stock_quantity)
SELECT a1.sdate,a1.shelf_id
     ,a2.SHELF_CODE,a6.business_characteristics,a1.gmv gmv,o_product_total_amount amount,a1.users,a1.orders,a5.order_num_distinct,a3.stock_quantity
FROM feods.fjr_shelf_dgmv a1
JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
JOIN fe.sf_shelf_machine a22 ON a22.shelf_id=a1.SHELF_ID AND a22.machine_type_id=1
LEFT JOIN feods.fjr_flag5_shelf a3 ON a3.sdate=a1.sdate AND a3.shelf_id=a1.SHELF_ID
LEFT JOIN test.temp_order_distinct a5 ON a5.sdate=a1.sdate AND a5.SHELF_ID=a1.SHELF_ID
LEFT JOIN test.temp_shelf_info a6 ON a6.SHELF_ID=a1.SHELF_ID
WHERE a1.sdate>=SUBDATE(@pdate,2) AND a1.sdate<ADDDATE(@pdate,1)
;
    #货架销售等级日报
INSERT INTO fe_dm.dm_ma_AutoShelf_SalesFlag_kpi_daily
    (sdate, shelf_id, sales_flag, shelf_code, business_characteristics, gmv, sale_quantity, order_num, stock_quantity, sale_sku, stock_sku)
SELECT
    t1.sdate,t1.SHELF_ID,t1.sales_flag,t3.SHELF_CODE,t2.business_characteristics
    ,SUM(gmv) gmv,SUM(sale_quantity) sale_quantity,SUM(order_num) order_num,SUM(stock_quantity) stock_quantity,SUM(sale_sku) sale_sku,SUM(stock_sku) stock_sku
FROM
    (SELECT
        t1.sdate,t1.SHELF_ID,t1.sales_flag
        ,SUM(gmv) gmv,SUM(quantity_shipped) sale_quantity ,COUNT(DISTINCT IF(quantity_shipped>0,ORDER_ID,NULL)) order_num,COUNT(DISTINCT IF(quantity_shipped>0,ORDER_ID,NULL) ) sale_sku
        ,0 stock_quantity,0 stock_sku
    FROM
        (SELECT #销售信息
            a1.ORDER_ID,DATE(a1.PAY_DATE) sdate,a1.SHELF_ID
            ,a1.PRODUCT_ID,IFNULL(a3.sales_flag,0) sales_flag,a1.SALE_PRICE,a1.quantity_shipped,a1.QUANTITY
            ,a1.SALE_PRICE*a1.quantity_shipped gmv
        FROM fe_dwd.dwd_pub_order_item_recent_one_month a1
        JOIN fe.sf_shelf_machine a22 ON a22.shelf_id=a1.SHELF_ID AND a22.machine_type_id=1
        LEFT JOIN test.temp_shelf_product_flag a3
            ON a3.shelf_id=a1.SHELF_ID AND a3.product_id=a1.PRODUCT_ID AND a3.sdate=DATE(a1.PAY_DATE)
        WHERE a1.PAY_DATE>=SUBDATE(@pdate,2) AND  a1.PAY_DATE<ADDDATE(@pdate,1) AND a1.ORDER_STATUS IN(6,7)
        ) t1
    GROUP BY t1.sdate, t1.SHELF_ID,t1.sales_flag
    UNION  ALL
    SELECT #库存信息
        a1.sdate, a1.shelf_id,IFNULL(a2.SALES_FLAG,0) SALES_FLAG
         ,0 gmv,0 quantity ,0 order_num,0 sku_sale
         ,SUM(stock_num) stock_quantity,COUNT(DISTINCT a1.product_id) stock_sku
    FROM feods.d_op_slot_his a1
    JOIN fe.sf_shelf_machine a22 ON a22.shelf_id=a1.SHELF_ID AND a22.machine_type_id=1
    LEFT JOIN test.temp_shelf_product_flag2 a2
        ON a2.shelf_id=a1.SHELF_ID AND a1.product_id=a2.PRODUCT_ID
               AND a2.sdate=a1.sdate
    WHERE a1.sdate>=SUBDATE(@pdate,2) AND a1.sdate<ADDDATE(@pdate,1)  AND stock_num>0
    GROUP BY a1.sdate,a1.shelf_id,a2.SALES_FLAG
    )t1
LEFT JOIN test.temp_shelf_info t2 ON t2.SHELF_ID=t1.SHELF_ID
LEFT JOIN fe_dwd.dwd_shelf_base_day_all t3 ON t3.shelf_id=t1.shelf_id
GROUP BY sdate,SHELF_ID,sales_flag ;


-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_autoshelf_kpi',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END