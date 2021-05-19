CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_product_monitor`()
BEGIN
SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
TRUNCATE fe_dm.dm_ma_shelf_product_monitor;
# 创建临时表
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_shelf_fill;
CREATE TEMPORARY TABLE fe_dm.temp_shelf_fill(INDEX(SHELF_ID)) AS
SELECT SHELF_ID,MAX(FILL_TIME) max_FILL_TIME
    FROM fe_dwd.dwd_fill_day_inc_recent_two_month
    WHERE FILL_TIME>=SUBDATE(CURDATE(),15)
    GROUP BY SHELF_ID;
DROP TEMPORARY TABLE IF EXISTS  fe_dm.tmp_shelf_product_sale;
CREATE TEMPORARY TABLE fe_dm.tmp_shelf_product_sale(INDEX(shelf_id,product_id)) AS
    SELECT a1.shelf_id,a1.product_id,a2.PRODUCT_NAME
        ,SUM(IF(a1.PAY_DATE>=SUBDATE(CURDATE(),7),quantity_act,0)) day7_salenum
        ,SUM(quantity_act) day30_salenum
        ,COUNT(DISTINCT ORDER_ID ) day30_ordernum
    FROM fe_dwd.dwd_pub_order_item_recent_one_month a1
    JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.product_id
    WHERE a1.PAY_DATE>=SUBDATE(CURDATE(),30) AND a1.PAY_DATE<CURDATE()
        AND a1.quantity_act>0
    GROUP BY a1.shelf_id, a1.product_id
;
#插入数据
INSERT INTO fe_dm.dm_ma_shelf_product_monitor
    (SHELF_ID, PRODUCT_ID, PRODUCT_NAME, max_FILL_TIME, day7_salenum,day30_salenum, order_num_30day, STOCK_QUANTITY)
SELECT
    t3.SHELF_ID,t3.PRODUCT_ID,t3.PRODUCT_NAME
    ,t2.max_FILL_TIME
    ,t3.day7_salenum,t3.day30_salenum,t3.day30_ordernum
    ,t4.STOCK_QUANTITY stock_product
FROM fe_dwd.dwd_shelf_base_day_all t1
JOIN fe_dm.temp_shelf_fill t2 ON t2.SHELF_ID=t1.SHELF_ID
JOIN fe_dm.tmp_shelf_product_sale t3 ON t3.SHELF_ID=t1.SHELF_ID AND IFNULL(t3.day7_salenum,0)<=2 AND  day30_ordernum BETWEEN 12 AND 21
JOIN fe_dwd.dwd_shelf_product_day_all t4 ON t4.SHELF_ID=t1.SHELF_ID AND t4.PRODUCT_ID=t3.product_id
WHERE t1.SHELF_TYPE IN (1,2,3,5) AND t1.SHELF_STATUS=2 AND t1.REVOKE_STATUS=1
;
UPDATE fe_dm.dm_ma_shelf_product_monitor a1
JOIN
    (SELECT shelf_id,SUM(STOCK_QUANTITY) shelf_STOCK_QUANTITY FROM fe_dwd.dwd_shelf_product_day_all GROUP BY SHELF_ID) a2
        ON a2.SHELF_ID=a1.SHELF_ID
SET a1.shelf_STOCK_QUANTITY=a2.shelf_STOCK_QUANTITY
;
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_product_monitor',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_product_monitor','dm_ma_shelf_product_monitor','纪伟铨');
END