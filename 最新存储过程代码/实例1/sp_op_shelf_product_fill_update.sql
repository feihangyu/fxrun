CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_shelf_product_fill_update`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @cdate := CURDATE();
SET @day_num := DAYOFWEEK(@cdate);
SET @ydate := SUBDATE(@cdate,1),
@month_num := MONTH(@cdate),
@pre_2month_sunday := SUBDATE(SUBDATE(@cdate,INTERVAL 2 MONTH),INTERVAL WEEKDAY(SUBDATE(@cdate,INTERVAL 2 MONTH)) + 1 DAY),
@pre_week := SUBDATE(@cdate,INTERVAL 1 WEEK),
@pre_day30 := SUBDATE(@cdate,INTERVAL 30 DAY),
@last_month_1 := SUBDATE(SUBDATE(@cdate, DAY(@cdate) - 1), INTERVAL 1 MONTH);
SET @last_month := DATE_FORMAT(DATE_SUB(@cdate, INTERVAL DAY(@cdate) DAY),'%Y-%m'); 
-- SET @last_month := '2019-12';
SET @cdate_num := WEEKDAY(@cdate) + 1;       -- 本周第几天
SET @cmonth := DATE_FORMAT(@cdate,'%Y%m');
SET @week_num := WEEK(SUBDATE(@cdate ,1));
SET @month_num := MONTH(@cdate);
SET @last_week_1 := SUBDATE(SUBDATE(@cdate,INTERVAL 1 WEEK),INTERVAL DAYOFWEEK(SUBDATE(@cdate,INTERVAL 1 WEEK))-2 DAY);
SET @last_week_7 := SUBDATE(@cdate,INTERVAL DAYOFWEEK(@cdate)-1 DAY);
SET @cmonth_1 := SUBDATE(@cdate,INTERVAL DAY(@cdate)-1 DAY);
-- 标配取MAX_QUANTITY
-- 补货逻辑口径 1min35s  156万数据量
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        f.business_name,
        a.shelf_id,
        a.product_id,
        e.SALES_FLAG,
        a.STOCK_QUANTITY,
        c.FILL_MODEL,
        a.DETAIL_ID,
        a.ITEM_ID,
        b.shelf_type,
        IFNULL(g.grade,'新装') AS shelf_level,
        a.SALE_PRICE,
        e.NEW_FLAG,
        IFNULL(a.MAX_QUANTITY,0) AS ALARM_QUANTITY,
        a.SHELF_FILL_FLAG,
        d.PRODUCT_TYPE,
        c.PRODUCT_CODE2,
        c.SECOND_TYPE_ID,
        c.product_name,
        c.TYPE_ID,
        c.fill_box_gauge
FROM
        fe.`sf_shelf_product_detail` a
        STRAIGHT_JOIN fe.`sf_shelf` b
                ON a.shelf_id = b.shelf_id
                AND a.data_flag = 1
                AND b.data_flag = 1
        STRAIGHT_JOIN feods.`fjr_city_business` f
                ON b.city = f.city
        STRAIGHT_JOIN fe.`sf_product` c
                ON a.product_id = c.product_id
                AND c.data_flag = 1
        LEFT JOIN feods.`zs_product_dim_sserp` d
                ON f.business_name = d.business_area
                AND a.product_id = d.PRODUCT_ID
        JOIN fe.`sf_shelf_product_detail_flag` e
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id
                AND e.data_flag = 1
        LEFT JOIN feods.`d_op_shelf_grade` g
                ON a.shelf_id = g.shelf_id
                AND g.month_id = @last_month
WHERE a.SHELF_FILL_FLAG = 1
        AND b.SHELF_STATUS = 2
        AND b.WHETHER_CLOSE = 2
        AND b.REVOKE_STATUS = 1
        AND b.shelf_type IN (1,2,3,5,6,7)
        AND c.product_code2 NOT LIKE 'ZC%' 
        AND (d.PRODUCT_TYPE IN ('原有','新增（试运行）','淘汰（替补）','预淘汰','新增（免费货）') OR d.PRODUCT_TYPE IS NULL)
;

DROP TEMPORARY TABLE IF EXISTS feods.`shelf_tmp`;
CREATE TEMPORARY TABLE feods.shelf_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        DISTINCT 
        shelf_id,
        business_name,
        shelf_type,
        shelf_level
FROM
        feods.shelf_product_tmp
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_1--@time_2",@time_1,@time_2);
-- 疑似虚假库存24s
-- 前两月销售等级为平销以上，当前严重滞销，补货规格为1库存小于2或补货规格大于1库存小于等于3，则该商品当前库存定义为虚假库存，体现虚假库存量，非虚假库存则为0；
DROP TEMPORARY TABLE IF EXISTS feods.`suspect_false_stock`;
CREATE TEMPORARY TABLE feods.suspect_false_stock(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        a.shelf_id,
        a.product_id,
        IFNULL(a.STOCK_QUANTITY,0) AS suspect_false_stock_qty
FROM
        feods.shelf_product_tmp a
        JOIN fe.`sf_shelf_product_weeksales_detail` b
                ON a.SALES_FLAG = 5
                AND b.sales_flag IN (1,2,3)
                AND a.STOCK_QUANTITY > 0
                AND ((a.FILL_MODEL = 1 AND a.STOCK_QUANTITY < 2) OR (a.FILL_MODEL > 1 AND a.STOCK_QUANTITY <= 3))
                AND b.stat_date = @pre_2month_sunday
                AND a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_2--@time_3",@time_2,@time_3);
-- 季节性因子 4s
DROP TEMPORARY TABLE IF EXISTS feods.`season_factor_tmp`;
CREATE TEMPORARY TABLE feods.season_factor_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        b.SECOND_TYPE_ID,
        CASE
                WHEN @month_num = 1 THEN month1_factor
                WHEN @month_num = 2 THEN month2_factor
                WHEN @month_num = 3 THEN month3_factor
                WHEN @month_num = 4 THEN month4_factor
                WHEN @month_num = 5 THEN month5_factor
                WHEN @month_num = 6 THEN month6_factor
                WHEN @month_num = 7 THEN month7_factor
                WHEN @month_num = 8 THEN month8_factor
                WHEN @month_num = 9 THEN month9_factor
                WHEN @month_num = 10 THEN month10_factor
                WHEN @month_num = 11 THEN month11_factor
                WHEN @month_num = 12 THEN month12_factor
        END AS season_factor
FROM
        fe.`sf_shelf` a
        JOIN feods.`fjr_city_business` c
            ON a.city = c.city
            AND a.data_flag = 1
        JOIN feods.`d_op_area_season_factor` b
                ON b.business_name = c.business_name
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_3--@time_4",@time_3,@time_4);
-- 近一周销量 10s
DROP TEMPORARY TABLE IF EXISTS feods.`sale_week_tmp`;
CREATE TEMPORARY TABLE feods.sale_week_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        a.shelf_id,
        b.product_id,
        SUM(QUANTITY) AS WEEK_SALE_NUM
FROM
        fe.`sf_order` a
        JOIN fe.`sf_order_item` b
            ON a.order_id = b.order_id
            AND a.ORDER_STATUS IN (2,6,7)
            AND a.PAY_DATE >= @pre_week
            AND a.data_flag = 1
            AND b.data_flag = 1
GROUP BY a.SHELF_ID,b.PRODUCT_ID
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_4--@time_5",@time_4,@time_5);
-- 在途订单数 2s
DROP TEMPORARY TABLE IF EXISTS feods.`fill_onload_tmp`;
CREATE TEMPORARY TABLE feods.fill_onload_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        a.shelf_id,
        b.product_id,
        a.ORDER_STATUS,
        a.order_id,
        SUM(b.ACTUAL_APPLY_NUM) AS ONWAY_NUM
FROM 
        fe.`sf_product_fill_order` a
        JOIN fe.`sf_product_fill_order_item` b
            ON a.order_id = b.order_id
            AND a.data_flag = 1
            AND b.data_flag = 1
            AND a.ORDER_STATUS IN (1,2,3)
            AND a.APPLY_TIME >= @pre_day30
            AND a.shelf_id IS NOT NULL
GROUP BY a.shelf_id,b.product_id
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_5--@time_6",@time_5,@time_6);
# 前置仓覆盖货架库存 37s
DROP TEMPORARY TABLE IF EXISTS feods.`prewarehouse_shelf_tmp`;
CREATE TEMPORARY TABLE feods.prewarehouse_shelf_tmp(
        KEY idx_warehouse_id(warehouse_id)
) AS 
SELECT
        a.warehouse_id,
        a.shelf_id
FROM
        fe.`sf_prewarehouse_shelf_detail` a
        JOIN feods.shelf_tmp b
                ON a.shelf_id = b.shelf_id
WHERE a.data_flag = 1
;

DROP TEMPORARY TABLE IF EXISTS feods.`prewarehouse_stock_tmp`;
CREATE TEMPORARY TABLE feods.prewarehouse_stock_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        b.product_id,
        b.available_stock
FROM 
        feods.prewarehouse_shelf_tmp a
        JOIN fe.`sf_prewarehouse_stock_detail` b
                ON a.warehouse_id = b.warehouse_id
                AND b.data_flag = 1 
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_6--@time_7",@time_6,@time_7);
# 大仓库存 1s
-- 去重
DROP TEMPORARY TABLE IF EXISTS feods.`warehouse_stock_tmp1`;
CREATE TEMPORARY TABLE feods.warehouse_stock_tmp1(
        KEY idx_business_area_product_bar(BUSINESS_AREA,PRODUCT_BAR)
) AS 
SELECT
        a.BUSINESS_AREA,
        a.PRODUCT_BAR,
        a.QUALITYQTY
FROM 
        feods.`PJ_OUTSTOCK2_DAY` a
        JOIN fe_dwd.`dwd_pub_warehouse_business_area` b
                ON a.WAREHOUSE_NUMBER = b.WAREHOUSE_NUMBER
                AND FPRODUCEDATE = @ydate
                AND b.data_flag = 1
;

DROP TEMPORARY TABLE IF EXISTS feods.`warehouse_stock_tmp`;
CREATE TEMPORARY TABLE feods.warehouse_stock_tmp(
        KEY idx_business_area_product_bar(BUSINESS_AREA,PRODUCT_BAR)
) AS 
SELECT
        BUSINESS_AREA,
        PRODUCT_BAR,
        MAX(QUALITYQTY) AS QUALITYQTY
FROM
        feods.warehouse_stock_tmp1
GROUP BY BUSINESS_AREA,PRODUCT_BAR
;

SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_7--@time_8",@time_7,@time_8);
-- 补货基础信息表 45s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_fill_info`;
CREATE TEMPORARY TABLE feods.shelf_product_fill_info(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.DETAIL_ID,
        a.ITEM_ID,
        a.PRODUCT_ID,
        a.SHELF_ID,
        a.shelf_type,
        a.shelf_level,
        a.SALE_PRICE,
        a.NEW_FLAG,
        a.SALES_FLAG,
        a.FILL_MODEL,
        a.ALARM_QUANTITY,
        a.SHELF_FILL_FLAG,
        IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0) AS STOCK_NUM,
        IFNULL(h.ONWAY_NUM,0) AS ONWAY_NUM,
        g.WEEK_SALE_NUM,
        a.PRODUCT_TYPE,
        IF(i.shelf_id IS NULL,1,2) AS warehouse_type,       -- (1:大仓,2:前置仓)
        IF(i.shelf_id IS NULL,j.QUALITYQTY,i.available_stock) AS warehouse_stock,
        a.SECOND_TYPE_ID,
        a.PRODUCT_CODE2,
        a.product_name,
        a.TYPE_ID,
        a.fill_box_gauge
FROM 
        feods.shelf_product_tmp a
        LEFT JOIN feods.prewarehouse_stock_tmp i
                ON i.shelf_id = a.shelf_id
                AND i.product_id = a.product_id
        LEFT JOIN feods.warehouse_stock_tmp j
                ON j.BUSINESS_AREA = a.business_name
                AND  j.PRODUCT_BAR = a.PRODUCT_CODE2
        LEFT JOIN feods.sale_week_tmp g
                ON a.shelf_id = g.shelf_id
                AND a.product_id = g.product_id
        LEFT JOIN feods.fill_onload_tmp h
                ON a.shelf_id = h.shelf_id
                AND a.product_id = h.product_id
;
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_8--@time_9",@time_8,@time_9);
-- 上周是否有出单 2s
DROP TEMPORARY TABLE IF EXISTS feods.`fill_last_week_tmp`;
CREATE TEMPORARY TABLE feods.fill_last_week_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        DISTINCT
        a.SHELF_ID
FROM
        fe.`sf_product_fill_order` a
        JOIN fe.`sf_product_fill_order_item` b
            ON a.order_id = b.order_id
            AND a.data_flag = 1
            AND b.data_flag = 1
            AND a.APPLY_TIME >= @last_week_1
            AND a.APPLY_TIME <= @last_week_7 
            AND a.FILL_TYPE = 2
            AND a.ORDER_STATUS IN (1,2,3,4)
;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_9--@time_10",@time_9,@time_10);
-- 出单日计数
DROP TEMPORARY TABLE IF EXISTS feods.sf_shelf_fill_day_config_tmp;
CREATE TEMPORARY TABLE feods.sf_shelf_fill_day_config_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        COUNT(*) AS fill_day_cnt
FROM
        fe.`sf_shelf_fill_day_config`
WHERE data_flag = 1
GROUP BY shelf_id
;
-- 补货周期和是否出单日判断 1s
DROP TEMPORARY TABLE IF EXISTS feods.`d_op_manager_fill_day_tmp`;
CREATE TEMPORARY TABLE feods.d_op_manager_fill_day_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        DISTINCT b.shelf_id,
--     1.若甲级货架，只有一个出单日，则补货周期为7天；
--     2.若甲级货架有两个及以上出单日，则补货周期根据配置表结果来；
-- 20200624 葛小冬：因上海目前店主人手不足，所以采用串点补货的方式，串点补货会延迟两天才能上架，所以请协助将补货逻辑中的 “补货周期增加一天”
        IF(d.business_name = '上海区',
        CASE
                WHEN d.business_name = '深圳区' AND d.shelf_level = '甲' 
                        THEN 7
                WHEN d.business_name = '鲁东区' AND d.shelf_level = '甲' AND d.shelf_type IN (1,2,3,6,7)
                        THEN 7
                WHEN d.business_name = '无锡区' AND d.shelf_level = '甲' AND d.shelf_type IN (1,2,3,6)
                        THEN 7
                WHEN d.shelf_level = '甲' AND f.fill_day_cnt = 1
                        THEN 7
                WHEN d.shelf_level = '甲' AND f.fill_day_cnt > 1
                        THEN e.fill_cycle
                ELSE
                        IFNULL(e.fill_cycle,
                                CASE
                                        WHEN d.shelf_level IN ('乙','新装')
                                                THEN 7
                                        WHEN d.shelf_level IN ('丙','丁') AND c.shelf_id IS NOT NULL 
                                                THEN 6
                                        WHEN d.shelf_level = '丙'
                                                THEN 14
                                        WHEN d.shelf_level = '丁'
                                                THEN 22
                                        ELSE 7
                                END)
        END + 1,
        CASE
                WHEN d.business_name = '深圳区' AND d.shelf_level = '甲' 
                        THEN 7
                WHEN d.business_name = '鲁东区' AND d.shelf_level = '甲' AND d.shelf_type IN (1,2,3,6,7)
                        THEN 7
                WHEN d.business_name = '无锡区' AND d.shelf_level = '甲' AND d.shelf_type IN (1,2,3,6)
                        THEN 7
                WHEN d.shelf_level = '甲' AND f.fill_day_cnt = 1
                        THEN 7
                WHEN d.shelf_level = '甲' AND f.fill_day_cnt > 1
                        THEN e.fill_cycle
                ELSE
                        IFNULL(e.fill_cycle,
                                CASE
                                        WHEN d.shelf_level IN ('乙','新装')
                                                THEN 7
                                        WHEN d.shelf_level IN ('丙','丁') AND c.shelf_id IS NOT NULL 
                                                THEN 6
                                        WHEN d.shelf_level = '丙'
                                                THEN 14
                                        WHEN d.shelf_level = '丁'
                                                THEN 22
                                        ELSE 7
                                END)
        END        
        )
         AS fill_cycle,
        b.fill_day_code AS fill_order_day,
        CASE
                WHEN d.business_name = '重庆区' THEN 15
                WHEN d.business_name = '深圳区' THEN 13
                ELSE 10
        END AS manager_push_order_limit
FROM 
        fe.`sf_shelf_fill_day_config` b
        LEFT JOIN feods.fill_last_week_tmp c
                ON b.shelf_id = c.shelf_id
        JOIN feods.shelf_tmp d
                ON  b.shelf_id = d.shelf_id
        LEFT JOIN fe_dm.`dm_op_fill_cycle_list` e
                ON d.business_name = e.business_name
                AND d.shelf_type = e.shelf_type
                AND d.shelf_level = e.grade
        LEFT JOIN feods.sf_shelf_fill_day_config_tmp f
                ON b.shelf_id = f.shelf_id
WHERE b.data_flag = 1
;
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_10--@time_11",@time_10,@time_11);
-- 安全库存 24s
DROP TEMPORARY TABLE IF EXISTS feods.`safe_stock_qty_tmp`;
CREATE TEMPORARY TABLE feods.safe_stock_qty_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        c.shelf_id,
        c.product_id,
        ROUND(
        CASE
                WHEN c.day_sale_qty <= 0.14 THEN 1
                WHEN c.day_sale_qty >= 0.14 AND c.day_sale_qty < 0.43 THEN 2
                WHEN (b.grade IN ('甲','乙','新装') OR b.grade IS NULL) AND c.day_sale_qty >= 0.43 
                        THEN IF(IF(2*c.day_sale_qty > 2,2*c.day_sale_qty,2) < 4,IF(2*c.day_sale_qty > 2,2*c.day_sale_qty,2),4)
                WHEN b.grade IN ('丙','丁') AND c.day_sale_qty >= 0.43 
                        THEN IF(IF(2*c.day_sale_qty > 2,2*c.day_sale_qty,2) < 3,IF(2*c.day_sale_qty > 2,2*c.day_sale_qty,2),3)
                ELSE 0
        END) AS safe_stock_qty
FROM 
        feods.shelf_product_tmp a
        LEFT JOIN feods.`d_op_shelf_grade` b
                ON b.month_id = @last_month
                AND a.shelf_id = b.shelf_id                 
        JOIN feods.`d_op_fill_day_sale_qty` c
                ON a.shelf_id = c.shelf_id
                AND a.product_id = c.product_id
;
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_11--@time_12",@time_11,@time_12);
-- 货架库存上限 1s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_stock_upper_limit_tmp`;
CREATE TEMPORARY TABLE feods.shelf_stock_upper_limit_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        CASE 
                WHEN b.shelf_id IS NOT NULL AND a.shelf_type IN (1,3)
                        THEN b.shelf_stock_upper_limit + 330
                WHEN b.shelf_id IS NOT NULL AND a.shelf_type IN (2,5,6)
                        THEN b.shelf_stock_upper_limit + 220
                WHEN b.shelf_id IS NULL AND a.shelf_type IN (1,3)
                        THEN 330
                WHEN b.shelf_id IS NULL AND a.shelf_type IN (2,5,6)
                        THEN 220
        END AS shelf_stock_upper_limit
FROM
        fe.`sf_shelf` a
        LEFT JOIN 
                (
                        SELECT 
                                b.`MAIN_SHELF_ID` AS shelf_id,
                                SUM(IF (a.shelf_type IN (1, 3), 330, 220)) AS shelf_stock_upper_limit
                        FROM
                                fe.`sf_shelf` a
                                JOIN fe.`sf_shelf_relation_record` b
                                    ON a.shelf_id = b.SECONDARY_SHELF_ID
                                    AND b.SHELF_HANDLE_STATUS = 9
                                    AND a.data_flag = 1
                                    AND b.data_flag = 1
                        GROUP BY b.`MAIN_SHELF_ID`
                ) b
                ON a.shelf_type IN (1,2,3,5,6)
                        AND a.SHELF_STATUS = 2
                        AND a.REVOKE_STATUS = 1
                        AND a.shelf_id = b.shelf_id
                        AND a.data_flag = 1
;
SET @time_13 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_12--@time_13",@time_12,@time_13);
-- 上个补货周期的日均销量(疑似虚假盘点) 1s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_fill_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_fill_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        a.product_id,
        a.day_sale_qty
FROM 
        feods.d_op_shelf_product_fill_update a   
        JOIN feods.suspect_false_stock b
                ON a.product_id = b.product_id 
                AND a.shelf_id = b.shelf_id
;
SET @time_14 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_13--@time_14",@time_13,@time_14);
-- 建议补货量 1min12s
DROP TEMPORARY TABLE IF EXISTS feods.`suggest_fill_tmp`;
CREATE TEMPORARY TABLE feods.suggest_fill_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        DISTINCT 
        a.PRODUCT_ID,
        a.SHELF_ID,
        IFNULL(e.day_sale_qty,0) AS day_sale_qty,
        IFNULL(h.fill_cycle,7) AS fill_cycle,
        IFNULL(m.fill_days,0) AS fill_days,
        IFNULL(n.safe_stock_qty,0) AS safe_stock_qty,
        IFNULL(g.suspect_false_stock_qty,0) AS suspect_false_stock_qty,
--         建议补货量
-- ①A.新品标识为1，销售标识不为空，建议补货量=标配数量-库存-在途
--  B.新品标识为空，销量标识为空，建议补货量=标配数量
-- ③有节假日备货：①②计算的量，再加上节假日备货量 + 虚假库存量
-- ②日均销量*（补货周期+上架周期）+安全库存量-当前库存-在途库存 + 虚假库存量
        FLOOR(IF(
        CASE 
                WHEN a.NEW_FLAG = 1 OR (a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL)
                        THEN a.ALARM_QUANTITY -  a.STOCK_NUM - a.ONWAY_NUM 
                WHEN a.product_type IN ('淘汰（替补）','新增（免费货）') AND a.warehouse_stock = 0
                        THEN 0
                WHEN f.is_holiday_stock_up = 1 
                        THEN a.ALARM_QUANTITY -  a.STOCK_NUM - a.ONWAY_NUM  + IF(g.shelf_id IS NULL,e.day_sale_qty,IFNULL(p.day_sale_qty,0))*(IFNULL(h.fill_cycle,7) + IFNULL(m.fill_days,0)) + IFNULL(n.safe_stock_qty,0)
                        -  a.STOCK_NUM - a.ONWAY_NUM + IF(f.is_holiday_stock_up = 1,DATEDIFF(f.holiday_stop_fill_date,f.holiday_recover_fill_date),0) * IF(g.shelf_id IS NULL,e.day_sale_qty,p.day_sale_qty) * f.holiday_stock_up_ratio
                        + IFNULL(g.suspect_false_stock_qty,0)
                ELSE IF(g.shelf_id IS NULL,e.day_sale_qty,IFNULL(p.day_sale_qty,0))*(IFNULL(h.fill_cycle,7) + IFNULL(m.fill_days,0)) + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM 
                        + IFNULL(g.suspect_false_stock_qty,0)
        END < 0,
        0,
        CASE 
                WHEN a.NEW_FLAG = 1 OR (a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL)
                        THEN a.ALARM_QUANTITY -  a.STOCK_NUM - a.ONWAY_NUM 
                WHEN a.product_type IN ('淘汰（替补）','新增（免费货）') AND a.warehouse_stock = 0 
                        THEN 0
                WHEN f.is_holiday_stock_up = 1 
                        THEN a.ALARM_QUANTITY -  a.STOCK_NUM - a.ONWAY_NUM  + IF(g.shelf_id IS NULL,e.day_sale_qty,IFNULL(p.day_sale_qty,0))*(IFNULL(h.fill_cycle,7) + IFNULL(m.fill_days,0)) + IFNULL(n.safe_stock_qty,0)
                        -  a.STOCK_NUM - a.ONWAY_NUM + IF(f.is_holiday_stock_up = 1,DATEDIFF(f.holiday_stop_fill_date,f.holiday_recover_fill_date),0) * IF(g.shelf_id IS NULL,e.day_sale_qty,p.day_sale_qty) * f.holiday_stock_up_ratio
                        + IFNULL(g.suspect_false_stock_qty,0)
                ELSE IF(g.shelf_id IS NULL,e.day_sale_qty,IFNULL(p.day_sale_qty,0))*(IFNULL(h.fill_cycle,7) + IFNULL(m.fill_days,0)) + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM 
                        + IFNULL(g.suspect_false_stock_qty,0)
        END
        )) AS SUGGEST_FILL_NUM   -- 建议补货量
FROM 
        feods.shelf_product_fill_info a
        JOIN feods.`d_op_fill_day_sale_qty` e   
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id 
        LEFT JOIN feods.d_op_holiday_stock_up_info f  
                ON a.shelf_id = f.shelf_id
        LEFT JOIN feods.suspect_false_stock g
                ON a.shelf_id = g.shelf_id
                AND a.product_id = g.product_id 
        LEFT JOIN feods.`d_op_manager_fill_day_tmp` h
                ON a.shelf_id = h.shelf_id
        LEFT JOIN feods.`d_op_cal_fill_days` m
                ON a.shelf_id = m.shelf_id
        LEFT JOIN feods.safe_stock_qty_tmp n   
                ON a.shelf_id = n.shelf_id
                AND a.product_id = n.product_id 
        LEFT JOIN feods.shelf_product_fill_tmp p
                ON a.shelf_id = p.shelf_id
                AND a.product_id = p.product_id 
;
SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_14--@time_15",@time_14,@time_15);
-- 当天建议补货总量 9s 
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_total_tmp`;
CREATE TEMPORARY TABLE feods.shelf_total_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        SUM(q.SUGGEST_FILL_NUM) AS total_suggest_fill_num, -- 当天建议补货总量
        SUM(a.STOCK_NUM) AS total_stock_qty,    -- 现有库存
        SUM(a.ONWAY_NUM) AS total_onway_qty     -- 在途
FROM
        feods.shelf_product_fill_info a
        JOIN feods.suggest_fill_tmp q
                ON a.shelf_id = q.shelf_id
                AND a.product_id = q.product_id 
GROUP BY a.`SHELF_ID`
;
SET @time_16 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_15--@time_16",@time_15,@time_16);
-- 压缩后建议补货量 18s
-- 新品未上架统一取标配（即建议补货量）
--         ①若货架建议补货总量+货架库存+在途<=货架上限，则单品压缩后建议补货量等于建议补货量；
--         ②若货架建议补货总量+货架库存+在途>货架上限;按以下规则计算：
--         A（单品库存+在途）/日均销<3，压缩建议量=日均销*3+安全库存-库存-在途+疑似虚假库存；
--         B.若（单品库存+在途）/日均销>=3,则压缩补货量为0
DROP TEMPORARY TABLE IF EXISTS feods.`reduce_suggest_fill_num_tmp`;
CREATE TEMPORARY TABLE feods.reduce_suggest_fill_num_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        a.shelf_id,
        a.product_id,
        FLOOR(CASE 
                WHEN a.NEW_FLAG = 1 OR a.NEW_FLAG IS NULL  
                        THEN q.SUGGEST_FILL_NUM 
                WHEN k.total_stock_qty + k.total_onway_qty > o.shelf_stock_upper_limit AND (a.STOCK_NUM + a.ONWAY_NUM) < 7 * IF(q.day_sale_qty = 0,0.01,q.day_sale_qty)
                        THEN q.day_sale_qty * 7 + q.safe_stock_qty - a.STOCK_NUM - a.ONWAY_NUM + q.suspect_false_stock_qty
                WHEN k.total_stock_qty + k.total_onway_qty > o.shelf_stock_upper_limit AND (a.STOCK_NUM + a.ONWAY_NUM) >= 7 * q.day_sale_qty
                        THEN 0
                WHEN k.total_stock_qty + k.total_onway_qty <= o.shelf_stock_upper_limit AND k.total_suggest_fill_num + k.total_stock_qty + k.total_onway_qty <= o.shelf_stock_upper_limit
                        THEN q.SUGGEST_FILL_NUM
                WHEN k.total_stock_qty + k.total_onway_qty <= o.shelf_stock_upper_limit AND k.total_suggest_fill_num + k.total_stock_qty + k.total_onway_qty > o.shelf_stock_upper_limit
                        THEN ((o.shelf_stock_upper_limit - k.total_stock_qty - k.total_onway_qty) / k.total_suggest_fill_num) * q.SUGGEST_FILL_NUM
        END) AS reduce_suggest_fill_num
FROM 
        feods.shelf_product_fill_info a
        JOIN feods.shelf_total_tmp k  
                ON a.shelf_id = k.shelf_id
        LEFT JOIN feods.shelf_stock_upper_limit_tmp o
                ON a.shelf_id = o.shelf_id
        JOIN feods.suggest_fill_tmp q
                ON a.shelf_id = q.shelf_id
                AND a.product_id = q.product_id
;
SET @time_17 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_16--@time_17",@time_16,@time_17);
-- 补货逻辑 1min19s
--         压缩后建议补货量取整
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_fill_update_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_fill_update_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        @cdate AS cdate,
        a.DETAIL_ID,
        a.ITEM_ID,
        a.PRODUCT_ID,
        a.product_name,
        a.PRODUCT_CODE2 AS product_fe,
        a.TYPE_ID,
        a.SHELF_ID,
        a.`SHELF_TYPE`,
        a.`shelf_level`,
        a.SALE_PRICE,
        a.NEW_FLAG,
        a.SALES_FLAG,
        a.FILL_MODEL,
        a.fill_box_gauge,
        a.ALARM_QUANTITY,       -- 标配数量
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        a.ONWAY_NUM,
        a.WEEK_SALE_NUM,     -- 近一周销量
        a.PRODUCT_TYPE,
        a.warehouse_type,
        a.warehouse_stock,
        h.fill_order_day,       -- 补货出单日
        q.fill_cycle,              -- 补货周期
        q.fill_days,             -- 上架周期
        f.is_holiday_stock_up,  -- 节假日是否备货
        IF(f.is_holiday_stock_up = 1,DATEDIFF(f.holiday_stop_fill_date,f.holiday_recover_fill_date),0) * q.day_sale_qty * f.holiday_stock_up_ratio AS holiday_stock_up_qty,-- 节假日备货量
        f.holiday_stock_up_ratio,       -- 节假日备货系数
        IF(f.is_holiday_stock_up = 1,DATEDIFF(f.holiday_stop_fill_date,f.holiday_recover_fill_date),0) AS holiday_stock_up_cycle,       -- 节假日备货周期
        f.holiday_stock_up_datetime,    -- 节假日备货时间
        f.holiday_stop_fill_date,       -- 节假日停止补货日期
        f.holiday_recover_fill_date,    -- 节假日恢复补货日期
        q.day_sale_qty * i.season_factor AS predict_day_sale_qty,       -- 预测日均销量
        q.day_sale_qty,        --  日均销量
        i.season_factor,        -- 季节性因子
        q.safe_stock_qty,       -- 安全库存量
        o.shelf_stock_upper_limit,      -- 货架库存上限
        k.total_suggest_fill_num + k.total_stock_qty + k.total_onway_qty AS stock_total_qty, -- 到货日货架库存总量
        q.suspect_false_stock_qty,       -- 疑似虚假库存量
        q.SUGGEST_FILL_NUM,    -- 建议补货量
        t.reduce_suggest_fill_num,      --         压缩后建议补货量
        --         压缩后建议补货量取整
--         新品（非盒装）压缩后，新品（非盒装）按照新品标配-库存-在途
-- 盒装商品（包含盒装新品）
        -- 用补货规格判断是否盒装：
        -- 1当前库存+在途大于等于1.5倍盒装规格（补货箱规），不补货
        -- 2.否则判断如下：
        -- ①若当前库存加在途大于0.5盒，小于1.5盒，补货量需求量大于0.5盒，则最多补一盒，否则不补；
        -- ②若当前库存加在途小于等于0.5盒且大于等于0.25盒（向上取整），补货需求大于等于0.3盒，最多补一盒，否则不补；
        -- ③若当前库存加在途小于0.25盒（向上取整）且日均销大于0.14，则补一盒
        -- ④若当前库存加在途小于0.25盒（向上取整）且日均销小于等于0.14且补货规格大于10，则不补；否则补一盒
        
-- 非盒装-水饮（按品类）
        -- ①若当前库存在途大于等于1.5倍箱规，不补货；
        -- ②如果库存加在途大于0.5倍箱规，如果建议补货量除以箱规大于等于0.75，则补一箱；若建议补货量除以箱规大于等于0.4小于等于0.6，则补半箱，小于0.25箱规不补货；其他按建议补货量
        -- ③如果库存加在途小于等于0.5倍箱规，且日均销大于0.14，如果建议补货量除以箱规大于等于0.75，则补一箱；若建议补货量除以箱规大于等于0.4小于0.6，则补半箱，小于0.25箱，补0.25箱；
        -- ④如果库存加在途小于等于0.5倍箱规，且日均销小于等于0.14，如果建议补货量除以箱规大于等于0.75，则补一箱；若建议补货量除以箱规大于等于0.4小于0.6，则补半箱，小于0.25箱，取建议补货量（建议量若为1，则补2个）
-- 非盒装-粥面（按品类）
        -- ①若当前库存+在途大于等于1.5倍箱规，不补货；
        -- ②否则判断：A=min(建议补货量，1.5倍箱规-库存-在途),若A/补货箱规>=0.85（向下取整）,补一箱，否则按A
-- 其他
        -- 按压缩后建议补货量，若超过补货箱规上限，最多补箱规，否则按压缩后建议补货量；
        -- 若最终补货量为1个的，则补2个
        CASE 
--                 区域淘汰替补商品，仓库库存小于单个货架单品压缩建议量，单品压缩取整量为0
                WHEN a.product_type IN ('淘汰（替补）','新增（免费货）') AND (a.warehouse_stock = 0 OR a.warehouse_stock < t.reduce_suggest_fill_num)
                        THEN 0
--                 1. 新品
                WHEN a.NEW_FLAG IS NULL AND a.STOCK_NUM = 0 AND a.ONWAY_NUM = 0 
                        THEN IF(q.SUGGEST_FILL_NUM = 1,2,q.SUGGEST_FILL_NUM)
                WHEN (a.NEW_FLAG IS NULL OR a.NEW_FLAG = 1) AND a.FILL_MODEL = 1
                        THEN IF(q.SUGGEST_FILL_NUM = 1,2,q.SUGGEST_FILL_NUM)
--                 2. 盒装
                WHEN a.FILL_MODEL > 1 AND q.day_sale_qty <= 0.07
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL < 5 AND a.fill_box_gauge > a.FILL_MODEL AND a.STOCK_NUM + a.ONWAY_NUM > 1.5 * a.fill_box_gauge 
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL < 5
                        THEN IFNULL(a.FILL_MODEL,0) * IF(CEILING(t.reduce_suggest_fill_num / IFNULL(a.fill_box_gauge,0)) > 6,6,CEILING(t.reduce_suggest_fill_num / IFNULL(a.fill_box_gauge,0)))
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND a.STOCK_NUM + a.ONWAY_NUM > 2 * a.FILL_MODEL 
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= 0.5 * a.FILL_MODEL AND t.reduce_suggest_fill_num >= 0.25 * a.FILL_MODEL
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= 0.5 * a.FILL_MODEL 
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM < 0.5 * a.FILL_MODEL AND t.reduce_suggest_fill_num > 0 AND t.reduce_suggest_fill_num <= a.FILL_MODEL
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM < 0.5 * a.FILL_MODEL AND t.reduce_suggest_fill_num > a.FILL_MODEL
                         THEN IFNULL(a.FILL_MODEL,0) * FLOOR(t.reduce_suggest_fill_num / IFNULL(a.FILL_MODEL,0))
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= 0.5 * a.FILL_MODEL AND t.reduce_suggest_fill_num > 0.3 * a.FILL_MODEL
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= 0.5 * a.FILL_MODEL AND t.reduce_suggest_fill_num <= 0.3 * a.FILL_MODEL
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL <= 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM < 0.5 * a.FILL_MODEL AND t.reduce_suggest_fill_num > 0
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND a.STOCK_NUM + a.ONWAY_NUM >= 1.5 * a.FILL_MODEL
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= a.FILL_MODEL AND  t.reduce_suggest_fill_num > 0.5 * a.FILL_MODEL 
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= a.FILL_MODEL 
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM < a.FILL_MODEL AND a.STOCK_NUM + a.ONWAY_NUM > 0.25 * a.FILL_MODEL AND  t.reduce_suggest_fill_num > 0.3 * a.FILL_MODEL 
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty >= 0.43 AND a.STOCK_NUM + a.ONWAY_NUM < a.FILL_MODEL AND a.STOCK_NUM + a.ONWAY_NUM <= 0.25 * a.FILL_MODEL  AND t.reduce_suggest_fill_num > 0
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= a.FILL_MODEL
                        THEN 0
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM < 0.25 * a.FILL_MODEL AND t.reduce_suggest_fill_num > 0
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= 0.25 * a.FILL_MODEL AND t.reduce_suggest_fill_num > 0.25 *  a.FILL_MODEL
                        THEN a.FILL_MODEL
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL > 20 AND q.day_sale_qty < 0.43 AND a.STOCK_NUM + a.ONWAY_NUM >= 0.25 * a.FILL_MODEL 
                        THEN 0
--                  3. 非盒装-水饮 / 粥面                
                WHEN a.FILL_MODEL = 1 AND a.STOCK_NUM + a.ONWAY_NUM >= 1.5 * a.fill_box_gauge AND a.TYPE_ID IN (1,3)
                        THEN 0
                WHEN a.FILL_MODEL = 1 AND a.TYPE_ID = 3 AND  t.reduce_suggest_fill_num <= 0
                        THEN 0
                WHEN a.FILL_MODEL = 1 AND a.TYPE_ID = 3 AND t.reduce_suggest_fill_num >= 0.75 * a.fill_box_gauge
                        THEN a.fill_box_gauge
                WHEN a.FILL_MODEL = 1 AND a.TYPE_ID = 3 AND t.reduce_suggest_fill_num >= 0.4 * a.fill_box_gauge AND t.reduce_suggest_fill_num <= 0.6 * a.fill_box_gauge
                        THEN 0.5 * a.fill_box_gauge
                WHEN a.FILL_MODEL = 1 AND a.TYPE_ID = 3 
                        THEN IF(t.reduce_suggest_fill_num = 1,2,t.reduce_suggest_fill_num) 
                WHEN a.FILL_MODEL = 1 AND a.TYPE_ID = 1 AND FLOOR(IF(t.reduce_suggest_fill_num > 1.5 * a.fill_box_gauge - a.STOCK_NUM - a.ONWAY_NUM,1.5 * a.fill_box_gauge - a.STOCK_NUM - a.ONWAY_NUM,t.reduce_suggest_fill_num)) / a.fill_box_gauge >= 0.85
                        THEN a.fill_box_gauge
                WHEN a.FILL_MODEL = 1 AND a.TYPE_ID = 1 AND FLOOR(IF(t.reduce_suggest_fill_num > 1.5 * a.fill_box_gauge - a.STOCK_NUM - a.ONWAY_NUM,1.5 * a.fill_box_gauge - a.STOCK_NUM - a.ONWAY_NUM,t.reduce_suggest_fill_num)) / a.fill_box_gauge < 0.85
                        THEN IF(t.reduce_suggest_fill_num > 1.5 * a.fill_box_gauge - a.STOCK_NUM - a.ONWAY_NUM,1.5 * a.fill_box_gauge - a.STOCK_NUM - a.ONWAY_NUM,IF(t.reduce_suggest_fill_num = 1,2,t.reduce_suggest_fill_num))
--                 4、其它
                WHEN t.reduce_suggest_fill_num > a.fill_box_gauge
                        THEN a.fill_box_gauge
                ELSE IF(t.reduce_suggest_fill_num = 1, 2,t.reduce_suggest_fill_num) 
        END AS reduce_suggest_fill_ceiling_num
FROM 
        feods.shelf_product_fill_info a
        LEFT JOIN feods.d_op_holiday_stock_up_info f  
                ON a.shelf_id = f.shelf_id
        LEFT JOIN feods.`d_op_manager_fill_day_tmp` h
                ON a.shelf_id = h.shelf_id
                AND SUBSTRING(h.fill_order_day,@day_num,1) = 1
        LEFT JOIN feods.season_factor_tmp i 
                ON a.shelf_id = i.shelf_id
                AND a.SECOND_TYPE_ID = i.SECOND_TYPE_ID
        JOIN feods.shelf_total_tmp k  
                ON a.shelf_id = k.shelf_id
        LEFT JOIN feods.shelf_stock_upper_limit_tmp o
                ON a.shelf_id = o.shelf_id
        JOIN feods.suggest_fill_tmp q
                ON a.shelf_id = q.shelf_id
                AND a.product_id = q.product_id 
        JOIN feods.reduce_suggest_fill_num_tmp t
                ON a.shelf_id = t.shelf_id
                AND a.product_id = t.product_id  
WHERE a.shelf_type IN (1,2,3,5)
;
SET @time_18 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_17--@time_18",@time_17,@time_18);
-- ==================================================================================================
-- 当天是否出单
-- 补货总金额 4s
DROP TEMPORARY TABLE IF EXISTS feods.`fill_tmp`;
CREATE TEMPORARY TABLE feods.fill_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        IFNULL(SUM(a.reduce_suggest_fill_ceiling_num * a.SALE_PRICE),0) AS total_fill_value,
        IFNULL(SUM(a.reduce_suggest_fill_ceiling_num),0) AS total_fill_qty,
        IFNULL(SUM(IF(a.reduce_suggest_fill_ceiling_num > 0,1,0)),0) AS total_fill_sku,
        IFNULL(SUM(a.STOCK_NUM * a.SALE_PRICE) + SUM(a.reduce_suggest_fill_ceiling_num * a.SALE_PRICE),0) AS shelf_total_value
FROM 
        feods.shelf_product_fill_update_tmp a
GROUP BY a.shelf_id
;
SET @time_19 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_18--@time_19",@time_18,@time_19);	
-- 缺货金额 1min
DROP TEMPORARY TABLE IF EXISTS feods.`offstock_tmp`;
CREATE TEMPORARY TABLE feods.offstock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        g.shelf_id,
        SUM(g.ct * (1 - g.ifsto)) AS offstock_sku,
        SUM(g.offstock_val) AS offstock_val
FROM 
        feods.`d_op_s_offstock` g
WHERE g.sdate = @ydate
GROUP BY g.shelf_id
;

SET @time_20 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_19--@time_20",@time_19,@time_20);
-- 综合补货优先级 1s
DROP TEMPORARY TABLE IF EXISTS feods.`com_fill_level_tmp`;
CREATE TEMPORARY TABLE feods.com_fill_level_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        f.shelf_id,
        f.total_fill_value,
        f.total_fill_qty,
        f.total_fill_sku,
        f.shelf_total_value,
        g.offstock_sku,
        g.offstock_val,
        CASE
                WHEN f.total_fill_value >= 500 THEN 10
                WHEN f.total_fill_value >= 450 THEN 8
                WHEN f.total_fill_value >= 300 THEN 6
                WHEN f.total_fill_value >= 150 THEN 4
                ELSE 2
        END AS fill_value_priority,
        CASE
                WHEN g.offstock_val >= 100 THEN 10
                WHEN g.offstock_val >= 80 THEN 8
                WHEN g.offstock_val >= 50 THEN 6
                WHEN g.offstock_val >= 30 THEN 4
                ELSE 2
        END AS offstock_level,
        IF(h.shelf_id IS NULL,10,5) AS whether_push_level,
        CASE
                WHEN f.total_fill_value >= 500 THEN 10
                WHEN f.total_fill_value >= 450 THEN 8
                WHEN f.total_fill_value >= 300 THEN 6
                WHEN f.total_fill_value >= 150 THEN 4
                ELSE 2
        END * 0.25 + 
        CASE
                WHEN g.offstock_val >= 100 THEN 10
                WHEN g.offstock_val >= 80 THEN 8
                WHEN g.offstock_val >= 50 THEN 6
                WHEN g.offstock_val >= 30 THEN 4
                ELSE 2
        END * 0.25 +
--         IF(h.shelf_id IS NULL,10,5)  * 0.5
        CASE 
                WHEN shelf_level IN ('甲','乙','新') THEN 10
                WHEN shelf_level IN ('丙','丁') AND  h.shelf_id IS NOT NULL THEN 3
                WHEN shelf_level IN ('丙','丁') AND  h.shelf_id IS NULL THEN 6
                ELSE 6
        END * 0.5
        AS com_fill_level
FROM
        feods.fill_tmp f
        LEFT JOIN feods.offstock_tmp g
                ON f.`SHELF_ID` = g.`shelf_id`
        LEFT JOIN feods.fill_last_week_tmp h
                ON f.`SHELF_ID` = h.`shelf_id`
        JOIN feods.shelf_tmp s
                ON f.shelf_id = s.shelf_id
;



SET @time_21 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_20--@time_21",@time_20,@time_21);
-- 当天是否出单
-- ①根据货架分组，设定对应货架对应出单日期，周一至周五
-- ②出单金额大于150，则出单
-- ③店主当天大于150出单数大于10个，则取从补货金额排名+缺货金额+上周是否出过单三个维度评估货架补货优先级，详细见“货架分组-出单排名”，当天出单量最大为10个。
-- 若10名以后综合优先级相同，按上周是否出单、缺货金额、补货金额排序
-- 先按店主和货架级别维度排序 1s
DROP TEMPORARY TABLE IF EXISTS feods.`push_fill_order_date_tmp`;
CREATE TEMPORARY TABLE feods.push_fill_order_date_tmp(
        KEY idx_manager_id(manager_id)
) AS 
SELECT
        a.manager_id,
        GROUP_CONCAT(f.shelf_id ORDER BY f.com_fill_level DESC,f.whether_push_level DESC,i.offstock_val DESC,h.total_fill_value DESC) AS push_fill_order_shelf_id
FROM 
        fe.`sf_shelf` a
        JOIN feods.com_fill_level_tmp f 
                ON a.`SHELF_ID` = f.`shelf_id`
                AND a.SHELF_STATUS = 2
                AND a.WHETHER_CLOSE = 2
                AND a.REVOKE_STATUS = 1
                AND a.data_flag = 1
        JOIN fe.`sf_shelf_fill_day_config` g 
                ON a.shelf_id = g.shelf_id 
        JOIN feods.fill_tmp h
                ON a.`SHELF_ID` = h.`shelf_id`
        LEFT JOIN feods.offstock_tmp i
                ON a.`SHELF_ID` = i.`shelf_id`
WHERE (a.shelf_type IN (1,2,3,5) AND f.total_fill_value > 150)
        AND g.data_flag = 1
        AND SUBSTRING(g.fill_day_code,@day_num,1) = 1
        AND f.com_fill_level >= 5
GROUP BY a.manager_id
;
SET @time_22 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_21--@time_22",@time_21,@time_22);
-- 按对应货架级别上限的数量,拆分并去重,获得当天需要推单的shelf_id  1s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_push_fill_order_date_tmp`;
CREATE TEMPORARY TABLE feods.shelf_push_fill_order_date_tmp 
AS 
SELECT 
        DISTINCT 
        SUBSTRING_INDEX(SUBSTRING_INDEX(a.`push_fill_order_shelf_id`,',',b.`number`),',',-1) AS push_shelf_id
FROM
        (
                SELECT 
                        c.push_fill_order_shelf_id,
                        d.manager_push_order_limit
                FROM 
                        fe.`sf_shelf` a
                        JOIN feods.push_fill_order_date_tmp c
                                ON a.manager_id = c.manager_id
                                AND a.data_flag = 1
                        JOIN feods.d_op_manager_fill_day_tmp d
                                ON a.shelf_id = d.shelf_id
        )  a
        JOIN feods.`fjr_number` b
                ON b.number <= IF((LENGTH(a.push_fill_order_shelf_id) - LENGTH(REPLACE(a.push_fill_order_shelf_id,',','')) + 1) <= a.manager_push_order_limit,
                (LENGTH(a.push_fill_order_shelf_id) - LENGTH(REPLACE(a.push_fill_order_shelf_id,',','')) + 1),a.manager_push_order_limit)
;
SET @time_23 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_22--@time_23",@time_22,@time_23);
-- 出单日结果表 2s
TRUNCATE TABLE feods.`d_op_auto_push_fill_date`;
INSERT INTO feods.`d_op_auto_push_fill_date`
(
        stat_date,
        BUSINESS_AREA,
        manager_name,
        manager_id,
        SHELF_ID,
        grade,
        is_full_time_manager,    --  1：全职，2：兼职
        is_prewarehouse_shelf,    -- 1：大仓，2：前置仓
        total_fill_value,
        total_fill_qty,
        total_fill_sku,
        shelf_total_value,
        offstock_sku,
        offstock_val,
        fill_value_priority,
        offstock_level,
        whether_push_level,
        com_fill_level,
        whether_push_order 
)
SELECT 
        @cdate AS stat_date,
        k.business_name AS `BUSINESS_AREA`,
        a.manager_name,
        a.manager_id,
        a.`SHELF_ID`,
        IFNULL(g.grade,'新装') AS grade,
        l.second_user_type AS is_full_time_manager,    --  1：全职，2：兼职
        IF(e.`shelf_id` IS NULL,1,2) AS is_prewarehouse_shelf,    -- 1：大仓，2：前置仓
        f.total_fill_value,
        f.total_fill_qty,
        f.total_fill_sku,
        f.shelf_total_value,
        f.offstock_sku,
        f.offstock_val,
        f.fill_value_priority,
        f.offstock_level,
        f.whether_push_level,
        f.com_fill_level,
        IF(j.push_shelf_id IS NULL,2,1) AS whether_push_order          -- 1：是，2：否
FROM 
        fe.`sf_shelf` a
        LEFT JOIN fe.`sf_prewarehouse_shelf_detail` e
                ON a.`SHELF_ID` = e.`shelf_id`
                AND a.SHELF_STATUS = 2
                AND a.WHETHER_CLOSE = 2
                AND a.REVOKE_STATUS = 1
                AND a.data_flag = 1
                AND e.data_flag = 1
        JOIN feods.com_fill_level_tmp f
                ON a.`SHELF_ID` = f.`shelf_id`
        LEFT JOIN feods.`d_op_shelf_grade` g
                ON a.`SHELF_ID` = g.`shelf_id`
                AND g.month_id = @last_month
        LEFT JOIN feods.shelf_push_fill_order_date_tmp j
                ON a.shelf_id = j.push_shelf_id
        JOIN feods.`fjr_city_business` k
                ON a.city = k.city
        JOIN fe.`pub_shelf_manager` l
                ON a.manager_id = l.manager_id
                AND l.data_flag = 1
;
SET @time_24 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_23--@time_24",@time_23,@time_24);
-- 截存30天的数据 1s
DELETE FROM feods.`d_op_auto_push_fill_date_his` WHERE stat_date < SUBDATE(@cdate,30) OR stat_date = CURDATE();
 INSERT INTO feods.d_op_auto_push_fill_date_his
 (
        stat_date,
        BUSINESS_AREA,
        manager_name,
        manager_id,
        SHELF_ID,
        grade,
        is_full_time_manager,
        is_prewarehouse_shelf,
        total_fill_value,
        total_fill_qty,
        total_fill_sku,
        shelf_total_value,
        offstock_sku,
        offstock_val,
        fill_value_priority,
        offstock_level,
        whether_push_level,
        com_fill_level,
        whether_push_order
)
SELECT
        stat_date,
        BUSINESS_AREA,
        manager_name,
        manager_id,
        SHELF_ID,
        grade,
        is_full_time_manager,
        is_prewarehouse_shelf,
        total_fill_value,
        total_fill_qty,
        total_fill_sku,
        shelf_total_value,
        offstock_sku,
        offstock_val,
        fill_value_priority,
        offstock_level,
        whether_push_level,
        com_fill_level,
        whether_push_order
FROM feods.`d_op_auto_push_fill_date`;
SET @time_25 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_24--@time_25",@time_24,@time_25);
-- =====================================================================================
-- 补货逻辑结果表 35s
TRUNCATE feods.d_op_shelf_product_fill_update;
INSERT INTO feods.d_op_shelf_product_fill_update
(
        cdate,
        DETAIL_ID,
        ITEM_ID,
        PRODUCT_ID,
        product_name,
        product_fe,
        SHELF_ID,
        SHELF_TYPE,
        shelf_level,
        SALE_PRICE,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        ALARM_QUANTITY,
        SHELF_FILL_FLAG,
        STOCK_NUM,
        ONWAY_NUM,
        WEEK_SALE_NUM,
        PRODUCT_TYPE,
        warehouse_type,
        warehouse_stock,
        shelf_group,
        fill_order_day,
        whether_push_order, 
        fill_cycle,
        fill_days,
        is_holiday_stock_up,
        holiday_stock_up_qty,
        holiday_stock_up_ratio,
        holiday_stock_up_cycle,
        holiday_stock_up_datetime,
        holiday_stop_fill_date,
        holiday_recover_fill_date,
        predict_day_sale_qty,
        day_sale_qty,
        season_factor,
        safe_stock_qty,
        shelf_stock_upper_limit,
        stock_total_qty,
        suspect_false_stock_qty,
        SUGGEST_FILL_NUM,
        reduce_suggest_fill_num,
        reduce_suggest_fill_ceiling_num
)
SELECT
        a.cdate,
        a.DETAIL_ID,
        a.ITEM_ID,
        a.PRODUCT_ID,
        a.product_name,
        a.product_fe,
        a.SHELF_ID,
        a.SHELF_TYPE,
        a.shelf_level,
        a.SALE_PRICE,
        a.NEW_FLAG,
        a.SALES_FLAG,
        a.FILL_MODEL,
        a.ALARM_QUANTITY,
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        a.ONWAY_NUM,
        a.WEEK_SALE_NUM,
        a.PRODUCT_TYPE,
        a.warehouse_type,
        a.warehouse_stock,
        NULL AS shelf_group,
        a.fill_order_day,
        IFNULL(b.whether_push_order,2) AS whether_push_order, 
        a.fill_cycle,
        a.fill_days,
        a.is_holiday_stock_up,
        a.holiday_stock_up_qty,
        a.holiday_stock_up_ratio,
        a.holiday_stock_up_cycle,
        a.holiday_stock_up_datetime,
        a.holiday_stop_fill_date,
        a.holiday_recover_fill_date,
        a.predict_day_sale_qty,
        a.day_sale_qty,
        a.season_factor,
        a.safe_stock_qty,
        a.shelf_stock_upper_limit,
        a.stock_total_qty,
        a.suspect_false_stock_qty,
        a.SUGGEST_FILL_NUM,
        IFNULL(a.reduce_suggest_fill_num,0) AS reduce_suggest_fill_num,
        IFNULL(a.reduce_suggest_fill_ceiling_num,0) AS reduce_suggest_fill_ceiling_num
FROM
        feods.shelf_product_fill_update_tmp a
        JOIN feods.d_op_auto_push_fill_date b
                ON a.shelf_id = b.shelf_id
;
SET @time_26 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_25--@time_26",@time_25,@time_26);
-- 补货逻辑结果表截存近30天数据
DELETE FROM feods.d_op_shelf_product_fill_update_his WHERE cdate < SUBDATE(@cdate,30) OR cdate = CURDATE();
INSERT INTO feods.d_op_shelf_product_fill_update_his
(
        cdate,
        PRODUCT_ID,
        SHELF_ID,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        ALARM_QUANTITY,
        STOCK_NUM,
        ONWAY_NUM,
        warehouse_stock,
        whether_push_order,
        fill_cycle,
        fill_days,
        day_sale_qty,
        safe_stock_qty,
        shelf_stock_upper_limit,
        stock_total_qty,
        suspect_false_stock_qty,
        SUGGEST_FILL_NUM,
        reduce_suggest_fill_num,
        reduce_suggest_fill_ceiling_num
)
SELECT
        cdate,
        PRODUCT_ID,
        SHELF_ID,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        ALARM_QUANTITY,
        STOCK_NUM,
        ONWAY_NUM,
        warehouse_stock,
        whether_push_order,
        fill_cycle,
        fill_days,
        day_sale_qty,
        safe_stock_qty,
        shelf_stock_upper_limit,
        stock_total_qty,
        suspect_false_stock_qty,
        SUGGEST_FILL_NUM,
        reduce_suggest_fill_num,
        reduce_suggest_fill_ceiling_num
FROM 
        feods.`d_op_shelf_product_fill_update`;
		
SET @time_27 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_26--@time_27",@time_26,@time_27);
-- ==========================================================================================
-- 自贩机补货逻辑

-- 自贩机基础信息 4s
DROP TEMPORARY TABLE IF EXISTS feods.`machine_info_tmp`;
CREATE TEMPORARY TABLE feods.machine_info_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        a.product_id,
        b.machine_type,         -- 机器类型
        SUM(f.slot_capacity_limit) AS total_slot_capacity_limit,        -- 总货道标配
        COUNT(*) AS slots,            -- 货道数
        MAX(f.slot_capacity_limit) AS slot_capacity_limit,          -- 货道标配
        a.qty_sto_slot AS slot_stock_num,    -- 货道库存
        a.qty_sto_sec AS second_stock_num,      -- 副柜库存        
        IF(MAX(f.slot_capacity_limit) <= 6,COUNT(*),COUNT(*) * 2) AS layout_qty,        -- 排面量
        b.prewh_falg AS is_prewarehouse_shelf
FROM
        feods.d_op_sp_shelf7_stock3 a
        JOIN feods.`d_op_shelf_info` b
                ON a.shelf_id = b.shelf_id
        JOIN fe.sf_shelf_machine_slot e
                ON e.shelf_id = a.shelf_id
                AND a.product_id = e.product_id
                AND e.data_flag = 1
        JOIN fe.`sf_shelf_machine_slot_type` f
                ON e.slot_type_id = f.slot_type_id
                AND f.data_flag = 1
GROUP BY a.shelf_id,a.product_id
;

SET @time_29 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_28--@time_29",@time_28,@time_29);	
-- 自贩机补货逻辑 2s
DROP TEMPORARY TABLE IF EXISTS feods.`machine_fill_tmp`;
CREATE TEMPORARY TABLE feods.machine_fill_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        @cdate AS cdate,
        a.DETAIL_ID,
        a.ITEM_ID,
        a.PRODUCT_ID,
        a.product_name,
        a.PRODUCT_CODE2 AS product_fe,
        a.SHELF_ID,
        a.`SHELF_TYPE`,
        a.`shelf_level`,
        a.SALE_PRICE,
        a.NEW_FLAG,
        a.SALES_FLAG,
        a.FILL_MODEL,
        r.machine_type,
        r.total_slot_capacity_limit,        -- 总货道标配
        r.slots,            -- 货道数
        r.slot_capacity_limit,          -- 货道标配
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        r.slot_stock_num,    -- 货道库存
        r.second_stock_num,      -- 副柜库存
        r.is_prewarehouse_shelf,
        a.warehouse_stock,
        a.ONWAY_NUM,
        a.WEEK_SALE_NUM,     -- 近一周销量
        NULL AS shelf_group,          -- 货架组
        'A11111' AS fill_order_day,       -- 补货出单日
        6 AS fill_cycle,              -- 补货周期
        1 AS fill_days,             -- 上架周期
        e.day_sale_qty,        --  日均销量
        r.layout_qty,           -- 排面量
        IFNULL(n.safe_stock_qty,0) AS safe_stock_qty,       -- 安全库存量
        g.suspect_false_stock_qty,       -- 疑似虚假库存量
--         建议补货量
        -- ①A.新品标识为1，销售标识不为空，建议补货量=货道数*货道标配-货架库存-在途
        --  B.新品标识为空，销量标识为空，仓库有库存，建议补货量=货道数*货道标配
        -- C.新品标识为空，销量标识为空，仓库无库存，建议补货量为0
        -- ②Min（A1,A2）
        -- A1:标配差额=标配+（日均销量*上架周期）-货架库存-在途库存
        -- A2:销售补货需求=日均销量*（补货周期+上架周期）+排面量+安全库存-货架库存-在途库存
        -- ③取整：建议补货量为负，都统一改为0；不足一个按一个补
        CEILING(IF(
        CASE   
                WHEN a.NEW_FLAG = 1
                        THEN r.total_slot_capacity_limit - a.STOCK_NUM - a.ONWAY_NUM
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock > 0
                        THEN r.total_slot_capacity_limit
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock = 0
                        THEN 0
                ELSE IF(r.total_slot_capacity_limit + (e.day_sale_qty * 1) - a.STOCK_NUM - a.ONWAY_NUM <= e.day_sale_qty * (6+1) + r.layout_qty + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM,
                r.total_slot_capacity_limit + (e.day_sale_qty * 1) - a.STOCK_NUM - a.ONWAY_NUM + IFNULL(g.suspect_false_stock_qty,0), -- 标配差额
                e.day_sale_qty * (6+1) + r.layout_qty + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM + IFNULL(g.suspect_false_stock_qty,0)        -- 销售补货需求
                        )
        END < 0,
        0,
        CASE 
                WHEN a.NEW_FLAG = 1
                        THEN r.total_slot_capacity_limit - a.STOCK_NUM - a.ONWAY_NUM
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock > 0
                        THEN r.total_slot_capacity_limit
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock = 0
                        THEN 0
                ELSE IF(r.total_slot_capacity_limit + (e.day_sale_qty * 1) - a.STOCK_NUM - a.ONWAY_NUM <= e.day_sale_qty * (6+1) + r.layout_qty + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM,
                r.total_slot_capacity_limit + (e.day_sale_qty * 1) - a.STOCK_NUM - a.ONWAY_NUM + IFNULL(g.suspect_false_stock_qty,0), -- 标配差额
                e.day_sale_qty * (6+1) + r.layout_qty + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM + IFNULL(g.suspect_false_stock_qty,0)        -- 销售补货需求
                        )
        END
        )) AS SUGGEST_FILL_NUM   -- 建议补货量
FROM 
        feods.shelf_product_fill_info a
        JOIN feods.`d_op_fill_day_sale_qty` e   
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id 
        LEFT JOIN feods.suspect_false_stock g
                ON a.shelf_id = g.shelf_id
                AND a.product_id = g.product_id 
        LEFT JOIN feods.safe_stock_qty_tmp n   
                ON a.shelf_id = n.shelf_id
                AND a.product_id = n.product_id 
        JOIN feods.machine_info_tmp r
                ON a.shelf_id = r.shelf_id
                AND a.product_id = r.product_id 
;
SET @time_30 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_29--@time_30",@time_29,@time_30);	
-- 自贩机建议补货总金额 1s
DROP TEMPORARY TABLE IF EXISTS feods.`machine_fill_value`;
CREATE TEMPORARY TABLE feods.machine_fill_value(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        shelf_id,
        SUM(SALE_PRICE * SUGGEST_FILL_NUM) AS total_fill_value
FROM
        feods.machine_fill_tmp
GROUP BY shelf_id
;
SET @time_31 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_30--@time_31",@time_30,@time_31);	
-- 自贩机补货结果表
TRUNCATE feods.d_op_machine_fill_update;
INSERT INTO feods.d_op_machine_fill_update
(
        cdate,
        DETAIL_ID,
        ITEM_ID,
        PRODUCT_ID,
        product_name,
        product_fe,
        SHELF_ID,
        SHELF_TYPE,
        shelf_level,
        SALE_PRICE,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        machine_type,
        total_slot_capacity_limit,      
        slots,          
        slot_capacity_limit,        
        SHELF_FILL_FLAG,
        STOCK_NUM,
        slot_stock_num,    
        second_stock_num,    
        is_prewarehouse_shelf,
        warehouse_stock,
        ONWAY_NUM,
        WEEK_SALE_NUM,    
        shelf_group,          
        fill_order_day,   
        fill_cycle,             
        fill_days,            
        day_sale_qty,      
        layout_qty,           
        safe_stock_qty,       
        suspect_false_stock_qty, 
        SUGGEST_FILL_NUM,
        whether_push_order
)
SELECT 
        cdate,
        DETAIL_ID,
        ITEM_ID,
        PRODUCT_ID,
        product_name,
        product_fe,
        a.SHELF_ID,
        SHELF_TYPE,
        shelf_level,
        SALE_PRICE,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        machine_type,
        total_slot_capacity_limit,        -- 总货道标配
        slots,            -- 货道数
        slot_capacity_limit,          -- 货道标配
        SHELF_FILL_FLAG,
        STOCK_NUM,
        slot_stock_num,    -- 货道库存
        second_stock_num,      -- 副柜库存
        is_prewarehouse_shelf,
        warehouse_stock,
        ONWAY_NUM,
        WEEK_SALE_NUM,     -- 近一周销量
        shelf_group,          -- 货架组
        fill_order_day,       -- 补货出单日
        fill_cycle,              -- 补货周期
        fill_days,             -- 上架周期
        day_sale_qty,        --  日均销量
        layout_qty,           -- 排面量
        safe_stock_qty,       -- 安全库存量
        suspect_false_stock_qty, 
        IFNULL(SUGGEST_FILL_NUM,0) AS SUGGEST_FILL_NUM,
        IF(b.total_fill_value >= 150 AND a.SUGGEST_FILL_NUM > 0,1,2) AS whether_push_order
FROM 
        feods.machine_fill_tmp a
        JOIN feods.machine_fill_value b
                ON a.shelf_id = b.shelf_id
;
SET @time_32 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_31--@time_32",@time_31,@time_32);	
-- ====================================================================================
-- 静态智能柜补货逻辑
-- 静态智能柜建议补货量 1s
DROP TEMPORARY TABLE IF EXISTS feods.`smart_shelf_fill_tmp`;
CREATE TEMPORARY TABLE feods.smart_shelf_fill_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        @cdate AS cdate,
        a.DETAIL_ID,
        a.PRODUCT_ID,
        a.SHELF_ID,
        a.`SHELF_TYPE`,
        a.`shelf_level`,
        a.SALE_PRICE,
        a.NEW_FLAG,
        a.SALES_FLAG,
        a.FILL_MODEL,
        a.ALARM_QUANTITY,
        r.machine_type,
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        r.prewh_falg AS is_prewarehouse_shelf,
        a.warehouse_stock,
        a.ONWAY_NUM,
        a.WEEK_SALE_NUM,     -- 近一周销量
        e.day_sale_qty,        --  日均销量
        IF(a.ALARM_QUANTITY <= 6 ,1,2)  AS layout_qty,      -- 排面量
        IFNULL(n.safe_stock_qty,0) AS safe_stock_qty,       -- 安全库存量
-- 建议补货量
-- ①A.新品标识为1，销售标识不为空，建议补货量=标配-货架库存-在途
--  B.新品标识为空，销量标识为空，仓库有库存，建议补货量=货道标配
-- C.新品标识为空，销量标识为空，仓库无库存，建议补货量为0
-- ②非新品：Min（A1,A2）
-- A1:标配差额=标配-货架总库存-在途库存
-- A2:销售补货需求=日均销量*7+排面量+安全库存-货架库存-在途库存
-- ③取整：建议补货量为负，都统一改为0；不足一个按一个补
        CEILING(IF(
        CASE 
                WHEN a.NEW_FLAG = 1
                        THEN a.ALARM_QUANTITY - a.STOCK_NUM - a.ONWAY_NUM
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock > 0
                        THEN a.ALARM_QUANTITY
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock = 0
                        THEN 0
                ELSE IF(a.ALARM_QUANTITY - a.STOCK_NUM - a.ONWAY_NUM  <= e.day_sale_qty * 7 + IF(a.ALARM_QUANTITY <= 6 ,1,2) + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM,
                a.ALARM_QUANTITY - a.STOCK_NUM - a.ONWAY_NUM + IFNULL(g.suspect_false_stock_qty,0), -- 标配差额
                e.day_sale_qty * 7 + IF(a.ALARM_QUANTITY <= 6 ,1,2) + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM+ IFNULL(g.suspect_false_stock_qty,0)        -- 销售补货需求
                        )
        END < 0,
        0,
        CASE 
                WHEN a.NEW_FLAG = 1
                        THEN a.ALARM_QUANTITY - a.STOCK_NUM - a.ONWAY_NUM
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock > 0
                        THEN a.ALARM_QUANTITY
                WHEN a.NEW_FLAG IS NULL AND a.SALES_FLAG IS NULL AND a.warehouse_stock = 0
                        THEN 0
                ELSE IF(a.ALARM_QUANTITY - a.STOCK_NUM - a.ONWAY_NUM  <= e.day_sale_qty * 7 + IF(a.ALARM_QUANTITY <= 6 ,1,2) + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM,
                a.ALARM_QUANTITY - a.STOCK_NUM - a.ONWAY_NUM + IFNULL(g.suspect_false_stock_qty,0), -- 标配差额
                e.day_sale_qty * 7 + IF(a.ALARM_QUANTITY <= 6 ,1,2) + IFNULL(n.safe_stock_qty,0) - a.STOCK_NUM - a.ONWAY_NUM+ IFNULL(g.suspect_false_stock_qty,0)        -- 销售补货需求
                        )
        END
        )) AS SUGGEST_FILL_NUM   -- 建议补货量
FROM 
        feods.shelf_product_fill_info a
        JOIN feods.`d_op_fill_day_sale_qty` e   
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id 
        LEFT JOIN feods.suspect_false_stock g
                ON a.shelf_id = g.shelf_id
                AND a.product_id = g.product_id 
        LEFT JOIN feods.safe_stock_qty_tmp n   
                ON a.shelf_id = n.shelf_id
                AND a.product_id = n.product_id 
        JOIN feods.`d_op_shelf_info` r
                ON a.shelf_id = r.shelf_id
        LEFT JOIN fe.sf_shelf_machine sm 
                ON a.SHELF_ID = sm.shelf_id 
                AND sm.data_flag = 1
        LEFT JOIN fe.sf_shelf_machine_type mt 
                ON sm.machine_type_id = mt.machine_type_id 
                AND mt.data_flag = 1
WHERE a.shelf_type = 6 AND mt.machine_type_code=3
--  r.machine_type LIKE '%静态柜'
;
SET @time_33 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_32--@time_33",@time_32,@time_33);	
-- 盒装商品取整
DROP TEMPORARY TABLE IF EXISTS feods.`smart_shelf_fill_ceiling_tmp`;
CREATE TEMPORARY TABLE feods.smart_shelf_fill_ceiling_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
    a.shelf_id,
    a.product_id,
    CASE
        WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM < 0.5 * b.fill_box_gauge  AND a.SUGGEST_FILL_NUM >= 0.25 * b.fill_box_gauge 
            THEN b.fill_box_gauge
        WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM > 0.25 * b.fill_box_gauge  AND a.STOCK_NUM < 0.5 * b.fill_box_gauge AND a.SUGGEST_FILL_NUM < 0.25 * b.fill_box_gauge 
            THEN 0
        WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM <= 0.25 * b.fill_box_gauge  AND a.day_sale_qty >= 0.14
            THEN b.fill_box_gauge
        WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM <= 0.25 * b.fill_box_gauge  AND a.day_sale_qty < 0.14
            THEN 0
        WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM >= 0.5 * b.fill_box_gauge AND a.SUGGEST_FILL_NUM >= 0.5 * b.fill_box_gauge 
            THEN b.fill_box_gauge
        WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM >= 0.5 * b.fill_box_gauge AND a.SUGGEST_FILL_NUM < 0.5 * b.fill_box_gauge 
            THEN 0
        ELSE IF(a.SUGGEST_FILL_NUM = 1, 2,a.SUGGEST_FILL_NUM) 
    END AS suggest_ceiling_fill_num
FROM
    feods.smart_shelf_fill_tmp a
    JOIN fe.`sf_product` b
        ON a.product_id = b.product_id
        AND b.data_flag = 1
;
SET @time_34 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_33--@time_34",@time_33,@time_34);	
-- 静态智能柜建议补货总金额 1s
DROP TEMPORARY TABLE IF EXISTS feods.`smart_shelf_fill_value`;
CREATE TEMPORARY TABLE feods.smart_shelf_fill_value(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        SUM(SALE_PRICE * b.suggest_ceiling_fill_num) AS total_fill_value
FROM
        feods.smart_shelf_fill_tmp a
        JOIN feods.smart_shelf_fill_ceiling_tmp b
            ON a.shelf_id = b.shelf_id
            AND a.product_id = b.product_id
GROUP BY a.shelf_id
;
SET @time_35 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_34--@time_35",@time_34,@time_35);	
-- 静态智能柜补货结果表
TRUNCATE feods.d_op_smart_shelf_fill_update;
INSERT INTO feods.d_op_smart_shelf_fill_update
(
        cdate,
        DETAIL_ID,
        PRODUCT_ID,
        SHELF_ID,
        SHELF_TYPE,
        `shelf_level`,
        SALE_PRICE,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        ALARM_QUANTITY,
        machine_type,
        SHELF_FILL_FLAG,
        STOCK_NUM,
        is_prewarehouse_shelf,
        warehouse_stock,
        ONWAY_NUM,
        WEEK_SALE_NUM,     -- 近一周销量
        day_sale_qty,        --  日均销量
        layout_qty,      -- 排面量
        safe_stock_qty,   
        SUGGEST_FILL_NUM,
        fill_value,
        total_fill_value,
        whether_push_order
)
SELECT 
        a.cdate,
        a.DETAIL_ID,
        a.PRODUCT_ID,
        a.SHELF_ID,
        a.`SHELF_TYPE`,
        a.`shelf_level`,
        a.SALE_PRICE,
        a.NEW_FLAG,
        a.SALES_FLAG,
        a.FILL_MODEL,
        a.ALARM_QUANTITY,
        a.machine_type,
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        a.is_prewarehouse_shelf,
        a.warehouse_stock,
        a.ONWAY_NUM,
        a.WEEK_SALE_NUM,     -- 近一周销量
        a.day_sale_qty,        --  日均销量
        a.layout_qty,      -- 排面量
        a.safe_stock_qty,   
        IFNULL(c.suggest_ceiling_fill_num,0) AS SUGGEST_FILL_NUM,
        IFNULL(c.suggest_ceiling_fill_num,0) * a.SALE_PRICE AS fill_value,
        b.total_fill_value,
        IF(b.total_fill_value >= 150 AND c.suggest_ceiling_fill_num > 0,1,2) AS whether_push_order
FROM 
        feods.smart_shelf_fill_tmp a
        JOIN feods.smart_shelf_fill_value b
                ON a.shelf_id = b.shelf_id
        JOIN feods.smart_shelf_fill_ceiling_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.product_id = c.product_id
;
DELETE FROM feods.d_op_smart_shelf_fill_update_his WHERE cdate < SUBDATE(@cdate,30) OR cdate = CURDATE();
INSERT INTO feods.d_op_smart_shelf_fill_update_his
(
        cdate,
        DETAIL_ID,
        PRODUCT_ID,
        SHELF_ID,
        SHELF_TYPE,
        `shelf_level`,
        SALE_PRICE,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        ALARM_QUANTITY,
        machine_type,
        SHELF_FILL_FLAG,
        STOCK_NUM,
        is_prewarehouse_shelf,
        warehouse_stock,
        ONWAY_NUM,
        WEEK_SALE_NUM,     -- 近一周销量
        day_sale_qty,        --  日均销量
        layout_qty,      -- 排面量
        safe_stock_qty,   
        SUGGEST_FILL_NUM,
        fill_value,
        total_fill_value,
        whether_push_order
)
SELECT
        cdate,
        DETAIL_ID,
        PRODUCT_ID,
        SHELF_ID,
        SHELF_TYPE,
        `shelf_level`,
        SALE_PRICE,
        NEW_FLAG,
        SALES_FLAG,
        FILL_MODEL,
        ALARM_QUANTITY,
        machine_type,
        SHELF_FILL_FLAG,
        STOCK_NUM,
        is_prewarehouse_shelf,
        warehouse_stock,
        ONWAY_NUM,
        WEEK_SALE_NUM,     -- 近一周销量
        day_sale_qty,        --  日均销量
        layout_qty,      -- 排面量
        safe_stock_qty,   
        SUGGEST_FILL_NUM,
        fill_value,
        total_fill_value,
        whether_push_order
FROM
        feods.d_op_smart_shelf_fill_update
;
SET @time_36 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_shelf_product_fill_update","@time_35--@time_36",@time_35,@time_36);	
COMMIT;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_shelf_product_fill_update',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
END