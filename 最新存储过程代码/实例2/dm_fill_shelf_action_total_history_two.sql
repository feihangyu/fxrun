CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_fill_shelf_action_total_history_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @month_id := DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH),'%Y-%m');  
SET @pre_week := WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 8 DAY));
SET @pre_sunday := SUBDATE(ADDDATE(CURDATE(),INTERVAL 8 - DAYOFWEEK(CURDATE()) DAY),7);
SET @pre_day7 := DATE_SUB(CURDATE(), INTERVAL 7 DAY);
SET @pre_6month := DATE_SUB(CURDATE(), INTERVAL 6 MONTH);
DELETE FROM fe_dm.dm_fill_shelf_action_history WHERE sdate = CURDATE();
DELETE FROM fe_dm.dm_fill_shelf_action_history WHERE sdate < DATE_SUB(CURDATE(), INTERVAL 15 DAY);
SET @time_1 := CURRENT_TIMESTAMP();
-- 库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`stock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.stock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        SUM(a.stock_quantity > 0) AS stock_sku,
        SUM(a.stock_quantity) AS stock_quantity,
        SUM(a.stock_quantity * a.sale_price) AS stock_value,
        SUM(IF(a.SALES_FLAG = 5 AND a.NEW_FLAG = 2,a.stock_quantity,0)) AS stock_quantity_5,
        SUM(IF(a.SALES_FLAG = 5 AND a.NEW_FLAG = 2,a.stock_quantity * a.sale_price,0)) AS stock_value_5
FROM
        fe_dwd.`dwd_shelf_product_day_all` a
GROUP BY a.shelf_id
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_1--@time_2",@time_1,@time_2);
#补货
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_tmp`;
CREATE TEMPORARY TABLE fe_dwd.fill_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        SUM(IF(a.order_status = 1,a.ACTUAL_apply_NUM,0)) AS apply_qty,
        MIN(IF(a.order_status = 1,a.apply_time,NULL)) AS min_apply_time,
        SUM(IF(a.order_status = 2,a.ACTUAL_send_NUM,0)) AS send_qty,
        MIN(IF(a.order_status = 2,a.send_time,NULL)) AS min_send_time,
        MAX(a.fill_time) AS max_fill_time
FROM
        fe_dwd.`dwd_fill_day_inc` a
WHERE a.order_status IN (1, 2, 3, 4)
        AND a.apply_time >= @pre_6month
GROUP BY a.shelf_id
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_2--@time_3",@time_2,@time_3);
#店主管理货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`manager_tmp`;
CREATE TEMPORARY TABLE fe_dwd.manager_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.city_name,
        IF(a.manager_type = '全职店主','全职店主','非全职店主') AS if_all_time,
        IF(e.shelf_id IS NOT NULL,'前置仓覆盖货架','普通货架') AS if_prewarehouse,
        a.shelf_id,
        a.SHELF_STATUS,
        a.SHELF_TYPE,
        a.ACTIVATE_TIME,
        a.REVOKE_STATUS,
        a.WHETHER_CLOSE,
        b.shelf_qty,
        a.relation_flag
FROM
        fe_dwd.`dwd_shelf_base_day_all`  a
        LEFT JOIN
                (
                        SELECT
                        manager_id,
                        COUNT(shelf_id) AS shelf_qty
                        FROM
                                fe_dwd.`dwd_shelf_base_day_all` 
                        WHERE SHELF_STATUS = 2
                        GROUP BY manager_id
                ) b
                ON a.manager_id = b.manager_id
        LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` e
                ON a.shelf_id = e.shelf_id
WHERE a.SHELF_STATUS = 2
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_3--@time_4",@time_3,@time_4);
#缺货损失
DROP TABLE IF EXISTS test.sale_tmp;
CREATE TABLE test.sale_tmp(
        shelf_id INT(8),
        product_id INT(8),
        quantity INT(8),
        PRIMARY KEY `idx_shelf_id_product_id` (`shelf_id`,product_id)
        ) ;
        
INSERT INTO test.sale_tmp
SELECT
        a.shelf_id,
        a.PRODUCT_ID,
        SUM(IF(a.order_status = 2,a.quantity,a.quantity_shipped)) AS quantity
FROM
        `fe_dwd`.`dwd_pub_order_item_recent_one_month` a
WHERE a.order_status IN (2, 6, 7)
        AND @pre_week =WEEKOFYEAR(pay_date) 
GROUP BY a.shelf_id,a.PRODUCT_ID
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_4--@time_5",@time_4,@time_5);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`offstock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.offstock_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        SUM(abc) AS queh_value,
        SUM(abc1) AS queh_value1
FROM
(
        SELECT
                a.shelf_id,
                a.PRODUCT_ID,
                IF(b.STOCK_QUANTITY <= 0,c.quantity / 7 * b.SALE_PRICE,NULL) AS abc,
                IF(b.STOCK_QUANTITY <= c.quantity / 7 * 2,c.quantity / 7 * b.SALE_PRICE,NULL) AS abc1
        FROM
                fe_dwd.`dwd_shelf_product_day_all_recent_32` a
                JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                        ON a.shelf_id = b.SHELF_ID
                        AND a.PRODUCT_ID = b.PRODUCT_ID
                        AND a.sdate = @pre_sunday
                        AND a.sales_flag IN (1, 2, 3)
                LEFT JOIN test.sale_tmp c
                        ON a.shelf_id = c.SHELF_ID
                        AND a.PRODUCT_ID = c.PRODUCT_ID

) t1
GROUP BY shelf_id
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_5--@time_6",@time_5,@time_6);
DROP TABLE IF EXISTS test.sale_tmp;
#补货需求申请
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_req_tmp`;
CREATE TEMPORARY TABLE fe_dwd.fill_req_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        `shelf_id`,
        COUNT(DISTINCT `sday`) AS requirement_times,
        MAX(DATE(load_time)) AS max_sdate
FROM 
        fe_dwd.`dwd_fillorder_requirement_information_his`
WHERE sday > DAY(SUBDATE(CURDATE(),INTERVAL 7 DAY))
        AND ((`total_price` > 100 AND `supplier_type` = 9) OR (`total_price` > 150 AND `supplier_type` = 2))
GROUP BY `shelf_id`
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_6--@time_7",@time_6,@time_7);
-- 近七天gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_day7_tmp`;
CREATE TEMPORARY TABLE fe_dwd.pre_day7_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        SUM(IF(ORDER_STATUS = 2,QUANTITY,a.quantity_shipped)) AS QUANTITY,
        SUM(IF(ORDER_STATUS = 2,QUANTITY * SALE_PRICE,a.quantity_shipped * a.SALE_PRICE)) AS gmv
FROM
        fe_dwd.`dwd_pub_order_item_recent_one_month` a
WHERE pay_date >= @pre_day7
        AND ORDER_STATUS IN (2, 6, 7)
GROUP BY a.shelf_id 
;
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_7--@time_8",@time_7,@time_8);
  INSERT INTO fe_dm.dm_fill_shelf_action_history (
    sdate,
    shelf_id,
    stock_quantity,
    stock_value,
    stock_quantity_5,
    stock_value_5,
    shelf_level,
    apply_qty,
    send_qty,
    max_fill_time,
    city_name,
    if_all_time,
    if_prewarehouse,
    SHELF_STATUS,
    SHELF_TYPE,
    ACTIVATE_TIME,
    REVOKE_STATUS,
    WHETHER_CLOSE,
    shelf_qty,
    queh_value,
    queh_value1,
    requirement_times,
    max_sdate,
    QUANTITY,
    gmv,
    shelf_level_sim,
    shelf_status_sim,
    stock_type,
    stock_sku_type,
    min_apply_time,
    min_send_time
  )
  SELECT
    CURDATE() AS sdate,
    t1.shelf_id,
    t1.stock_quantity,
    t1.stock_value,
    t1.stock_quantity_5,
    t1.stock_value_5,
    CASE 
        WHEN t2.grade = '甲' AND t4.relation_flag = 1 THEN '甲级2'
        WHEN t2.grade = '乙' AND t4.relation_flag = 1  THEN '乙级2'
        WHEN t2.grade = '甲'  THEN '甲级'
        WHEN t2.grade = '乙'  THEN '乙级'
        WHEN t2.grade = '丙' AND t4.relation_flag = 1  THEN '丙级2'
        WHEN t2.grade = '丁' AND t4.relation_flag = 1  THEN '丁级2'
        WHEN t2.grade = '丙'  THEN '丙级'
        WHEN t2.grade = '丁'  THEN '丁级'
    END 
    AS shelf_level,
    t3.apply_qty,
    t3.send_qty,
    t3.max_fill_time,
    t4.city_name,
    t4.if_all_time,
    t4.if_prewarehouse,
    t4.SHELF_STATUS,
    t4.SHELF_TYPE,
    t4.ACTIVATE_TIME,
    t4.REVOKE_STATUS,
    t4.WHETHER_CLOSE,
    t4.shelf_qty,
    t5.queh_value,
    t5.queh_value1,
    t6.requirement_times,
    t6.max_sdate,
    t7.QUANTITY,
    t7.gmv,
    CASE
        WHEN t2.grade = '新装' THEN '新装'
        WHEN t2.grade IN ('甲','乙') THEN '甲乙级'
        WHEN t2.grade IN ('丙','丁') THEN '丙丁级'
        ELSE '其他'
    END aaa,
    CASE
        WHEN t4.REVOKE_STATUS = 1 AND t4.WHETHER_CLOSE = 1
                THEN '关闭未撤架'
        WHEN t4.REVOKE_STATUS <> 1 AND t4.WHETHER_CLOSE = 1
                THEN '关闭撤架过程中'
        WHEN t4.REVOKE_STATUS <> 1 AND t4.WHETHER_CLOSE = 2
                THEN '未关闭撤架过程中'
        WHEN t4.REVOKE_STATUS = 1 AND t4.WHETHER_CLOSE = 2
                THEN '正常货架'
    END AS '货架状态',
    CASE
        WHEN t2.grade = '新装' AND t4.shelf_type IN (1, 3) AND t1.stock_quantity < 180
                THEN '库存不足'
        WHEN t2.grade = '新装' AND t4.shelf_type IN (2, 5) AND t1.stock_quantity < 110
                THEN '库存不足'
        WHEN t2.grade IN ('甲', '乙') AND t4.relation_flag = 1 AND t1.stock_quantity < 300        -- 甲乙级关联货架
                THEN '库存不足'
        WHEN t2.grade IN ('甲', '乙') AND t4.shelf_type IN (1, 3) AND t1.stock_quantity < 180
                THEN '库存不足'
        WHEN t2.grade IN ('甲', '乙') AND t4.shelf_type IN (2, 5) AND t1.stock_quantity < 110
                THEN '库存不足'
        WHEN t2.grade IN ('丙', '丁') AND t1.stock_quantity < 200 AND t4.relation_flag = 1        -- 丙丁级关联货架
                THEN '库存不足'
        WHEN t2.grade IN ('丙', '丁') AND t4.shelf_type IN (1, 3) AND t1.stock_quantity < 110
                THEN '库存不足'
        WHEN t2.grade IN ('丙', '丁') AND t4.shelf_type IN (2, 5) AND t1.stock_quantity < 90
                THEN '库存不足'
        WHEN t4.shelf_type = 6 AND t1.stock_quantity < 110
                THEN '库存不足'
        WHEN t4.shelf_type = 8 AND t1.stock_quantity < 100
                THEN '库存不足'
        ELSE '其他'
    END AS stock_type,
    CASE
        WHEN t4.shelf_type IN (1, 3) AND t1.stock_sku < 30
                THEN 'SKU不足'
        WHEN t4.shelf_type IN (2, 5) AND t1.stock_sku < 10
                THEN 'SKU不足'
        WHEN t4.shelf_type = 8 AND t1.stock_sku < 15
                THEN 'SKU不足'
        ELSE '其他'
    END AS stock_sku_type,
    min_apply_time,
    min_send_time
FROM
        fe_dwd.stock_tmp t1 
        JOIN fe_dwd.`dwd_shelf_base_day_all` t2
                ON t1.shelf_id = t2.shelf_id
        LEFT JOIN fe_dwd.fill_tmp t3
                ON t1.shelf_id = t3.shelf_id
        LEFT JOIN fe_dwd.manager_tmp t4
                ON t1.shelf_id = t4.shelf_id
        LEFT JOIN fe_dwd.`offstock_tmp` t5
                ON t1.shelf_id = t5.shelf_id
        LEFT JOIN fe_dwd.fill_req_tmp t6
                ON t1.shelf_id = t6.shelf_id
        LEFT JOIN fe_dwd.pre_day7_tmp t7
                ON t1.shelf_id = t7.shelf_id;
				
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_8--@time_9",@time_8,@time_9);
      
DELETE FROM fe_dm.dm_fill_shelf_action_total_history WHERE sdate = CURDATE();
INSERT INTO fe_dm.dm_fill_shelf_action_total_history (
        sdate,
        stype1,
        stype_ren,
        if_all_time,
        if_prewarehouse,
        city_name,
        shelf_level_sim,
        shelf_status_sim,
        stock_type,
        stock_sku_type,
        shelf_qty,
        queh_value,
        stock_value,
        stock_value_5
)
SELECT
        sdate,
        CASE
                WHEN stock_type = '库存不足' AND IFNULL(stock_quantity, 0) <= 0
                        THEN '库存异常，风控介入'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_level IN ('甲级2', '乙级2') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 300
                        THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (1, 3) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 180
                        THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (2, 5) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
                        THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
                        THEN '补货已申请补货，补货后库存不足，补少了'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('丙丁级') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_level IN ('丙级2', '丁级2') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 200
                        THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('丙丁级') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (1, 3) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
                        THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('丙丁级') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (2, 5) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 90
                        THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
                WHEN stock_type = '库存不足' AND shelf_status_sim NOT IN ('正常货架')
                        THEN '货架状态导致库存不足的货架不能补货，风控介入核查'
                WHEN stock_type = '库存不足' AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0 AND IFNULL(requirement_times, 0) > 0
                        THEN '库存不足，有推单，无在途，提醒下单人员下补货单'
                WHEN stock_type = '库存不足' AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0 AND IFNULL(requirement_times, 0) <= 0
                        THEN '库存不足，无推单，无在途，检查补货逻辑，优化系统补货'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND stock_sku_type = 'SKU不足' AND shelf_level_sim IN ('甲乙级', '新装')
                        THEN '高销缺品，总部商品组适当上新品'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('甲乙级', '新装') AND IFNULL(quantity, 0) > 0
                        THEN '良好货架,不做处理'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('甲乙级', '新装') AND IFNULL(quantity, 0) <= 0
                        THEN '甲乙级新装货架库存充足，近7天0销，总部丁峰跟进'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('丙丁级') AND IFNULL(quantity, 0) > 0
                        THEN '丙丁级库存充足，近7天动销，不做处理'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('丙丁级') AND IFNULL(quantity, 0) <= 0
                        THEN '丙丁级库存充足，近7天0销，不做处理'
        ELSE '其它'
        END AS stype1,
        CASE
                WHEN stock_type = '库存不足' AND IFNULL(stock_quantity, 0) <= 0
                        THEN '总部风控'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_level IN ('甲级2', '乙级2') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 300
                        THEN '店主'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (1, 3) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 180
                        THEN '店主'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (2, 5) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
                        THEN '店主'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('甲乙级', '新装') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
                        THEN '总部补货组'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('丙丁级') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_level IN ('丙级2', '丁级2') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 200
                        THEN '店主'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('丙丁级') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (1, 3) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
                        THEN '店主'
                WHEN stock_type = '库存不足' AND shelf_level_sim IN ('丙丁级') AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0 AND shelf_type IN (2, 5) AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 90
                        THEN '店主'
                WHEN stock_type = '库存不足' AND shelf_status_sim NOT IN ('正常货架') 
                        THEN '总部风控'
                WHEN stock_type = '库存不足' AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0 AND IFNULL(requirement_times, 0) > 0
                        THEN '总部补货组'
                WHEN stock_type = '库存不足' AND shelf_status_sim IN ('正常货架') AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0 AND IFNULL(requirement_times, 0) <= 0
                        THEN '总部补货组'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND stock_sku_type = 'SKU不足' AND shelf_level_sim IN ('甲乙级', '新装')
                        THEN '总部商品'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('甲乙级', '新装') AND IFNULL(quantity, 0) > 0
                        THEN '无'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('甲乙级', '新装') AND IFNULL(quantity, 0) <= 0
                        THEN '总部丁峰'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('丙丁级') AND IFNULL(quantity, 0) > 0
                        THEN '无'
                WHEN stock_type = '其他' AND stock_quantity >= 90 AND shelf_level_sim IN ('丙丁级') AND IFNULL(quantity, 0) <= 0
                        THEN '总部丁峰'
                ELSE '其它'
        END AS stype_ren,
        if_all_time,
        if_prewarehouse,
        city_name,
        shelf_level_sim,
        shelf_status_sim,
        stock_type,
        stock_sku_type,
        COUNT(DISTINCT shelf_id) AS shelf_qty,
        SUM(IFNULL(queh_value, 0)) AS queh_value,
        SUM(IFNULL(stock_value, 0)) AS stock_value,
        SUM(IFNULL(stock_value_5, 0)) AS stock_value_5
FROM
        fe_dm.dm_fill_shelf_action_history a
WHERE shelf_status = 2
        AND shelf_type IN (1, 2, 3, 5)
        AND sdate = CURDATE()
GROUP BY stype1,stype_ren,if_all_time,if_prewarehouse,city_name,shelf_level_sim,shelf_status_sim,stock_type,stock_sku_type;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_fill_shelf_action_total_history_two","@time_9--@time_10",@time_9,@time_10);
    
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_fill_shelf_action_total_history_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_fill_shelf_action_history','dm_fill_shelf_action_total_history_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_fill_shelf_action_total_history','dm_fill_shelf_action_total_history_two','宋英南');
 
END