CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_buhuo_shelf_action`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @month_id := DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH),'%Y-%m');  
SET @pre_week := WEEKOFYEAR(DATE_SUB(CURDATE(), INTERVAL 8 DAY));
  DELETE
  FROM
    feods.zs_buhuo_shelf_action_history
  WHERE sdate = CURDATE();
  DELETE
  FROM
    feods.zs_buhuo_shelf_action_history
  WHERE sdate = DATE_SUB(CURDATE(), INTERVAL 15 DAY);
SET @time_3 := CURRENT_TIMESTAMP();
  INSERT INTO feods.zs_buhuo_shelf_action_history (
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
      WHEN t4.REVOKE_STATUS = 1
      AND t4.WHETHER_CLOSE = 1
      THEN '关闭未撤架'
      WHEN t4.REVOKE_STATUS <> 1
      AND t4.WHETHER_CLOSE = 1
      THEN '关闭撤架过程中'
      WHEN t4.REVOKE_STATUS <> 1
      AND t4.WHETHER_CLOSE = 2
      THEN '未关闭撤架过程中'
      WHEN t4.REVOKE_STATUS = 1
      AND t4.WHETHER_CLOSE = 2
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
      WHEN t4.shelf_type IN (1, 3)
      AND t1.stock_sku < 30
      THEN 'SKU不足'
      WHEN t4.shelf_type IN (2, 5)
      AND t1.stock_sku < 10
      THEN 'SKU不足'
      WHEN t4.shelf_type = 8
      AND t1.stock_sku < 15
      THEN 'SKU不足'
      ELSE '其他'
    END AS stock_sku_type,
    min_apply_time,
    min_send_time
  FROM
    (#库存
     SELECT
      a.shelf_id,
      sum(a.stock_quantity>0) AS stock_sku,
      SUM(a.stock_quantity) AS stock_quantity,
      SUM(a.stock_quantity * a.sale_price) AS stock_value,
      SUM(IF(a.SALES_FLAG = 5 AND a.NEW_FLAG = 2,a.stock_quantity,0)) AS stock_quantity_5,
      SUM(IF(a.SALES_FLAG = 5 AND a.NEW_FLAG = 2,a.stock_quantity * a.sale_price,0)) AS stock_value_5
    FROM
      fe_dwd.`dwd_shelf_product_day_all` a
     GROUP BY a.shelf_id
     ) t1 
LEFT JOIN feods.d_op_shelf_grade t2
      ON t1.shelf_id = t2.shelf_id
      AND t2.month_id = @month_id
LEFT JOIN
      (#补货情况
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
        AND a.apply_time > SUBDATE(CURDATE(),INTERVAL 6 MONTH)
       GROUP BY a.shelf_id) t3
      ON t1.shelf_id = t3.shelf_id
    LEFT JOIN
      (#店主管理货架数
       SELECT
        a.city_name,
        IF(a.manager_type = '全职店主','全职店主','非全职店主') AS if_all_time,
        CASE
          WHEN e.shelf_id IS NOT NULL
          THEN '前置仓覆盖货架'
          ELSE '普通货架'
        END AS if_prewarehouse,
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
          (SELECT
            manager_id,
            COUNT(shelf_id) AS shelf_qty
          FROM
            fe_dwd.`dwd_shelf_base_day_all` 
          WHERE SHELF_STATUS = 2
          GROUP BY manager_id) b
          ON a.manager_id = b.manager_id
        LEFT JOIN
          (SELECT DISTINCT
            t.shelf_id
          FROM
            fe.sf_prewarehouse_shelf_detail t
          WHERE t.data_flag = 1) e
          ON a.shelf_id = e.shelf_id
      WHERE SHELF_STATUS = 2) t4
      ON t1.shelf_id = t4.shelf_id
    LEFT JOIN
      (#缺货损失
       SELECT
        shelf_id,
        SUM(abc) AS queh_value,
        SUM(abc1) AS queh_value1
      FROM
        (SELECT
          a.shelf_id,
          a.PRODUCT_ID,
          b.STOCK_QUANTITY,
          b.SALE_PRICE,
          c.quantity,
          CASE
            WHEN b.STOCK_QUANTITY <= 0
            THEN c.quantity / 7 * b.SALE_PRICE
          END AS abc,
          CASE
            WHEN b.STOCK_QUANTITY <= c.quantity / 7 * 2
            THEN c.quantity / 7 * b.SALE_PRICE
          END AS abc1
        FROM
          fe.sf_shelf_product_weeksales_detail a
          LEFT JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
            ON a.shelf_id = b.SHELF_ID
            AND a.PRODUCT_ID = b.PRODUCT_ID
          LEFT JOIN
            (SELECT
              a.shelf_id,
              a.PRODUCT_ID,
              SUM(
                CASE
                  WHEN a.order_status = 2
                  THEN a.quantity
                  ELSE a.quantity_shipped
                END
              ) AS quantity
            FROM
              `fe_dwd`.`dwd_order_item_refund_day` a
            WHERE a.order_status IN (2, 6, 7)
                AND @pre_week =WEEKOFYEAR(pay_date) 
            GROUP BY a.shelf_id,a.PRODUCT_ID) c
            ON a.shelf_id = c.SHELF_ID
            AND a.PRODUCT_ID = c.PRODUCT_ID
        WHERE a.STAT_DATE = DATE_SUB(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL (
              CASE
                WHEN DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                ) = 0
                THEN 7
                ELSE DATE_FORMAT(
                  DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                  '%w'
                )
              END
            ) DAY
          )
          AND a.sales_flag IN (1, 2, 3)) t1
      GROUP BY shelf_id) t5
      ON t1.shelf_id = t5.shelf_id
    LEFT JOIN
      (#补货需求申请
        SELECT 
                a.`shelf_id`,
                COUNT(DISTINCT a.`sday`) AS requirement_times,
                MAX(DATE(a.add_time)) AS max_sdate
        FROM 
                feods.`d_op_sf_fillorder_requirement_his` a
                JOIN feods.`d_op_sf_fillorder_requirement_item_his` b
                        ON a.sday = b.`sday`
                        AND a.`requirement_id` = b.`requirement_id`
                        AND a.sday > DAY(SUBDATE(CURDATE(),INTERVAL 7 DAY))
        WHERE ((a.`total_price` > 100 AND a.`supplier_type` = 9) OR (a.`total_price` > 150 AND a.`supplier_type` = 2))
        GROUP BY a.`shelf_id`
      ) t6
      ON t1.shelf_id = t6.shelf_id
    LEFT JOIN
      (SELECT
        a.shelf_id,
        SUM(
          CASE
            WHEN ORDER_STATUS = 2
            THEN QUANTITY
            ELSE a.quantity_shipped
          END
        ) AS QUANTITY,
        SUM(
          CASE
            WHEN ORDER_STATUS = 2
            THEN QUANTITY * SALE_PRICE
            ELSE a.quantity_shipped * a.SALE_PRICE
          END
        ) AS gmv
      FROM
        fe_dwd.`dwd_order_item_refund_day` a
      WHERE pay_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        AND pay_date < CURDATE()
        AND ORDER_STATUS IN (2, 6, 7)
      GROUP BY a.shelf_id ) t7
      ON t1.shelf_id = t7.shelf_id;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_buhuo_shelf_action","@time_3--@time_5",@time_3,@time_5);
      
      
      
  DELETE
  FROM
    feods.pj_buhuo_shelf_action_total_history
  WHERE sdate = CURDATE();
SET @time_8 := CURRENT_TIMESTAMP();
  INSERT INTO feods.pj_buhuo_shelf_action_total_history (
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
      WHEN stock_type = '库存不足'
      AND IFNULL(stock_quantity, 0) <= 0
      THEN '库存异常，风控介入'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('甲级2', '乙级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 300
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 180
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      THEN '补货已申请补货，补货后库存不足，补少了'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('丙级2', '丁级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 200
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 90
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim NOT IN ('正常货架')
      THEN '货架状态导致库存不足的货架不能补货，风控介入核查'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) > 0
      THEN '库存不足，有推单，无在途，提醒下单人员下补货单'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) <= 0
      THEN '库存不足，无推单，无在途，检查补货逻辑，优化系统补货'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND stock_sku_type = 'SKU不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      THEN '高销缺品，总部商品组适当上新品'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) > 0
      THEN '良好货架,不做处理'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) <= 0
      THEN '甲乙级新装货架库存充足，近7天0销，总部丁峰跟进'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) > 0
      THEN '丙丁级库存充足，进7天动销，不做处理'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) <= 0
      THEN '丙丁级库存充足，进7天0销，不做处理'
      ELSE '其它'
    END AS stype1,
    CASE
      WHEN stock_type = '库存不足'
      AND IFNULL(stock_quantity, 0) <= 0
      THEN '总部风控'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('甲级2', '乙级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 300
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 180
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      THEN '总部补货组'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('丙级2', '丁级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 200
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 90
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim NOT IN ('正常货架')
      THEN '总部风控'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) > 0
      THEN '总部补货组'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) <= 0
      THEN '总部补货组'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND stock_sku_type = 'SKU不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      THEN '总部商品'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) > 0
      THEN '无'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) <= 0
      THEN '总部丁峰'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) > 0
      THEN '无'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) <= 0
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
    feods.zs_buhuo_shelf_action_history a
  WHERE shelf_status = 2
    AND shelf_type IN (1, 2, 3, 5)
    AND sdate = CURDATE()
  GROUP BY
    CASE
      WHEN stock_type = '库存不足'
      AND IFNULL(stock_quantity, 0) <= 0
      THEN '库存异常，风控介入'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('甲级2', '乙级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 300
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 180
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      THEN '补货已申请补货，补货后库存不足，补少了'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('丙级2', '丁级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 200
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 90
      THEN '补货已申请补货，补货后库存充足，提醒小哥及时上架'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim NOT IN ('正常货架')
      THEN '货架状态导致库存不足的货架不能补货，风控介入核查'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) > 0
      THEN '库存不足，有推单，无在途，提醒下单人员下补货单'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) <= 0
      THEN '库存不足，无推单，无在途，检查补货逻辑，优化系统补货'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND stock_sku_type = 'SKU不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      THEN '高销缺品，总部商品组适当上新品'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) > 0
      THEN '良好货架,不做处理'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) <= 0
      THEN '甲乙级新装货架库存充足，近7天0销，总部丁峰跟进'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) > 0
      THEN '丙丁级库存充足，进7天动销，不做处理'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) <= 0
      THEN '丙丁级库存充足，进7天0销，不做处理'
      ELSE '其它'
    END,
    CASE
      WHEN stock_type = '库存不足'
      AND IFNULL(stock_quantity, 0) <= 0
      THEN '总部风控'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('甲级2', '乙级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 300
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 180
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      THEN '总部补货组'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_level IN ('丙级2', '丁级2')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 200
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (1, 3)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 110
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_level_sim IN ('丙丁级')
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) > 0
      AND shelf_type IN (2, 5)
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) + IFNULL(stock_quantity, 0) >= 90
      THEN '店主'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim NOT IN ('正常货架')
      THEN '总部风控'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) > 0
      THEN '总部补货组'
      WHEN stock_type = '库存不足'
      AND shelf_status_sim IN ('正常货架')
      AND IFNULL(apply_qty, 0) + IFNULL(send_qty, 0) <= 0
      AND IFNULL(requirement_times, 0) <= 0
      THEN '总部补货组'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND stock_sku_type = 'SKU不足'
      AND shelf_level_sim IN ('甲乙级', '新装')
      THEN '总部商品'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) > 0
      THEN '无'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('甲乙级', '新装')
      AND IFNULL(quantity, 0) <= 0
      THEN '总部丁峰'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) > 0
      THEN '无'
      WHEN stock_type = '其他'
      AND stock_quantity >= 90
      AND shelf_level_sim IN ('丙丁级')
      AND IFNULL(quantity, 0) <= 0
      THEN '总部丁峰'
      ELSE '其它'
    END,
    if_all_time,
    if_prewarehouse,
    city_name,
    shelf_level_sim,
    shelf_status_sim,
    stock_type,
    stock_sku_type;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_buhuo_shelf_action","@time_8--@time_10",@time_8,@time_10);
    
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_buhuo_shelf_action',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
    
    
  COMMIT;
END