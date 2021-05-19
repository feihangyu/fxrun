CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_shelf_manager_monitor_result`()
BEGIN
-- =============================================
-- Author:	物流店主组
-- Create date: 2019/07/8
-- Modify date: 
-- Description:	
-- 	DW层宽表，BI平台上店主看板/店主监控看板的模型宽表（每天1时25分更新）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
--   保留4个月的历史统计数据
  DELETE
  FROM
    fe_dm.dm_shelf_manager_monitor_result
  WHERE stadate = DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)
  OR stadate < DATE_SUB(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)-DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1,INTERVAL 3 MONTH);
--   插入店主维度统计数据
  INSERT INTO fe_dm.dm_shelf_manager_monitor_result (
    city,
    BRANCH_CODE,
    sf_code,
    real_name,
    manager_id,
    manager_type,
    shelf_id,
    shelf_code,
    shelf_status,
    REVOKE_STATUS,
    shelf_type,
    shelf_level,
    ACTIVATE_TIME,
    REVOKE_TIME,
    WHETHER_CLOSE,
    GMV,
    AMOUNT,
    quantity,
    monthEnd_operateQty,
    max_operate_time,
    valid_fillQty,
    valid_cargoQty,
    max_fill_time,
    loss_value,
    loss_divisor,
    new_stock_amount,
    new_stock_qty,
    PACKAGE_MODEL,
    yesterday_GMV,
    yesterday_AMOUNT,
    yesterday_qty,
    yesterday_valid_fillQty,
    yesterday_valid_cargoQty,
    actual_send_num,
    payment_money,
    stadate
  )
SELECT
    a.city_name AS '城市',
    a.BRANCH_CODE AS '分部代码',
    a.sf_code AS '顺丰工号',
    a.real_name AS '店主名称',
    a.manager_id AS '店主ID',
    a.`manager_type` AS '是否全职店主',
    a.shelf_id AS '货架ID',
    a.shelf_code AS '货架编码',
    a.shelf_status AS '货架状态',
    a.REVOKE_STATUS AS '撤架状态',
    a.shelf_type AS '货架类型',
    a.`grade` AS '货架等级',
    a.ACTIVATE_TIME AS '激活时间',
    a.REVOKE_TIME AS '撤架时间',
    a.WHETHER_CLOSE AS '是否关闭',
    c.GMV AS 'GMV',
    c.AMOUNT AS '实收',
    e.qty AS '盘点次数',
    e.yuedi_qty AS '月底盘点次数',
    e.max_OPERATE_TIME AS '最近一次盘点时间',
    f.youx_buh AS '有效补货次数',
    g.youx_diaoh AS '有效调货次数',
    f.max_fill_time AS '最近一次补货时间',
    h.huos_value AS '盗损金额',
    h.huosun_fenmu AS '盗损金额+GMV',
    i.stock_value_new AS '现在库存金额',
    i.stock_qty_new AS '现在库存数量',
    j.PACKAGE_MODEL AS '货架组合类型',
    c.y_GMV AS '昨日GMV',
    c.y_AMOUNT AS '昨日实收',
    e.y_qty AS '昨日盘点次数',
    f.y_youx_buh AS '昨日有效补货次数',
    g.y_youx_diaoh AS '昨日有效调货次数',
    w.ACTUAL_SEND_NUM AS '前七天补货在途数量',
    m.PAYMENT_MONEY AS '补款金额',
    DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AS '截存日期'  -- 更新统计结果数据的日期（数据含该日期的）
  FROM
    fe_dwd.dwd_shelf_base_day_all a
    LEFT JOIN
      (SELECT
        s.shelf_id,
        SUM(s.pay_amount) AS AMOUNT,
        SUM(s.y_AMOUNT) AS y_AMOUNT,
        SUM(s.GMV) AS GMV,
        SUM(s.y_GMV) AS y_GMV
      FROM
        (SELECT
          f.shelf_id,
          f.`ORDER_ID`,
          f.`PAY_AMOUNT`* COUNT(DISTINCT f.pay_id)-SUM(IFNULL(f.refund_amount,0)) AS pay_amount,
          CASE
            WHEN DATE_FORMAT(f.pay_date, '%Y%m%d') = DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            )
            THEN f.`PAY_AMOUNT`* COUNT(DISTINCT f.pay_id)-SUM(IFNULL(f.refund_amount,0))
          END AS y_AMOUNT,
          SUM(IF(f.refund_amount>0,f.quantity_act,f.`QUANTITY`) * f.`SALE_PRICE`) AS GMV,
          SUM(
            CASE
              WHEN DATE_FORMAT(f.pay_date, '%Y%m%d') = DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%Y%m%d'
              )
              THEN IF(f.refund_amount>0,f.quantity_act,f.`QUANTITY`) * f.`SALE_PRICE`
            END
          ) AS y_GMV
        FROM
          fe_dwd.`dwd_order_item_refund_day` f
        WHERE f.pay_date >= DATE_ADD(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
          )
          AND f.pay_date < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        GROUP BY f.`ORDER_ID`) s
      GROUP BY s.shelf_id) c    -- 销售数据
      ON a.shelf_id = c.shelf_id
    LEFT JOIN
      (SELECT
        b.SHELF_ID,
        COUNT(
          DISTINCT DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d')
        ) AS qty,
        COUNT(
          DISTINCT
          CASE
            WHEN DAY(b.OPERATE_TIME) >= 25
            THEN DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d')
          END
        ) AS yuedi_qty,
        MAX(b.OPERATE_TIME) AS max_OPERATE_TIME,
        COUNT(
          DISTINCT
          CASE
            WHEN DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d') = DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            )
            THEN DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d')
          END
        ) AS y_qty
      FROM
        fe_dwd.`dwd_check_base_day_inc` b
      WHERE b.operate_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND b.operate_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        AND b.DATA_FLAG = 1
      GROUP BY b.SHELF_ID) e   -- 盘点数据
      ON a.shelf_id = e.shelf_id
    LEFT JOIN
      (SELECT
        f.shelf_id,
        COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10
            THEN DATE_FORMAT(f.FILL_TIME, '%Y%m%d')
          END
        ) AS youx_buh,
        COUNT(
          DISTINCT
          CASE
            WHEN ABS(f.PRODUCT_NUM) > 10
            AND DATE_FORMAT(f.FILL_TIME, '%Y%m%d') = DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            )
            THEN DATE_FORMAT(f.FILL_TIME, '%Y%m%d')
          END
        ) AS y_youx_buh,
        MAX(f.fill_time) AS max_fill_time
      FROM
        fe_dwd.`dwd_fill_day_inc` f
      WHERE f.order_status IN (3, 4)
        AND f.shelf_id IN (SELECT DISTINCT h.shelf_id FROM fe_dwd.dwd_shelf_base_day_all h WHERE h.shelf_type <> 9)
        AND f.supplier_type <> 1
        AND f.fill_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND f.fill_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      GROUP BY f.shelf_id) f    -- 货架补货数据
      ON a.shelf_id = f.shelf_id
    LEFT JOIN
      (SELECT
        g.shelf_id,
        COUNT(
          DISTINCT
          CASE
            WHEN g.PRODUCT_NUM > 10
            THEN DATE_FORMAT(g.FILL_TIME, '%Y%m%d')
          END
        ) AS youx_diaoh,
        COUNT(
          DISTINCT
          CASE
            WHEN g.PRODUCT_NUM > 10
            AND DATE_FORMAT(g.FILL_TIME, '%Y%m%d') = DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            )
            THEN DATE_FORMAT(g.FILL_TIME, '%Y%m%d')
          END
        ) AS y_youx_diaoh,
        MAX(g.fill_time) AS max_transfer_time
      FROM
        fe_dwd.`dwd_fill_day_inc` g      
      WHERE g.order_status IN (3, 4)
        AND g.shelf_id IN (SELECT DISTINCT s.shelf_id FROM fe_dwd.dwd_shelf_base_day_all s WHERE s.shelf_type <> 9)
        AND g.supplier_type = 1
        AND g.fill_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND g.fill_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      GROUP BY g.shelf_id) g    -- 货架调货数据
      ON a.shelf_id = g.shelf_id
    LEFT JOIN
      (SELECT
        m.shelf_id,
        (
          IFNULL(m.huosun, 0) + IFNULL(m.bk_money, 0) - IFNULL(m.total_error_value, 0)
        ) AS huos_value,
        (
          ABS(
            IFNULL(m.huosun, 0) + IFNULL(m.bk_money, 0) - IFNULL(m.total_error_value, 0)
          ) + ABS(IFNULL(m.sale_value, 0))
        ) AS huosun_fenmu
      FROM
        fe_dm.dm_pj_zs_goods_damaged m
      WHERE m.smonth = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m'
        )) h    -- 货架货损数据
      ON a.shelf_id = h.shelf_id
    LEFT JOIN
      (SELECT
        a.shelf_id,
        a.stock_sum AS stock_value_new,
        a.stock_quantity AS stock_qty_new
      FROM
        fe_dwd.`dwd_shelf_day_his` a
      WHERE a.sdate = CURRENT_DATE) i   -- 库存数据（近似更新日当天库存）
      ON a.shelf_id = i.shelf_id
    LEFT JOIN
        (SELECT
          a.MAIN_SHELF_ID,
          MAX(a.PACKAGE_MODEL) AS PACKAGE_MODEL
        FROM
          fe_dwd.`dwd_sf_shelf_relation_record` a
        WHERE a.DATA_FLAG = 1
          AND a.SHELF_HANDLE_STATUS = 9
        GROUP BY a.MAIN_SHELF_ID) j    -- 关联货架数据
      ON a.shelf_id = j.MAIN_SHELF_ID
    LEFT JOIN
      (SELECT
        spfo.SHELF_ID,
        SUM(spfo.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM
      FROM
        fe_dwd.`dwd_fill_day_inc` spfo
      WHERE spfo.ORDER_STATUS IN (2,5)
        AND spfo.`SUPPLIER_TYPE` IN (2,9)
        AND spfo.apply_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND spfo.apply_time < CURDATE()
      GROUP BY spfo.SHELF_ID) w   -- 补货订单在途商品数据
      ON a.shelf_id = w.shelf_id
    LEFT JOIN
      (SELECT
        p.shelf_id,
        SUM(p.PAYMENT_MONEY) AS PAYMENT_MONEY
      FROM
        fe_dwd.`dwd_sf_after_payment` p
      WHERE p.payment_date >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND p.payment_date < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        AND p.PAYMENT_STATUS = 2
      GROUP BY p.shelf_id) m    -- 补付款数据
      ON a.shelf_id = m.shelf_id
  WHERE a.shelf_status IN (2, 5)
      OR (
        a.revoke_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND a.revoke_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      );
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_shelf_manager_monitor_result',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('蔡松林@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_manager_monitor_result','dm_shelf_manager_monitor_result','蔡松林');
 
END