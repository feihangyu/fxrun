CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_zs_shelf_manager_monitor_result`()
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
--   保留4个月的历史统计数据
  DELETE
  FROM
    feods.`zs_shelf_manager_monitor_result`
  WHERE stadate = DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)
  OR stadate < DATE_SUB(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)-DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1,INTERVAL 3 MONTH);
-- 每天增量更新数据 
-- DELETE
-- FROM
--   feods.`zs_shelf_manager_monitor_result`
-- WHERE stadate = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY);
--   插入店主维度统计数据
  INSERT INTO feods.zs_shelf_manager_monitor_result (
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
    k.city_name AS '城市',
    b.BRANCH_CODE AS '分部代码',
    b.sf_code AS '顺丰工号',
    b.real_name AS '店主名称',
    a.manager_id AS '店主ID',
    CASE
      WHEN b.second_user_type = 1
      THEN '全职店主'
      when b.second_user_type = 2
      then '兼职店主'
    END AS '是否全职店主',
    a.shelf_id AS '货架ID',
    a.shelf_code AS '货架编码',
    a.shelf_status AS '货架状态',
    a.REVOKE_STATUS AS '撤架状态',
    a.shelf_type AS '货架类型',
    LEFT(sf.shelf_level,2) AS '货架等级',
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
    f.max_fill_time as '最近一次补货时间',
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
    fe.sf_shelf a
    LEFT JOIN fe.pub_shelf_manager b
      ON a.manager_id = b.manager_id
      and b.data_flag =1 
    LEFT JOIN feods.`fjr_city_business` k
      ON a.city = k.city
    LEFT JOIN feods.`pj_shelf_level_ab` sf
      ON a.shelf_id = sf.shelf_id
      AND STR_TO_DATE(CONCAT(sf.smonth,'01'),'%Y%m%d')=date_sub(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),interval 1 month)
    LEFT JOIN
      (SELECT
        s.shelf_id,
        SUM(s.AMOUNT) AS AMOUNT,
        SUM(s.y_AMOUNT) AS y_AMOUNT,
        SUM(s.GMV) AS GMV,
        SUM(s.y_GMV) AS y_GMV
      FROM
        (SELECT
          f.shelf_id,
          f.`ORDER_ID`,
          f.`PRODUCT_TOTAL_AMOUNT` AS AMOUNT,
          CASE
            WHEN DATE_FORMAT(f.ORDER_DATE, '%Y%m%d') = DATE_FORMAT(
              DATE_SUB(CURDATE(), INTERVAL 1 DAY),
              '%Y%m%d'
            )
            THEN f.PRODUCT_TOTAL_AMOUNT
          END AS y_AMOUNT,
          SUM(if(f.order_status = 2,e.QUANTITY,e.quantity_shipped) * e.SALE_PRICE) AS GMV,
          SUM(
            CASE
              WHEN DATE_FORMAT(f.ORDER_DATE, '%Y%m%d') = DATE_FORMAT(
                DATE_SUB(CURDATE(), INTERVAL 1 DAY),
                '%Y%m%d'
              )
              THEN IF(f.order_status = 2,e.QUANTITY,e.quantity_shipped) * e.SALE_PRICE
            END
          ) AS y_GMV
        FROM
          fe.sf_order_item AS e,
          fe.sf_order AS f
        WHERE e.order_id = f.ORDER_ID
          AND f.ORDER_STATUS in (2,6,7)
          AND f.order_date >= DATE_ADD(
            DATE_SUB(CURDATE(), INTERVAL 1 DAY),
            INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
          )
          AND f.order_date < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
          and e.data_flag =1 and f.data_flag =1 
        GROUP BY f.SHELF_ID,
          f.`ORDER_ID`) s
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
        fe.sf_shelf_check b
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
        max(f.fill_time) as max_fill_time
      FROM
        fe.sf_product_fill_order f
      WHERE f.order_status IN (3, 4)
--         AND f.fill_type IN (1, 2, 8, 9, 4, 7)
        and f.shelf_id in (select distinct h.shelf_id from fe.`sf_shelf` h where h.data_flag = 1 and h.shelf_type <> 9)
        and f.supplier_type <> 1
        AND f.fill_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND f.fill_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        and f.data_flag =1 
      GROUP BY f.shelf_id) f    -- 货架补货数据
      ON a.shelf_id = f.shelf_id
    LEFT JOIN
      (SELECT
        g.shelf_id,
        COUNT(DISTINCT DATE_FORMAT(g.FILL_TIME, '%Y%m%d')),
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
        max(g.fill_time) as max_transfer_time
      FROM
        fe.sf_product_fill_order g      
      WHERE g.order_status IN (3, 4)
--         AND g.fill_type IN (6, 11)
        and g.shelf_id in (select distinct s.shelf_id from fe.`sf_shelf` s where s.data_flag =1 and s.shelf_type <> 9)
        and g.supplier_type = 1
        AND g.fill_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND g.fill_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        and g.data_flag = 1
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
        feods.pj_zs_goods_damaged m
      WHERE m.smonth = DATE_FORMAT(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          '%Y%m'
        )) h    -- 货架货损数据
      ON a.shelf_id = h.shelf_id
    LEFT JOIN
      (SELECT
        a.shelf_id,
        SUM(a.stock_quantity * a.sale_price) AS stock_value_new,
        SUM(a.stock_quantity) AS stock_qty_new
      FROM
        fe.sf_shelf_product_detail a
      where a.data_flag = 1 
      GROUP BY a.shelf_id) i   -- 库存数据（近似更新日当天库存）
      ON a.shelf_id = i.shelf_id
    LEFT JOIN
        (SELECT
          a.MAIN_SHELF_ID,
          MAX(a.PACKAGE_MODEL) AS PACKAGE_MODEL
        FROM
          fe.sf_shelf_relation_record a
        WHERE a.DATA_FLAG = 1
          AND a.SHELF_HANDLE_STATUS = 9
        GROUP BY a.MAIN_SHELF_ID) j    -- 关联货架数据
      ON a.shelf_id = j.MAIN_SHELF_ID
    LEFT JOIN
      (SELECT
        spfi.SHELF_ID,
        SUM(spfi.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM
      FROM
        fe.sf_product_fill_order_item AS spfi,
        fe.sf_product_fill_order AS spfo
      WHERE spfo.ORDER_ID = spfi.ORDER_ID
        AND spfo.DATA_FLAG = 1
        and spfi.data_flag = 1 
        AND spfo.ORDER_STATUS IN (1, 2)
        AND spfo.apply_time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        AND spfo.apply_time < CURDATE()
      GROUP BY spfi.SHELF_ID) w   -- 补货订单在途商品数据
      ON a.shelf_id = w.shelf_id
    LEFT JOIN
      (SELECT
        p.shelf_id,
        SUM(p.PAYMENT_MONEY) AS PAYMENT_MONEY
      FROM
        fe.sf_after_payment p
      WHERE p.payment_date >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND p.payment_date < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
        AND p.PAYMENT_STATUS = 2
      GROUP BY p.shelf_id) m    -- 补付款数据
      ON a.shelf_id = m.shelf_id
  WHERE a.data_flag =1 
    and (
      a.shelf_status IN (2, 5)
      OR (
        a.revoke_time >= DATE_ADD(
          DATE_SUB(CURDATE(), INTERVAL 1 DAY),
          INTERVAL - DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)) + 1 DAY
        )
        AND a.revoke_time < DATE_ADD(LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 DAY)),INTERVAL 1 DAY)
      )
    );
    
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_zs_shelf_manager_monitor_result',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
COMMIT;
END