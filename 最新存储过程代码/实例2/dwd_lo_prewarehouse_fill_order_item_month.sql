CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_lo_prewarehouse_fill_order_item_month`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 2019/08/13
-- Modify date: 
-- Description:	
-- 	补货出入库订单流向中间表（每天的1时17分）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
DELETE FROM fe_dwd.dwd_lo_prewarehouse_fill_order_item_month
WHERE smonth= DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m');
INSERT INTO fe_dwd.dwd_lo_prewarehouse_fill_order_item_month (
  smonth,
  order_id,
  SUPPLIER_ID,
  shelf_id,
  task_id,
  fill_type,
  FILL_TIME,
  PRODUCT_ID,
  QUALITY_NUM,
  PURCHASE_PRICE
)
      SELECT
        DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m') AS smonth,
        a.order_id,   -- 订单
        a.supplier_id,   -- 供出方ID
        a.shelf_id,      -- 补入方ID     
        c.task_id,       -- 串点任务ID
        1 AS fill_type,
        a.fill_time,
        a.product_id,    -- 补给的商品ID
        a.actual_send_num,   -- 补给商品数量
        e.purchase_price    -- 补给商品采购价
      FROM
        fe_dwd.`dwd_fill_day_inc` a
      LEFT JOIN
        fe_dwd.dwd_sf_order_logistics_task_record c
      ON a.order_id = c.order_id
      AND c.data_flag=1
      LEFT JOIN
        fe_dwd.dwd_product_base_day_all d
      ON a.product_id = d.product_id
      LEFT JOIN
        (SELECT DISTINCT
           m.fnumber,
           m.f_bgj_poprice AS purchase_price
        FROM
          fe_dwd.dwd_sserp_t_bd_material m) e
      ON d.product_code2 = e.fnumber
      WHERE a.apply_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
      AND a.apply_time < CURRENT_DATE
      AND a.supplier_type NOT IN (9)
      AND a.shelf_id IN (SELECT DISTINCT k.shelf_id FROM fe_dwd.dwd_shelf_base_day_all k WHERE k.shelf_type=9 AND k.data_flag=1)  -- 前置仓入库订单
      UNION
      SELECT
        DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m') AS smonth,
        a.order_id,
        a.supplier_id,
        a.shelf_id,
        c.task_id,
        2 AS fill_type,
        a.fill_time,
        a.product_id,
        a.actual_send_num,
        e.purchase_price
      FROM
        fe_dwd.`dwd_fill_day_inc` a
      LEFT JOIN
        fe_dwd.dwd_sf_order_logistics_task_record c
      ON a.order_id = c.order_id
      AND c.data_flag=1
      LEFT JOIN
        fe_dwd.dwd_product_base_day_all d
      ON a.product_id = d.product_id
      LEFT JOIN
        (SELECT DISTINCT
           m.fnumber,
           m.f_bgj_poprice AS purchase_price
        FROM
          fe_dwd.`dwd_sserp_t_bd_material` m) e
      ON d.product_code2 = e.fnumber
      WHERE a.apply_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
      AND a.apply_time < CURRENT_DATE
      AND a.supplier_type IN (9)
      AND a.shelf_id NOT IN (SELECT DISTINCT k.shelf_id FROM fe_dwd.dwd_shelf_base_day_all k WHERE k.shelf_type=9 AND k.data_flag=1)   -- 前置仓出库订单
      UNION
      SELECT
        DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m') AS smonth,
        a.order_id,
        a.supplier_id,
        a.shelf_id,
        c.task_id,
        3 AS fill_type,
        a.fill_time,
        a.product_id,
        a.actual_send_num,
        e.purchase_price
      FROM
        fe_dwd.`dwd_fill_day_inc` a
      LEFT JOIN
        fe_dwd.dwd_sf_order_logistics_task_record c
      ON a.order_id = c.order_id
      AND c.data_flag=1
      LEFT JOIN
        fe_dwd.dwd_product_base_day_all d
      ON a.product_id = d.product_id
      LEFT JOIN
        (SELECT DISTINCT
           m.fnumber,
           m.f_bgj_poprice AS purchase_price
        FROM
          fe_dwd.`dwd_sserp_t_bd_material` m) e
      ON d.product_code2 = e.fnumber
      WHERE a.apply_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
      AND a.apply_time < CURRENT_DATE
      AND a.supplier_type IN (9)
      AND a.shelf_id IN (SELECT DISTINCT k.shelf_id FROM fe_dwd.dwd_shelf_base_day_all k WHERE k.shelf_type=9 AND k.data_flag=1);    -- 前置站间调货订单
      
      -- 更新上月在途补货订单的上架时间
      UPDATE fe_dwd.dwd_lo_prewarehouse_fill_order_item_month a
      JOIN (SELECT
         t.order_id,
         t.apply_time,
         t.fill_time
       FROM
         fe_dwd.`dwd_fill_day_inc` t
       WHERE t.apply_time >= DATE_SUB(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),INTERVAL 1 MONTH)
       AND t.apply_time < DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
       GROUP BY t.order_id) b
      ON a.order_id = b.order_id
      SET a.fill_time = b.fill_time
      WHERE a.smonth = DATE_FORMAT(DATE_SUB(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),INTERVAL 1 DAY),'%Y%m')
      AND a.data_flag=1
      AND a.fill_time IS NULL
      AND b.fill_time IS NOT NULL;
	  
	  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_lo_prewarehouse_fill_order_item_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_lo_prewarehouse_fill_order_item_month','dwd_lo_prewarehouse_fill_order_item_month','蔡松林');
COMMIT;   
END