CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_dm_lo_order_logistics_task_data`()
BEGIN
  -- =============================================
-- Author:	运作串点任务
-- Create date: 2020/06/28
-- Modify date: 
-- Description:	
--    全量覆盖物流串点任务结果表（每天的）
-- 
-- =============================================
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
-- 可进行串点任务订单明细
DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_order_p;
CREATE TEMPORARY TABLE fe_dm.fill_order_p(KEY idx_ORDER_ID (ORDER_ID)) AS
SELECT
  a.`ORDER_ID`,
  d.task_id, 
  SUM(b.`ACTUAL_SEND_NUM` * p.F_BGJ_POPRICE) p1
FROM
  fe.`sf_product_fill_order` a
  JOIN fe.`sf_product_fill_order_item` b
    ON a.`ORDER_ID` = b.`ORDER_ID`
    AND a.`DATA_FLAG` = 1
    AND b.`DATA_FLAG` = 1
  JOIN fe.`sf_order_logistics_task_record` d
    ON a.`ORDER_ID` = d.`ORDER_ID`
LEFT JOIN (SELECT c.`PRODUCT_ID`, m.F_BGJ_POPRICE
FROM fe.`sf_product` c
LEFT JOIN (SELECT DISTINCT FNUMBER,F_BGJ_POPRICE
FROM sserp.T_BD_MATERIAL) m
    ON c.`PRODUCT_CODE2` = m.`FNUMBER`
WHERE c.`DATA_FLAG` = 1) p
    ON b.`PRODUCT_ID` = p.`PRODUCT_ID`
WHERE a.`DATA_FLAG` = 1
GROUP BY a.`ORDER_ID`
;
-- 已创建物流任务总订单金额
DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_task_p;
CREATE TEMPORARY TABLE fe_dm.fill_task_p(KEY idx_task_id (task_id)) AS
SELECT
  task_id,SUM(p1) price
FROM fe_dm.fill_order_p
GROUP BY task_id
;
-- 删除前一天数据，并把昨天数据逻辑删除
DELETE FROM fe_dm.`dm_lo_order_logistics_task_data` WHERE DATA_FLAG = 2;
UPDATE fe_dm.`dm_lo_order_logistics_task_data` SET DATA_FLAG = 2;
-- 插入截至到昨天最新数据
INSERT INTO fe_dm.`dm_lo_order_logistics_task_data` (
 `business_name`,
  `task_id`,
  `task_no`,
  `task_status`,
  `create_time`,
  `execute_start_time`,
  `execute_finish_time`,
  `order_id`,
  `total_price`,
  `shelf_id`,
  `SHELF_CODE`,
  `all_cost_money`,
  `share_cost_money`,
  `vehicle_type_name`,
  `logistics_manager_name`,
  `logistics_excute_name`
)
SELECT
  s.`business_name`,
  t.`task_id`,
  t.`task_no`,
  t.`task_status`,
  t.`add_time` create_time,
  t.`execute_start_time`,
  t.`execute_finish_time`,
  tr.`order_id`,
  ROUND(o.`p1`,2) total_price,
  s.`shelf_id`,
  s.`SHELF_CODE`,
  t.cost_money all_cost_money,
  ROUND((o.`p1`/t1.price) * t.cost_money,2) share_cost_money,
  c.`vehicle_type_name`,
  t.`logistics_manager_name`,
  t.`logistics_excute_name`
FROM
  fe.`sf_order_logistics_task` t
  LEFT JOIN fe.`sf_order_logistics_task_record` tr
    ON t.`task_id` = tr.`task_id`
    AND tr.`data_flag` = 1
  LEFT JOIN fe_dwd.`dwd_shelf_base_day_all` s
    ON tr.`shelf_id` = s.`shelf_id`
    AND s.`DATA_FLAG` = 1
  LEFT JOIN fe_dm.fill_order_p o
    ON tr.`order_id` = o.`ORDER_ID`
  LEFT JOIN fe_dm.fill_task_p t1
    ON t.`task_id` = t1.`task_id`
  LEFT JOIN fe.sf_vehicle_type_info c
    ON t.`vehicle_type` = c.`vehicle_type_id`
    AND c.`data_flag` = 1
WHERE t.`data_flag` = 1
  AND t.`add_time` < CURDATE()
;
 
    
  -- 执行日志
  CALL sh_process.`sp_sf_dw_task_log` (
    'sp_dm_lo_order_logistics_task_data',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangyunfeng@', @user, @timestamp)
  );
  COMMIT;
END