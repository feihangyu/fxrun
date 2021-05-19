CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_pub_shelf_first_order_info`()
BEGIN 
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
-- 取货架首次下单成功的时间。换架后取换架首单时间
  SET @start_date = SUBDATE(CURDATE(),INTERVAL 8 DAY);  -- 当天前8天  
-- 取8天的数据，防止出错
-- 最近订单的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_2_1`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_1
SELECT shelf_id,MIN(order_date) order_date
FROM fe_dwd.dwd_pub_order_item_recent_one_month -- 订单表  0527 修改为成功下单
WHERE order_date >= @start_date
GROUP BY shelf_id;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_2_2`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_2
SELECT shelf_id,MIN(order_date) order_date
FROM fe_dwd.dwd_op_out_of_system_order_yht  -- 未对接系统自贩机数据
WHERE order_date >= @start_date
GROUP BY shelf_id;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_2_3`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_3
SELECT shelf_id,MIN(order_date) order_date
FROM fe_dwd.dwd_out_of_system_auto_order_insert  -- 未对接系统智能柜数据
WHERE order_date >= @start_date
GROUP BY shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_2_4`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_4
SELECT * FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_1
UNION ALL 
SELECT * FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_2
UNION ALL 
SELECT * FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_3;
-- 聚合一下取首单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_2`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_2
SELECT shelf_id,MIN(order_date) order_date
FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_2_4
GROUP BY shelf_id;
CREATE INDEX idx_dwd_pub_shelf_first_order_info_tmp_2
ON fe_dwd.dwd_pub_shelf_first_order_info_tmp_2  (shelf_id,order_date);
-- 找出新增的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_3`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_3
SELECT a.shelf_id
FROM fe_dwd.`dwd_pub_shelf_first_order_info_tmp_2` a
LEFT JOIN fe_dwd.dwd_pub_shelf_first_order_info b 
ON a.shelf_id = b.shelf_id
WHERE b.shelf_id IS NULL ;
-- 提取新增的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_4`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_4
SELECT a.* FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_2 a
JOIN fe_dwd.dwd_pub_shelf_first_order_info_tmp_3 b 
ON a.shelf_id = b.shelf_id;
-- 删除一下已有的
DELETE a.* FROM fe_dwd.dwd_pub_shelf_first_order_info a
WHERE a.shelf_id IN 
(
SELECT b.shelf_id FROM 
fe_dwd.dwd_pub_shelf_first_order_info_tmp_3 b
);
INSERT INTO fe_dwd.`dwd_pub_shelf_first_order_info`
(
shelf_id,
first_order_date
)
SELECT shelf_id ,order_date FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_4
;
-- 取所有换架的
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_1`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_1
SELECT shelf_id ,MAX(audit_time) audit_time
 FROM fe.sf_shelf_change_apply 
WHERE new_shelf_type = 6
AND audit_status =2
AND data_flag =1
GROUP BY shelf_id 
;
CREATE INDEX idx_dwd_pub_shelf_first_order_info_tmp_1
ON fe_dwd.dwd_pub_shelf_first_order_info_tmp_1  (shelf_id);
-- 找出新增换架的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_5_1`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_5_1
SELECT a.shelf_id
FROM fe_dwd.`dwd_pub_shelf_first_order_info_tmp_1` a
LEFT JOIN fe_dwd.dwd_pub_shelf_first_order_info b 
ON a.shelf_id = b.shelf_id
WHERE b.shelf_id IS NULL ;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_5`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_5
SELECT a.shelf_id ,a.audit_time
FROM fe_dwd.`dwd_pub_shelf_first_order_info_tmp_1` a
JOIN fe_dwd.dwd_pub_shelf_first_order_info_tmp_5_1 b 
ON a.shelf_id = b.shelf_id;
CREATE INDEX idx_dwd_pub_shelf_first_order_info_tmp_5
ON fe_dwd.dwd_pub_shelf_first_order_info_tmp_5  (shelf_id,audit_time);
-- 取换架后首单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_pub_shelf_first_order_info_tmp_6`;
CREATE TEMPORARY TABLE fe_dwd.dwd_pub_shelf_first_order_info_tmp_6
SELECT a.shelf_id,MIN(a.order_date)  order_date
FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_2 a   -- 最近8天的订单
JOIN fe_dwd.dwd_pub_shelf_first_order_info_tmp_5 b 
ON a.shelf_id = b.shelf_id 
AND a.order_date >= b.audit_time
GROUP BY shelf_id;
-- 删除一下已有的 可能存在换架的，因此要删除
DELETE a.* FROM fe_dwd.dwd_pub_shelf_first_order_info a
WHERE a.shelf_id IN 
(
SELECT b.shelf_id FROM 
fe_dwd.dwd_pub_shelf_first_order_info_tmp_5_1 b
);
INSERT INTO fe_dwd.`dwd_pub_shelf_first_order_info`
(
shelf_id,
first_order_date
)
SELECT shelf_id ,order_date FROM fe_dwd.dwd_pub_shelf_first_order_info_tmp_6
;
UPDATE fe_dwd.dwd_pub_shelf_first_order_info AS b
JOIN fe_dwd.dwd_shelf_base_day_all a 
ON a.shelf_id = b.shelf_id
SET b.shelf_type = a.shelf_type,
 b.shelf_type_desc = a.shelf_type_desc;  
 
	
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_pub_shelf_first_order_info',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
  COMMIT;
END