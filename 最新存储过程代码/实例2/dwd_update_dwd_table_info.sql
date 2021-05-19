CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_update_dwd_table_info`()
BEGIN
   SET @start_date = SUBDATE(CURDATE(),INTERVAL 1 DAY);  -- 当天前一天
   SET @start_date2 = SUBDATE(CURDATE(),INTERVAL 2 DAY);  -- 当天前二天   
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
    

-- add by lishilong 20200714
-- 找出有发生修改的重复订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_1;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_1 
(PRIMARY KEY idx_shelf_id_product_id(order_id,order_item_id)) AS
SELECT DISTINCT t.order_id,t.order_item_id
FROM
(
SELECT order_id,order_item_id,pay_id,COUNT(*) 
FROM fe_dwd.`dwd_pub_order_item_recent_two_month`
GROUP BY order_id,order_item_id,pay_id
HAVING COUNT(*) >1
) t;


-- 找出最早的那条记录
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_2 AS
SELECT 
    b.order_id,
    b.order_item_id,
    SUBSTRING_INDEX(GROUP_CONCAT(b.row_id ORDER BY load_time ),',',1) AS row_id
FROM
    `fe_dwd`.`dwd_lsl_order_item_tmp_1` a
    JOIN fe_dwd.dwd_pub_order_item_recent_one_month  b
    ON a.order_id = b.order_id
    AND a.order_item_id= b.order_item_id    
GROUP BY shelf_id,product_id;


DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_order_item_tmp_2_2;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_order_item_tmp_2_2 AS
SELECT 
    b.order_id,
    b.order_item_id,
    SUBSTRING_INDEX(GROUP_CONCAT(b.row_id ORDER BY load_time ),',',1) AS row_id
FROM
    `fe_dwd`.`dwd_lsl_order_item_tmp_1` a
    JOIN fe_dwd.dwd_pub_order_item_recent_two_month  b
    ON a.order_id = b.order_id
    AND a.order_item_id= b.order_item_id    
GROUP BY shelf_id,product_id;



-- 做删除处理
DELETE  a.* FROM fe_dwd.dwd_pub_order_item_recent_one_month a 
INNER JOIN fe_dwd.dwd_lsl_order_item_tmp_2 b
ON a.row_id = b.row_id;  


DELETE  a.* FROM fe_dwd.dwd_pub_order_item_recent_two_month a 
INNER JOIN fe_dwd.dwd_lsl_order_item_tmp_2_2 b
ON a.row_id = b.row_id;  


  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_update_dwd_table_info',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
COMMIT;
END