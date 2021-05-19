CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_manual_fill_monitor`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @cdate := CURDATE();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @pre_day2 := SUBDATE(CURDATE(),2);
SET @pre_day14 := SUBDATE(CURDATE(),14);
SET @pre_day15 := SUBDATE(CURDATE(),15);
SET @pre_day30 := SUBDATE(CURDATE(),30);
SET @pre_6_month := SUBDATE(CURDATE(),INTERVAL 6 MONTH);      -- 截存半年的数据
-- 在途
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`onload_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.onload_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT 
        a.shelf_id,
        a.product_id,
        SUM(a.actual_apply_num) AS onload_qty
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.order_status IN (1,2,3)
        AND a.apply_time >= @pre_day30
GROUP BY a.shelf_id,a.product_id
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_1--@time_2",@time_1,@time_2);
-- 补货订单口径
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        a.order_id,
        a.apply_time,
        a.SHELF_ID,
        a.PRODUCT_ID,
        a.actual_apply_num,
        a.ACTUAL_FILL_NUM,
        a.STOCK_NUM,
        a.surplus_reason,
        a.audit_status,
        a.order_status
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.FILL_TYPE = 1
        AND a.apply_time >= @stat_date
        AND a.apply_time < @cdate
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_2--@time_3",@time_2,@time_3);
DELETE FROM `fe_dm`.dm_op_manual_fill_monitor WHERE DATE(apply_time) =  @stat_date OR apply_time < @pre_6_month;
INSERT INTO `fe_dm`.dm_op_manual_fill_monitor
(
        order_id,
        apply_time,
        business_name,
        SHELF_ID,
        PRODUCT_ID,
        actual_apply_num,
        ACTUAL_FILL_NUM,
        STOCK_NUM,
        STOCK_QUANTITY,
        day_sale_qty,
        turnover_days, -- 当前库存周转
        shelf_fill_reason,    -- 补货原因        
        group_buy_date,       -- 团购出货时间
        push_activity_type,     -- 地推类型
        is_surplus,       -- 是否超量(1:是，0：否)
        surplus_reason,
        audit_status,
        shelf_type,
        product_name,
        order_status
)
SELECT
        a.order_id,
        a.apply_time,
        f.business_name,
        a.SHELF_ID,
        a.PRODUCT_ID,
        a.actual_apply_num,
        a.ACTUAL_FILL_NUM,
        a.STOCK_NUM,
        b.STOCK_QUANTITY,
        c.day_sale_qty,
        ROUND((IFNULL(b.STOCK_QUANTITY,0) + IFNULL(d.onload_qty,0)) / c.day_sale_qty,2) AS turnover_days, -- 当前库存周转
        e.shelf_fill_reason,    -- 补货原因        
        IF(e.shelf_fill_reason = 2,e.shelf_fill_remark,NULL) AS group_buy_date,       -- 团购出货时间
        IF(e.shelf_fill_reason = 5,e.shelf_fill_remark,NULL) AS push_activity_type,     -- 地推类型
        IF(a.surplus_reason IS NULL,0,1) AS is_surplus,       -- 是否超量(1:是，0：否)
        a.surplus_reason,
        a.audit_status,
        f.shelf_type,
        g.product_name,
        a.order_status
FROM
        fe_dwd.fill_tmp a 
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.`SHELF_ID` = b.`SHELF_ID`
                AND a.`PRODUCT_ID` = b.`PRODUCT_ID`
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` c
                ON a.`SHELF_ID` = c.`shelf_id`
                AND a.`PRODUCT_ID` = c.`product_id`
        LEFT JOIN fe_dwd.onload_tmp d
                ON a.shelf_id = d.shelf_id
                AND a.`PRODUCT_ID` = d.product_id
        LEFT JOIN fe_dwd.dwd_sf_product_fill_order_extend e
                ON a.`order_id` = e.`order_id`
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` f
                ON a.shelf_id = f.`shelf_id`
        JOIN fe_dwd.`dwd_product_base_day_all` g
                ON b.product_id = g.product_id
WHERE f.shelf_type != 4        --  剔除虚拟货架
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_3--@time_4",@time_3,@time_4);
-- =======================================================================================
-- 对前15天数据进行更新
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_15_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_15_tmp (
        KEY idx_order_id_shelf_id_product_id(order_id,shelf_id,product_id)
) AS
SELECT
        a.order_id,
        a.SHELF_ID,
        a.PRODUCT_ID,
        a.ACTUAL_FILL_NUM,
        b.STOCK_QUANTITY,
        c.day_sale_qty,
        ROUND((IFNULL(b.STOCK_QUANTITY,0) + IFNULL(d.onload_qty,0)) / c.day_sale_qty,2) AS turnover_days, -- 当前库存周转  
        IF(LENGTH(a.surplus_reason)>0,1,0) AS is_surplus,       -- 是否超量(1:是，0：否)
        a.surplus_reason,
        a.audit_status,
        a.order_status
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.`SHELF_ID` = b.`SHELF_ID`
                AND a.`PRODUCT_ID` = b.`PRODUCT_ID`
                AND a.FILL_TYPE = 1
                AND a.apply_time >= @pre_day15
                AND a.apply_time < @stat_date
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` c
                ON a.`SHELF_ID` = c.`shelf_id`
                AND a.`PRODUCT_ID` = c.`product_id`
        LEFT JOIN fe_dwd.onload_tmp d
                ON a.shelf_id = d.shelf_id
                AND a.`PRODUCT_ID` = d.product_id
;
-- 订单下单后第15天，货架商品库存未补过货
-- 近14天上架订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`order_fill_14_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.order_fill_14_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        DISTINCT 
        a.SHELF_ID,
        a.PRODUCT_ID
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.FILL_TYPE IN (1,2,8,9)
        AND a.order_status = 4
        AND a.apply_time >= @pre_day14
        AND a.apply_time < @stat_date
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_4--@time_5",@time_4,@time_5);
UPDATE  
        `fe_dm`.dm_op_manual_fill_monitor a
        JOIN fe_dwd.fill_15_tmp b
                ON a.order_id = b.order_id
                AND a.`SHELF_ID` = b.shelf_id
                AND a.`PRODUCT_ID` = b.product_id
SET  a.ACTUAL_FILL_NUM = b.ACTUAL_FILL_NUM,
        a.STOCK_QUANTITY = b.STOCK_QUANTITY,
        a.day_sale_qty = b.day_sale_qty,
        a.turnover_days = b.turnover_days , -- 当前库存周转  
        a.is_surplus = b.is_surplus,       -- 是否超量(1:是，0：否)
        a.surplus_reason = b.surplus_reason,
        a.audit_status = b.audit_status,  
        a.order_status = b.order_status
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_5--@time_6",@time_5,@time_6);
-- ===========================================================================
-- 补货原因
-- 订单下单后第15天，货架商品库存超过30天周转的清单
UPDATE  
        `fe_dm`.dm_op_manual_fill_monitor
SET error_type = '正常补货超周转'
WHERE @pre_day15 = DATE(apply_time)
        AND turnover_days > 30
        AND shelf_fill_reason = 1
        AND order_status IN (3,4)
        AND STOCK_QUANTITY > 10
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_6--@time_7",@time_6,@time_7);
-- 订单下单后第15天，货架商品库存超过30天的清单
UPDATE  
        `fe_dm`.dm_op_manual_fill_monitor a
        LEFT JOIN fe_dwd.order_fill_14_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
SET error_type = '军营备货超周转'
WHERE @pre_day15 = DATE(apply_time)
        AND turnover_days > 30
        AND shelf_fill_reason = 3
        AND order_status IN (3,4)
        AND STOCK_QUANTITY > 10
        AND b.shelf_id IS NULL
;
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_7--@time_8",@time_7,@time_8);
-- ①团购出货日第二天，订单内货架商品周转超过30天，定义为团购疑似未出货；
-- ②订单上架日到团购出货日之间，销售数量小于订单申请数量，定义为团购疑似未出货
-- 从下单到团购出货日之前未再补过货
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`group_buy_tmp1`;   
CREATE TEMPORARY TABLE fe_dwd.group_buy_tmp1 (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT 
        shelf_id,
        product_id,
        apply_time,
        group_buy_date
FROM
        `fe_dm`.dm_op_manual_fill_monitor 
WHERE @cdate = ADDDATE(group_buy_date,1)
        AND turnover_days > 30
        AND shelf_fill_reason = 2
        AND order_status IN (3,4)
        AND STOCK_QUANTITY > 10
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`group_buy_tmp2`;   
CREATE TEMPORARY TABLE fe_dwd.group_buy_tmp2 (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        DISTINCT
        a.shelf_id,
        a.product_id
FROM
      fe_dwd.group_buy_tmp1 a
      LEFT JOIN fe_dwd.`dwd_fill_day_inc_recent_two_month` b
        ON a.shelf_id = b.shelf_id 
        AND a.product_id = b.product_id
WHERE b.FILL_TYPE IN (1,2,8,9)
        AND b.order_status = 4
        AND SUBDATE(b.apply_time,1) BETWEEN a.apply_time AND a.group_buy_date
;
UPDATE  
        `fe_dm`.dm_op_manual_fill_monitor a
        LEFT JOIN fe_dwd.group_buy_tmp2 b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
SET error_type = '团购疑似未出货'
WHERE @cdate = ADDDATE(group_buy_date,1)
        AND turnover_days > 30
        AND shelf_fill_reason = 2
        AND order_status IN (3,4)
        AND STOCK_QUANTITY > 10
        AND b.shelf_id IS NULL
;
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_8--@time_9",@time_8,@time_9);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`sale_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.sale_tmp (
        KEY idx_date_shelf_id_product_id(pay_date,shelf_id,product_id)
) AS
SELECT
        DATE(PAY_DATE) AS PAY_DATE,
        shelf_id,
        product_id,
        SUM(QUANTITY) AS QUANTITY
FROM
        fe_dwd.`dwd_pub_order_item_recent_one_month`
GROUP BY DATE(PAY_DATE),shelf_id,product_id
;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_9--@time_10",@time_9,@time_10);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`group_buy_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.group_buy_tmp (
        KEY idx_order_id_shelf_id_product_id(order_id,shelf_id,product_id)
) AS
SELECT
        a.order_id,
        a.shelf_id,
        a.product_id
FROM
        `fe_dm`.dm_op_manual_fill_monitor a
        JOIN fe_dwd.sale_tmp b
        ON a.shelf_id = b.shelf_id
        AND a.product_id = b.product_id
        AND b.pay_date BETWEEN a.apply_time AND a.group_buy_date
        AND @cdate = ADDDATE(a.group_buy_date,1)
WHERE a.order_status IN (3,4)
GROUP BY a.order_id,a.shelf_id,a.product_id
HAVING SUM(b.QUANTITY) < max(a.ACTUAL_FILL_NUM)
;
UPDATE 
        `fe_dm`.dm_op_manual_fill_monitor a
        JOIN fe_dwd.group_buy_tmp b
                ON a.order_id = b.order_id
                AND a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
                AND a.turnover_days > 20
SET a.error_type = '团购疑似未出货'
;
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_10--@time_11",@time_10,@time_11);
-- 每周一回顾上一周，该原因的货架商品，周转大于30天的明细
UPDATE  
        `fe_dm`.dm_op_manual_fill_monitor
SET error_type = '地推遗留高库存'
WHERE DAYOFWEEK(CURDATE()) = 2
        AND DATE(apply_time) BETWEEN SUBDATE(CURDATE(),INTERVAL 1 WEEK) AND CURDATE()
        AND turnover_days > 30
        AND shelf_fill_reason = 5
        AND STOCK_QUANTITY > 10
;
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_11--@time_12",@time_11,@time_12);
-- 每月5号货架库存是否清零，否则判断为该类型
UPDATE  
        `fe_dm`.dm_op_manual_fill_monitor
SET error_type = '月结疑似库存未清零'
WHERE @stat_date = DATE_FORMAT(CURDATE(),'%Y-%m-05')
        AND shelf_fill_reason = 4
        AND STOCK_QUANTITY > 0
;
SET @time_13 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_manual_fill_monitor","@time_12--@time_13",@time_12,@time_13);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_manual_fill_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_manual_fill_monitor','dm_op_manual_fill_monitor','宋英南');
 
COMMIT;
	END