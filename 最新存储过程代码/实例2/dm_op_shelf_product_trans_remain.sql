CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_product_trans_remain`()
BEGIN
-- =============================================
-- Author:	调货
-- Create date: 2020/07/03
-- Modify date: 
-- Description:	
-- 	调货申请参考表（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @cdate := CURDATE();
TRUNCATE TABLE fe_dm.dm_op_shelf_product_trans_remain;
INSERT INTO fe_dm.dm_op_shelf_product_trans_remain
(
        cdate,
        DETAIL_ID,
        PRODUCT_ID,
        product_name,
        product_fe,
        SHELF_ID,
        SHELF_TYPE,
        shelf_level,
        SALE_PRICE,
        day_sale_qty,
        NEW_FLAG,
        SALES_FLAG,
        fill_level,
        FILL_MODEL,
        fill_box_gauge,
        ALARM_QUANTITY,
        SHELF_FILL_FLAG,
        STOCK_NUM,
        ONWAY_NUM,
        warehouse_type,
        whether_push_order,
        SUGGEST_FILL_NUM,
        SUGGEST_REMAIN_NUM
)
SELECT
        a.cdate,
        a.DETAIL_ID,
        a.PRODUCT_ID,
        a.product_name,
        a.product_fe,
        a.SHELF_ID,
        a.SHELF_TYPE,
        a.shelf_level,
        a.SALE_PRICE,
        a.day_sale_qty,
        a.NEW_FLAG,
        a.SALES_FLAG,
        b.fill_level,
        a.FILL_MODEL,
        c.`fill_box_gauge`,
        a.ALARM_QUANTITY,
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        a.ONWAY_NUM,
        a.warehouse_type,
        a.whether_push_order,
        a.reduce_suggest_fill_ceiling_num AS SUGGEST_FILL_NUM,
        CASE 
                WHEN b.fill_level = 6 AND a.FILL_MODEL = 1
                        THEN a.ALARM_QUANTITY + 1
                WHEN b.fill_level = 6 AND a.FILL_MODEL > 1
                        THEN a.ALARM_QUANTITY + IFNULL(a.FILL_MODEL,0)
                WHEN a.FILL_MODEL = 1
                        THEN IF(CEILING( a.day_sale_qty * ( IFNULL( a.fill_cycle, 7 ) + IFNULL( a.fill_days, 0 ) ) + IFNULL( a.safe_stock_qty, 0 ) + IFNULL( a.suspect_false_stock_qty, 0 ) +1 ) = 1,2,
                        CEILING( a.day_sale_qty * ( IFNULL( a.fill_cycle, 7 ) + IFNULL( a.fill_days, 0 ) ) + IFNULL( a.safe_stock_qty, 0 ) + IFNULL( a.suspect_false_stock_qty, 0 ) +1 ))
                WHEN a.FILL_MODEL > 1 AND a.STOCK_NUM + a.ONWAY_NUM >= 1.5 * a.fill_model 
                        THEN 2 * a.fill_model
                WHEN a.FILL_MODEL > 1 AND b.fill_level = 6 AND a.STOCK_NUM + a.ONWAY_NUM > 0
                        THEN a.ALARM_QUANTITY + IFNULL(a.FILL_MODEL,0)
                WHEN a.FILL_MODEL > 1 AND b.fill_level != 6 AND b.day_sale_qty <= 0.07 
                        THEN IFNULL(a.FILL_MODEL,0)
                WHEN a.FILL_MODEL > 1 AND a.FILL_MODEL < 5 
                        THEN IF(a.FILL_MODEL > c.`fill_box_gauge`,a.FILL_MODEL,c.`fill_box_gauge`)
                WHEN a.FILL_MODEL <= 20 AND b.day_sale_qty >= 0.43
                        THEN 2 * a.FILL_MODEL
                WHEN a.FILL_MODEL > 20
                        THEN 2 * a.FILL_MODEL
        END AS SUGGEST_REMAIN_NUM
FROM
        fe_dm.`dm_op_shelf_product_fill_update` a
        JOIN fe_dm.`dm_op_fill_day_sale_qty` b
                ON a.`SHELF_ID` = b.`shelf_id`
                AND a.`PRODUCT_ID` = b.`product_id`
        JOIN fe_dwd.`dwd_product_base_day_all` c
                ON a.`PRODUCT_ID` = c.`PRODUCT_ID`
UNION ALL
SELECT
        a.cdate,
        a.DETAIL_ID,
        a.PRODUCT_ID,
        c.product_name,
        c.product_code2 AS product_fe,
        a.SHELF_ID,
        a.SHELF_TYPE,
        a.shelf_level,
        a.SALE_PRICE,
        a.day_sale_qty,
        a.NEW_FLAG,
        a.SALES_FLAG,
        b.fill_level,
        a.FILL_MODEL,
        c.`fill_box_gauge`,
        a.ALARM_QUANTITY,
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        a.ONWAY_NUM,
        IF(a.is_prewarehouse_shelf = '是',2,1) AS warehouse_type,
        a.whether_push_order,
        a.SUGGEST_FILL_NUM,
        a.day_sale_qty * 7 + IF(a.alarm_quantity <=6,1,2) + IFNULL(safe_stock_qty,0) + 1 AS SUGGEST_REMAIN_NUM
--         a.day_sale_qty * 7 + IF(a.alarm_quantity <=6,1,2) + IFNULL(safe_stock_qty,0) + IFNULL(suspect_false_stock_qty,0) + 1 as SUGGEST_REMAIN_NUM
FROM
        fe_dm.`dm_op_smart_shelf_fill_update` a
        JOIN fe_dm.`dm_op_fill_day_sale_qty` b
                ON a.`SHELF_ID` = b.`shelf_id`
                AND a.`PRODUCT_ID` = b.`product_id`
        JOIN fe_dwd.`dwd_product_base_day_all` c
                ON a.`PRODUCT_ID` = c.`PRODUCT_ID`
UNION ALL
SELECT
        a.cdate,
        a.DETAIL_ID,
        a.PRODUCT_ID,
        a.product_name,
        a.product_fe,
        a.SHELF_ID,
        a.SHELF_TYPE,
        a.shelf_level,
        a.SALE_PRICE,
        a.day_sale_qty,
        a.NEW_FLAG,
        a.SALES_FLAG,
        b.fill_level,
        a.FILL_MODEL,
        c.`fill_box_gauge`,
        a.total_slot_capacity_limit AS ALARM_QUANTITY,
        a.SHELF_FILL_FLAG,
        a.STOCK_NUM,
        a.ONWAY_NUM,
        IF(a.is_prewarehouse_shelf = '是',2,1) AS warehouse_type,
        a.whether_push_order,
        a.SUGGEST_FILL_NUM,
        a.total_slot_capacity_limit AS SUGGEST_REMAIN_NUM
FROM
        fe_dm.`dm_op_machine_fill_update` a
        JOIN fe_dm.`dm_op_fill_day_sale_qty` b
                ON a.`SHELF_ID` = b.`shelf_id`
                AND a.`PRODUCT_ID` = b.`product_id`
        JOIN fe_dwd.`dwd_product_base_day_all` c
                ON a.`PRODUCT_ID` = c.`PRODUCT_ID`
;
-- product_id in (2874,871,2487)
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_product_trans_remain',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_product_trans_remain','dm_op_shelf_product_trans_remain','宋英南');
END