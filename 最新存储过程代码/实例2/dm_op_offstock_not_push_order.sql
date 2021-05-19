CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_offstock_not_push_order`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/05/20
-- Modify date: 
-- Description:	
-- 	货架商品缺货未出单原因分析（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := CURDATE();
SET @pre_3month := SUBDATE(@stat_date,INTERVAL 3 MONTH);
SET @pre_1day := SUBDATE(@stat_date,1);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.offstock_tmp;
CREATE TEMPORARY TABLE fe_dwd.offstock_tmp(      
        KEY idx_shelf_id_product_id(shelf_id,product_id)
 ) AS 
SELECT
        shelf_id,
        product_id,
        shelf_type,
        2 AS shelf_status,
        whether_close2 AS whether_close,
        revoke_status1 AS revoke_status,
        shelf_level,
        stock_num AS yesterday_stock_num,
        product_type_class
FROM
        fe_dm.`dm_op_sp_offstock`
WHERE reason_classify = '2未生成补货需求'
        AND ((shelf_type IN (1,2,3,5,6) AND sales_flag IN (1,2,3) AND shelf_fill_flag = 1) OR shelf_type = 7)
;
-- 智能柜库存基准值
DROP TEMPORARY TABLE IF EXISTS fe_dwd.stock_rate_tmp;
CREATE TEMPORARY TABLE fe_dwd.stock_rate_tmp(      
        KEY idx_shelf_id(shelf_id)
 ) AS 
SELECT
        DISTINCT 
        c.shelf_id,
        a.STOCK_RATE
FROM
        fe_dwd.`dwd_package_information` a
        JOIN fe_dwd.`dwd_sf_shelf_package_detail` b
                ON a.PACKAGE_ID = b.PACKAGE_ID
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON b.shelf_id = c.shelf_id
                AND c.shelf_type = 6
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.check_tmp;
CREATE TEMPORARY TABLE fe_dwd.check_tmp(      
        KEY idx_shelf_id_product_id(shelf_id,product_id)
 ) AS 
SELECT
        SHELF_ID,
        PRODUCT_ID
FROM
        fe_dwd.`dwd_check_base_day_inc`
WHERE OPERATE_TIME >= @pre_1day
        AND OPERATE_TIME < @stat_date
        AND STOCK_NUM > 0
        AND CHECK_NUM = 0
;
DELETE FROM fe_dm.dm_op_offstock_not_push_order  WHERE stat_date = @stat_date OR stat_date < @pre_3month;
INSERT INTO fe_dm.dm_op_offstock_not_push_order 
(
        stat_date,
        shelf_id,
        SHELF_CODE,
        product_id,
        MANAGER_ID,
        shelf_type,
        shelf_status,
        whether_close,
        revoke_status,
        shelf_level,
        FILL_MODEL,
        yesterday_stock_num,
        STOCK_QUANTITY,
        day_sale_qty,
        NEW_FLAG,
        product_type_class,
        ALARM_QUANTITY,
        stock_total_qty,
        STOCK_RATE,
        slot_status, 
        offstock_not_push_reason 
)
SELECT
        @stat_date AS stat_date,
        a.shelf_id,
        c.`SHELF_CODE`,
        a.product_id,
        c.`MANAGER_ID`,
        a.shelf_type,
        a.shelf_status,
        a.whether_close,
        a.revoke_status,
        a.shelf_level,
        IFNULL(b.FILL_MODEL,f.FILL_MODEL) AS FILL_MODEL,
        a.yesterday_stock_num,
        d.`STOCK_QUANTITY`,
        IFNULL(b.`day_sale_qty`,f.day_sale_qty) AS day_sale_qty,
        IFNULL(b.`NEW_FLAG`,f.NEW_FLAG) AS NEW_FLAG,
        a.product_type_class,
        IFNULL(b.ALARM_QUANTITY,f.ALARM_QUANTITY) AS ALARM_QUANTITY,
        b.stock_total_qty,
        e.STOCK_RATE,
        j.slot_status,
        CASE
                WHEN (b.shelf_id IS NOT NULL OR f.shelf_id IS NOT NULL) AND IFNULL(b.reduce_suggest_fill_ceiling_num,f.SUGGEST_FILL_NUM) > 0 
                        THEN '1.缺货原有判定异常'
                WHEN (b.shelf_id IS NOT NULL OR f.shelf_id IS NOT NULL) AND (IFNULL(b.`NEW_FLAG`,f.NEW_FLAG) = 1 OR IFNULL(b.`NEW_FLAG`,f.NEW_FLAG) IS NULL) AND IFNULL(b.ALARM_QUANTITY,f.ALARM_QUANTITY) = 0 AND IFNULL(b.reduce_suggest_fill_ceiling_num,f.SUGGEST_FILL_NUM) = 0 
                        THEN '2.新品标配为0'
                WHEN a.product_type_class = '淘汰' AND (IFNULL(b.warehouse_stock,f.warehouse_stock) = 0 OR IFNULL(b.warehouse_stock,f.warehouse_stock) IS NULL) 
                        THEN '3.淘汰品仓库无库存'
                WHEN IFNULL(b.STOCK_NUM,f.STOCK_NUM) > 0 AND i.shelf_id IS NOT NULL
                        THEN '4.虚库存盘点后库存为0'
                WHEN IFNULL(b.FILL_MODEL,f.FILL_MODEL) > 1 AND g.fill_box_gauge > 10 AND IFNULL(b.`day_sale_qty`,f.day_sale_qty) < 0.14 AND IFNULL(b.reduce_suggest_fill_ceiling_num,f.SUGGEST_FILL_NUM) = 0 
                        THEN '4.盒装严重滞销大规格不补货'
                WHEN b.reduce_suggest_fill_ceiling_num = 0 AND b.SUGGEST_FILL_NUM > 0
                        THEN '5.货架容量超上限补货量被压缩'
                WHEN b.shelf_id IS NOT NULL OR f.shelf_id IS NOT NULL
                        THEN '其他'
                WHEN c.shelf_type = 6 AND c.type_name LIKE '%动态柜' AND (d.MAX_QUANTITY = 0 OR d.MAX_QUANTITY IS NULL)
                        THEN '1.商品无标配'
                WHEN c.shelf_type = 6 AND c.type_name LIKE '%动态柜' AND d.stock_quantity > e.STOCK_RATE
                        THEN '2.货架库存过多，未到触发值'
                WHEN c.shelf_type = 6 AND c.type_name LIKE '%动态柜'
                        THEN '其他'
                WHEN c.shelf_type = 7 AND (j.slot_capacity_limit = 0 OR j.slot_capacity_limit IS NULL)
                        THEN '1.商品无标配'
                WHEN  c.shelf_type = 7 AND j.slot_status != 1
                        THEN '2.货道状态未同步'
                WHEN  c.shelf_type = 7
                        THEN '其他'
        END AS offstock_not_push_reason
FROM
        fe_dwd.offstock_tmp a
        LEFT JOIN fe_dm.`dm_op_shelf_product_fill_update_his` b
                ON b.cdate = @pre_1day
                AND a.shelf_id = b.`SHELF_ID`
                AND a.product_id = b.`PRODUCT_ID`
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.shelf_id = c.shelf_id
        JOIN fe_dwd.`dwd_shelf_product_day_all` d
                ON a.shelf_id = d.`SHELF_ID`
                AND a.product_id = d.`PRODUCT_ID`
        LEFT JOIN fe_dwd.stock_rate_tmp e
                ON a.shelf_id = e.shelf_id
        LEFT JOIN fe_dm.`dm_op_smart_shelf_fill_update_his` f
                ON f.cdate = @pre_1day
                AND a.shelf_id = f.`SHELF_ID`
                AND a.product_id = f.`PRODUCT_ID`
        JOIN fe_dwd.`dwd_product_base_day_all` g
                ON a.product_id = g.product_id
        LEFT JOIN fe_dwd.`dwd_shelf_day_his` h
                ON a.shelf_id = h.shelf_id
                AND h.sdate = @pre_1day
        LEFT JOIN fe_dwd.check_tmp i
                ON a.shelf_id = i.shelf_id
                AND a.product_id = i.product_id
        LEFT JOIN fe_dwd.`dwd_shelf_machine_slot_type` j
                ON a.shelf_id = j.shelf_id
                AND a.product_id = j.product_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_offstock_not_push_order',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_not_push_order','dm_op_offstock_not_push_order','宋英南');
END