CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_manager_product_trans_monitor`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURDATE();
SET @sdate_monday   := SUBDATE(@sdate,INTERVAL WEEKDAY(@sdate) DAY);
SET @smonth := DATE_FORMAT(@sdate,'%Y%m');
-- 跟踪时间段内的销售情况 12s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`sale_tmp`;
CREATE TEMPORARY TABLE fe_dwd.sale_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT 
        a.stat_date,
        a.shelf_id,
        a.product_id,
        SUM(b.QUANTITY) AS sale_qty,
        SUM(b.QUANTITY * b.SALE_PRICE) AS sale_value,
        SUM(b.REAL_TOTAL_PRICE) AS REAL_TOTAL_PRICE
FROM 
        fe_dm.`dm_op_manager_product_trans_list` a
        JOIN `fe_dwd`.`dwd_pub_order_item_recent_two_month` b
                ON b.PAY_DATE > ADDDATE(a.stat_date,1) 
                AND b.PAY_DATE < @sdate
                AND a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
GROUP BY a.stat_date,a.shelf_id,a.PRODUCT_ID
;
DELETE FROM fe_dm.dm_op_manager_product_trans_monitor WHERE sdate < SUBDATE(@sdate,INTERVAL 2 MONTH);     
INSERT INTO fe_dm.dm_op_manager_product_trans_monitor
(
        sdate,
        version_stat_date,
        BUSINESS_AREA,
        shelf_id,
        PRODUCT_ID,
        PRODUCT_NAME,
        CATEGORY_NAME,
        sale_qty,
        sale_value,
        REAL_TOTAL_PRICE,
        stock_quantity_old,
        suggest_trans_out_qty,
        stock_quantity_now,
        SALE_PRICE,
        shelf_level_t,
        shelf_code,
        SHELF_NAME,
        MANAGER_ID,
        MANAGER_NAME,
        BRANCH_NAME,
        BRANCH_CODE,
        warehouse_id,
        SHELF_TYPE,
        PRODUCT_TYPE,
        second_user_type,
        SALES_FLAG
)
SELECT
        @sdate AS sdate,
        a.stat_date AS version_stat_date,
        a.BUSINESS_AREA,
        a.shelf_id,
        a.PRODUCT_ID,
        a.PRODUCT_NAME,
        b.`CATEGORY_NAME`,
        c.sale_qty,
        c.sale_value,
        c.REAL_TOTAL_PRICE,
        a.stock_quantity AS stock_quantity_old,
        a.trans_out_qty AS suggest_trans_out_qty,
        d.stock_quantity AS stock_quantity_now,
        d.SALE_PRICE,
        CASE 
                WHEN h.shelf_level = 1 THEN '新'
                WHEN h.shelf_level = 2 THEN '甲'
                WHEN h.shelf_level = 3 THEN '乙'
                WHEN h.shelf_level = 4 THEN '丙'
                WHEN h.shelf_level = 5 THEN '丁'
        END AS shelf_level_t,
        a.shelf_code,
        a.SHELF_NAME,
        a.MANAGER_ID,
        a.MANAGER_NAME,
        h.BRANCH_NAME,
        h.BRANCH_CODE,
        f.prewarehouse_id AS warehouse_id,
        h.SHELF_TYPE,
        g.PRODUCT_TYPE,
        IF(h.manager_type = '全职店主',1,2) AS second_user_type,
        d.SALES_FLAG
FROM 
        fe_dm.`dm_op_manager_product_trans_list` a
        JOIN `fe_dwd`.`dwd_product_base_day_all` b
                ON a.`product_id` = b.`PRODUCT_ID`
        LEFT JOIN fe_dwd.sale_tmp c
                ON c.stat_date = a.stat_date
                AND a.`shelf_id` = c.shelf_id
                AND a.`product_id` = c.product_id
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` d
                ON a.`shelf_id` = d.shelf_id
                AND a.`product_id` = d.product_id
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` f
                ON a.`shelf_id` = f.shelf_id
        LEFT JOIN fe_dwd.`dwd_pub_product_dim_sserp` g
                ON a.business_area = g.business_area
                AND a.PRODUCT_ID = g.PRODUCT_ID
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` h
                ON a.shelf_id = h.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_manager_product_trans_monitor',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_manager_product_trans_monitor','dm_op_manager_product_trans_monitor','宋英南');
END