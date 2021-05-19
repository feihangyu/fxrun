CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_offstock_loss`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/06/23
-- Modify date: 
-- Description:	
-- 	全量缺货损失（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @pre_day30 := SUBDATE(@stat_date,30);
SET @pre_3month := SUBDATE(@stat_date,INTERVAL 3 MONTH);
SET @pre_2year := SUBDATE(@stat_date,INTERVAL 2 YEAR);
SET @pre_2ym := DATE_FORMAT(@pre_2year,'%Y-%m');
SET @month_id := DATE_FORMAT(@stat_date,'%Y-%m');
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
-- 月度汇总的要2年
-- 后台存3个月，网易放45天
-- 1.1：货架维度
DELETE FROM fe_dm.dm_op_shelf_offstock_loss WHERE stat_date = @stat_date OR stat_date < @pre_3month;
INSERT INTO fe_dm.dm_op_shelf_offstock_loss
(
        stat_date,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        shelf_id,
        shelf_classify,
        SALES_FLAG,
        normal_fill_sku,
        offstock_sku,
        offstock_lose,
        sum_value,
        offstock_lose_rate
)
SELECT
        @stat_date AS stat_date,
        a.`region_name`,
        a.`business_name`,
        a.`zone_name`,
        a.`zone_code`,
        a.shelf_id,
        CASE
                WHEN ACTIVATE_TIME > @pre_day30
                        THEN '新装'
                WHEN REVOKE_STATUS = 1 AND WHETHER_CLOSE = 2
                        THEN '正常'
                ELSE '货架异常'
        END AS shelf_classify,
        CASE
                WHEN b.SALES_FLAG IN (1,2,3) 
                        THEN '爆畅平'
                WHEN b.SALES_FLAG IN (4,5) 
                        THEN '滞销'
        END AS SALES_FLAG,
        COUNT(*) AS normal_fill_sku,
        SUM(b.STOCK_QUANTITY <= 0) AS offstock_sku,
        ROUND(SUM(IF(b.STOCK_QUANTITY <= 0,IFNULL(d.day_sale_qty,0) * b.SALE_PRICE,0)),2) AS offstock_lose,
        ROUND(SUM(IFNULL(d.day_sale_qty,0) * b.SALE_PRICE),2) AS sum_value,
        ROUND(SUM(IF(b.STOCK_QUANTITY <= 0,IFNULL(d.day_sale_qty,0) * b.SALE_PRICE,0)) / SUM(IFNULL(d.day_sale_qty,0) * b.SALE_PRICE),2) AS offstock_lose_rate
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b
                ON a.`shelf_id` = b.`SHELF_ID` 
        JOIN fe_dwd.`dwd_pub_product_dim_sserp` c
                ON a.business_name = c.business_area
                AND b.product_id = c.product_id
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` d
                ON b.shelf_id = d.shelf_id
                AND b.product_id = d.product_id
WHERE a.`SHELF_STATUS` = 2
        AND a.`shelf_type` IN (1,2,3,6,7)
        AND b.`SHELF_FILL_FLAG` = 1
GROUP BY a.shelf_id,
        CASE
                WHEN b.SALES_FLAG IN (1,2,3) 
                        THEN '爆畅平'
                WHEN b.SALES_FLAG IN (4,5) 
                        THEN '滞销'
        END
;
-- 1.2：货架-汇总
DELETE FROM fe_dm.dm_op_shelf_offstock_loss_total WHERE month_id = @month_id OR month_id < @pre_2ym;
INSERT INTO fe_dm.dm_op_shelf_offstock_loss_total
(
        month_id,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        shelf_id,
        shelf_classify,
        SALES_FLAG,
        normal_fill_sku,
        offstock_sku,
        offstock_lose,
        offstock_lose_rate
)
SELECT
        @month_id AS month_id,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        shelf_id,
        shelf_classify,
        SALES_FLAG,
        SUM(normal_fill_sku) AS normal_fill_sku,
        SUM(offstock_sku) AS offstock_sku,
        SUM(offstock_lose) AS offstock_lose,
        ROUND(SUM(offstock_lose) / SUM(sum_value),2)  AS offstock_lose_rate
FROM
        fe_dm.dm_op_shelf_offstock_loss
WHERE stat_date >= @cur_month_01 
GROUP BY shelf_id,SALES_FLAG
;
-- 2.1：商品维度
DELETE FROM fe_dm.dm_op_product_type_offstock_loss WHERE stat_date = @stat_date OR stat_date < @pre_3month;
INSERT INTO fe_dm.dm_op_product_type_offstock_loss
(
        stat_date,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        product_id,
        product_code2,
        PRODUCT_TYPE,
        normal_fill_shelfs,
        offstock_shelfs,
        offstock_lose,
        sum_value,
        offstock_lose_rate
)
SELECT
        @stat_date AS stat_date,
        a.`region_name`,
        a.`business_name`,
        a.`zone_name`,
        a.`zone_code`,
        b.product_id,
        e.product_code2,
        IF(c.PRODUCT_TYPE IN ('新增（试运行）','原有'),c.PRODUCT_TYPE,'非正常品') AS PRODUCT_TYPE,
        COUNT(*) AS normal_fill_shelfs,
        SUM(b.STOCK_QUANTITY <= 0) AS offstock_shelfs,
        ROUND(SUM(IF(b.STOCK_QUANTITY <= 0,IFNULL(d.day_sale_qty,0) * b.SALE_PRICE,0)),2) AS offstock_lose,
        ROUND(SUM(IFNULL(d.day_sale_qty,0) * b.SALE_PRICE),2) AS sum_value,
        ROUND(SUM(IF(b.STOCK_QUANTITY <= 0,IFNULL(d.day_sale_qty,0) * b.SALE_PRICE,0)) / SUM(IFNULL(d.day_sale_qty,0) * b.SALE_PRICE),2) AS offstock_lose_rate
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b
                ON a.`shelf_id` = b.`SHELF_ID` 
        JOIN fe_dwd.`dwd_pub_product_dim_sserp` c
                ON a.business_name = c.business_area
                AND b.product_id = c.product_id
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` d
                ON b.shelf_id = d.shelf_id
                AND b.product_id = d.product_id
        JOIN fe_dwd.`dwd_product_base_day_all` e
                ON e.product_id = b.product_id
WHERE a.`SHELF_STATUS` = 2
        AND a.`shelf_type` IN (1,2,3,6,7)
        AND b.`SHELF_FILL_FLAG` = 1
GROUP BY a.`business_name`,
        a.`zone_code`,
        b.product_id
;
-- 2.2：商品-汇总
DELETE FROM fe_dm.dm_op_product_type_offstock_loss_total WHERE month_id = @month_id OR month_id < @pre_2ym;
INSERT INTO fe_dm.dm_op_product_type_offstock_loss_total
(
        month_id,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        product_id,
        product_code2,
        PRODUCT_TYPE,
        normal_fill_shelfs,
        offstock_shelfs,
        offstock_lose,
        offstock_lose_rate
)
SELECT
        @month_id AS month_id,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        product_id,
        product_code2,
        PRODUCT_TYPE,
        SUM(normal_fill_shelfs) AS normal_fill_shelfs,
        SUM(offstock_shelfs) AS offstock_shelfs,
        SUM(offstock_lose) AS offstock_lose,
        ROUND(SUM(offstock_lose) / SUM(sum_value),2) AS offstock_lose_rate
FROM
        fe_dm.dm_op_product_type_offstock_loss
WHERE stat_date >= @cur_month_01 
GROUP BY `business_name`,
        `zone_code`,
        product_id
;
-- 3.1：商品等级
DELETE FROM fe_dm.dm_op_product_level_offstock_loss WHERE stat_date = @stat_date OR stat_date < @pre_3month;
INSERT INTO fe_dm.dm_op_product_level_offstock_loss
(
        stat_date,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        product_id,
        product_code2,
        SALES_FLAG,
        normal_fill_shelfs,
        offstock_shelfs,
        offstock_lose,
        sum_value,
        offstock_lose_rate
)
SELECT
        @stat_date AS stat_date,
        a.`region_name`,
        a.`business_name`,
        a.`zone_name`,
        a.`zone_code`,
        b.product_id,
        e.product_code2,
        CASE
                WHEN b.SALES_FLAG IN (1,2,3) 
                        THEN '爆畅平'
                WHEN b.SALES_FLAG IN (4,5) 
                        THEN '滞销'
        END AS SALES_FLAG,
        COUNT(*) AS normal_fill_shelfs,
        SUM(b.STOCK_QUANTITY <= 0) AS offstock_shelfs,
        ROUND(SUM(IF(b.STOCK_QUANTITY <= 0,IFNULL(d.day_sale_qty,0) * b.SALE_PRICE,0)),2) AS offstock_lose,
        ROUND(SUM(IFNULL(d.day_sale_qty,0) * b.SALE_PRICE),2) AS sum_value,
        ROUND(SUM(IF(b.STOCK_QUANTITY <= 0,IFNULL(d.day_sale_qty,0) * b.SALE_PRICE,0)) / SUM(IFNULL(d.day_sale_qty,0) * b.SALE_PRICE),2) AS offstock_lose_rate
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b
                ON a.`shelf_id` = b.`SHELF_ID` 
        JOIN fe_dwd.`dwd_pub_product_dim_sserp` c
                ON a.business_name = c.business_area
                AND b.product_id = c.product_id
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` d
                ON b.shelf_id = d.shelf_id
                AND b.product_id = d.product_id
        JOIN fe_dwd.`dwd_product_base_day_all` e
                ON e.product_id = b.product_id
WHERE a.`SHELF_STATUS` = 2
        AND a.`shelf_type` IN (1,2,3,6,7)
        AND b.`SHELF_FILL_FLAG` = 1
GROUP BY a.`business_name`,
        a.`zone_code`,
        b.product_id,
        CASE
                WHEN b.SALES_FLAG IN (1,2,3) 
                        THEN '爆畅平'
                WHEN b.SALES_FLAG IN (4,5) 
                        THEN '滞销'
        END
;
-- 2.2：商品-汇总
DELETE FROM fe_dm.dm_op_product_level_offstock_loss_total WHERE month_id = @month_id OR month_id < @pre_2ym;
INSERT INTO fe_dm.dm_op_product_level_offstock_loss_total
(
        month_id,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        product_id,
        product_code2,
        SALES_FLAG,
        normal_fill_shelfs,
        offstock_shelfs,
        offstock_lose,
        offstock_lose_rate
)
SELECT
        @month_id AS month_id,
        `region_name`,
        `business_name`,
        `zone_name`,
        `zone_code`,
        product_id,
        product_code2,
        SALES_FLAG,
        SUM(normal_fill_shelfs) AS normal_fill_shelfs,
        SUM(offstock_shelfs) AS offstock_shelfs,
        SUM(offstock_lose) AS offstock_lose,
        ROUND(SUM(offstock_lose) / SUM(sum_value),2) AS offstock_lose_rate
FROM
        fe_dm.dm_op_product_level_offstock_loss
WHERE stat_date >= @cur_month_01 
GROUP BY `business_name`,
        `zone_code`,
        product_id,
        SALES_FLAG
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_offstock_loss',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_offstock_loss','dm_op_shelf_offstock_loss','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_offstock_loss_total','dm_op_shelf_offstock_loss','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_type_offstock_loss','dm_op_shelf_offstock_loss','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_type_offstock_loss_total','dm_op_shelf_offstock_loss','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_level_offstock_loss','dm_op_shelf_offstock_loss','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_product_level_offstock_loss_total','dm_op_shelf_offstock_loss','宋英南');
END