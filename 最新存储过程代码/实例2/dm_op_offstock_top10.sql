CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_offstock_top10`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/06/05
-- Modify date: 
-- Description:	
-- 	地区TOP商品缺货率 / 地区高增长商品缺货率（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := SUBDATE(CURDATE(),1);
SET @month_id := DATE_FORMAT(@sdate,'%Y-%m');
SET @pre_14day := SUBDATE(@sdate,14);
SET @pre_1month := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 1 MONTH),'%Y-%m');
SET @pre_2month := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 2 MONTH),'%Y-%m');
-- 地区TOP商品缺货率
-- 地区单个商品上个月总GMV排名前10商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.gmv_top10;
CREATE TEMPORARY TABLE fe_dwd.gmv_top10 (KEY (business_name))
SELECT
        business_name,
        GROUP_CONCAT(product_id ORDER BY gmv DESC) AS com_product_id
FROM
        fe_dm.dm_op_area_product_mgmv
WHERE month_id = @month_id
GROUP BY business_name
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`area_product_top10`;
CREATE TEMPORARY TABLE fe_dwd.area_product_top10(KEY (business_name))
AS 
SELECT 
        DISTINCT 
        a.business_name,
        SUBSTRING_INDEX(SUBSTRING_INDEX(a.`com_product_id`,',',b.`number`),',',-1) AS product_id,
        b.number AS rank_flag
FROM
        fe_dwd.gmv_top10  a
        JOIN fe_dwd.`dwd_pub_number` b
                ON b.number <= IF((LENGTH(a.com_product_id) - LENGTH(REPLACE(a.com_product_id,',','')) + 1) <= 10,
                (LENGTH(a.com_product_id) - LENGTH(REPLACE(a.com_product_id,',','')) + 1),10)
;
-- 缺货率
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`offstock_tmp`;
CREATE TEMPORARY TABLE fe_dwd.offstock_tmp(KEY (business_name,product_id)) 
AS 
SELECT
        business_name,
        product_id,
        reason_classify,
        SUM(offstock_ct) AS offstock_ct,
        SUM(ct) AS ct,
        ROUND(SUM(offstock_ct) / SUM(ct),2) AS offstock_rate
FROM
(
        SELECT
                business_name,
                product_id,
                reason_classify,
                COUNT(*) - SUM(stock_quantity > 0)  AS offstock_ct,
                COUNT(*) AS ct
        FROM
                fe_dm.`dm_op_sp_offstock`
        WHERE shelf_type IN (1,2,3,5,6)
                AND sales_flag IN (1,2,3)
                AND shelf_fill_flag = 1
        GROUP BY business_name,product_id,reason_classify
        UNION ALL
        SELECT
                business_name,
                product_id,
                reason_classify,
                COUNT(*) - SUM(stock_num > 0) AS offstock_ct,
                COUNT(*) AS ct
        FROM
                fe_dm.`dm_op_offstock_slot`
        WHERE sdate = @sdate
        GROUP BY business_name,product_id,reason_classify
) t1
GROUP BY business_name,product_id,reason_classify
;
-- 保留近14天
DELETE FROM fe_dm.dm_op_offstock_top10 WHERE sdate = @sdate OR sdate < @pre_14day;
INSERT INTO fe_dm.dm_op_offstock_top10
(
        sdate,
        business_name,
        product_id,
        reason_classify,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        offstock_ct,
        ct,
        offstock_rate,
        rank_flag
)
SELECT
        @sdate AS sdate,
        t1.business_name,
        t1.product_id,
        t1.reason_classify,
        t3.PRODUCT_CODE2,
        t3.PRODUCT_NAME,
        t1.offstock_ct,
        t1.ct,
        t1.offstock_rate,
        t4.rank_flag
FROM
        fe_dwd.offstock_tmp t1
        JOIN fe_dwd.`dwd_product_base_day_all` t3
                ON t1.product_id = t3.product_id
        JOIN fe_dwd.area_product_top10 t4
                ON t1.business_name = t4.business_name
                AND t1.product_id = t4.product_id
;
-- ==============================================================================================
-- 地区高增长商品缺货率
-- 地区高增长商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`high_growth_tmp1`;
CREATE TEMPORARY TABLE fe_dwd.high_growth_tmp1(KEY (business_name,product_id)) 
AS 
SELECT
        c.business_name,
        t.product_id,
        SUM(t.days_sto) AS shelf_sto_days,
        SUM(s.gmv) AS gmv,
        ROUND(SUM(s.gmv) / SUM(t.days_sto),2) AS day_shelf_gmv
FROM
        fe_dm.dm_op_product_shelf_sto_month t
        LEFT JOIN fe_dm.dm_op_product_shelf_sal_month s
                ON t.month_id = s.month_id
                AND t.product_id = s.product_id
                AND t.shelf_id = s.shelf_id
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON t.`shelf_id` = c.`shelf_id`
WHERE t.month_id = @pre_1month
GROUP BY c.business_name,t.product_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`high_growth_tmp2`;
CREATE TEMPORARY TABLE fe_dwd.high_growth_tmp2(KEY (business_name,product_id)) 
AS 
SELECT
        c.business_name,
        t.product_id,
        SUM(t.days_sto) AS shelf_sto_days,
        SUM(s.gmv) AS gmv,
        ROUND(SUM(s.gmv) / SUM(t.days_sto),2) AS day_shelf_gmv
FROM
        fe_dm.dm_op_product_shelf_sto_month t
        LEFT JOIN fe_dm.dm_op_product_shelf_sal_month s
                ON t.month_id = s.month_id
                AND t.product_id = s.product_id
                AND t.shelf_id = s.shelf_id
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON t.`shelf_id` = c.`shelf_id`
WHERE t.month_id = @pre_2month
GROUP BY c.business_name,t.product_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`high_growth_tmp3`;
CREATE TEMPORARY TABLE fe_dwd.high_growth_tmp3(KEY (business_name,product_id)) 
AS 
SELECT
        a.business_name,
        a.product_id
FROM
        fe_dwd.high_growth_tmp1 a
        JOIN fe_dwd.high_growth_tmp2 b
                ON a.business_name = b.business_name
                AND a.product_id = b.product_id
WHERE IFNULL(a.day_shelf_gmv,0) - IFNULL(b.day_shelf_gmv,0) >= 0.2
;
DELETE FROM fe_dm.dm_op_offstock_high_growth WHERE sdate = @sdate OR sdate < @pre_14day;
INSERT INTO fe_dm.dm_op_offstock_high_growth
(
        sdate,
        business_name,
        product_id,
        reason_classify,
        PRODUCT_CODE2,
        PRODUCT_NAME,
        offstock_ct,
        ct,
        offstock_rate
)
SELECT
        @sdate AS sdate,
        t1.business_name,
        t1.product_id,
        t1.reason_classify,
        t3.PRODUCT_CODE2,
        t3.PRODUCT_NAME,
        t1.offstock_ct,
        t1.ct,
        t1.offstock_rate
FROM
        fe_dwd.offstock_tmp t1
        JOIN fe_dwd.`dwd_pub_product_dim_sserp` t2
                ON t1.business_name = t2.business_area
                AND t1.product_id = t2.product_id
                AND t2.PRODUCT_TYPE IN ('原有','新增（试运行）')
        JOIN fe_dwd.`dwd_product_base_day_all` t3
                ON t1.product_id = t3.product_id
        JOIN fe_dwd.high_growth_tmp3 t4
                ON t1.business_name = t4.business_name
                AND t1.product_id = t4.product_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_offstock_top10',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_top10','dm_op_offstock_top10','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_high_growth','dm_op_offstock_top10','宋英南');
END