CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_fill_not_push_order`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/03/27
-- Modify date: 
-- Description:	
-- 	补货系统未出单异常排查（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @last_month := SUBDATE(CURDATE(),INTERVAL 1 MONTH);
SET @sdate := CURDATE();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @day_num := DAY(@stat_date);
SET @pre_3month := SUBDATE(CURDATE(),INTERVAL 3 MONTH);
SET @time_1 := CURRENT_TIMESTAMP();
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        business_name,
        shelf_id,
        shelf_type,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        CLOSE_REMARK,
        CLOSE_TIME,
        CLOSE_TYPE,
        ACTIVATE_TIME,
        manager_type,
        inner_flag,
        is_prewarehouse_cover,
        SHELF_STATUS,
        grade
FROM
        fe_dwd.`dwd_shelf_base_day_all`
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_1--@time_2",@time_1,@time_2);
-- 货架正常品当前库存 5min
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`normal_fill_sku_qty_tmp`;
CREATE TEMPORARY TABLE fe_dwd.normal_fill_sku_qty_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        t.shelf_id,
        SUM(t.STOCK_QUANTITY) AS normal_stock_qty,
        SUM(IF(t.SHELF_FILL_FLAG = 1,1,0)) AS normal_fill_sku_qty
FROM 
        (
                SELECT 
                        a.`SHELF_ID`,
                        a.`PRODUCT_ID`,
                        b.`business_name`,
                        a.STOCK_QUANTITY,
                        a.SHELF_FILL_FLAG
                FROM
                        fe_dwd.`dwd_shelf_product_day_all` a
                JOIN fe_dwd.`dwd_shelf_base_day_all` b
                        ON a.shelf_id = b.shelf_id
        ) t
        JOIN fe_dwd.dwd_pub_product_dim_sserp c FORCE INDEX(idx_product_id_business_area)               
                ON c.product_id = t.product_id
                AND c.business_area = t.business_name
                AND c.PRODUCT_TYPE IN ('原有','新增（免费货）','新增（试运行）','预淘汰')
GROUP BY t.shelf_id
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_2--@time_3",@time_2,@time_3);
-- 货架库存周转天数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`turnover_days_tmp`;
CREATE TEMPORARY TABLE fe_dwd.turnover_days_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        ROUND(sto_val / (gmv/@day_num),2) AS turnover_days
FROM
        fe_dm.`dm_op_stock_shelf` 
WHERE sdate = @stat_date
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_3--@time_4",@time_3,@time_4);
-- 近1个月补货上架次数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`recent_1month_fill_qty_tmp`;
CREATE TEMPORARY TABLE fe_dwd.recent_1month_fill_qty_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        COUNT(DISTINCT order_id) AS recent_1month_fill_qty
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month`
WHERE FILL_TIME >= @last_month
        AND fill_type IN (1,2,4,7,8,9)
GROUP BY shelf_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_4--@time_5",@time_4,@time_5);
-- 在途订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`onload_tmp`;
CREATE TEMPORARY TABLE fe_dwd.onload_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        COUNT(DISTINCT order_id) AS onload_order_qty
FROM
        fe_dwd.`dwd_fill_day_inc`
WHERE apply_time >= @pre_3month
        AND FILL_TYPE IN (1,2,4,7,8,9)
        AND order_status IN (1,2)
GROUP BY shelf_id
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_5--@time_6",@time_5,@time_6);
-- 关联货架库存标准
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`relation_shelf_standard`;
CREATE TEMPORARY TABLE fe_dwd.relation_shelf_standard(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        CASE 
                WHEN a.shelf_type IN (1,3) AND a.grade IN ('甲','乙','新装')  THEN 180 + IFNULL(b.relation_standard,0)
                WHEN a.shelf_type IN (1,3) AND a.grade IN ('丙','丁')  THEN 130 + IFNULL(b.relation_standard,0)
                WHEN (a.shelf_type IN (2,5) OR a.type_name LIKE '%静态柜' ) AND a.grade IN ('甲','乙','新装')  THEN 110 + IFNULL(b.relation_standard,0)
                WHEN (a.shelf_type IN (2,5) OR a.type_name LIKE '%静态柜' ) AND a.grade IN ('丙','丁')  THEN 90 + IFNULL(b.relation_standard,0)
        END AS relation_shelf_standard
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        LEFT JOIN 
                (
                        SELECT 
                                a.`MAIN_SHELF_ID` AS shelf_id,
                                SUM(
                                        CASE 
                                                WHEN a.shelf_type IN (1,3) AND a.grade IN ('甲','乙','新装') THEN 180
                                                WHEN a.shelf_type IN (1,3) AND a.grade IN ('丙','丁') THEN 130
                                                WHEN (a.shelf_type IN (2,5) OR a.type_name LIKE '%静态柜' ) AND a.grade IN ('甲','乙','新装') THEN 110
                                                WHEN (a.shelf_type IN (2,5) OR a.type_name LIKE '%静态柜' ) AND a.grade IN ('丙','丁') THEN 90
                                        END
                                ) AS relation_standard
                        FROM
                                fe_dwd.`dwd_shelf_base_day_all` a
                        WHERE a.SHELF_HANDLE_STATUS = 9
                        GROUP BY a.`MAIN_SHELF_ID`
                ) b
                ON a.shelf_id = b.shelf_id
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_6--@time_7",@time_6,@time_7);
-- 历史30天内正常运算天数\补货金额低于150次数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`auto_push_tmp`;
CREATE TEMPORARY TABLE fe_dwd.auto_push_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        COUNT(*) AS cal_qty,
        SUM(IF(a.total_fill_value < 150,1,0)) AS fill_value_less_150_qty,
        SUM(IF(d.normal_stock_qty >= i.relation_shelf_standard,1,0)) AS stock_reach_standard_days,
        SUM(IF(a.whether_push_order = 1,1,0)) AS push_order_qty
FROM
        fe_dm.`dm_op_auto_push_fill_date_his` a              
        LEFT JOIN fe_dwd.normal_fill_sku_qty_tmp d
                ON a.shelf_id = d.shelf_id
        LEFT JOIN fe_dwd.relation_shelf_standard i
                ON a.shelf_id = i.shelf_id
GROUP BY a.shelf_id
;
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_7--@time_8",@time_7,@time_8);
DELETE FROM fe_dm.dm_op_fill_not_push_order WHERE sdate = CURDATE() OR sdate < @last_month; 
INSERT INTO fe_dm.dm_op_fill_not_push_order
(
        sdate,
        business_name,
        shelf_id,
        shelf_type,
        SHELF_STATUS,
        grade,
        REVOKE_STATUS,
        WHETHER_CLOSE,
        CLOSE_REMARK,
        CLOSE_TIME,
        CLOSE_TYPE,
        ACTIVATE_TIME,
        manager_type,
        inner_flag,
        is_prewarehouse_cover,
        fill_day_code,
        cal_qty,
        fill_value_less_150_qty,
        normal_stock_qty,
        stock_reach_standard_days,
        turnover_days,
        normal_fill_sku_qty,
        low_stock_days,
        recent_1month_fill_qty,
        onload_order_qty,
        push_order_qty,
        error_reason
)
SELECT 
        @sdate AS sdate,
        a.business_name,
        a.shelf_id,
        a.shelf_type,
        a.SHELF_STATUS,
        a.grade,
        a.REVOKE_STATUS,
        a.WHETHER_CLOSE,
        a.CLOSE_REMARK,
        a.CLOSE_TIME,
        a.CLOSE_TYPE,
        a.ACTIVATE_TIME,
        a.manager_type,
        a.inner_flag,
        a.is_prewarehouse_cover,
        b.fill_day_code,
        c.cal_qty,
        c.fill_value_less_150_qty,
        d.normal_stock_qty,
        c.stock_reach_standard_days,
        e.turnover_days,
        d.normal_fill_sku_qty,
        c.cal_qty - c.stock_reach_standard_days AS low_stock_days,
        f.recent_1month_fill_qty,
        g.onload_order_qty,
        c.push_order_qty,
        CASE 
                WHEN a.REVOKE_STATUS != 1 OR a.SHELF_STATUS NOT IN (1,2)
                        THEN '1-货架撤架或注销'
                WHEN a.WHETHER_CLOSE = 1
                        THEN '2-货架关闭'
                WHEN (a.grade IN ('甲','乙','新装') AND c.cal_qty < 7 ) OR (a.grade = '丁' AND c.cal_qty < 22 ) OR (a.grade NOT IN ('甲','乙','新装','丁')  AND c.cal_qty < 22 )
                        THEN '3-货架开启时间不足'
                WHEN b.fill_day_code IS NULL 
                        THEN '4-货架出单日为空'
                WHEN d.normal_stock_qty >= i.relation_shelf_standard
                        THEN '5-当前库存充足'
                WHEN c.stock_reach_standard_days > c.cal_qty * 0.75
                        THEN '6-货架库存一直充足'
                WHEN (a.grade = '甲' AND e.turnover_days >= 4) OR (a.grade IN ('乙','新装') AND e.turnover_days >= 7) OR (a.grade = '丙' AND e.turnover_days >= 14) OR (a.grade = '丁' AND e.turnover_days >= 22)
                        THEN '7-货架周转满足'
                WHEN f.recent_1month_fill_qty > 0
                        THEN '8-系统出单前已补货'
                WHEN g.onload_order_qty > 0
                        THEN '9-有在途订单'
                WHEN a.grade = '丁' AND c.fill_value_less_150_qty >= c.cal_qty * 0.5 
                        THEN '10-严重滞销货架补货金额不足150'
                WHEN c.fill_value_less_150_qty >= c.cal_qty * (2/3)
                        THEN '11-补货金额一直不足150'
                ELSE '12-人工排查'
        END AS error_reason
FROM
        fe_dwd.shelf_tmp a
        LEFT JOIN fe_dwd.dwd_sf_shelf_fill_day_config b                
                ON a.shelf_id = b.`shelf_id`
                AND b.data_flag = 1
        LEFT JOIN fe_dwd.auto_push_tmp c
                ON a.shelf_id = c.shelf_id
        LEFT JOIN fe_dwd.normal_fill_sku_qty_tmp d
                ON a.shelf_id = d.shelf_id
        LEFT JOIN fe_dwd.turnover_days_tmp e
                ON a.shelf_id = e.shelf_id
        LEFT JOIN fe_dwd.recent_1month_fill_qty_tmp f
                ON a.shelf_id = f.shelf_id
        LEFT JOIN fe_dwd.onload_tmp g
                ON a.shelf_id = g.shelf_id
        LEFT JOIN fe_dwd.`dwd_shelf_machine_info` h
                ON a.shelf_id = h.shelf_id 
        LEFT JOIN fe_dwd.relation_shelf_standard i
                ON a.shelf_id = i.shelf_id 
;
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_fill_not_push_order","@time_8--@time_9",@time_8,@time_9);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_fill_not_push_order',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_fill_not_push_order','dm_op_fill_not_push_order','宋英南');
END