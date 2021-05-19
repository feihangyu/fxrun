CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_cal_fill_reasonable_month_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
        
SET @sdate := CURDATE(),
@ydate := SUBDATE(@sdate,1),
@last_month_1 := SUBDATE(SUBDATE(@sdate, DAY(@sdate) - 1), INTERVAL 1 MONTH),   -- 上个月1号
@smonth_01 := DATE_FORMAT(@ydate,'%Y-%m-01'),
@ydate_month := DATE_FORMAT(@ydate,'%Y-%m'),
@two_year_month := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 2 YEAR),'%Y-%m'),
@y_m_last := DATE_FORMAT(@last_month_1,'%Y-%m');
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 补货建议合理性分析明细表(留存历史一个月)
DELETE FROM fe_dm.dm_op_cal_fill_reasonable WHERE @smonth_01  > ydate OR ydate = @ydate;
INSERT INTO fe_dm.dm_op_cal_fill_reasonable
(
        ydate,
        REGION_AREA,
        BUSINESS_AREA,
        shelf_id,
        PRODUCT_ID,
        PRODUCT_NAME,
        manager_name,
        manager_id,
        grade,
        FILL_TYPE,
        ACTUAL_APPLY_NUM,
        STOCK_NUM,
        day_sale_qty,
        pre_turnover,
        post_turnover,
        whether_fill_qty_reasonable,
        whether_fill_time_reasonable
)
SELECT 
        @ydate AS ydate,
        c.region_name AS REGION_AREA,
        c.business_name AS BUSINESS_AREA,
        a.shelf_id,
        a.`PRODUCT_ID`,
        g.PRODUCT_NAME,
        c.REAL_NAME AS manager_name,
        c.manager_id,
        c.grade,
        a.`FILL_TYPE`,
        SUM(a.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
        a.STOCK_NUM,
        f.day_sale_qty,
        a.STOCK_NUM / f.day_sale_qty AS pre_turnover,
        (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty AS post_turnover,
        CASE    
                WHEN f.day_sale_qty = 0 AND a.STOCK_NUM = 0 AND IFNULL(a.STOCK_NUM / f.day_sale_qty,0) = 0 
                        THEN '合理'
                WHEN FILL_TYPE = 2 AND (h.NEW_FLAG = 1 OR h.NEW_FLAG IS NULL)
                        THEN '合理'
                WHEN FILL_TYPE = 2 AND g.FILL_MODEL > 1 AND f.day_sale_qty < 0.07 AND g.fill_box_gauge > 10
                        THEN '过多'
                WHEN FILL_TYPE = 2 AND g.FILL_MODEL > 1 AND a.ACTUAL_APPLY_NUM > g.fill_box_gauge 
                        THEN '过多'
                WHEN FILL_TYPE = 2 AND g.FILL_MODEL > 1
                        THEN '合理' 
                WHEN FILL_TYPE IN (1,8,9) AND ((c.grade IN ('甲','乙')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty > 14 )  OR (c.grade IN ('丙','丁')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty > 30 ))
                        THEN '过多'
                WHEN FILL_TYPE IN (1,8,9) AND ((c.grade IN ('甲','乙')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty < 7 )  OR (c.grade IN ('丙','丁')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty < 10 ))
                        THEN '过少'
                WHEN FILL_TYPE IN (1,8,9) AND ((c.grade IN ('甲','乙')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty >= 7 AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty <= 14)  OR 
                        (c.grade IN ('丙','丁')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty >= 10 AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty <= 30))
                        THEN '合理'
                WHEN f.day_sale_qty = 0 AND a.STOCK_NUM = 0 AND IFNULL(a.STOCK_NUM / f.day_sale_qty,0) = 0 
                        THEN '合理'
        END AS whether_fill_qty_reasonable,
        CASE
                WHEN f.day_sale_qty = 0 AND a.STOCK_NUM = 0 AND IFNULL(a.STOCK_NUM / f.day_sale_qty,0) = 0 
                        THEN '合理'
                WHEN FILL_TYPE = 2 AND (h.NEW_FLAG = 1 OR h.NEW_FLAG IS NULL)
                        THEN '合理'
                WHEN FILL_TYPE = 2 AND g.FILL_MODEL > 1 AND a.STOCK_NUM < 0.25 * g.fill_box_gauge AND a.ACTUAL_APPLY_NUM > 0 AND f.day_sale_qty > 0.07 
                        THEN '过晚'
                WHEN FILL_TYPE = 2 AND g.FILL_MODEL > 1 AND a.STOCK_NUM > 1.5 * g.fill_box_gauge AND a.ACTUAL_APPLY_NUM > 0 
                        THEN '过早'
                WHEN FILL_TYPE = 2 AND g.FILL_MODEL > 1
                        THEN '合理'
                WHEN FILL_TYPE IN (1,8,9) AND ((c.grade IN ('甲','乙')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty > 14 )  OR (c.grade IN ('丙','丁')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty > 30 ))
                        THEN '过早'
                WHEN FILL_TYPE IN (1,8,9) AND ((c.grade IN ('甲','乙')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty < 7 )  OR (c.grade IN ('丙','丁')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty < 10 ))
                        THEN '过少'
                WHEN FILL_TYPE IN (1,8,9) AND ((c.grade IN ('甲','乙')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty >= 7 AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty <= 14)  OR 
                        (c.grade IN ('丙','丁')  AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty >= 10 AND (a.STOCK_NUM + a.ACTUAL_APPLY_NUM) / f.day_sale_qty <= 30))
                        THEN '合理'
        END AS whether_fill_time_reasonable
FROM 
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON c.SHELF_TYPE IN (1,2,3,5,6,7)
                AND c.SHELF_STATUS = 2
                AND a.APPLY_TIME >= @ydate
                AND a.`APPLY_TIME` < @sdate
                AND a.FILL_TYPE IN (1,2,8,9)
                AND a.ORDER_STATUS IN (1,2,3,4)
                AND a.shelf_id = c.shelf_id
        JOIN fe_dm.`dm_op_fill_day_sale_qty` f
                ON a.shelf_id = f.shelf_id
                AND a.product_id = f.product_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` g
                ON a.product_id = g.product_id
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` h
                ON a.shelf_id = h.shelf_id
                AND a.product_id = h.product_id
GROUP BY a.`SHELF_ID`,a.`PRODUCT_ID`,a.fill_type
;
# 补货建议合理性分析中间表(累计本月的数据)
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_reasonable_tmp`;
CREATE TEMPORARY TABLE fe_dwd.fill_reasonable_tmp (
KEY idx_shelf_id_fill_type(shelf_id,fill_type)
) AS
SELECT 
        @ydate_month AS ydate_month,
        REGION_AREA,
        BUSINESS_AREA,
        shelf_id,
        FILL_TYPE,
        SUM(IF(whether_fill_qty_reasonable = '过少',1,0)) AS fill_little_qty,
        SUM(IF(whether_fill_qty_reasonable = '过多',1,0)) AS fill_much_qty,
        SUM(IF(whether_fill_qty_reasonable = '合理',1,0)) AS fill_reasonable_qty,
        SUM(IF(whether_fill_time_reasonable = '过早',1,0)) AS fill_time_early_qty,
        SUM(IF(whether_fill_time_reasonable = '过晚',1,0)) AS fill_time_late_qty,
        SUM(IF(whether_fill_time_reasonable = '合理',1,0)) AS fill_time_reasonable_qty
FROM fe_dm.dm_op_cal_fill_reasonable 
GROUP BY shelf_id,fill_type
;
# 补货建议合理性分析结果表(每天累计更新,月最后一天保存当月的累计数据,保留最近2年) ;
-- 如果是每月1号，保留数据
DELETE FROM  fe_dm.dm_op_cal_fill_reasonable_month WHERE ydate_month = @ydate_month AND @sdate != DATE_FORMAT(@sdate,'%Y-%m-01') OR @two_year_month > ydate_month;
INSERT INTO fe_dm.dm_op_cal_fill_reasonable_month
(
        ydate_month,
        REGION_AREA,
        BUSINESS_AREA,
        shelf_id,
        FILL_TYPE,
        fill_time_early_qty,
        fill_time_late_qty,
        fill_time_reasonable_qty,
        fill_time_early_ratio,
        fill_time_late_ratio,
        fill_time_reasonable_ratio,
        fill_much_qty,
        fill_little_qty,
        fill_reasonable_qty,
        fill_much_ratio,
        fill_little_ratio,
        fill_reasonable_ratio
)
SELECT 
        ydate_month,
        REGION_AREA,
        BUSINESS_AREA,
        shelf_id,
        FILL_TYPE,
        fill_time_early_qty,
        fill_time_late_qty,
        fill_time_reasonable_qty,
        fill_time_early_qty / (fill_time_early_qty + fill_time_late_qty + fill_time_reasonable_qty) AS fill_time_early_ratio,
        fill_time_late_qty / (fill_time_early_qty + fill_time_late_qty + fill_time_reasonable_qty)  AS fill_time_late_ratio,
        fill_time_reasonable_qty / (fill_time_early_qty + fill_time_late_qty + fill_time_reasonable_qty)  AS fill_time_reasonable_ratio,
        fill_much_qty,
        fill_little_qty,
        fill_reasonable_qty,
        fill_much_qty / (fill_much_qty + fill_little_qty + fill_reasonable_qty) AS fill_much_ratio,
        fill_little_qty / (fill_much_qty + fill_little_qty + fill_reasonable_qty) AS fill_little_ratio,
        fill_reasonable_qty / (fill_much_qty + fill_little_qty + fill_reasonable_qty) AS fill_reasonable_ratio
FROM 
        fe_dwd.fill_reasonable_tmp
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_cal_fill_reasonable_month_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_cal_fill_reasonable','dm_op_cal_fill_reasonable_month_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_cal_fill_reasonable_month','dm_op_cal_fill_reasonable_month_two','宋英南');
COMMIT;
	END