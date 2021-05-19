CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_product_sto_sal_day30`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @pre_day30 := SUBDATE(CURDATE(),30);
SET @cur_month_01 := DATE_FORMAT(@stat_date,'%Y-%m-01');
-- 近30天 
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`pre_day30_tmp`;
CREATE TEMPORARY TABLE fe_dwd.pre_day30_tmp(
       PRIMARY KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        shelf_id,
        product_id,
        SUM(stock_quantity > 0) AS stock_day30,
        SUM(sal_qty > 0) AS sal_day30,
        COUNT(*) AS stock_sal_day30,
        SUM(sal_qty) AS sal_qty_day30
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_30_days`
WHERE sdate >= @pre_day30
        AND (stock_quantity > 0 OR sal_qty > 0)                 -- 有库存或有销售
GROUP BY shelf_id,product_id
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_day30","@time_1--@time_2",@time_1,@time_2);	
-- 本月 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`cur_month_tmp`;
CREATE TEMPORARY TABLE fe_dwd.cur_month_tmp(
        PRIMARY KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        shelf_id,
        product_id,
        SUM(stock_quantity > 0) AS cur_month_stock_days,
        SUM(sal_qty > 0) AS cur_month_sal_days,
        COUNT(*) AS cur_month_stock_sal_days,
        SUM(stock_quantity) AS cur_month_total_stock_qty,
        SUM(sal_qty) AS cur_month_sal_qty
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_30_days`
WHERE sdate >= @cur_month_01
        AND (stock_quantity > 0 OR sal_qty > 0)                -- 有库存或有销售
GROUP BY shelf_id,product_id
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_day30","@time_2--@time_3",@time_2,@time_3);	
-- 本月一号
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`cur_month_01_tmp`;
CREATE TEMPORARY TABLE fe_dwd.cur_month_01_tmp(
        PRIMARY KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        shelf_id,
        product_id,
        stock_quantity
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_30_days`
WHERE sdate = @cur_month_01
        AND (stock_quantity > 0 OR sal_qty > 0)                -- 有库存或有销售
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_day30","@time_3--@time_4",@time_3,@time_4);	
TRUNCATE TABLE fe_dwd.dwd_shelf_product_sto_sal_day30;
INSERT INTO fe_dwd.dwd_shelf_product_sto_sal_day30
(
        stat_date,
        business_name,
        shelf_id,
        product_id,
        sale_price,
        stock_day30,
        sal_day30,
        stock_sal_day30,
        sal_qty_day30,
        start_stock_qty,
        stock_quantity,
        cur_month_stock_days,
        cur_month_sal_days,
        cur_month_stock_sal_days,
        cur_month_total_stock_qty,
        cur_month_sal_qty
)
SELECT
        @stat_date AS stat_date,
        c.business_name,
        a.shelf_id,
        a.product_id,
        e.sale_price,
        a.stock_day30,
        a.sal_day30,
        a.stock_sal_day30,
        a.sal_qty_day30,
        d.stock_quantity AS start_stock_qty,
        e.stock_quantity,
        b.cur_month_stock_days,
        b.cur_month_sal_days,
        b.cur_month_stock_sal_days,
        b.cur_month_total_stock_qty,
        b.cur_month_sal_qty
FROM
        fe_dwd.pre_day30_tmp a
        LEFT JOIN fe_dwd.cur_month_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.shelf_id = c.`shelf_id`
        LEFT JOIN fe_dwd.cur_month_01_tmp d
                ON a.shelf_id = d.shelf_id
                AND a.product_id = d.product_id
        JOIN fe_dwd.`dwd_shelf_product_day_all` e
                ON a.shelf_id = e.shelf_id
                AND a.product_id = e.product_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_sto_sal_day30","@time_4--@time_5",@time_4,@time_5);	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_shelf_product_sto_sal_day30',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_shelf_product_sto_sal_day30','dwd_shelf_product_sto_sal_day30','李世龙');
 
END