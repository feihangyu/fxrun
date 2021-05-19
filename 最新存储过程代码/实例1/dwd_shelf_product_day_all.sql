CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_shelf_product_day_all`()
BEGIN
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
	
SET @time_1 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_tmp_1_1`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_tmp_1_1 (PRIMARY KEY (product_id,BUSINESS_AREA,shelf_id))AS		
 SELECT a1.PRODUCT_ID, a1.SHELF_ID, a2.BUSINESS_name AS BUSINESS_AREA
    FROM fe.sf_shelf_product_detail a1 
    JOIN fe_dwd.dwd_shelf_base_day_all a2 
	ON a1.SHELF_ID=a2.SHELF_ID 
WHERE a1.DATA_FLAG = 1;	
	
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_1--@time_2",@time_1,@time_2);
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_lsl_shelf_product_tmp_1`;
CREATE TEMPORARY TABLE fe_dwd.dwd_lsl_shelf_product_tmp_1 (PRIMARY KEY (product_id,shelf_id))AS	  
  SELECT DISTINCT a1.PRODUCT_ID, a1.SHELF_ID, a4.purchase_price
    FROM fe_dwd.dwd_lsl_shelf_product_tmp_1_1 a1
    LEFT JOIN
       fe_dm.`dm_sc_current_dynamic_purchase_price` a4 
		ON a1.PRODUCT_ID=a4.PRODUCT_ID 
		AND a1.BUSINESS_AREA=a4.business_area;

    
CREATE INDEX dwd_lsl_shelf_product_tmp_1
ON fe_dwd.dwd_lsl_shelf_product_tmp_1 (shelf_id,product_id);
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_3--@time_4",@time_3,@time_4);
-- 自贩机货架商品库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_detail_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_detail_tmp (PRIMARY KEY (shelf_id, product_id))
SELECT
        a.`SHELF_ID`,
        a.`PRODUCT_ID`,
        a.`STOCK_QUANTITY`
FROM
        fe.`sf_shelf_product_detail` a
        JOIN fe.`sf_shelf` b
                ON a.`SHELF_ID` = b.`SHELF_ID`
                AND b.`SHELF_TYPE` = 7
WHERE a.`DATA_FLAG` = 1
        AND b.`DATA_FLAG` = 1
        AND ! ISNULL(a.shelf_id)
        AND ! ISNULL(a.product_id)
;
SET @time_04 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_4--@time_04",@time_4,@time_04);
-- 自贩机货道库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.machine_slot_tmp;
CREATE TEMPORARY TABLE fe_dwd.machine_slot_tmp (PRIMARY KEY (shelf_id, product_id))
SELECT
        t.shelf_id, 
        t.product_id, 
        SUM(t.stock_num) slot_stock_num
FROM
        fe.sf_shelf_machine_slot t
        JOIN fe.`sf_shelf` s
                ON t.shelf_id = s.shelf_id
WHERE s.shelf_type = 7
        AND t.data_flag = 1
        AND s.data_flag = 1
        AND ! ISNULL(t.shelf_id)
        AND ! ISNULL(t.product_id)
GROUP BY t.shelf_id, t.product_id
;
SET @time_05 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_04--@time_05",@time_04,@time_05);
-- 自贩机副柜库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.machine_second_tmp;
CREATE TEMPORARY TABLE fe_dwd.machine_second_tmp (PRIMARY KEY (shelf_id, product_id))
SELECT
        t.shelf_id, 
        msd.product_id, 
        SUM(msd.stock_num) second_stock_num
FROM
        fe.sf_shelf_machine_second t
        JOIN fe.sf_shelf_machine_second_detail msd
                ON t.machine_second_id = msd.machine_second_id
                AND msd.data_flag = 1
        JOIN fe.sf_shelf s
                ON t.shelf_id = s.shelf_id
WHERE s.shelf_type = 7
        AND t.data_flag = 1
        AND msd.data_flag = 1
        AND s.data_flag = 1
        AND ! ISNULL(t.shelf_id)
        AND ! ISNULL(msd.product_id)
GROUP BY t.shelf_id, msd.product_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_05--@time_5",@time_05,@time_5);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.machine_second_tmp_rst;
CREATE TEMPORARY TABLE fe_dwd.machine_second_tmp_rst (PRIMARY KEY (shelf_id, product_id))
SELECT
        a.shelf_id,
        a.product_id,
        IFNULL(b.slot_stock_num,0) + IFNULL(c.second_stock_num,0) AS STOCK_QUANTITY
FROM
        fe_dwd.shelf_product_detail_tmp a
        LEFT JOIN fe_dwd.machine_slot_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
        LEFT JOIN fe_dwd.machine_second_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.product_id = c.product_id
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_5--@time_6",@time_5,@time_6);
-- 每天开始插入数据之前删掉之前的数据
TRUNCATE TABLE fe_dwd.dwd_shelf_product_day_all;
INSERT INTO fe_dwd.dwd_shelf_product_day_all
(
DETAIL_ID,
ITEM_ID,
PRODUCT_ID,
SHELF_ID,
MAX_QUANTITY,
ALARM_QUANTITY,
STOCK_QUANTITY,
SALE_PRICE,
FIRST_FILL_TIME,
PURCHASE_PRICE,
MANAGER_FILL_FLAG,
SHELF_FILL_FLAG,
PACKAGE_FLAG,
NEAR_DATE,
DANGER_FLAG,
production_date,
risk_source,
SALES_FLAG,
NEW_FLAG,
NEAR_DAYS,
SALES_STATUS,
NEAR_DATE_SOURCE_FLAG,
operate_sale_reason,
business_status,
allow_fill_status,
operate_fill_status,
operate_fill_reason,
allow_sale_status,
operate_sale_status,
smart_fill_status,
add_time,
add_user_id
)
SELECT 
a.DETAIL_ID,
a.ITEM_ID,
a.PRODUCT_ID,
a.SHELF_ID,
a.MAX_QUANTITY,
a.ALARM_QUANTITY,
-- a.STOCK_QUANTITY,
IFNULL(d.STOCK_QUANTITY,a.STOCK_QUANTITY) STOCK_QUANTITY,
a.SALE_PRICE,
b.FIRST_FILL_TIME,
IFNULL(c.PURCHASE_PRICE,a.PURCHASE_PRICE) PURCHASE_PRICE,
b.manager_fill_flag,
a.SHELF_FILL_FLAG,
a.PACKAGE_FLAG,
b.NEAR_DATE,
b.DANGER_FLAG,
b.production_date,
b.risk_source,
b.SALES_FLAG,
b.NEW_FLAG,
b.NEAR_DAYS,
b.SALES_STATUS,
b.NEAR_DATE_SOURCE_FLAG,
b.operate_sale_reason,
b.business_status,
b.allow_fill_status,
b.operate_fill_status,
b.operate_fill_reason,
b.allow_sale_status,
b.operate_sale_status,
b.smart_fill_status,
b.add_time,
b.add_user_id
FROM
fe.sf_shelf_product_detail a
LEFT JOIN
fe.sf_shelf_product_detail_flag b
ON a.DETAIL_ID = b.DETAIL_ID
LEFT JOIN
fe_dwd.dwd_lsl_shelf_product_tmp_1 c
ON a.PRODUCT_ID = c.PRODUCT_ID
AND a.shelf_id = c.shelf_id
LEFT JOIN
fe_dwd.machine_second_tmp_rst d
ON a.PRODUCT_ID = d.PRODUCT_ID
AND a.shelf_id = d.shelf_id
WHERE  a.data_flag =1
AND b.data_flag =1;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_shelf_product_day_all","@time_4--@time_5",@time_4,@time_5);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_shelf_product_day_all',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
  COMMIT;
	
END