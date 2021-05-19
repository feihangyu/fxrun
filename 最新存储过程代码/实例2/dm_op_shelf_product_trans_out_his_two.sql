CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_product_trans_out_his_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 地区货架商品维度30天加权销(临时表：fe_dwd.shelf_product_30sales_temp)
SET @tday := CONCAT( 'DAY' ,DAYOFMONTH(CURDATE()) , '_QUANTITY');
SET @yday := CONCAT( 'DAY' ,DAYOFMONTH(DATE_SUB(CURDATE(),INTERVAL 1 DAY)), '_QUANTITY');
SET @smonth := DATE_FORMAT(CURDATE(),'%Y-%m');
SET @cdate := CURDATE();
SET @sdate := SUBDATE(CURDATE(),1);
SET @pre_day30 := SUBDATE(@cdate,30);
SET @pre_day2 := SUBDATE(@sdate,2);
-- 昨天的库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.pre_day2_shelf_product_tmp;
CREATE TEMPORARY TABLE fe_dwd.pre_day2_shelf_product_tmp
(KEY idx_shelf_id_product_id(shelf_id,product_id)) 
SELECT
        shelf_id,
        product_id,
        stock_quantity
FROM
        fe_dwd.`dwd_shelf_product_day_all_recent_32`
WHERE sdate = @pre_day2

;
-- 前30天gmv+补付款
DROP TEMPORARY TABLE IF EXISTS fe_dwd.gmv_after_payment_tmp;
CREATE TEMPORARY TABLE fe_dwd.gmv_after_payment_tmp
(KEY idx_shelf_id(shelf_id)) 
SELECT 
        `SHELF_ID`,
        (SUM(`GMV`) + SUM(`AFTER_PAYMENT_MONEY`))/SUM(`GMV`) AS A
FROM fe_dwd.`dwd_shelf_day_his` 
WHERE `sdate` <= @sdate
        AND `sdate` >= @pre_day30
GROUP BY 1
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_30sales_temp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_30sales_temp
(KEY idx_shelf_id_product_id(shelf_id,product_id)) 
AS 
SELECT 
        a.`BUSINESS_AREA`,
        a.`shelf_id`,
        c.SHELF_CODE,
        c.`SF_CODE`,
        c.`REAL_NAME`,
        a.`product_id`,
        b.`PRODUCT_CODE2`,
        b.`PRODUCT_NAME`,
        t1.`SALES_FLAG`,
        t1.stock_quantity AS tday_Q,
        t2.stock_quantity AS yday_Q,
        (a.`day_sale_qty` * 30) AS month_sale_qty,
        IFNULL(t0.`A`,1) AS A,
        (a.`day_sale_qty` * 30 * IFNULL(t0.`A`,1)) AS month_sale_qty_A,
        t1.SHELF_FILL_FLAG,
        c.shelf_type,
        b.fill_model,
        t1.SALE_PRICE,
        t1.MAX_QUANTITY
FROM 
        fe_dm.dm_op_fill_day_sale_qty a
        JOIN fe_dwd.`dwd_product_base_day_all` b
                ON a.`product_id` = b.`PRODUCT_ID`
                AND b.FILL_MODEL = 1
        JOIN fe_dwd.`dwd_shelf_base_day_all` c
                ON a.`SHELF_ID` = c.`SHELF_ID`
        LEFT JOIN fe_dwd.gmv_after_payment_tmp t0
                ON a.`shelf_id` = t0.`SHELF_ID`
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` t1
                ON a.`shelf_id` = t1.`SHELF_ID`
                AND a.`product_id` = t1.`PRODUCT_ID`
        LEFT JOIN fe_dwd.pre_day2_shelf_product_tmp t2
                ON a.shelf_id = t2.SHELF_ID
                AND a.product_id = t2.PRODUCT_ID
WHERE a.`day_sale_qty` > 0
        AND c.SHELF_TYPE IN (1,2,3,6)
        AND c.SHELF_STATUS = 2
        AND c.WHETHER_CLOSE = 2
        AND c.REVOKE_STATUS = 1
        AND c.manager_type = '全职店主'
        AND @cdate >= ADDDATE(FIRST_FILL_TIME,30)
GROUP BY a.`shelf_id`,a.product_id
;
-- 地区店主商品维度标准差（fe_dwd.std_manager_product_temp,std_manager_product > 4）
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`std_manager_product_temp`;
CREATE TEMPORARY TABLE fe_dwd.std_manager_product_temp
(KEY idx_sf_code_product_id(SF_CODE,product_id)) 
AS 
SELECT 
        a.`SF_CODE`,
        a.`PRODUCT_ID`,
        STD(a.month_sale_qty_A) AS std_manager_product
FROM fe_dwd.shelf_product_30sales_temp a
GROUP BY 1,2
HAVING std_manager_product > 4;
-- 货架商品维度可做调货商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`trans_out_tmp`;
CREATE TEMPORARY TABLE fe_dwd.trans_out_tmp (
KEY idx_shelf_id_product_id (shelf_id,product_id)
) AS
SELECT 
        a.`BUSINESS_AREA`,
        a.`SHELF_ID`,
        a.SHELF_CODE,
        a.`SF_CODE`,
        a.`REAL_NAME`,
        d.prewarehouse_id AS warehouse_id,
        a.`PRODUCT_ID`,
        a.`PRODUCT_CODE2`,
        a.SALE_PRICE,
        a.`PRODUCT_NAME`,
        a.FILL_MODEL,
        a.`SALES_FLAG`,
        a.tday_Q,
        a.yday_Q,
        a.month_sale_qty_A,
        IF(a.SHELF_FILL_FLAG = 2,a.MAX_QUANTITY,CEILING(c.day_sale_qty * (IFNULL(c.fill_cycle,0) + IFNULL(c.fill_days,0) + 1) + c.safe_stock_qty + c.suspect_false_stock_qty) )  AS remain_qty
FROM fe_dwd.shelf_product_30sales_temp a
JOIN fe_dwd.std_manager_product_temp b
    ON a.`SF_CODE` = b.`SF_CODE`
    AND a.`PRODUCT_ID` = b.`PRODUCT_ID`
    AND a.`SALES_FLAG` IN (4,5)
LEFT JOIN fe_dm.`dm_op_shelf_product_fill_update` c
    ON a.shelf_id = c.`SHELF_ID`
    AND a.product_id = c.`PRODUCT_ID`
LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` d
    ON a.shelf_id = d.shelf_id
LEFT JOIN fe_dm.`dm_sc_preware_daily_report` e
    ON e.sdate = @sdate
    AND d.prewarehouse_id = e.warehouse_id
    AND a.product_id = e.product_id
WHERE ((e.`sales_level_flag` IN (1,2) AND e.valid_turnover_days < 15)
                OR (e.`sales_level_flag` IN (3,4,5) AND e.valid_turnover_days < 8))
        AND ( (a.SHELF_FILL_FLAG = 1 AND a.tday_Q - CEILING(c.day_sale_qty * (IFNULL(c.fill_cycle,0) + IFNULL(c.fill_days,0) + 1) + c.safe_stock_qty + c.suspect_false_stock_qty) >0) 
          OR (a.SHELF_FILL_FLAG = 2 AND a.tday_Q  >0) )
        AND a.BUSINESS_AREA IN ('安徽区','北京区','大连区','东莞区','佛山区','福州区','广州区','河南区','黑龙江区','湖南区','吉林区','济南区','冀州区','江西区','鲁东区','南京区','南通区','宁波区','泉州区','厦门区','山西区','陕西区','上海区','深圳区','沈阳区','四川区','苏州区','天津区','无锡区','浙北区','重庆区')
;
# 无人货架建议调出量前10的SKU
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`com_trans_out_product_id_10`;
CREATE TEMPORARY TABLE fe_dwd.com_trans_out_product_id_10(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        GROUP_CONCAT(b.product_id ORDER BY b.tday_Q - b.remain_qty DESC) AS trans_out_product_id
FROM 
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`trans_out_tmp` b
                ON a.shelf_id = b.shelf_id
                AND a.shelf_type != 6
GROUP BY a.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`trans_out_product_id_top10`;
CREATE TEMPORARY TABLE fe_dwd.trans_out_product_id_top10 
AS 
SELECT 
        a.shelf_id,
        SUBSTRING_INDEX(SUBSTRING_INDEX(a.`trans_out_product_id`,',',b.`number`),',',-1) AS product_id
FROM
        fe_dwd.`com_trans_out_product_id_10` a
        JOIN fe_dwd.`dwd_pub_number` b
                ON b.number <= IF((LENGTH(a.trans_out_product_id) - LENGTH(REPLACE(a.trans_out_product_id,',','')) + 1) <= 10,
                (LENGTH(a.trans_out_product_id) - LENGTH(REPLACE(a.trans_out_product_id,',','')) + 1),10)
;
# 智能柜调库存数量TOP5的商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`com_trans_out_product_id`;
CREATE TEMPORARY TABLE fe_dwd.com_trans_out_product_id(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        GROUP_CONCAT(b.product_id ORDER BY b.tday_Q DESC) AS trans_out_product_id
FROM 
        fe_dwd.`dwd_shelf_base_day_all` a
        JOIN fe_dwd.`trans_out_tmp` b
                ON a.shelf_id = b.shelf_id
                AND a.shelf_type = 6
GROUP BY a.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`trans_out_product_id_top5`;
CREATE TEMPORARY TABLE fe_dwd.trans_out_product_id_top5 
AS 
SELECT 
        a.shelf_id,
        SUBSTRING_INDEX(SUBSTRING_INDEX(a.`trans_out_product_id`,',',b.`number`),',',-1) AS product_id
FROM
        fe_dwd.`com_trans_out_product_id` a
        JOIN fe_dwd.`dwd_pub_number` b
                ON b.number <= IF((LENGTH(a.trans_out_product_id) - LENGTH(REPLACE(a.trans_out_product_id,',','')) + 1) <= 5,
                (LENGTH(a.trans_out_product_id) - LENGTH(REPLACE(a.trans_out_product_id,',','')) + 1),5)
;
TRUNCATE TABLE `fe_dm`.`dm_op_shelf_product_trans_out_list`;
INSERT INTO `fe_dm`.`dm_op_shelf_product_trans_out_list`
(
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_CODE,
        SF_CODE,
        REAL_NAME,
        warehouse_id,
        PRODUCT_ID,
        PRODUCT_CODE2,
        SALE_PRICE,
        PRODUCT_NAME,
        FILL_MODEL,
        SALES_FLAG,
        tday_Q,
        yday_Q,
        month_sale_qty_A,
        remain_qty
)
SELECT 
        a.*
FROM 
        fe_dwd.trans_out_tmp a
        JOIN fe_dwd.trans_out_product_id_top10 b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
;
INSERT INTO `fe_dm`.`dm_op_shelf_product_trans_out_list`
(
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_CODE,
        SF_CODE,
        REAL_NAME,
        warehouse_id,
        PRODUCT_ID,
        PRODUCT_CODE2,
        SALE_PRICE,
        PRODUCT_NAME,
        FILL_MODEL,
        SALES_FLAG,
        tday_Q,
        yday_Q,
        month_sale_qty_A,
        remain_qty
)
SELECT
        a.*
FROM
        fe_dwd.trans_out_tmp a
        JOIN fe_dwd.trans_out_product_id_top5 b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
;
-- 截存30天数据
DELETE FROM `fe_dm`.`dm_op_shelf_product_trans_out_his` WHERE cdate < @pre_day30 OR cdate = @cdate;
INSERT INTO fe_dm.`dm_op_shelf_product_trans_out_his`
(
        cdate,
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_CODE,
        SF_CODE,
        REAL_NAME,
        warehouse_id,
        PRODUCT_ID,
        PRODUCT_CODE2,
        SALE_PRICE,
        PRODUCT_NAME,
        FILL_MODEL,
        SALES_FLAG,
        tday_Q,
        yday_Q,
        month_sale_qty_A,
        remain_qty
)
SELECT
        @cdate AS cdate,
        BUSINESS_AREA,
        SHELF_ID,
        SHELF_CODE,
        SF_CODE,
        REAL_NAME,
        warehouse_id,
        PRODUCT_ID,
        PRODUCT_CODE2,
        SALE_PRICE,
        PRODUCT_NAME,
        FILL_MODEL,
        SALES_FLAG,
        tday_Q,
        yday_Q,
        month_sale_qty_A,
        remain_qty
FROM `fe_dm`.`dm_op_shelf_product_trans_out_list`;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_product_trans_out_his_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
 
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_product_trans_out_list','dm_op_shelf_product_trans_out_his_two','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_product_trans_out_his','dm_op_shelf_product_trans_out_his_two','宋英南');
COMMIT;
	END