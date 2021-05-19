CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_in_out_stock`()
    SQL SECURITY INVOKER
BEGIN
        DECLARE l_test VARCHAR(1);
        DECLARE l_row_cnt INT;
        DECLARE CODE CHAR(5) DEFAULT '00000';
        DECLARE done INT;
        DECLARE l_table_owner   VARCHAR(64);
        DECLARE l_city          VARCHAR(64);
        DECLARE l_task_name     VARCHAR(64);
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
                DECLARE EXIT HANDLER FOR SQLEXCEPTION
                BEGIN
                        GET DIAGNOSTICS CONDITION 1
                        CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
                        CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
                END; 
        SET l_task_name = 'sp_op_in_out_stock';
        SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate := DATE_FORMAT(SUBDATE(CURDATE(),INTERVAL 1 MONTH),'%Y-%m-01');     -- 期初
SET @edate := DATE_FORMAT(CURDATE(),'%Y-%m-01');     -- 期末
SET @y_m := DATE_FORMAT(SUBDATE(CURDATE(),INTERVAL 1 MONTH),'%Y-%m');
SET @ym := DATE_FORMAT(SUBDATE(CURDATE(),INTERVAL 1 MONTH),'%Y%m');
-- 期初库存 22s  
DROP TEMPORARY TABLE IF EXISTS feods.`start_tmp`;
CREATE TEMPORARY TABLE feods.start_tmp(
        KEY idx_area_product_id(business_name,product_id)
) AS 
SELECT 
        b.`business_name`,
        a.`PRODUCT_ID`,
        SUM(IF(a.DAY1_QUANTITY > 0,a.DAY1_QUANTITY,0)) AS start_stock_qty      
FROM
        fe.`sf_shelf_product_stock_detail` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.`SHELF_ID` = b.`shelf_id`
WHERE a.`STAT_DATE` = @y_m
GROUP BY b.`business_name`, a.`PRODUCT_ID`
;
-- 进 1s
DROP TEMPORARY TABLE IF EXISTS feods.`in_tmp`;
CREATE TEMPORARY TABLE feods.in_tmp(
        KEY idx_area_product_id(business_name,product_id)
) AS 
SELECT
        a.business_area AS business_name,
        a.product_id,
        a.weighted_average_price,  -- 加权平均价(采购价)
        a.actual_shelf_qty,    -- 本期实际上架量数量
        a.actual_shelf_qty * a.weighted_average_price AS actual_shelf_value       -- 本期实际上架金额(采购价)
FROM
        feods.`D_MP_shelf_system_temp_table_main` a
WHERE a.STAT_DATE =  @ym
    AND a.data_flag = 1
;
-- 销 5s
DROP TEMPORARY TABLE IF EXISTS feods.`sale_tmp`;
CREATE TEMPORARY TABLE feods.sale_tmp(
        KEY idx_area_product_id(business_name,product_id)
) AS 
SELECT
        b.business_name,
        a.product_id,
        SUM(a.qty_sal) AS qty_sal,
        SUM(a.gmv) / SUM(a.qty_sal) AS avg_sale_price,
        SUM(a.gmv) AS gmv,
        SUM(a.discount) AS discount,
        SUM(a.coupon) AS coupon,
        SUM(a.gmv) - SUM(a.discount) - SUM(a.coupon) AS real_value
FROM
        feods.`d_op_product_shelf_sal_month` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.`shelf_id` = b.`shelf_id`
WHERE a.`month_id` = @y_m
GROUP BY b.`business_name`,a.`product_id`
;
-- 销售数量、金额、优惠券、折扣 18s
-- DROP TEMPORARY TABLE IF EXISTS feods.`discount_tmp`;
-- CREATE TEMPORARY TABLE feods.discount_tmp (
-- KEY idx_area_product_id (business_name,product_id)
-- ) AS
-- SELECT
--         b.business_name,
--         a.`product_id`,
--         SUM(a.quantity) AS sale_qty,
--         SUM(a.quantity * a.sale_price) AS sale_value,
--         SUM(IF(a.discount_amount > 0,a.quantity,0)) AS discount_qty,
--         SUM(a.discount_amount) AS discount_amount,
--         SUM(IF(a.COUPON_AMOUNT > 0,a.quantity,0)) AS coupon_qty,
--         SUM(a.COUPON_AMOUNT) AS COUPON_AMOUNT
-- FROM
--         `fe_dwd`.`dwd_order_item_refund_day` a
--         JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
--                 ON a.`shelf_id` = b.`shelf_id`
-- WHERE PAY_DATE BETWEEN @sdate AND @edate
-- GROUP BY b.business_name,a.`product_id`
-- ;
-- 退货、调货 7s
DROP TEMPORARY TABLE IF EXISTS feods.`refund_tmp`;
CREATE TEMPORARY TABLE feods.refund_tmp(
        KEY idx_area_product_id(business_name,product_id)
) AS 
SELECT
        b.business_name,
        a.product_id,
        SUM(IF(a.FILL_TYPE IN (5,11),a.ACTUAL_FILL_NUM,0)) AS refund_qty,
        SUM(IF(a.FILL_TYPE IN (6,7),a.ACTUAL_FILL_NUM,0)) AS trans_qty
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
                AND a.FILL_TIME BETWEEN @sdate AND @edate
GROUP BY b.business_name,a.product_id
;
-- 商品清单 1s
DROP TEMPORARY TABLE IF EXISTS feods.`product_tmp`;
CREATE TEMPORARY TABLE feods.product_tmp(
        KEY idx_area_product_id(business_name,product_id)
) AS 
SELECT
        a.business_area AS business_name,
        a.PRODUCT_ID,
        MIN(INDATE_NP) AS earliest_in_date,
        MAX(INDATE_NP) AS latest_in_date,
        COUNT(DISTINCT INDATE_NP) AS in_qty,
        DATEDIFF(@cdate,MAX(INDATE_NP)) AS duration
FROM
        feods.`zs_product_dim_sserp_his` a
WHERE a.PUB_TIME BETWEEN @sdate AND @edate
        AND INDATE_NP != '0000-00-00 00:00:00'
GROUP BY a.business_area,a.PRODUCT_ID
;
-- 期末库存(按风险标识)1min41s
DROP TEMPORARY TABLE IF EXISTS feods.`danger_tmp`;
CREATE TEMPORARY TABLE feods.danger_tmp(
        KEY idx_area_product_id(business_name,product_id)
) AS 
SELECT
        b.business_name,
        a.PRODUCT_ID,
        SUM(IF(a.DANGER_FLAG IS NULL,IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),0)) AS danger_null_stock_qty,
        SUM(IF(a.DANGER_FLAG = 1,IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),0)) AS danger_1_stock_qty,
        SUM(IF(a.DANGER_FLAG = 2,IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),0)) AS danger_2_stock_qty,
        SUM(IF(a.DANGER_FLAG = 3,IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),0)) AS danger_3_stock_qty,
        SUM(IF(a.DANGER_FLAG = 4,IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),0)) AS danger_4_stock_qty,
        SUM(IF(a.DANGER_FLAG = 5,IF(a.STOCK_QUANTITY > 0,a.STOCK_QUANTITY,0),0)) AS danger_5_stock_qty
FROM 
        feods.`d_op_shelf_product_detail_combine1` a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
GROUP BY b.business_name,a.PRODUCT_ID
;
DELETE FROM feods.d_op_in_out_stock WHERE stat_date = @y_m;
INSERT INTO feods.d_op_in_out_stock
(
        stat_date,
        business_name,
        product_id,
        PRODUCT_CODE2,
        product_name,
        second_type_name,
        sub_type_name,
        weighted_average_price,      -- 加权平均价（采购价）
        avg_sale_price,   -- 平均销售价
        start_stock_qty,       -- 期初数量
        start_stock_value,    -- 期初金额
        earliest_in_date,     -- 最早引入时间
        latest_in_date,       -- 最近一次引入时间
        in_qty,       -- 引用次数
        duration,     -- 时长
        actual_shelf_qty,     -- 架上验收数量
        actual_shelf_value,   -- 架上验收金额(采购价)
        qty_sal,     -- 销售数量
        sale_value,      --  销售金额
        discount_value,    --  折扣金额
        coupon_value,           -- 优惠券金额
        real_value,        -- 实收金额
        refund_qty,        -- 退货数量
        refund_value,          -- 退货金额(采购价)
        trans_qty,    -- 调入调出数量
        trans_value,         -- 调入调出金额(采购价)
        danger_null_stock_qty,        -- 风险标识null的期末库存
        danger_1_stock_qty,    -- 风险标识1的期末库存
        danger_2_stock_qty,    -- 风险标识2的期末库存
        danger_3_stock_qty,    -- 风险标识3的期末库存
        danger_4_stock_qty,    -- 风险标识4的期末库存
        danger_5_stock_qty,    -- 风险标识5的期末库存
        danger_null_stock_value,          -- 风险标识null的期末库存金额
        danger_1_stock_value,       -- 风险标识1的期末库存金额
        danger_2_stock_value,       -- 风险标识2的期末库存金额
        danger_3_stock_value,       -- 风险标识3的期末库存金额
        danger_4_stock_value,       -- 风险标识4的期末库存金额
        danger_5_stock_value       -- 风险标识5的期末库存金额  
)
SELECT
        @y_m AS stat_date,
        a.business_name,
        a.product_id,
        b.`PRODUCT_CODE2`,
        b.product_name,
        b.second_type_name,
        b.sub_type_name,
        IFNULL(c.weighted_average_price,0) AS weighted_average_price,      -- 加权平均价（采购价）
        IFNULL(d.avg_sale_price,0) AS avg_sale_price,   -- 平均销售价
        IFNULL(a.start_stock_qty,0) AS start_stock_qty,       -- 期初数量
        IFNULL(a.start_stock_qty * c.weighted_average_price,0) AS start_stock_value,    -- 期初金额
        i.earliest_in_date,     -- 最早引入时间
        i.latest_in_date,       -- 最近一次引入时间
        IFNULL(i.in_qty,0) AS in_qty,       -- 引用次数
        IFNULL(i.duration,0) AS duration,     -- 时长
        IFNULL(c.actual_shelf_qty,0) AS actual_shelf_qty,     -- 架上验收数量
        IFNULL(c.actual_shelf_value,0) AS actual_shelf_value,   -- 架上验收金额(采购价)
        IFNULL(d.qty_sal,0) AS qty_sal,     -- 销售数量
        IFNULL(d.gmv,0) AS sale_value,      --  销售金额
        IFNULL(d.discount,0) AS discount_value,    --  折扣金额
        IFNULL(d.coupon,0) AS coupon_value,           -- 优惠券金额
        IFNULL(d.real_value,0) AS real_value,        -- 实收金额
        IFNULL(f.refund_qty,0) AS refund_qty,        -- 退货数量
        IFNULL(f.refund_qty * c.weighted_average_price,0) AS refund_value,          -- 退货金额(采购价)
        IFNULL(f.trans_qty,0) AS trans_qty,    -- 调入调出数量
        IFNULL(f.trans_qty * c.weighted_average_price,0) AS trans_value,         -- 调入调出金额(采购价)
        IFNULL(j.danger_null_stock_qty,0) AS danger_null_stock_qty,        -- 风险标识null的期末库存
        IFNULL(j.danger_1_stock_qty,0) AS danger_1_stock_qty,    -- 风险标识1的期末库存
        IFNULL(j.danger_2_stock_qty,0) AS danger_2_stock_qty,    -- 风险标识2的期末库存
        IFNULL(j.danger_3_stock_qty,0) AS danger_3_stock_qty,    -- 风险标识3的期末库存
        IFNULL(j.danger_4_stock_qty,0) AS danger_4_stock_qty,    -- 风险标识4的期末库存
        IFNULL(j.danger_5_stock_qty,0) AS danger_5_stock_qty,    -- 风险标识5的期末库存
        IFNULL(j.danger_null_stock_qty * c.weighted_average_price,0) AS danger_null_stock_value,          -- 风险标识null的期末库存金额
        IFNULL(j.danger_1_stock_qty * c.weighted_average_price,0) AS danger_1_stock_value,       -- 风险标识1的期末库存金额
        IFNULL(j.danger_2_stock_qty * c.weighted_average_price,0) AS danger_2_stock_value,       -- 风险标识2的期末库存金额
        IFNULL(j.danger_3_stock_qty * c.weighted_average_price,0) AS danger_3_stock_value,       -- 风险标识3的期末库存金额
        IFNULL(j.danger_4_stock_qty * c.weighted_average_price,0) AS danger_4_stock_value,       -- 风险标识4的期末库存金额
        IFNULL(j.danger_5_stock_qty * c.weighted_average_price,0) AS danger_5_stock_value       -- 风险标识5的期末库存金额
FROM
        feods.start_tmp a
        JOIN `fe_dwd`.`dwd_product_base_day_all` b
                ON a.`product_id` = b.`PRODUCT_ID`
        LEFT JOIN  feods.in_tmp c
                ON a.business_name = c.business_name
                AND b.PRODUCT_CODE2 = c.product_id
        LEFT JOIN feods.sale_tmp d
                ON a.business_name = d.business_name
                AND a.product_id = d.product_id
        LEFT JOIN feods.refund_tmp f
                ON a.business_name = f.business_name
                AND a.product_id = f.product_id
        LEFT JOIN feods.product_tmp i
                ON a.business_name = i.business_name
                AND a.product_id = i.product_id 
        LEFT JOIN feods.danger_tmp j
                ON a.business_name = j.business_name
                AND a.product_id = j.product_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_in_out_stock',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
END