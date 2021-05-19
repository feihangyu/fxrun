CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_fill_day_sale_qty`()
    SQL SECURITY INVOKER
BEGIN
SET @cdate := CURDATE();
SET @ydate := SUBDATE(@cdate,INTERVAL 1 DAY);
SET @sdate := SUBDATE(@cdate,INTERVAL 30 DAY);
SET @pre_6month := SUBDATE(@cdate,INTERVAL 6 MONTH);
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @end_date = CURDATE(); 
SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
-- 货架口径 1s 34255个货架
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_tmp`;
CREATE TEMPORARY TABLE feods.shelf_tmp(
       PRIMARY KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        b.business_name,
        a.shelf_id,
        a.WHETHER_CLOSE,
        a.CLOSE_TIME
FROM
        fe.`sf_shelf` a
        JOIN feods.`fjr_city_business` b
                ON a.city = b.city
                AND a.data_flag = 1
                AND a.SHELF_STATUS = 2
                AND a.REVOKE_STATUS = 1
                AND a.shelf_type IN (1,2,3,5,6,7,8)
WHERE ! ISNULL(a.shelf_id)
;
-- 当天是否有效  
-- 当天补货量
DROP TEMPORARY TABLE IF EXISTS feods.`curdate_fill_tmp`;
CREATE TEMPORARY TABLE feods.curdate_fill_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        b.shelf_id,
        b.product_id,
        MIN(b.STOCK_NUM) AS STOCK_NUM,
        SUM(ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM
FROM
    fe.`sf_product_fill_order` a
    JOIN fe.`sf_product_fill_order_item` b
        ON a.order_id = b.order_id
        AND a.FILL_TIME > @cdate
        AND a.FILL_TYPE IN (1,2,3,4,5,6,7,8,9)
        AND a.data_flag = 1
        AND b.data_flag = 1
GROUP BY b.shelf_id,b.product_id
;
-- 当天销量 1s
DROP TEMPORARY TABLE IF EXISTS feods.`cur_sal_order_tmp`;
CREATE TEMPORARY TABLE feods.cur_sal_order_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        shelf_id,
        product_id,
        SUM(QUANTITY) AS QUANTITY
FROM
(
        SELECT
                a.shelf_id,
                b.product_id,
                SUM(b.QUANTITY) AS QUANTITY
        FROM
                fe.`sf_order` a
                JOIN fe.`sf_order_item` b
                        ON a.PAY_DATE >= @cdate
                        AND a.ORDER_STATUS IN (2,6,7)
                        AND a.data_flag = 1
                        AND b.data_flag = 1
                        AND a.ORDER_ID = b.ORDER_ID
        GROUP BY a.shelf_id,b.product_id
        UNION ALL
        SELECT
                shelf_id,
                product_id,
                SUM(amount) AS QUANTITY
        FROM
                fe_dwd.`dwd_op_out_of_system_order_yht` 
        WHERE pay_date >= @ydate
                AND pay_date < @cdate
                AND data_flag = 1
        GROUP BY shelf_id,product_id
) a
GROUP BY shelf_id,product_id
;
-- 当天有效销量(对超卖商品进行修正)
DROP TEMPORARY TABLE IF EXISTS feods.`cur_order_tmp`;
CREATE TEMPORARY TABLE feods.cur_order_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        a.product_id,
        IF(a.QUANTITY > IFNULL(b.stock_quantity,0) + IFNULL(ACTUAL_FILL_NUM,0),IFNULL(b.stock_quantity,0) + IFNULL(ACTUAL_FILL_NUM,0),a.QUANTITY) AS QUANTITY
FROM
        feods.cur_sal_order_tmp a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b   -- 当天0点结存的库存
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
        LEFT JOIN feods.curdate_fill_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.product_id = c.product_id
WHERE IFNULL(b.stock_quantity,0) + IFNULL(ACTUAL_FILL_NUM,0) >0
;
-- =========================================================================================
-- 计算是否参与活动类型为4、5的货架商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_1_1;
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_1_1 (
        KEY idx_order(order_id,product_id)
        ) AS
# 没有商品编号的活动订单，也没有活动号
SELECT
    a.activity_id
    , a.order_id
    , a.shelf_id
    , a.`GOODS_ID` AS product_id
FROM
    fe.`sf_order_activity` a
WHERE a.`data_flag` = 1
    AND a.order_status = 2 #1为已取消
     AND a.pay_date >= @start_date
    AND a.pay_date < @end_date
    AND a.`GOODS_ID` IS NULL;
CREATE INDEX idx_dwd_dwd_lsl_shelf_1_1_1
ON fe_dwd.dwd_lsl_shelf_1_1_1 (order_id);	
-- 没有商品编号的，从订单表里取。而且这种折扣也基本为0 ;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_1;
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_1 (
        KEY idx_order(order_id,product_id)
        ) AS
SELECT DISTINCT 
    a.activity_id
    , a.order_id
    , a.shelf_id		
	,b.product_id
FROM fe_dwd.dwd_lsl_shelf_1_1_1 a
LEFT JOIN 
        (
        SELECT
                a.ORDER_ID,
                a.shelf_id,
                b.product_id
        FROM
                fe.`sf_order` a
                JOIN fe.`sf_order_item` b
                        ON a.PAY_DATE >= @cdate
                        AND a.ORDER_STATUS IN (2,6,7)
                        AND a.data_flag = 1
                        AND b.data_flag = 1
                        AND a.ORDER_ID = b.ORDER_ID
        ) b
ON a.order_id = b.order_id;
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_2;   
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_2 (
        KEY idx_order(order_id,product_id)
        ) AS
#正常显示的订单和商品
SELECT
    a.activity_id
    , a.order_id
    , a.shelf_id
    , a.`GOODS_ID` AS product_id
FROM
    fe.`sf_order_activity` a
WHERE a.`GOODS_ID` IS NOT NULL
    AND a.`data_flag` = 1
    AND a.`GOODS_ID` NOT LIKE "%,%"
    AND a.order_status = 2 #1为已取消
    AND a.pay_date >= @start_date
    AND a.pay_date < @end_date;
# 商品中逗号的    
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1_3;    
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1_3 (
        KEY idx_order(order_id,product_id)
        ) AS
SELECT
    a.`ACTIVITY_ID`
    , a.`ORDER_ID`
    , a.`SHELF_ID`
    , SUBSTRING_INDEX(
        SUBSTRING_INDEX(a.`GOODS_ID`, ",", n.`number` + 1)
        , ","
        , - 1
    ) AS product_id
FROM
    fe.`sf_order_activity` a
    JOIN fe_dwd.`dwd_pub_number` n
        ON n.number <= LENGTH(a.`GOODS_ID`) - LENGTH(REPLACE(a.`GOODS_ID`, ",", ""))
WHERE a.`data_flag` = 1
    AND a.order_status = 2 #1为已取消
     AND a.`GOODS_ID` LIKE "%,%"
     AND a.pay_date >= @start_date
    AND a.pay_date < @end_date;
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_lsl_shelf_1;    
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.dwd_lsl_shelf_1 AS	
SELECT * FROM 	fe_dwd.dwd_lsl_shelf_1_1
UNION ALL
SELECT * FROM 	fe_dwd.dwd_lsl_shelf_1_2
UNION ALL
SELECT * FROM 	fe_dwd.dwd_lsl_shelf_1_3;	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.activity_tmp;    
CREATE TEMPORARY TABLE IF NOT EXISTS  fe_dwd.activity_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT 
DISTINCT
o.`shelf_id`,
o.product_id
FROM 
fe_dwd.dwd_lsl_shelf_1 o
LEFT JOIN fe.sf_product_activity a   #活动生效表
ON o.`activity_id` = a.`activity_id`
AND a.`activity_state` = 2 # 已确认
AND a.`data_flag` =1
WHERE a.activity_type IN (4,5)
;
        
-- =============================================================================================
-- 严重滞销商品当天是否参与促销活动 2s
DROP TEMPORARY TABLE IF EXISTS feods.`unsale_cur_tmp`;
CREATE TEMPORARY TABLE feods.unsale_cur_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT 
        b.shelf_id,
        b.product_id
FROM 
        fe_dwd.activity_tmp b
        JOIN fe.`sf_shelf_product_detail_flag` d
                ON b.shelf_id = d.shelf_id
                AND b.`PRODUCT_ID` = d.product_id
                AND d.sales_flag = 5
                AND d.DATA_FLAG = 1
;
-- 1min40s
DROP TEMPORARY TABLE IF EXISTS feods.`shelf_product_tmp`;
CREATE TEMPORARY TABLE feods.shelf_product_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id),
        PRIMARY KEY idx_detail_id(DETAIL_ID)
) AS 
SELECT
        a.DETAIL_ID,
        c.business_name,
        a.shelf_id,
        a.product_id,
        d.fill_box_gauge,
        c.WHETHER_CLOSE,
        c.CLOSE_TIME,
        a.STOCK_QUANTITY,
        a.SHELF_FILL_FLAG,
        b.new_flag,
        b.sales_flag
FROM
        fe.`sf_shelf_product_detail` a
        JOIN  fe.`sf_shelf_product_detail_flag` b
                ON a.detail_id = b.detail_id
                AND a.data_flag = 1
                AND b.data_flag = 1
        JOIN feods.shelf_tmp c
                ON a.shelf_id = c.shelf_id
        STRAIGHT_JOIN fe.`sf_product` d 
                ON a.product_id = d.product_id
                AND d.data_flag = 1
;
-- 当日是否有效 1min23s
DROP TEMPORARY TABLE IF EXISTS feods.`cur_sale_tmp`;
CREATE TEMPORARY TABLE feods.cur_sale_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id),
        PRIMARY KEY idx_detail_id(DETAIL_ID)
) AS 
SELECT
        a.DETAIL_ID,
        a.business_name,
        a.shelf_id,
        a.product_id,
--         IF(IF(f.shelf_id IS NOT NULL OR (g.QUANTITY > IFNULL(2 * a.fill_box_gauge,50)) OR e.sdate IS NOT NULL OR (g.QUANTITY IS NULL AND a.STOCK_QUANTITY <= 0) OR (h.shelf_id IS NOT NULL  AND h.STOCK_NUM <= 0 AND g.QUANTITY IS NULL),0,1) = 1 
--             AND  a.WHETHER_CLOSE = 2,1,0)AS is_valid,
        IF(IF(f.shelf_id IS NOT NULL OR (g.QUANTITY > IFNULL(2 * a.fill_box_gauge,50)) OR (g.QUANTITY IS NULL AND a.STOCK_QUANTITY <= 0) OR (h.shelf_id IS NOT NULL  AND h.STOCK_NUM <= 0 AND g.QUANTITY IS NULL),0,1) = 1 
            AND  a.WHETHER_CLOSE = 2,1,0)AS is_valid,
        IFNULL(g.QUANTITY,0) AS  curdate_sale_qty,
        a.SHELF_FILL_FLAG
FROM 
        feods.shelf_product_tmp a
-- 关联活动表需要释放注释代码
--         LEFT JOIN feods.`d_op_his_activity_day_list` e
--                 ON @cdate BETWEEN e.sdate AND e.edate    
        LEFT JOIN feods.unsale_cur_tmp f
                ON a.shelf_id = f.shelf_id
                AND a.product_id = f.product_id
        LEFT JOIN feods.`cur_order_tmp` g
                ON a.shelf_id = g.shelf_id
                AND a.product_id = g.product_id
        LEFT JOIN feods.curdate_fill_tmp h
                ON a.shelf_id = h.shelf_id
                AND a.product_id = h.product_id 
;
-- 近30天是否有效和有效销量 2min47s
-- 近30天\14天\7天有效天数和有效销量 
DROP TEMPORARY TABLE IF EXISTS feods.`cur_sto_sal_tmp`;
CREATE TEMPORARY TABLE feods.cur_sto_sal_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id),
        PRIMARY KEY idx_detail_id(DETAIL_ID)
) AS 
SELECT 
        a.DETAIL_ID,
        a.shelf_id,
        a.product_id,
        a.is_valid AS v1,
        IF(a.is_valid = 1,a.curdate_sale_qty,0) AS s1,
        v1  AS v2,
        s1  AS s2,
        v2  AS v3,
        s2  AS s3,
        v3  AS v4,
        s3  AS s4,
        v4  AS v5,
        s4  AS s5,
        v5  AS v6,
        s5  AS s6,
        v6  AS v7,
        s6  AS s7,
        v7  AS v8,
        s7  AS s8,
        v8  AS v9,
        s8  AS s9,
        v9  AS v10,
        s9  AS s10,
        v10 AS v11,
        s10 AS s11,
        v11 AS v12,
        s11 AS s12,
        v12 AS v13,
        s12 AS s13,
        v13 AS v14,
        s13 AS s14,
        v14 AS v15,
        s14 AS s15,
        v15 AS v16,
        s15 AS s16,
        v16 AS v17,
        s16 AS s17,
        v17 AS v18,
        s17 AS s18,
        v18 AS v19,
        s18 AS s19,
        v19 AS v20,
        s19 AS s20,
        v20 AS v21,
        s20 AS s21,
        v21 AS v22,
        s21 AS s22,
        v22 AS v23,
        s22 AS s23,
        v23 AS v24,
        s23 AS s24,
        v24 AS v25,
        s24 AS s25,
        v25 AS v26,
        s25 AS s26,
        v26 AS v27,
        s26 AS s27,
        v27 AS v28,
        s27 AS s28,
        v28 AS v29,
        s28 AS s29,
        v29 AS v30,
        s29 AS s30,
        IFNULL(v1,0) +  IFNULL(v2,0) +  IFNULL(v3,0) +  IFNULL(v4,0) +  IFNULL(v5,0) +  IFNULL(v6,0) +  IFNULL(a.is_valid,0) AS valid_days_7,
        IFNULL(s1,0) +  IFNULL(s2,0) +  IFNULL(s3,0) +  IFNULL(s4,0) +  IFNULL(s5,0) +  IFNULL(s6,0) +  IFNULL(IF(a.is_valid = 1,a.curdate_sale_qty,0) ,0) AS valid_sale_7,
        IFNULL(v1,0) +  IFNULL(v2,0) +  IFNULL(v3,0) +  IFNULL(v4,0) +  IFNULL(v5,0) +  IFNULL(v6,0) +  IFNULL(v7,0) + 
                IFNULL(v8,0) +  IFNULL(v9,0) +  IFNULL(v10,0) +  IFNULL(v11,0) +  IFNULL(v12,0) +  IFNULL(v13,0) +  IFNULL(a.is_valid,0) AS valid_days_14,
        IFNULL(s1,0) +  IFNULL(s2,0) +  IFNULL(s3,0) +  IFNULL(s4,0) +  IFNULL(s5,0) +  IFNULL(s6,0) +  IFNULL(s7,0) +
                IFNULL(s8,0) +  IFNULL(s9,0) +  IFNULL(s10,0) +  IFNULL(s11,0) +  IFNULL(s12,0) +  IFNULL(s13,0) +  IFNULL(IF(a.is_valid = 1,a.curdate_sale_qty,0) ,0) AS valid_sale_14,
        IFNULL(v1,0) +  IFNULL(v2,0) +  IFNULL(v3,0) +  IFNULL(v4,0) +  IFNULL(v5,0) +  IFNULL(v6,0) +  IFNULL(v7,0) + 
                IFNULL(v8,0) +  IFNULL(v9,0) +  IFNULL(v10,0) +  IFNULL(v11,0) +  IFNULL(v12,0) +  IFNULL(v13,0) +  IFNULL(v14,0) +
                IFNULL(v15,0) +  IFNULL(v16,0) +  IFNULL(v17,0) +  IFNULL(v18,0) +  IFNULL(v19,0) +  IFNULL(v20,0) +  IFNULL(v21,0) +
                IFNULL(v22,0) +  IFNULL(v23,0) +  IFNULL(v24,0) +  IFNULL(v25,0) +  IFNULL(v26,0) +  IFNULL(v27,0) +  IFNULL(v28,0) + IFNULL(v29,0) +  IFNULL(a.is_valid,0)
                AS valid_days_30,
        IFNULL(s1,0) +  IFNULL(s2,0) +  IFNULL(s3,0) +  IFNULL(s4,0) +  IFNULL(s5,0) +  IFNULL(s6,0) +  IFNULL(s7,0) + 
                IFNULL(s8,0) +  IFNULL(s9,0) +  IFNULL(s10,0) +  IFNULL(s11,0) +  IFNULL(s12,0) +  IFNULL(s13,0) +  IFNULL(s14,0) + 
                IFNULL(s15,0) +  IFNULL(s16,0) +  IFNULL(s17,0) +  IFNULL(s18,0) +  IFNULL(s19,0) +  IFNULL(s20,0) +  IFNULL(s21,0) + 
                IFNULL(s22,0) +  IFNULL(s23,0) +  IFNULL(s24,0) +  IFNULL(s25,0) +  IFNULL(s26,0) +  IFNULL(s27,0) +  IFNULL(s28,0) + IFNULL(s29,0) +  IFNULL(IF(a.is_valid = 1,a.curdate_sale_qty,0) ,0)
                AS valid_sale_30
FROM
        feods.cur_sale_tmp a
        LEFT JOIN feods.`d_op_valid_sto_sal_day30` b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
;

TRUNCATE TABLE feods.`d_op_valid_sto_sal_day30`;
INSERT INTO feods.`d_op_valid_sto_sal_day30`
(
        shelf_id,
        product_id,
        v1,s1,v2,s2,v3,s3,v4,s4,v5,s5,v6,s6,v7,s7,v8,s8,v9,s9,v10,s10,v11,s11,v12,s12,v13,s13,v14,s14,v15,s15,v16,s16,v17,s17,v18,s18,v19,s19,v20,s20,v21,s21,
        v22,s22,v23,s23,v24,s24,v25,s25,v26,s26,v27,s27,v28,s28,v29,s29,v30,s30
)
SELECT 
        shelf_id,
        product_id,
        v1,s1,v2,s2,v3,s3,v4,s4,v5,s5,v6,s6,v7,s7,v8,s8,v9,s9,v10,s10,v11,s11,v12,s12,v13,s13,v14,s14,v15,s15,v16,s16,v17,s17,v18,s18,v19,s19,v20,s20,v21,s21,
        v22,s22,v23,s23,v24,s24,v25,s25,v26,s26,v27,s27,v28,s28,v29,s29,v30,s30
FROM feods.cur_sto_sal_tmp
;
-- =================================================================================================================
-- 日均销量过程表 3min52s
-- 1.若截至当天商品连续有效天数大于等于30天，则日均销用连续有效30天销售/30天，否则，到第2步
-- （初始：前一天日均销用当天往前30天内有效销售/30天内有效天数（用9月1到9月30）），若无销售，默认0.06；
-- 2.若连续有效天数小于30天，判断当天是否有效
-- 1.1若当天无效，则为前一天日均销
-- 1.2若当天有效，则为(前一天日均销*29+当天有效销量)/30
-- 3.若为新品，则判断库存和引进时间
-- 3.1.若新品引进一直无库存（新品表示为空），则日均销为0；
-- 3.2从有库存第一天开始（新品表示为1），截至当天小于等于14天，日均销=有效销售/有效天数
-- 3.3有库存第一天开始，截至当天大于14天，商品变为原有品，用1-2的逻辑进行计算
DROP TEMPORARY TABLE IF EXISTS feods.`day_sale_qty`;
CREATE TEMPORARY TABLE feods.day_sale_qty (
        KEY idx_shelf_id_product_id(shelf_id,product_id),
        PRIMARY KEY idx_detail_id(DETAIL_ID)
        ) AS
SELECT 
        a.DETAIL_ID,
        b.business_name,
        a.shelf_id,
        a.product_id,
        a.SALES_FLAG,
        a.NEW_FLAG,
        b.SHELF_FILL_FLAG,
        b.is_valid,     -- 是否有效
        IF(b.is_valid = 1,@cdate,NULL) AS recent_valid_date,  -- 最近一次有效天数日期
        IF(b.is_valid = 1,h.recent_valid_days + 1,0) AS recent_valid_days,  -- 最近一次连续有效天数
        IFNULL(ROUND(CASE 
                WHEN a.NEW_FLAG IS NULL 
                        THEN 0
                WHEN a.NEW_FLAG = 1 AND j.valid_days_14 = 0
                        THEN 0
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 = 0
                        THEN 0.08
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 <= 2 AND j.valid_days_14 = 1
                        THEN  j.valid_sale_14 / j.valid_days_14 * 0.5
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 <= 4 AND j.valid_days_14 = 1
                        THEN  j.valid_sale_14 / j.valid_days_14 * 0.4
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 > 4 AND j.valid_days_14 = 1
                        THEN  j.valid_sale_14 / j.valid_days_14 * 0.35
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 <= 4 AND j.valid_days_14 IN (2,3,4)
                        THEN  j.valid_sale_14 / j.valid_days_14 
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 > 4 AND j.valid_days_14 = 2
                        THEN  j.valid_sale_14 / j.valid_days_14 * 0.85
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 > 4 AND j.valid_days_14 = 3
                        THEN  j.valid_sale_14 / j.valid_days_14 * 0.9
                WHEN a.NEW_FLAG = 1 AND j.valid_sale_14 > 4 AND j.valid_days_14 = 4
                        THEN  j.valid_sale_14 / j.valid_days_14 * 0.95
                WHEN a.NEW_FLAG = 1 AND j.valid_days_14 > 4
                        THEN  j.valid_sale_14 / j.valid_days_14 
                WHEN b.is_valid = 1 AND h.recent_valid_days < 30 
                        THEN (h.day_sale_qty_30 * 29 + b.curdate_sale_qty) / 30
                WHEN b.is_valid = 1 AND h.recent_valid_days >= 30 
                        THEN j.valid_sale_30 / 30
                WHEN b.is_valid = 0
                        THEN h.day_sale_qty_30
        END,4),0.06) AS day_sale_qty_30
FROM 
        feods.`shelf_product_tmp` a
        STRAIGHT_JOIN feods.cur_sale_tmp b 
                ON a.DETAIL_ID = b.DETAIL_ID
        LEFT JOIN feods.d_op_fill_day_sale_qty h
                ON a.shelf_id = h.shelf_id
                AND a.product_id = h.product_id
        STRAIGHT_JOIN feods.`cur_sto_sal_tmp` j
                ON a.DETAIL_ID = j.DETAIL_ID
;
-- 综合日均销 2min18s
--    ①增加7天/14天有效日均销（近7天（14天）有效销售/有效天数），若有效销售和天数均为0，则为空；
--         若有效销售为0，有效天数不为0，则7天（14天）有效销售为0；
--     ②增加“综合日均销”概念，计算规则如下：
--     A.若7天有效销售为空且14天有效销售为空,综合日均销=30天冻结日均销；
--     B.若若7天有效销售为空且14天有效销售不为空,综合日均销=30天冻结日均销*70%+14天有效销售*30%；
--     C.若7天有效销售不为空且14天有效销售为空,综合日均销=30天冻结日均销*80%+7天有效销售*20%；
--     D.7天/14天有效销售均不为空，=30天冻结日均销*50%+14天有效销售*30%+7天有效销售*20%
DROP TEMPORARY TABLE IF EXISTS feods.`com_day_sale_qty`;
CREATE TEMPORARY TABLE feods.com_day_sale_qty (
        KEY idx_shelf_id_product_id(shelf_id,product_id),
        PRIMARY KEY idx_detail_id(DETAIL_ID)
        ) AS
SELECT 
        a.DETAIL_ID,
        a.shelf_id,
        a.product_id,
        ROUND(b.valid_sale_7 / b.valid_days_7,4) AS day_sale_qty_7,              -- 近7天日均销
        ROUND(b.valid_sale_14 / b.valid_days_14,4) AS day_sale_qty_14,              -- 近14天日均销
        ROUND(CASE 
                WHEN IF(b.valid_sale_7 = 0 AND b.valid_days_7 = 0,0,1) = 0 AND IF(b.valid_sale_14 = 0 AND b.valid_days_14 = 0,0,1) = 0
                        THEN a.day_sale_qty_30
                WHEN IF(b.valid_sale_7 = 0 AND b.valid_days_7 = 0,0,1) = 0 
                        THEN a.day_sale_qty_30 * 0.8 + ROUND(b.valid_sale_14 / b.valid_days_14,4) * 0.2
                WHEN IF(b.valid_sale_14 = 0 AND b.valid_days_14 = 0,0,1) = 0
                        THEN a.day_sale_qty_30 * 0.9 + ROUND(b.valid_sale_7 / b.valid_days_7,4) * 0.1
                ELSE a.day_sale_qty_30 * 0.7 + ROUND(b.valid_sale_14 / b.valid_days_14,4) * 0.2 + ROUND(b.valid_sale_7 / b.valid_days_7,4) * 0.1
        END,4) AS day_sale_qty        -- 综合日均销
FROM 
        feods.day_sale_qty a
        JOIN feods.cur_sto_sal_tmp b
                ON a.DETAIL_ID = b.DETAIL_ID
;
-- 1min45s
-- 防错机制：确保今天的数据正常插入前，不能清空原表。
DROP TEMPORARY TABLE IF EXISTS feods.d_op_fill_day_sale_qty_test;
CREATE TEMPORARY TABLE feods.d_op_fill_day_sale_qty_test LIKE feods.d_op_fill_day_sale_qty;
INSERT INTO feods.d_op_fill_day_sale_qty_test
(
        BUSINESS_AREA,
        stat_date,
        shelf_id,
        product_id,
        SALES_FLAG,
        is_valid,
        recent_valid_date,
        recent_valid_days,
        SHELF_FILL_FLAG,
        day_sale_qty_7,     
        day_sale_qty_14,        
        day_sale_qty_30,       
        day_sale_qty,       
        fill_level 
)
SELECT 
        a.business_name AS BUSINESS_AREA,
        @cdate AS stat_date,
        a.shelf_id,
        a.product_id,
        a.SALES_FLAG,
        a.is_valid,
        a.recent_valid_date,
        IFNULL(a.recent_valid_days,0) AS recent_valid_days,
        a.SHELF_FILL_FLAG,
        b.day_sale_qty_7,              -- 近7天有效日均销
        b.day_sale_qty_14,            -- 近14天有效日均销
        IFNULL(a.day_sale_qty_30,0) AS day_sale_qty_30,            -- 近30天冻结日均销
        IFNULL(b.day_sale_qty,0) AS day_sale_qty,                  -- 综合日均销                    
        CASE
                WHEN a.NEW_FLAG = 1 OR a.NEW_FLAG IS NULL THEN 6
                WHEN b.day_sale_qty >= 0.71 THEN 1
                WHEN b.day_sale_qty >= 0.43 THEN 2
                WHEN b.day_sale_qty >= 0.14 THEN 3
                WHEN b.day_sale_qty >= 0.07 THEN 4
                WHEN b.day_sale_qty < 0.07 THEN 5
        END AS fill_level                               -- 补货等级
FROM 
        feods.day_sale_qty a
        JOIN feods.com_day_sale_qty b
                ON a.DETAIL_ID = b.DETAIL_ID
;
-- 日均销量结果表 10s
TRUNCATE feods.d_op_fill_day_sale_qty;
INSERT INTO feods.d_op_fill_day_sale_qty
(
        BUSINESS_AREA,
        stat_date,
        shelf_id,
        product_id,
        SALES_FLAG,
        is_valid,
        recent_valid_date,
        recent_valid_days,
        SHELF_FILL_FLAG,
        day_sale_qty_7,     
        day_sale_qty_14,        
        day_sale_qty_30,       
        day_sale_qty,       
        fill_level 
)
SELECT 
        BUSINESS_AREA,
        stat_date,
        shelf_id,
        product_id,
        SALES_FLAG,
        is_valid,
        recent_valid_date,
        recent_valid_days,
        SHELF_FILL_FLAG,
        day_sale_qty_7,     
        day_sale_qty_14,        
        day_sale_qty_30,       
        day_sale_qty,       
        fill_level                            -- 补货等级
FROM 
        feods.d_op_fill_day_sale_qty_test
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_op_fill_day_sale_qty',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('宋英南@', @user, @timestamp));
COMMIT;
END