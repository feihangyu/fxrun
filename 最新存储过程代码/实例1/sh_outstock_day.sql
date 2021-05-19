CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_outstock_day`()
    SQL SECURITY INVOKER
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  TRUNCATE TABLE feods.sf_order_item_temp;
SET @time_3 := CURRENT_TIMESTAMP();
  INSERT INTO feods.sf_order_item_temp (
    ORDER_ITEM_ID,
    ORDER_ID,
    SHELF_ID,
    SHELF_ID_SHARD,
    SUPPLIER_ID,
    PRODUCT_ID,
    QUANTITY,
    COST_PRICE,
    PURCHASE_PRICE,
    SALE_PRICE,
    DISCOUNT_AMOUNT,
    REAL_TOTAL_PRICE,
    PRODUCT_NAME,
    LIMIT_BUY_ID,
    ORDER_DATE,
    ORDER_STATUS
  )
  SELECT
    t1.ORDER_ITEM_ID,
    t1.ORDER_ID,
    t2.SHELF_ID,
    t1.SHELF_ID_SHARD,
    t1.SUPPLIER_ID,
    t1.PRODUCT_ID,
    CASE
      WHEN t2.order_status = 2
      THEN t1.quantity
      ELSE t1.quantity_shipped
    END AS quantity,
    t1.COST_PRICE,
    t1.PURCHASE_PRICE,
    t1.SALE_PRICE,
    t1.DISCOUNT_AMOUNT,
    t1.REAL_TOTAL_PRICE,
    t1.PRODUCT_NAME,
    t1.LIMIT_BUY_ID,
    t2.pay_date AS order_date,
    t2.order_status
  FROM
    fe.sf_order_item t1,
    fe.sf_order t2
  WHERE t2.order_id = t1.order_id
    AND t2.pay_date >= DATE_SUB(CURDATE(), INTERVAL 31 DAY)
    AND t2.pay_date < CURDATE()
    AND t2.order_status IN (2, 6, 7);
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_outstock_day","@time_3--@time_5",@time_3,@time_5);
  # tab1中t1临时表
DROP TEMPORARY TABLE IF EXISTS feods.pj_outstock2_tab1_t1;
SET @time_8 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE feods.pj_outstock2_tab1_t1
  (INDEX idx_warehouse_product(warehouse_code,product_code2),
  INDEX idx_business_product(business_area,product_code2))
   AS
  SELECT
    f.BUSINESS_AREA ,-- AS '区域'
    f.BIG_AREA ,-- AS '大区'
    e.fnumber AS warehouse_code ,--  '仓库代码',
    b.FNAME AS warehouse_name ,-- '仓库名称',
    a.FMATERIALID , -- '商品ID',
    d.fnumber AS product_code2 ,--  '商品条码',
    c.fname  AS product_name , -- '商品名称',
    i.fname , -- AS '类别'
    d.f_bgj_poprice, -- AS '采购价'
    f.SAFT_DAY ,-- AS '安全天数'
    SUM(a.fbaseqty) FBASEQTY, -- AS '库存数量'
    SUM(IF(g.FNAME = '正品',IFNULL(a.fbaseqty,0),0))  AS QUALITYQTY,-- '正品库存量'
    SUM(IF(g.FNAME = '次品',IFNULL(a.fbaseqty,0),0)) AS INFERQUAQTY -- '次品库存量'
  FROM
    sserp.T_STK_INVENTORY a
    LEFT JOIN sserp.T_BD_STOCK_L b
      ON a.FSTOCKID = b.FSTOCKID
    LEFT JOIN sserp.T_BD_MATERIAL_L c
      ON a.FMATERIALID = c.FMATERIALID
    LEFT JOIN sserp.T_BD_MATERIAL d
      ON a.FMATERIALID = d.FMATERIALID
    LEFT JOIN sserp.T_BD_STOCK e
      ON a.FSTOCKID = e.FSTOCKID
    LEFT JOIN sserp.ZS_DC_BUSINESS_AREA f
      ON e.fnumber = f.DC_CODE
    LEFT JOIN sserp.T_BD_STOCKSTATUS_L g
      ON g.FSTOCKSTATUSID = a.FSTOCKSTATUSID
    LEFT JOIN sserp.T_BD_MATERIALGROUP_L i
      ON d.FMATERIALGROUP = i.fid
   GROUP BY f.BUSINESS_AREA,
           warehouse_code,
           a.FMATERIALID
       ;
 
-- SELECT *
-- FROM feods.pj_outstock2_tab1_t1 ;  
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_outstock_day","@time_10--@time_11",@time_10,@time_11);
  # tab1中t2临时表， 
DROP TEMPORARY TABLE IF EXISTS feods.pj_outstock2_tab1_t2;
SET @time_14 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE feods.pj_outstock2_tab1_t2 
(INDEX idx_warehouse_product(warehouse_code,product_code2))
  AS
  SELECT
    b.F_BGJ_FSTOREHOUSET AS warehouse_code,
    d.fnumber AS product_code2,
    COUNT(
      DISTINCT DATE_FORMAT(b.fdate, '%Y%m%d')
    ) AS outstock_day , -- '有出库天数',
    SUM(FQTY * d.F_BGJ_POPRICE) AS outstock_amount  -- '出库金额'
  FROM
    sserp.T_STK_OUTSTOCKAPPLYENTRY a
    JOIN sserp.T_STK_OUTSTOCKAPPLY b
      ON a.FID = b.FID
    JOIN sserp.T_BD_MATERIAL_L c
      ON a.FMATERIALID = c.FMATERIALID
    JOIN sserp.T_BD_MATERIAL d
      ON a.FMATERIALID = d.FMATERIALID
  WHERE b.fdate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    AND b.fdate < CURDATE()
    AND FBILLTYPEID = '5b0e65b177a7e1'
    AND b.FCLOSEFLAG = 0
    AND b.FCLOSESTATUS = 'B'
    AND b.FCANCELSTATUS = 'A'
  GROUP BY warehouse_code,
    product_code2;
# 替换后面的子查询tab2，区域商品级宽表建设好之后，可直接替换，将节省2分钟
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_business_product_tmp;
CREATE TEMPORARY TABLE feods.d_sc_business_product_tmp 
(INDEX idx_business_product(business_area,product_code2))
  AS
SELECT
    t1.REGION_AREA
    , t1.BUSINESS_AREA
    , t1.PRODUCT_ID
    , t1.product_code2
    , IFNULL(t4.sales_shelf_cnt, 0) sales_shelf_cnt
    , IFNULL(t3.add_7day_sales, 0) add_7day_sales
    , t1.pre_freeze_stock
    , t1.pre_available_stock
    , t2.pre_cover_shelf
FROM
    (SELECT
        s.REGION_AREA
        , s.BUSINESS_AREA
        , s.PRODUCT_ID
        , s.`product_code2`
        , SUM(s.freeze_stock) AS pre_freeze_stock -- '前置仓冻结库存量',
        , SUM(s.available_stock) AS pre_available_stock -- '前置仓正常库存量'
     FROM
        feods.pj_prewarehouse_stock_detail s
    WHERE s.`check_date` = SUBDATE(CURDATE(), 1)
    GROUP BY s.REGION_AREA
        , s.BUSINESS_AREA
        , s.PRODUCT_ID) t1
    JOIN
        (SELECT
            t.`business_area`
            , COUNT(DISTINCT t.`shelf_id`) pre_cover_shelf -- '前置仓覆盖货架数'
         FROM
            fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` t
        WHERE t.`shelf_status` = 2
        GROUP BY t.`business_area`) t2
        ON t1.business_area = t2.business_area
    LEFT JOIN
        (SELECT
            s.`business_name`
            , t.`product_id`
            , SUM(t.qty_sal30) add_7day_sales -- '近30天总销量'
         FROM
            feods.d_op_sp_avgsal30 t
            JOIN fe_dwd.`dwd_shelf_base_day_all` s
                ON t.shelf_id = s.`shelf_id`
        GROUP BY s.`business_name`
            , t.`product_id`) t3
        ON t1.business_area = t3.business_name
        AND t1.product_id = t3.product_id
    LEFT JOIN
        (SELECT
            s.`business_name`
            , a.product_id
            , SUM(a.sale_price * a.quantity) AS gmv -- GMV
            , SUM(a.quantity) AS sal_qty -- 销量
            , SUM(a.REAL_TOTAL_PRICE) REAL_TOTAL_PRICE -- 实收
            , SUM(a.quantity_act) sal_qty_shipped -- 实际出货量
            , SUM(a.sale_price * a.quantity_act) AS gmv_shipped -- gmv_shipped
            , SUM(a.discount_amount) discount_amount
             , COUNT(DISTINCT a.`shelf_id`) sales_shelf_cnt -- '有销量货架总数'
         FROM
            `fe_dwd`.`dwd_order_item_refund_day` a
            JOIN fe_dwd.`dwd_shelf_base_day_all` s
                ON a.`shelf_id` = s.`shelf_id`
        WHERE a.PAY_DATE >= SUBDATE(CURDATE(), 1)
            AND a.PAY_DATE < CURDATE()
        GROUP BY s.`business_name`
            , a.product_id) t4
        ON t1.business_area = t4.business_name
        AND t1.product_id = t4.product_id
;
# 子查询tab2中的正常补货数据
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_business_product_stock_tmp;
CREATE TEMPORARY TABLE feods.d_sc_business_product_stock_tmp 
(INDEX idx_business_product(business_name,product_code2))
  AS
SELECT s.`business_name`
,t.`PRODUCT_ID`
,p.product_code2
,COUNT(DISTINCT(IF(t.SHELF_FILL_FLAG = 1,t.`SHELF_ID`,NULL))) normal_fill_cnt -- '正常补货货架'
,COUNT(DISTINCT(IF(t.STOCK_QUANTITY > 0,t.`SHELF_ID`,NULL))) stock_shelf_cnt -- '有库存货架数'
FROM fe_dwd.`dwd_shelf_product_day_all` t
JOIN fe_dwd.`dwd_shelf_base_day_all` s
ON t.`SHELF_ID` = s.`shelf_id`
JOIN fe_dwd.`dwd_product_base_day_all` p
ON t.product_id = p.product_id
GROUP BY s.`business_name`,t.`PRODUCT_ID`
; 
  
SET @time_17 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_outstock_day","@time_16--@time_17",@time_16,@time_17);
DELETE FROM feods.PJ_OUTSTOCK2_DAY WHERE FPRODUCEDATE = SUBDATE(CURDATE(),1);
SET @time_20 := CURRENT_TIMESTAMP();
  INSERT INTO feods.PJ_OUTSTOCK2_DAY (
    FPRODUCEDATE,
    BIG_AREA,
    BUSINESS_AREA,
    WAREHOUSE_NUMBER,
    WAREHOUSE_NAME,
    FMATERIALID,
    PRODUCT_BAR,
    PRODUCT_NAME,
    PRODUCT_CATEGORY,
    F_BGJ_POPRICE,
    SAFT_DAY,
    FBASEQTY,
    QUALITYQTY,
    INFERQUAQTY,
    OUTSTOCK_DAY,
    OUTSTOCK_AMOUNT,
    PRODUCT_TYPE,
    normal_fill_cnt,
    stock_shelf_cnt,
    sales_shelf_cnt,
    add_7day_sales,
    pre_freeze_stock,
    pre_available_stock,
    pre_cover_shelf
  )
-- DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_business_product_stock_result;
-- CREATE TEMPORARY TABLE fe_dm.dm_sc_business_product_stock_result 
-- as
  SELECT SUBDATE(CURDATE(),1) AS sdate,
    t1.big_area,
    t1.business_area,
    t1.warehouse_code,
    t1.warehouse_name,
    t1.FMATERIALID,
    t1.product_code2,
    t1.product_name,
    t1.fname,
    t1.f_bgj_poprice,
    15 AS SAFT_DAY,
    t1.FBASEQTY,
    t1.QUALITYQTY,
    t1.INFERQUAQTY,
   
    t2.outstock_day,
    t2.outstock_amount,
    t3.product_type,
    
    t5.normal_fill_cnt,
    t5.stock_shelf_cnt,
    t4.sales_shelf_cnt,
    t4.add_7day_sales,
    t4.pre_freeze_stock,
    t4.pre_available_stock,
    t4.pre_cover_shelf
    
  FROM feods.pj_outstock2_tab1_t1 t1
  LEFT JOIN feods.pj_outstock2_tab1_t2 t2
  ON t1.warehouse_code = t2.warehouse_code
  AND t1.product_code2 = t2.product_code2
  
  LEFT JOIN feods.zs_product_dim_sserp t3
  ON t1.business_area = t3.business_area
  AND t1.product_code2 = t3.product_fe 
  
  LEFT JOIN feods.d_sc_business_product_tmp t4
  ON t1.business_area = t4.business_area
  AND t1.product_code2 = t4.product_code2
  
  LEFT JOIN feods.d_sc_business_product_stock_tmp t5
  ON t1.business_area = t5.business_name
  AND t1.product_code2 = t5.product_code2  
  ;
SET @time_29 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sh_outstock_day","@time_27--@time_29",@time_27,@time_29);
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_outstock_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
  COMMIT;
END