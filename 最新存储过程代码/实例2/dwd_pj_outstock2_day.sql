CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_pj_outstock2_day`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
    # tab1中t1临时表
DROP TEMPORARY TABLE IF EXISTS fe_dm.pj_outstock2_tab1_t1;
SET @time_8 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE fe_dm.pj_outstock2_tab1_t1
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
    fe_dwd.dwd_sserp_t_stk_inventory a
    LEFT JOIN fe_dwd.dwd_sserp_t_bd_stock_l b
      ON a.fstockid = b.fstockid
    LEFT JOIN fe_dwd.dwd_sserp_t_bd_material_l c
      ON a.fmaterialid = c.fmaterialid
    LEFT JOIN fe_dwd.dwd_sserp_t_bd_material d
      ON a.fmaterialid = d.fmaterialid
    LEFT JOIN fe_dwd.dwd_sserp_t_bd_stock e
      ON a.fstockid = e.fstockid
    LEFT JOIN fe_dwd.dwd_sserp_zs_dc_business_area f
      ON e.fnumber = f.dc_code
    LEFT JOIN fe_dwd.dwd_sserp_t_bd_stockstatus_l g
      ON g.fstockstatusid = a.fstockstatusid
    LEFT JOIN fe_dwd.dwd_sserp_t_bd_materialgroup_l i
      ON d.fmaterialgroup = i.fid
   GROUP BY f.business_area,
           warehouse_code,
           a.fmaterialid
       ;
 
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pj_outstock2_day","@time_8--@time_11",@time_8,@time_11);
  # tab1中t2临时表， 
DROP TEMPORARY TABLE IF EXISTS fe_dm.pj_outstock2_tab1_t2;
SET @time_14 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dm.pj_outstock2_tab1_t2 
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
    fe_dwd.dwd_sserp_T_STK_OUTSTOCKAPPLYENTRY a
    JOIN fe_dwd.dwd_sserp_T_STK_OUTSTOCKAPPLY b
      ON a.FID = b.FID
    JOIN fe_dwd.dwd_sserp_T_BD_MATERIAL_L c
      ON a.FMATERIALID = c.FMATERIALID
    JOIN fe_dwd.dwd_sserp_T_BD_MATERIAL d
      ON a.FMATERIALID = d.FMATERIALID
  WHERE b.fdate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    AND b.fdate < CURDATE()
    AND FBILLTYPEID = '5b0e65b177a7e1'
    AND b.FCLOSEFLAG = 0
    AND b.FCLOSESTATUS = 'B'
    AND b.FCANCELSTATUS = 'A'
  GROUP BY warehouse_code,
    product_code2;
SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pj_outstock2_day","@time_14--@time_15",@time_14,@time_15);
# 替换后面的子查询tab2，区域商品级宽表建设好之后，可直接替换，将节省2分钟
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_business_product_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_business_product_tmp 
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
        fe_dm.dm_prewarehouse_stock_detail s
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
            fe_dm.dm_op_sp_avgsal30 t
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
SET @time_16 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pj_outstock2_day","@time_15--@time_16",@time_15,@time_16);
# 子查询tab2中的正常补货数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.dm_sc_business_product_stock_tmp;
CREATE TEMPORARY TABLE fe_dm.dm_sc_business_product_stock_tmp 
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
CALL sh_process.sql_log_info("dwd_pj_outstock2_day","@time_16--@time_17",@time_16,@time_17);
DELETE FROM fe_dwd.dwd_pj_outstock2_day WHERE FPRODUCEDATE = SUBDATE(CURDATE(),1);
SET @time_20 := CURRENT_TIMESTAMP();
  INSERT INTO fe_dwd.dwd_pj_outstock2_day (
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
    
  FROM fe_dm.pj_outstock2_tab1_t1 t1
  LEFT JOIN fe_dm.pj_outstock2_tab1_t2 t2
  ON t1.warehouse_code = t2.warehouse_code
  AND t1.product_code2 = t2.product_code2
  
  LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp t3
  ON t1.business_area = t3.business_area
  AND t1.product_code2 = t3.product_fe 
  
  LEFT JOIN fe_dm.dm_sc_business_product_tmp t4
  ON t1.business_area = t4.business_area
  AND t1.product_code2 = t4.product_code2
  
  LEFT JOIN fe_dm.dm_sc_business_product_stock_tmp t5
  ON t1.business_area = t5.business_name
  AND t1.product_code2 = t5.product_code2  
  ;
  
SET @time_18 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pj_outstock2_day","@time_17--@time_18",@time_17,@time_18);
-- 插入没有出现在仓库中，但在运营清单里面的新增试运行和原有品；
DELETE FROM fe_dwd.dwd_pj_outstock2_day  WHERE FPRODUCEDATE = SUBDATE(CURDATE(),1) AND ISNULL(FMATERIALID);  
INSERT INTO fe_dwd.dwd_pj_outstock2_day  
( FPRODUCEDATE,
    BUSINESS_AREA,
    BIG_AREA,
    WAREHOUSE_NUMBER,
    WAREHOUSE_NAME,
    PRODUCT_BAR,
    PRODUCT_NAME,
    PRODUCT_CATEGORY,
    F_BGJ_POPRICE,
    SAFT_DAY,
    product_type
)
SELECT SUBDATE(CURDATE(),1) sdate
, t.`business_area`
, w.`region_area`
, w.`WAREHOUSE_NUMBER`
, w.`warehouse_name`
, t.`PRODUCT_FE` 
, t.`PRODUCT_NAME`
, p.`fname_type`
, p.`F_BGJ_POPRICE`
, d.SAFT_DAY
, t.`PRODUCT_TYPE`
FROM fe_dwd.`dwd_pub_product_dim_sserp` t
JOIN fe_dwd.`dwd_pub_warehouse_business_area` w
ON w.`business_area` = t.`business_area` 
AND w.`data_flag` = 1
AND t.`PRODUCT_TYPE` IN ('新增（试运行）','原有')
JOIN fe_dwd.`dwd_product_base_day_all` p
ON t.`PRODUCT_ID` = p.`PRODUCT_ID`
JOIN fe_dwd.`dwd_sserp_zs_dc_business_area` d
ON  w.WAREHOUSE_NUMBER =   d.DC_CODE
LEFT JOIN fe_dwd.dwd_PJ_OUTSTOCK2_DAY t1
ON t.`business_area` = t1.`BUSINESS_AREA`
AND t.`PRODUCT_FE` = t1.`PRODUCT_BAR`
AND t1.`FPRODUCEDATE` = SUBDATE(CURDATE(),1)
WHERE ISNULL(t1.`BUSINESS_AREA`) 
;
  
  
SET @time_19 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dwd_pj_outstock2_day","@time_18--@time_19",@time_18,@time_19);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_pj_outstock2_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pj_outstock2_day','dwd_pj_outstock2_day','吴婷');
COMMIT;
    END