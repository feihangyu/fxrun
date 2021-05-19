CREATE DEFINER=`feprocess`@`%` PROCEDURE `pj_poorderlist_day`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();        
DROP TEMPORARY TABLE IF EXISTS  feods.pj_poorderlist_tmp;
CREATE TEMPORARY TABLE feods.pj_poorderlist_tmp
(KEY idx_fbillno (FBILLNO),
KEY idx_fbillno_material (FBILLNO,FMATERIALID),
KEY idx_warehouse_fnumber(warehouse_name,product_code2)
)
AS
SELECT
    b.FDATE , #AS '申请时间'
    b.FPURCHASERID , #AS '采购用户ID'
    c.FNAME AS purchaser_name, #AS '采购员名称'
    b.FBILLNO,
    k.BUSINESS_AREA,
    k.BIG_AREA,
    b.FSUPPLIERID ,#AS '供应商ID'
    d.FNAME AS supplier_addresss, #AS '供应商地址'
    d.FSHORTNAME , #AS '供应商名称'
    a.FMATERIALID , #AS '物料ID'	
    a.FSEQ,
    h.fnumber AS product_code2, #AS 'FE码'
    e.FNAME ,#AS '物料名称'
    i.fname AS product_type ,#AS '类别'
    CASE
      WHEN b.FDOCUMENTSTATUS = 'A'
      THEN '新建'
      WHEN b.FDOCUMENTSTATUS = 'Z'
      THEN '暂存'
      WHEN b.FDOCUMENTSTATUS = 'B'
      THEN '审核中'
      WHEN b.FDOCUMENTSTATUS = 'C'
      THEN '已审核'
      WHEN b.FDOCUMENTSTATUS = 'D'
      THEN '重新审核'
    END FDOCUMENTSTATUS , #AS '单据状态'
    CASE
      WHEN b.FCLOSESTATUS = 'A'
      THEN '未关闭'
      WHEN b.FCLOSESTATUS = 'B'
      THEN '已关闭'
    END FCLOSESTATUS, #AS '关闭状态'
    CASE
      WHEN b.FCANCELSTATUS = 'A'
      THEN '未作废'
      WHEN b.FCANCELSTATUS = 'B'
      THEN '已作废'
    END FCANCELSTATUS, #AS '作废状态'
    CASE
      WHEN a.FMRPCLOSESTATUS = 'A'
      THEN '正常'
      WHEN a.FMRPCLOSESTATUS = 'B'
      THEN '业务关闭'
    END FMRPCLOSESTATUS, #AS '业务关闭'
    b.F_BGJ_FSTOREHOUSE , #AS '收料仓库ID'
    b.F_BGJ_FWAREHOUSER , #AS '仓库管理员'
    b.F_BGJ_FWAREHOUSEADD , #AS '仓库地址'
    f.FNAME AS warehouse_name, #AS '仓库名称'
    k.DC_CODE, # 仓库编码
    a.FNOTE , #AS '备注'
    SUM(a.FSALQTY) FSALQTY, #AS '采购申请数量'
 --    SUM(
--       a.F_BGJ_FPURCASEPRICE * a.F_BGJ_FQTYS
--     ) AS '采购申请金额',
    SUM(l.FTAXPRICE*a.FSALQTY) purchase_amount , #AS '采购申请金额'
    SUM(l.FTAXPRICE*a.FSALQTY)/SUM(a.FSALQTY)purchase_price , #AS '采购单价'
    a.FGIVEAWAY,
    b.`F_BGJ_FISINSTOCK` # 是否需要入库1是0否（用于计算在途）
    FROM sserp.T_PUR_POORDERENTRY a  
    JOIN sserp.T_PUR_POORDER b
    ON a.FID = b.FID
    --  and b.FBILLNO = 'CGDD20200512067'
    AND b.FDATE >= SUBDATE(CURDATE(),INTERVAL 6 MONTH)
    LEFT JOIN sserp.V_BD_BUYER_L c
    ON b.FPURCHASERID = c.fid
    JOIN sserp.T_BD_SUPPLIER_L d
    ON b.FSUPPLIERID = d.FSUPPLIERID
    JOIN sserp.T_BD_MATERIAL_L e
    ON a.FMATERIALID = e.FMATERIALID
    JOIN sserp.T_BD_STOCK_L f
    ON b.F_BGJ_FSTOREHOUSE = f.FSTOCKID
    JOIN sserp.T_BD_MATERIAL h
    ON a.FMATERIALID = h.FMATERIALID
    JOIN sserp.T_BD_MATERIALGROUP_L i
    ON h.FMATERIALGROUP = i.fid
    JOIN sserp.T_BD_STOCK j
    ON f.FSTOCKID = j.FSTOCKID
    JOIN sserp.ZS_DC_BUSINESS_AREA k
    ON j.fnumber = k.DC_CODE
    JOIN sserp.T_PUR_POORDERENTRY_F l
    ON a.fid = l.FID
    AND a.fentryid = l.FENTRYID
  GROUP BY b.FDATE,
    a.FGIVEAWAY,
    b.FPURCHASERID,
    c.FNAME,
    b.FBILLNO,
    b.FSUPPLIERID,
    d.FNAME,
    d.FSHORTNAME,
    a.FMATERIALID,
    h.fnumber,
    e.FNAME,
    i.fname,
    b.F_BGJ_FSTOREHOUSE,
    b.F_BGJ_FWAREHOUSER,
    b.F_BGJ_FWAREHOUSEADD,
    f.FNAME,
    a.FNOTE,
    a.FMATERIALID
    -- ,a.FSEQ
    ;
#  通知收料 
DROP TEMPORARY TABLE IF EXISTS  feods.pj_poorderlist_receive_tmp;
CREATE TEMPORARY TABLE feods.pj_poorderlist_receive_tmp
(KEY idx_FORDERBILLNO (FORDERBILLNO,FMATERIALID)) 
AS
SELECT
      FORDERBILLNO,
      FMATERIALID,
      FSEQ,
      SUM(FACTLANDQTY) AS '通知收料数量'
    FROM
      sserp.T_PUR_RECEIVEENTRY a
      LEFT JOIN sserp.T_PUR_RECEIVE b
        ON a.fid = b.fid
    GROUP BY FORDERBILLNO,
      FMATERIALID;
 
# 入库单
 
DROP TEMPORARY TABLE IF EXISTS  feods.pj_poorderlist_instock_tmp;
CREATE TEMPORARY TABLE feods.pj_poorderlist_instock_tmp
(KEY idx_fpoorderno(fpoorderno,FMATERIALID)) 
AS
SELECT
      t1.FBILLNO,
      t1.fpoorderno,
      t1.FMATERIALID,
      t1.FSEQ,
      t1.FGIVEAWAY,
      t1.FDATE,
      t1.qty,
      CASE
        WHEN t2.FMATERIALID IS NOT NULL
        THEN 1
        ELSE 0
      END AS stype
    FROM
      (SELECT
        b.FBILLNO,
        a.fpoorderno,
        a.FMATERIALID,
        a.FGIVEAWAY,
        a.FSEQ,
        b.FDATE,
        SUM(a.FREALQTY) AS qty
      FROM
        sserp.T_STK_INSTOCKENTRY a
        LEFT JOIN sserp.T_STK_INSTOCK b
          ON a.fid = b.fid
      GROUP BY b.FBILLNO,
        a.fpoorderno,
        a.FMATERIALID,
        a.FGIVEAWAY
       --  ,a.FSEQ
        ) t1
      LEFT JOIN
        (SELECT
          a.fpoorderno,
          a.FMATERIALID,
          a.FSEQ,
          MIN(b.FDATE) AS min_fdate
        FROM
          sserp.T_STK_INSTOCKENTRY a
          LEFT JOIN sserp.T_STK_INSTOCK b
            ON a.fid = b.fid
        GROUP BY a.fpoorderno,
          a.FMATERIALID
          ) t2
        ON t1.fpoorderno = t2.fpoorderno
        AND t1.FMATERIALID = t2.FMATERIALID
;
    
# 退料单
DROP TEMPORARY TABLE IF EXISTS  feods.pj_poorderlist_back1;
CREATE TEMPORARY TABLE feods.pj_poorderlist_back1
(KEY idx_forderno(forderno,FMATERIALID)) 
AS
SELECT
      forderno,
      FMATERIALID,
      a.FSEQ,
      SUM(frmrealqty) AS qty
    FROM
      sserp.T_PUR_MRBENTRY a
      LEFT JOIN sserp.T_PUR_MRB b
        ON a.fid = b.fid
    GROUP BY forderno,
      FMATERIALID
      -- ,a.FSEQ
;
    -- AND t1.FSEQ = t4.FSEQ
DROP TEMPORARY TABLE IF EXISTS  feods.pj_poorderlist_back2;
CREATE TEMPORARY TABLE feods.pj_poorderlist_back2
(KEY idx_forderno(forderno,FMATERIALID)) 
AS 
SELECT
      forderno,
      FMATERIALID,
      FSEQ,
      SUM(FBASEMRQTY) AS qty
    FROM
      sserp.T_PUR_MRAPPENTRY a
      LEFT JOIN sserp.T_PUR_MRAPP b
        ON a.fid = b.fid
    GROUP BY forderno,
      FMATERIALID
       -- ,FSEQ
 ;
 
 # 下单前一天的库存情况
DROP TEMPORARY TABLE IF EXISTS  feods.d_sc_warehouse_stock_tmp;
CREATE TEMPORARY TABLE feods.d_sc_warehouse_stock_tmp
(KEY idx_sdate_warehouse_materialid(sdate,warehouse_number,fmaterialid)) 
SELECT 
    ADDDATE(p.fproducedate,1) AS sdate
    , p.warehouse_number
    , p.product_bar
    , p.FMATERIALID
    , p.outstock_day
    , p.qualityqty
    , p.outstock_amount
    , p.f_bgj_poprice
    , p.product_type 
FROM feods.PJ_OUTSTOCK2_DAY p
WHERE p.FPRODUCEDATE >= SUBDATE(SUBDATE(CURDATE(),1),INTERVAL 6 MONTH )
AND (p.qualityqty > 0 OR p.outstock_day > 0 )
;
DELETE FROM feods.pj_poorderlist_day WHERE purchase_time >= SUBDATE(CURDATE(),INTERVAL 6 MONTH);
INSERT INTO feods.pj_poorderlist_day
(
big_area,
business_area,
purchase_time,
purchase_staff_id,
purchase_staff,
fbillno,
supplier_id,
supplier_address,
supplier_name,
material_id,
product_code2,
material_name,
purchase_type,
order_status,
close_status,
invalid_status,
business_close,
warehouse_id,
warehouse_keeper, 
warehouse_address,
warehouse_name,
note,
fgiveaway,
apply_qty,
purchase_amount,
purchase_price,
receive_billno,
receive_qty,
instock_billno,
instock_time,
delivery_term,
actual_instock_qty,
return_billno,
return_qty,
outstock_day_before,
output_qualityqty_before,
outstock_amount_before,
outstock_price_before,
outstock_type_beore,
outstock_day_after,
output_qualityqty_after,
outstock_amount_after,
outstock_price_after,
FSEQ,
warehouse_number,
F_BGJ_FISINSTOCK
)
SELECT 
  t1.BIG_AREA,
  t1.BUSINESS_AREA,
  DATE(t1.FDATE) AS purchase_time  , #AS '申请时间'
  t1.FPURCHASERID , #AS '采购用户ID'
  t1.purchaser_name, #AS '采购员名称'
  t1.FBILLNO,
  t1.FSUPPLIERID ,#AS '供应商ID'
  t1.supplier_addresss, #AS '供应商地址'
  t1.FSHORTNAME , #AS '供应商名称'
  t1.FMATERIALID , #AS '物料ID'	
  t1.product_code2, #AS 'FE码'
  t1.FNAME ,#AS '物料名称'
  t1.product_type ,#AS '类别' 
  t1.FDOCUMENTSTATUS , #AS '单据状态' 
  t1.FCLOSESTATUS, #AS '关闭状态'
  t1.FCANCELSTATUS, #AS '作废状态'
  t1.FMRPCLOSESTATUS, #AS '业务关闭'
  t1.F_BGJ_FSTOREHOUSE , #AS '收料仓库ID'
  t1.F_BGJ_FWAREHOUSER , #AS '仓库管理员'
  t1.F_BGJ_FWAREHOUSEADD , #AS '仓库地址'
  t1.warehouse_name, #AS '仓库名称'
  t1.FNOTE , #AS '备注'
--   t1.FGIVEAWAY,  
--   t1.FSALQTY, #AS '采购申请数量'
--   t1.purchase_amount , #AS '采购申请金额'
--   t1.purchase_price , #AS '采购单价'
--     
  CASE
    WHEN t1.FGIVEAWAY = 1
    THEN '赠品'
    ELSE '非赠品'
  END AS '是否赠品',
  CASE
    WHEN t3.stype = 0
    THEN NULL
    ELSE t1.FSALQTY
  END AS '采购申请数量',
  CASE
    WHEN t3.stype = 0
    THEN NULL
    ELSE t1.purchase_amount
  END AS '采购申请金额',
  t1.purchase_price AS '采购单价',
  '' AS '收料通知单号',
  CASE
    WHEN t3.stype = 1
    AND t1.FGIVEAWAY = 0
    THEN IFNULL(t2.通知收料数量, 0) - IFNULL(t4.qty, 0) - IFNULL(t5.qty, 0)
    ELSE NULL
  END AS '通知收料数量',
  t3.FBILLNO AS '入库单号',
  t3.FDATE AS '入库时间',
  DATEDIFF(t3.FDATE,t1.FDATE) AS '交期',
  CASE
    WHEN t3.stype = 0
    THEN t3.qty
    ELSE t3.qty - IFNULL(t5.qty, 0)
  END AS '实际入库数量',
  '' AS '退料单号',
  CASE
    WHEN t3.stype = 0
    AND t1.FGIVEAWAY = 1
    THEN NULL
    ELSE t4.qty
  END AS '退料数量',
  -- p.订单前有出库天数,
--   p.订单前正品库存量,
--   p.订单前出库金额,
--   p.订单前采购价,
--   p.库存类别,
   p.OUTSTOCK_DAY ,
   p.QUALITYQTY ,
   p.OUTSTOCK_AMOUNT ,
   p.F_BGJ_POPRICE,
   p.PRODUCT_type , 
--   m.到库后有出库天数,
--   m.到库后正品库存量,
--   m.到库后出库金额,
--   m.到库后采购价,
m.OUTSTOCK_DAY ,
m.QUALITYQTY ,
m.OUTSTOCK_AMOUNT ,
m.F_BGJ_POPRICE ,      
t1.FSEQ,
t1.DC_CODE,
t1.F_BGJ_FISINSTOCK
FROM
feods.pj_poorderlist_tmp  t1
LEFT JOIN
feods.pj_poorderlist_receive_tmp t2
ON t1.FBILLNO = t2.FORDERBILLNO
AND t1.FMATERIALID = t2.FMATERIALID
LEFT JOIN
feods.pj_poorderlist_instock_tmp t3
ON t1.FBILLNO = t3.fpoorderno
AND t1.FMATERIALID = t3.FMATERIALID
AND t1.FGIVEAWAY = t3.FGIVEAWAY
LEFT JOIN feods.pj_poorderlist_back1 t4
ON t1.FBILLNO = t4.forderno
AND t1.FMATERIALID = t4.FMATERIALID
LEFT JOIN feods.pj_poorderlist_back2 t5
ON t1.FBILLNO = t5.forderno
AND t1.FMATERIALID = t5.FMATERIALID
LEFT JOIN feods.d_sc_warehouse_stock_tmp p
ON t1.FDATE = p.sdate #订货前一天的周转天数和正品库存量
AND t1.DC_CODE = p.warehouse_number #仓库名称
AND t1.FMATERIALID = p.FMATERIALID #关联FE码，商品ID在仓库表里面不全
LEFT JOIN feods.PJ_OUTSTOCK2_DAY m
ON t1.warehouse_name = m.WAREHOUSE_NAME #仓库名称
AND t1.product_code2 = m.PRODUCT_BAR #关联FE码，商品ID在仓库表里面不全
AND m.FPRODUCEDATE = t3.FDATE #到货后周转天数和正品库存量
;
 
# 当日仓库在途量
TRUNCATE feods.d_sc_warehouse_onload;
INSERT INTO feods.d_sc_warehouse_onload
(sdate,
region_area,
business_area,
warehouse_name,
warehouse_number,
product_id,
product_code2,
product_name,
product_category,
FBASEQTY,
QUALITYQTY,
apply_qty,
purchase_amount,
actual_instock_qty,
onload_qty
)
SELECT
    SUBDATE(CURDATE(),1) AS sdate
    , t.region_area
    , t.business_area
    , t.warehouse_name
    , t.warehouse_number 
    , dp.product_id
    , t.product_code2 
    , dp.product_name AS product_name
    , dp.category_name AS product_category
    , IFNULL(p.FBASEQTY,0) AS FBASEQTY
    , IFNULL(p.QUALITYQTY,0) AS QUALITYQTY
    ,SUM(IFNULL(pur.apply_qty,0)) AS apply_qty
    ,SUM(IFNULL(pur.purchase_amount,0))AS purchase_amount
    ,SUM(IFNULL(pur.actual_instock_qty,0)) AS actual_instock_qty
    ,SUM(IFNULL(pur.apply_qty,0) - IFNULL(pur.actual_instock_qty, 0)) AS onload_qty
FROM
(SELECT  big_area AS region_area,warehouse_name,warehouse_number,business_area,product_code2
FROM feods.`pj_poorderlist_day` pur
WHERE pur.order_status IN ('已审核','审核中')
AND pur.close_status = '未关闭'
AND pur.invalid_status = '未作废'
AND pur.business_close = '正常'
AND pur.`F_BGJ_FISINSTOCK` = 1
UNION 
SELECT p.big_area,p.warehouse_name,p.warehouse_number,p.business_area,product_bar
FROM
    feods.PJ_OUTSTOCK2_DAY p
JOIN feods.`wt_warehouse_business_area` w
ON p.`WAREHOUSE_NUMBER` = w.`WAREHOUSE_NUMBER`
AND w.`data_flag` = 1
AND FPRODUCEDATE >= DATE_SUB(CURDATE(),INTERVAL 1 DAY)
AND p.product_bar NOT LIKE "W%") t
LEFT JOIN feods.PJ_OUTSTOCK2_DAY p          
ON p.warehouse_name = t.warehouse_name
AND p.product_bar = product_code2
AND FPRODUCEDATE >= DATE_SUB(CURDATE(),INTERVAL 1 DAY)
LEFT JOIN feods.`pj_poorderlist_day` pur
ON pur.warehouse_name = t.`WAREHOUSE_NAME`
AND pur.product_code2 = t.product_code2
AND pur.order_status IN ('已审核','审核中')
AND pur.close_status = '未关闭'
AND pur.invalid_status = '未作废'
AND pur.business_close = '正常'
AND pur.`F_BGJ_FISINSTOCK` = 1
JOIN fe_dwd.`dwd_product_base_day_all` dp
ON t.product_code2 = dp.product_code2
GROUP BY t.business_area,t.product_code2
;
     
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'pj_poorderlist_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
  
COMMIT;
    END