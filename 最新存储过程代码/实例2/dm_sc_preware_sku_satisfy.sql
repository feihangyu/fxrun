CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_preware_sku_satisfy`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY); 
SET @sdate1 = CURDATE();
-- SET @sdate2 = IF(@sdate >= '2020-01-31','2019-12-15',@sdate);
-- SET @sdate3 = IF(@sdate >= '2020-01-31','2019-12-16',@sdate);
SET @a = 1, @b = 0.4;
SET @sdate2 = @sdate;
SET @sdate3 = @sdate;
# 结果集
DELETE FROM fe_dm.dm_sc_preware_sku_satisfy WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_sku_satisfy
(sdate,
region_area,
business_area,
warehouse_id,
shelf_code,
shelf_name,
PRODUCT_TYPE,
product_id,
product_code2,
product_name,
adjust_sale_flag,
available_stock,
avg_send_num,
fifteen_quantity,
fifteen_qty_discount,
preware_sale_flag,
thiry_quantity,
thirty_qty_discount,
before_fifteen,
satisfy
)
SELECT @sdate AS '统计日期'
, sf.region_area
, sf.business_area
, sf.warehouse_id
, sf.shelf_code
, sf.shelf_name
, s.PRODUCT_TYPE
, sf.product_id
, sf.product_code2
, s.product_name
, sf.sale_flag AS adjust_sale_flag
, IFNULL(c.AVAILABLE_STOCK,0) AS AVAILABLE_STOCK
, IFNULL(fo.send_noholiday,0)/14  AS '前14天日均出库量'
, IFNULL(f.quantity,0) AS '近15天销量'
, IFNULL(f.discount_sale_qty,0) AS '近15天销量8折'
, f.sale_flag AS preware_sale_flag
, sf.quantity AS '近30天销量'
, sf.discount_sale_qty AS '近30天销量(8折以上)' # 8折以上销售的
, sf.quantity - IFNULL(f.quantity,0) AS '前15天销量'
,CASE 
-- WHEN ft.sale_flag IN ("爆款","畅销","平销") AND IFNULL(t1.avg_sale_qty,0) * 3 <= c.AVAILABLE_STOCK THEN '满足'
-- WHEN ft.sale_flag IN ("滞销","严重滞销") AND  IFNULL(t1.avg_sale_qty,0) * 2 <= c.AVAILABLE_STOCK THEN '满足'
# 2019-12月13日修改，12月12日开始数据更新,逻辑为以下
-- WHEN IFNULL((fo.send_noholiday/14)* @a,0) * 2 <= c.AVAILABLE_STOCK THEN '满足' 
# 20200401修改，数据更新从4月1日开始，更新为以下
WHEN f.sale_flag IN ("爆款","畅销") AND IFNULL((fo.send_noholiday/14)* @a,0) * 2 <= c.AVAILABLE_STOCK THEN "满足"
WHEN f.sale_flag IN ("爆款","畅销") AND c.AVAILABLE_STOCK >= 15 THEN "满足"
WHEN f.sale_flag IN ("平销","滞销","严重滞销") AND IFNULL((fo.send_noholiday/14)* @a,0) * 2 <= c.AVAILABLE_STOCK THEN "满足"
WHEN f.sale_flag IN ("平销","滞销","严重滞销") AND c.AVAILABLE_STOCK >= 10 THEN "满足"
ELSE '不满足' 
END AS 'sku满足'
FROM fe_dm.dm_preware_shelf_sales_thirty sf
JOIN fe_dwd.`dwd_pub_product_dim_sserp` s
ON sf.business_area = s.BUSINESS_AREA 
AND sf.PRODUCT_ID = s.PRODUCT_ID
AND s.PRODUCT_TYPE IN ("新增（试运行）","原有")
AND sf.sdate = @sdate3
LEFT JOIN fe_dm.dm_prewarehouse_stock_detail c   #### 当天前置仓库存量
ON sf.warehouse_id = c.warehouse_id 
AND sf.product_id = c.product_id
AND c.check_date = @sdate
-- LEFT JOIN feods.`d_sc_preware_outbound_seven_day` fo
# 2020-04-02日改为以下，重新用户14天日均
LEFT JOIN fe_dwd.dwd_preware_outbound_forteen_day fo
ON sf.warehouse_id = fo.warehouse_id
AND sf.product_id = fo.product_id
AND fo.sdate = @sdate2
-- LEFT JOIN feods.`pj_preware_sales_fifteen` f
-- 20200401由于销售等级改为15天，所以去掉left
JOIN fe_dm.dm_preware_sales_fifteen f 
ON sf.warehouse_id = f.warehouse_id
AND sf.product_id = f.product_id
AND f.sdate = @sdate3
;
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_preware_sku_satisfy',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_sc_preware_sku_satisfy','dm_sc_preware_sku_satisfy','吴婷');
COMMIT;
    END