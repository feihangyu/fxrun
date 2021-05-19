CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_sc_shelf_package`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
TRUNCATE TABLE feods.d_sc_shelf_packages;
INSERT INTO feods.d_sc_shelf_packages
(sdate,
region_area,
business_area,
shelf_id,
shelf_type,
product_id,
product_code2,
product_name,
package_id,
sale_price,
product_type,
stock,
shelf_fill_flag
)
SELECT
    CURDATE() AS sdate
    , w.region_area
    , c.`business_name`
    , s.shelf_id
    ,
    CASE
        s.shelf_type
        WHEN 1
        THEN "四层标准货架"
        WHEN 2
        THEN "冰箱"
        WHEN 3
        THEN "五层防鼠货架"
        WHEN 4
        THEN "虚拟货架"
        WHEN 5
        THEN "冰柜"
        WHEN 6
        THEN "智能货柜"
        WHEN 7
        THEN "自动贩卖机"
        WHEN 8
        THEN "校园货架"
    END AS shelf_type
  
    ,t3.`PRODUCT_ID`
    , t5.product_code2
    , t5.product_name
    ,GROUP_CONCAT(t1.package_id ORDER BY t1.package_id SEPARATOR "'" ) AS package_id
    ,GROUP_CONCAT(t3.sale_price ORDER BY t1.package_id SEPARATOR "'")  AS sale_price
    , t4.product_type
    , IFNULL(t6.STOCK_QUANTITY,0) AS stock
    , t6.shelf_fill_flag
FROM fe.`sf_shelf` s
JOIN feods.`fjr_city_business` c
ON s.`CITY` = c.city 
AND s.`DATA_FLAG` = 1
AND s.`SHELF_STATUS` = 2
JOIN feods.`wt_warehouse_business_area` w
ON c.business_name = w.business_area
AND w.data_flag = 1 
JOIN fe.`sf_shelf_package_detail` t1
ON s.`shelf_id` = t1.`SHELF_ID`
AND t1.`DATA_FLAG` = 1
JOIN fe.`sf_package` t2
ON t1.`PACKAGE_ID` = t2.`PACKAGE_ID`
AND t2.`DATA_FLAG` =1
AND t2.`STATU_FLAG` = 1 #商品包为启用状态
JOIN 
(SELECT DISTINCT t2.`PACKAGE_ID`,t1.`DETAIL_ID`,t1.`PRODUCT_ID`,t2.sale_price,t2.SHELF_FILL_FLAG
FROM fe.`sf_supplier_product_detail` t1
JOIN fe.`sf_package_item` t2
ON t1.`DETAIL_ID` = t2.`RELATION_ID`
AND t1.`DATA_FLAG` = 1
AND t2.`DATA_FLAG` = 1
AND t2.SHELF_FILL_FLAG = 1
AND t2.SHELF_FILL_FLAG = 1
) t3
ON t2.`PACKAGE_ID` = t3.package_id
JOIN fe.`sf_product` t5
ON t3.`PRODUCT_ID` = t5.product_id
LEFT JOIN feods.`zs_product_dim_sserp` t4
ON c.business_name = t4.business_area
AND t3.product_id = t4.product_id
-- and t4.product_type in ("新增（试运行）","新增（免费货）")
LEFT JOIN fe.`sf_shelf_product_detail` t6
ON s.shelf_id = t6.shelf_id
AND t3.product_id = t6.product_id
AND t6.data_flag = 1
GROUP BY s.shelf_id,t3.product_id; 
    
-- TRUNCATE TABLE  feods.d_sc_shelf_packages_onsale;
DELETE FROM  feods.d_sc_shelf_packages_onsale WHERE sdate = CURDATE();
INSERT INTO feods.d_sc_shelf_packages_onsale
(sdate,
region_area,
business_area,
product_id,
product_code2,
product_name,
product_type,
shelf_cnt,
onsale_shelf,
onsale_rate
)
SELECT sdate
, region_area
, business_area
, product_id
, product_code2
, product_name
, product_type
,COUNT(shelf_id) AS shelf_cnt,COUNT(IF(stock >0 , shelf_id, NULL)) AS onsale_shelf,COUNT(IF(stock >0 , shelf_id, NULL))/COUNT(shelf_id) AS onsale_rate
FROM feods.d_sc_shelf_packages
-- WHERE product_type IN ("新增（试运行）","新增（免费货）")
-- WHERE  shelf_fill_flag = 1
GROUP BY business_area,product_id;    
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_sc_shelf_package',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
   COMMIT;
   
END