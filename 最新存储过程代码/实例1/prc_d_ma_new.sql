CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_new`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  SET @sdate := DATE_ADD(CURDATE(), INTERVAL -1 DAY),
  @user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  /*新品 上架14天内 新品销售标签*/  
DELETE FROM feods.d_ma_new_product_daily_statistics WHERE sdate = @sdate;
INSERT INTO feods.`d_ma_new_product_daily_statistics`(sdate, business_name, GMV, QTY, DISCOUNT_AMOUNT,SALE_SHELF_NUM,on_shelf_stock_qty,on_shelf_stock_shelf_num , pay_aoumnt )
SELECT DATE(a1.sdate) 日期,
       b1.business_name,
       SUM(a1.gmv) GMV,
       SUM(a1.sal_qty) QTY ,
       SUM(a1.DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,
       COUNT(DISTINCT CASE WHEN a1.sal_qty > 0 THEN a1.shelf_id ELSE NULL END)  SALE_SHELF_NUM,
       SUM(a1.stock_quantity) on_shelf_stock_qty,
       COUNT(DISTINCT CASE WHEN a1.stock_quantity > 0 THEN a1.shelf_id ELSE NULL END) on_shelf_stock_shelf_num,
       sum(a1.REAL_TOTAL_PRICE) aoumnt
FROM `fe_dwd`.`dwd_shelf_product_day_all_recent_32` a1
JOIN `fe_dwd`.`dwd_shelf_base_day_all` b1 ON a1.shelf_id = b1.shelf_id
WHERE a1.new_flag = 1
AND b1.`business_name` NOT IN ('内蒙古区','惠州区','冀北区','烟台市','台州区') 
AND a1.`sdate` >= SUBDATE(CURDATE(), INTERVAL 1 DAY)
AND a1.`sdate` < CURDATE()
GROUP BY b1.business_name
;
  
/*2019年12月新增*/
/*失败重跑删掉数据*/
DELETE FROM `fe_dm`.dm_ma_area_daily WHERE sdate = @sdate;
/*货架商品标签-新品运营标识*/
DROP TABLE IF EXISTS `feods`.d_ma_valid_shelf_temp;	
CREATE TEMPORARY TABLE `feods`.d_ma_valid_shelf_temp(KEY(business_name), KEY(shelf_id),KEY(product_id))	## 有效货架商品新品标签-新增 免费货
AS	
SELECT c.shelf_id,c.product_id, b.business_name	
FROM fe_dwd.`dwd_shelf_base_day_all` b
JOIN feods.`zs_shelf_product_flag` c ON c.shelf_id = b.shelf_id  
WHERE b.business_name NOT IN ('内蒙古区','惠州区','冀北区','烟台市','台州区') 	
AND b.SHELF_STATUS = 2	
and b.REVOKE_STATUS = 1
and b.WHETHER_CLOSE = 2
AND b.data_flag = 1 
AND c.ext8 IN (1,2) 	-- 新品标签-新增 免费货
;
INSERT INTO `fe_dm`.dm_ma_area_daily (sdate, business_name, GMV, QTY, DISCOUNT_AMOUNT,SALE_SHELF_NUM,on_shelf_stock_qty,on_shelf_stock_shelf_num, pay_aoumnt)
SELECT 
       DATE(a1.sdate) 日期,
       b1.business_name,
       SUM(a1.gmv) GMV,
       SUM(a1.sal_qty) QTY ,
       SUM(a1.DISCOUNT_AMOUNT) DISCOUNT_AMOUNT,
       COUNT(DISTINCT CASE WHEN a1.sal_qty > 0 THEN a1.shelf_id ELSE NULL END)  SALE_SHELF_NUM,
       SUM(a1.stock_quantity) on_shelf_stock_qty,
       COUNT(DISTINCT CASE WHEN a1.stock_quantity > 0 THEN a1.shelf_id ELSE NULL END) on_shelf_stock_shelf_num,
       sum(a1.REAL_TOTAL_PRICE) aoumnt
FROM `fe_dwd`.`dwd_shelf_product_day_all_recent_32` a1
JOIN `feods`.d_ma_valid_shelf_temp b1 ON a1.shelf_id = b1.shelf_id and a1.`product_id` = b1.product_id
WHERE 
 a1.`sdate` >= SUBDATE(CURDATE(), INTERVAL 1 DAY)
AND a1.`sdate` < CURDATE()
GROUP BY b1.business_name
;

-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_ma_new',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
  COMMIT;
END