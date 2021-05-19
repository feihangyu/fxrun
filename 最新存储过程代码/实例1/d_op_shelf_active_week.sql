CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_op_shelf_active_week`()
BEGIN
SET    @week_end := SUBDATE(CURRENT_DATE,DAYOFWEEK(CURRENT_DATE) - 1);
SET    @add_day := ADDDATE(@week_end, 1);
SET    @week_start := SUBDATE(@week_end, 6);   
SET    @end_date = CURDATE();
SET    @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
SET    @run_date := CURRENT_DATE();
SET    @user := CURRENT_USER();
SET    @timestamp := CURRENT_TIMESTAMP();
	   
DELETE FROM feods.d_op_shelf_active_week
WHERE week_end = @week_end;
-- 已激活货架信息
DROP TEMPORARY TABLE IF EXISTS feods.shelf_active_tmp;
CREATE TEMPORARY TABLE feods.shelf_active_tmp(PRIMARY KEY (shelf_id))
SELECT business_name,
       shelf_id
FROM fe_dwd.dwd_shelf_base_day_all
WHERE shelf_status = 2; -- 已激活
-- 给临时表test.shelf_tmp添加索引
CREATE INDEX idx_tmp_shelf_id ON feods.shelf_active_tmp (shelf_id);
-- 货架商品周销售数据
DROP TEMPORARY TABLE IF EXISTS feods.shelf_product_sale_tmp;
CREATE TEMPORARY TABLE feods.shelf_product_sale_tmp(PRIMARY KEY (shelf_id,product_id))
SELECT shelf_id,
       product_id,
       SUM(qty)qty
FROM
(
SELECT o.shelf_id,
       o.product_id,
       SUM(o.quantity_act) qty
FROM fe_dwd.dwd_pub_order_item_recent_one_month o
JOIN feods.shelf_active_tmp t ON o.shelf_id = t.shelf_id  -- 已激活货架信息
WHERE o.pay_date >= @week_start
AND o.pay_date < @add_day
GROUP BY o.shelf_id,o.product_id
UNION ALL -- 未对接系统自贩机数据
SELECT shelf_id,
       product_id,
       SUM(amount)amount
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @week_start
AND pay_date < @add_day
GROUP BY shelf_id,product_id
)a
GROUP  BY shelf_id,product_id
HAVING qty > 0;
-- 货架周有库存/有销售sku
DROP TEMPORARY TABLE IF EXISTS feods.shelf_stock_sku_tmp;
CREATE TEMPORARY TABLE feods.shelf_stock_sku_tmp(PRIMARY KEY (shelf_id,product_id))
SELECT t.shelf_id,
       t.product_id
FROM (SELECT shelf_id,product_id FROM feods.op_shelf_week_product_stock_detail_tmp WHERE week_end = @week_end) t
UNION
SELECT s.shelf_id,
       s.product_id
FROM feods.shelf_product_sale_tmp s;   # 货架商品周销售数据
-- 货架动销率(有销售sku/有库存sku)
INSERT INTO feods.d_op_shelf_active_week
(week_end,
 business_name,
 shelf_id,
 sto_sku,
 sale_sku,
 active_rate,
 load_time
)
SELECT @week_end week_end,
       p.business_name,
       p.shelf_id,
       IFNULL(u.sto_sku,0)sto_sku,
       IFNULL(s.sale_sku,0)sale_sku,
       IFNULL(s.sale_sku,0)/ (CASE WHEN IFNULL(u.sto_sku,0)=0 THEN 1 ELSE u.sto_sku END)  active_rate, 
       CURRENT_TIMESTAMP AS load_time
FROM feods.shelf_active_tmp p      -- 已激活货架信息
LEFT JOIN -- 有库存sku
(SELECT shelf_id,
        COUNT(product_id)sto_sku
FROM feods.shelf_stock_sku_tmp s   -- 货架周有库存/有销售sku
GROUP BY shelf_id
)u ON p.shelf_id = u.shelf_id
LEFT JOIN -- 有销售sku
(SELECT shelf_id,
        COUNT(product_id)sale_sku
FROM feods.shelf_product_sale_tmp   # 货架商品周销售数据
GROUP BY shelf_id
)s ON p.shelf_id = s.shelf_id;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'd_op_shelf_active_week',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('唐进（朱星华）@', @user, @timestamp)
  );
COMMIT;
END