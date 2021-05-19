CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_autoshelf_stock_and_sale`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
set @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE) + 1),
    @week_start := SUBDATE(@week_end,6),
    @add_week_end := ADDDATE(@week_end ,1),
    @month_id := DATE_FORMAT(@week_end,'%Y-%m');
delete from fe_dm.`dm_op_autoshelf_stock_and_sale` where week_end = @week_end ;
	   
-- 自贩机   
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_area_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_area_tmp AS
SELECT business_name,
       shelf_id,
       shelf_code,
       DATE(activate_time)activate_time
FROM fe_dwd.dwd_shelf_base_day_all s 
WHERE shelf_type = 7
AND shelf_status = 2;
-- 周有库存及有销售sku
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sku_tmp;
CREATE TEMPORARY TABLE fe_dwd.sku_tmp AS
SELECT a.business_name,
       SUM(a.sto_sku)sto_sku,
       SUM(a.sale_sku)sale_sku
FROM feods.d_op_shelf_active_week a
JOIN fe_dwd.shelf_area_tmp s ON a.shelf_id = s.shelf_id
WHERE week_end = @week_end
GROUP BY a.business_name;
-- 周最后一天的货架商品库存数据
SELECT
    @sql_str1 := CONCAT(
      "CREATE TEMPORARY TABLE fe_dwd.shelf_stock_tmp AS ",
      "SELECT p.business_name,t.shelf_id,t.product_id,d.sale_price,0",
      GROUP_CONCAT(
        CONCAT("+t.t", DAY(t.sdate)) SEPARATOR ' '
      ),
      " stock",
      " FROM feods.d_op_sp_sal_sto_detail t 
        join fe_dwd.shelf_area_tmp p on t.shelf_id = p.shelf_id
        join fe_dwd.dwd_shelf_product_day_all d on p.shelf_id = d.shelf_id and t.product_id = d.product_id
        WHERE t.month_id = @month_id and(0 ",
      GROUP_CONCAT(
        CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
      ),
      ")"
    )
  FROM feods.fjr_work_days t
  WHERE DATE(t.sdate) = @week_end;
PREPARE sql_exe FROM @sql_str1;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_stock_tmp;
EXECUTE sql_exe;
-- 周最后一天库存金额
DROP TEMPORARY TABLE IF EXISTS fe_dwd.stock_tmp;
CREATE TEMPORARY TABLE fe_dwd.stock_tmp AS
SELECT business_name,
       SUM(stock * sale_price) stock
FROM fe_dwd.shelf_stock_tmp
GROUP BY business_name;
-- 货架周销量、gmv
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_sale_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_sale_tmp AS
SELECT business_name,
       SUM(week_amount)week_amount,
       SUM(week_gmv)week_gmv
FROM
(
SELECT s.business_name,
       o.shelf_id,
       SUM(o.quantity)week_amount,
       SUM(o.quantity * o.sale_price)week_gmv
FROM fe_dwd.dwd_pub_order_item_recent_two_month o
JOIN fe_dwd.shelf_area_tmp s ON o.shelf_id = s.shelf_id
WHERE o.pay_date >= @week_start
AND o.pay_date < @add_week_end
GROUP BY s.business_name,o.shelf_id
UNION ALL -- 未对接澳柯玛周销售数据
SELECT b.business_name,
       t.shelf_id,
       SUM(amount)week_amount,
       SUM(total)week_gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht t
JOIN fe_dwd.dwd_shelf_base_day_all b ON t.shelf_id = b.shelf_id
WHERE t.pay_date >= @week_start
AND t.pay_date < @add_week_end
AND t.refund_status = '无'
GROUP BY b.business_name,t.shelf_id
)a
GROUP BY business_name;
INSERT INTO fe_dm.`dm_op_autoshelf_stock_and_sale`
(week_end
,business_name
,sto_sku
,sale_sku
,stock
,week_amount
,week_gmv
,load_time
)
SELECT @week_end week_end,
       w.business_name,
       w.sto_sku,
       w.sale_sku,
       a.stock,
       p.week_amount,
       p.week_gmv,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dwd.sku_tmp w
LEFT JOIN fe_dwd.stock_tmp a ON w.business_name = a.business_name
LEFT JOIN fe_dwd.shelf_sale_tmp p ON p.business_name = w.business_name
GROUP BY w.business_name;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_op_autoshelf_stock_and_sale',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('唐进(朱星华)@', @user, @timestamp)
  );
  COMMIT;	
END