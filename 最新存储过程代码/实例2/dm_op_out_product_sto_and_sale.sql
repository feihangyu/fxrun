CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_out_product_sto_and_sale`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SELECT @sdate := CURRENT_DATE,
       @sub_1 := SUBDATE(@sdate,1),
       @month_id := DATE_FORMAT(@sub_1,'%Y-%m'),
       @month_start := CONCAT(@month_id,'-01'),
       @month_end :=ADDDATE(LAST_DAY(@month_start),1);
SET @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE) + 1);
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SET @time_1 := CURRENT_TIMESTAMP();
-- 当前淘汰品清单
DROP TEMPORARY TABLE IF EXISTS fe_dm.dim_tmp;
CREATE TEMPORARY TABLE fe_dm.dim_tmp (PRIMARY KEY (business_area,product_id))
SELECT business_area,
       product_id,
       product_type
FROM fe_dwd.dwd_pub_product_dim_sserp   -- zs_product_dim_sserp
WHERE product_type IN ('停补','停补（替补）','淘汰','淘汰（替补）','退出');
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_out_product_sto_and_sale","@time_1--@time_2",@time_1,@time_2);
-- 期初库存金额
DROP TEMPORARY TABLE IF EXISTS fe_dm.start_sto_tmp;
CREATE TEMPORARY TABLE fe_dm.start_sto_tmp (PRIMARY KEY(business_name,product_id))
SELECT business_name,product_id,IFNULL(SUM(stock_quantity * sale_price),0) sto_val 
FROM fe_dwd.dwd_shelf_product_sto_sal_30_days        
WHERE sdate=SUBDATE(CONCAT(DATE_FORMAT(CURRENT_DATE,'%Y-%m'),'-01'),INTERVAL 1 DAY)  -- 此处与实例1 sf_shelf_product_stock_detail表存在1天的库存差异，这个宽表sdate为fe库表T-1的数据
GROUP BY business_name,product_id;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_out_product_sto_and_sale","@time_2--@time_3",@time_2,@time_3);
-- 当前库存金额
DROP TEMPORARY TABLE IF EXISTS fe_dm.cur_sto_tmp;
CREATE TEMPORARY TABLE fe_dm.cur_sto_tmp (PRIMARY KEY(business_name,product_id))
SELECT b.business_name,product_id,IFNULL(SUM(a.stock_quantity * a.sale_price),0) sto_val 
FROM fe_dwd.dwd_shelf_product_day_all a   -- 该宽表库存与 sf_shelf_product_stock_detail保持一致，只保留一天的数据
JOIN fe_dwd.dwd_shelf_base_day_all b
ON a.shelf_id=b.shelf_id
GROUP BY b.business_name,product_id;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_out_product_sto_and_sale","@time_3--@time_4",@time_3,@time_4);
-- 当月累计补货上架金额
DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_tmp;
CREATE TEMPORARY TABLE fe_dm.fill_tmp (PRIMARY KEY (business_name,product_id))
SELECT b.business_name,
       a.product_id,
       SUM(actual_fill_num * sale_price)fill_val
FROM fe_dwd.dwd_fill_day_inc_recent_two_month a  force index(idx_dwd_replenish_FILL_TIME)  -- 此处为优化
JOIN fe_dwd.dwd_shelf_base_day_all b ON a.shelf_id = b.shelf_id
JOIN fe_dm.dim_tmp c ON b.business_name = c.business_area AND a.product_id = c.product_id
WHERE a.fill_time >= @month_start
AND a.fill_time < @month_end
AND order_status = 4
AND a.fill_type IN (1,2,3,8,9)
GROUP BY b.business_name,a.product_id;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_out_product_sto_and_sale","@time_4--@time_5",@time_4,@time_5);
-- 当月累计gmv
DROP TEMPORARY TABLE IF EXISTS fe_dm.sale_tmp;
CREATE TEMPORARY TABLE fe_dm.sale_tmp (PRIMARY KEY (business_name,product_id))
SELECT a.business_name,
       a.product_id,
       a.gmv,
       a.discount
FROM fe_dm.dm_op_area_product_mgmv a  -- fjr_area_product_mgmv 
JOIN fe_dm.dim_tmp b ON a.business_name = b.business_area AND a.product_id = b.product_id
WHERE month_id = @month_id;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_out_product_sto_and_sale","@time_5--@time_6",@time_5,@time_6);
-- 每周一更新并结存
delete from fe_dm.`dm_op_out_product_sto_and_sale` where sdate=@week_end;
INSERT INTO fe_dm.`dm_op_out_product_sto_and_sale`
(
sdate,
business_area,
product_id,
product_type,
start_sto_val,
cur_sto_val,
fill_val,
gmv,
discount
)
SELECT @week_end sdate,
       a.business_area,
       a.product_id,
       a.product_type,
       b.sto_val AS start_sto_val,
       c.sto_val AS cur_sto_val,
       d.fill_val,
       e.gmv,
       e.discount
FROM fe_dm.dim_tmp a
LEFT JOIN fe_dm.start_sto_tmp b ON a.business_area = b.business_name AND a.product_id = b.product_id
LEFT JOIN fe_dm.cur_sto_tmp c ON a.business_area = c.business_name AND a.product_id = c.product_id
LEFT JOIN fe_dm.fill_tmp d ON a.business_area = d.business_name AND a.product_id = d.product_id
LEFT JOIN fe_dm.sale_tmp e ON a.business_area = e.business_name AND a.product_id = e.product_id;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_out_product_sto_and_sale","@time_6--@time_7",@time_6,@time_7);
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_out_product_sto_and_sale',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_out_product_sto_and_sale','dm_op_out_product_sto_and_sale','朱星华');
  COMMIT;	
END