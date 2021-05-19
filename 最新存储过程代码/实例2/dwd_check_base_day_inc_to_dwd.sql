CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_check_base_day_inc_to_dwd`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
-- 盘点宽表数据从fe_temp 到 fe_dwd 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_check_base_day_inc_tmp1;
CREATE TEMPORARY TABLE fe_dwd.dwd_check_base_day_inc_tmp1 AS
SELECT a.check_id,a.shelf_id,a.product_id    -- 取盘点id 货架id 商品id ,三个组合确定一条记录 
FROM 
fe_temp.dwd_check_base_day_inc b -- 小表   小表要放在前面作为驱动表，加快速度
 JOIN fe_dwd.dwd_check_base_day_inc a   -- 大表 
    ON a.check_id = b.check_id AND a.shelf_id=b.shelf_id AND a.product_id=b.product_id
    AND b.load_time >= CURRENT_DATE ;   -- 修复数据，从02-27号开始：此处需要改为 load_time>=subdate(current_date,interval x day) and load_time<subdate(current_date,interval x-1 day)
CREATE INDEX idx_check_shelf_product_id
ON fe_dwd.dwd_check_base_day_inc_tmp1(check_id,shelf_id,product_id);
DELETE a.* FROM fe_dwd.dwd_check_base_day_inc a   -- 先删除共同的部分  按照订单号删除即可
JOIN  fe_dwd.dwd_check_base_day_inc_tmp1  b
    ON a.check_id = b.check_id AND a.shelf_id=b.shelf_id AND a.product_id=b.product_id ;
INSERT INTO fe_dwd.dwd_check_base_day_inc 
SELECT *
FROM fe_temp.dwd_check_base_day_inc 
WHERE load_time >= CURRENT_DATE ;   -- 修复数据，从02-27号开始：此处需要改为 load_time>=subdate(current_date,interval 3 day) and load_time<subdate(current_date,interval 2 day)
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_check_base_day_inc_to_dwd',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('李世龙@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_check_base_day_inc','dwd_check_base_day_inc_to_dwd','李世龙');
  COMMIT;	
END