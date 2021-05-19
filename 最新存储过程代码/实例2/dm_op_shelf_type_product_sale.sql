CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_type_product_sale`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET    @week_end := SUBDATE(CURRENT_DATE,DAYOFWEEK(CURRENT_DATE) - 1),
       @add_day := ADDDATE(@week_end, 1),
       @week_start := SUBDATE(@week_end, 6);
	   
DELETE FROM fe_dm.`dm_op_shelf_type_product_sale` WHERE week_end=@week_end;	   
	   
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_type_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_type_tmp (PRIMARY KEY (shelf_id)) AS
SELECT business_name,
       shelf_id,
       shelf_name,
       type_name,
       CASE WHEN s.shelf_type IN (1,2,3,5,8) THEN '无人货架' 
            WHEN s.shelf_type = 6 THEN '智能货柜'
            WHEN s.shelf_type = 7 AND type_name LIKE '%中吉%' THEN '中吉'
            WHEN s.shelf_type = 7 AND type_name LIKE '%澳柯玛%' THEN '澳柯玛'
       ELSE '其他货架类型'
       END AS shelf_type
FROM fe_dwd.dwd_shelf_base_day_all s
WHERE shelf_type IN (1,2,3,5,6,7,8);
-- 给临时表添加索引
CREATE INDEX idx_shelf_id
ON fe_dm.shelf_type_tmp (shelf_id);
-- 商品统计周有销售的货架数 （区域 商品id维度 货架数）
DROP TEMPORARY TABLE IF EXISTS fe_dm.sal_tmp;
CREATE TEMPORARY TABLE fe_dm.sal_tmp AS
SELECT s.business_name,
       s.shelf_type,
       a.product_id,
       COUNT(DISTINCT a.shelf_id) shelfs_sal
FROM
  (
   SELECT shelf_id,product_id
   FROM fe_dwd.dwd_pub_order_item_recent_one_month
   WHERE pay_date >= @week_start
   AND pay_date < @add_day
   GROUP BY shelf_id,product_id
   UNION
   SELECT shelf_id,product_id
   FROM fe_dwd.dwd_op_out_of_system_order_yht
   WHERE pay_date >= @week_start
   AND pay_date < @add_day
   AND refund_status = '无'
   GROUP BY shelf_id,product_id
   )a
JOIN fe_dm.shelf_type_tmp s ON a.shelf_id = s.shelf_id
GROUP BY s.business_name,s.shelf_type,a.product_id;
-- 商品周有库存的货架数
DROP TEMPORARY TABLE IF EXISTS fe_dm.sto_stat_tmp;
CREATE TEMPORARY TABLE fe_dm.sto_stat_tmp AS
SELECT s.business_name,
       s.shelf_type,
       t.product_id,
       COUNT(*) shelfs_sto
FROM fe_dm.dm_op_shelf_week_product_stock_detail_tmp t
JOIN fe_dm.shelf_type_tmp s ON t.shelf_id = s.shelf_id
GROUP BY s.business_name,s.shelf_type,t.product_id;
-- 各种货架类型的周商品动销率
INSERT INTO fe_dm.dm_op_shelf_type_product_sale
(week_end
,business_name
,shelf_type
,product_id
,product_code2
,product_name
,seocnd_type
,sub_type
,shelfs_sal
,shelfs_sto
,load_time
)
SELECT @week_end week_end,
       t.business_name,
       t.shelf_type,
       t.product_id,
       p.product_code2,
       p.product_name,
       p.second_type_name,
       p.sub_type_name,
       SUM(t.shelfs_sal) shelfs_sal,
       SUM(t.shelfs_sto) shelfs_sto,
       CURRENT_TIMESTAMP AS load_time
FROM
( SELECT  t.business_name,
          t.shelf_type,
          t.product_id,
          t.shelfs_sal,  -- 有销售货架数
          0 shelfs_sto   -- 有库存货架数
  FROM fe_dm.sal_tmp t
  UNION ALL
  SELECT t.business_name,
         t.shelf_type,
         t.product_id,
         0 shelfs_sal,    -- 有销售货架数
         t.shelfs_sto     -- 有库存货架数
  FROM fe_dm.sto_stat_tmp t
) t
JOIN fe_dwd.dwd_product_base_day_all p ON p.product_id = t.product_id
GROUP BY t.business_name,t.shelf_type,t.product_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_type_product_sale',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_type_product_sale','dm_op_shelf_type_product_sale','朱星华');
  COMMIT;	
END