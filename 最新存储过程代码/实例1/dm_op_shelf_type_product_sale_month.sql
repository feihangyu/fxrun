CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_op_shelf_type_product_sale_month`()
BEGIN
SELECT @sdate := CURRENT_DATE,
       @sub_1 := SUBDATE(@sdate, 1),
       @month_start := SUBDATE(@sub_1, DAY(@sub_1) - 1),
       @month_id := DATE_FORMAT(@sub_1, '%Y-%m');
	   
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.`dm_op_shelf_type_product_sale_month` WHERE month_id = @month_id;	   
	   
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_type_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_type_tmp (PRIMARY KEY (shelf_id)) AS
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
ON fe_dwd.shelf_type_tmp (shelf_id);
-- 商品统计月有销售的货架数 （区域 商品id维度 货架数）
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sal_tmp;
CREATE TEMPORARY TABLE fe_dwd.sal_tmp AS
SELECT s.business_name,
       s.shelf_type,
       a.product_id,
       COUNT(DISTINCT a.shelf_id) shelfs_sal,
       SUM(amount)amount,
       SUM(gmv)gmv,
       SUM(pay_total)pay_total
FROM
  (
   SELECT shelf_id,
          product_id,
          SUM(quantity_act)amount,
          SUM(quantity_act * sale_price)gmv,
          SUM(quantity_act * sale_price) - SUM(discount_amount) pay_total
   FROM fe_dwd.dwd_pub_order_item_recent_two_month   
   WHERE pay_date >= @month_start
   AND pay_date < @sdate
   GROUP BY shelf_id,product_id
   UNION ALL
   SELECT shelf_id,
          product_id,
          SUM(amount)amount,
          SUM(total)gmv,
          SUM(pay_total)pay_total
   FROM fe_dwd.dwd_op_out_of_system_order_yht
   WHERE pay_date >= @month_start
   AND pay_date < @sdate
   AND refund_status = '无'
   GROUP BY shelf_id,product_id
   )a
JOIN fe_dwd.shelf_type_tmp s ON a.shelf_id = s.shelf_id
GROUP BY s.business_name,s.shelf_type,a.product_id;
-- 商品月有库存的货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sto_stat_tmp;
CREATE TEMPORARY TABLE fe_dwd.sto_stat_tmp AS
SELECT s.business_name,
       s.shelf_type,
       t.product_id,
       COUNT(*) shelfs_sto
FROM (SELECT shelf_id,product_id FROM feods.d_op_product_shelf_sto_month WHERE month_id = @month_id) t
JOIN fe_dwd.shelf_type_tmp s ON t.shelf_id = s.shelf_id
GROUP BY s.business_name,s.shelf_type,t.product_id;
-- 各种货架类型的月商品动销率
INSERT INTO fe_dm.dm_op_shelf_type_product_sale_month
(month_id
,business_name
,shelf_type
,product_id
,product_code2
,product_name
,seocnd_type
,sub_type
,shelfs_sal
,shelfs_sto
,amount
,gmv
,pay_total
,load_time
)
SELECT @month_id month_id,
       t.business_name,
       t.shelf_type,
       t.product_id,
       p.product_code2,
       p.product_name,
       p.second_type_name,
       p.sub_type_name,
       SUM(t.shelfs_sal) shelfs_sal,
       SUM(t.shelfs_sto) shelfs_sto,
       SUM(t.amount)amount,
       SUM(t.gmv)gmv,
       SUM(t.pay_total)pay_total,
       CURRENT_TIMESTAMP AS load_time
FROM
( SELECT  t.business_name,
          t.shelf_type,
          t.product_id,
          t.shelfs_sal,     -- 有销售货架数
          0 AS shelfs_sto,  -- 有库存货架数
          t.amount,
          t.gmv,
          t.pay_total
  FROM fe_dwd.sal_tmp t
  UNION ALL
  SELECT t.business_name,
         t.shelf_type,
         t.product_id,
         0 AS shelfs_sal,   -- 有销售货架数
         t.shelfs_sto,      -- 有库存货架数
         0 AS amount,
         0 AS gmv,
         0 AS pay_total
  FROM fe_dwd.sto_stat_tmp t
) t
JOIN fe_dwd.dwd_product_base_day_all p ON p.product_id = t.product_id
GROUP BY t.business_name,t.shelf_type,t.product_id;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_op_shelf_type_product_sale_month',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙（朱星华）@', @user, @timestamp)
  );
  COMMIT;
END