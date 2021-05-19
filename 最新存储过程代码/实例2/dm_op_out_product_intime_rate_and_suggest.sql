CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_out_product_intime_rate_and_suggest`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := CURRENT_DATE;   #当天
SET @sdate1 := SUBDATE(CURRENT_DATE,INTERVAL 1 DAY);  #前一天
SET @week_end := SUBDATE(@sdate,weekday(@sdate)+1);   #每周日
SET @week_start := SUBDATE(@sdate,weekday(@sdate));   #每周一
SET @y_m := DATE_FORMAT(@sdate,'%Y-%m');
SET @month_start := CONCAT(@y_m,'-01');
SET  @sub_8week := SUBDATE(@sdate,INTERVAL 8 WEEK);
-- 上月最新清单版本号1
SELECT @version_id1 := t.version_id
FROM fe_dwd.dwd_op_dim_date t
WHERE t.sdate <= SUBDATE(@month_start,1)
AND t.edate > SUBDATE(@month_start,1);
-- 统计月最新清单版本号2
SELECT @version_id2 := t.version_id
FROM fe_dwd.dwd_op_dim_date t
WHERE t.sdate <= @sdate
AND t.edate > @sdate;
-- 地区商品可补货货架占比(不含自贩机)
DROP TEMPORARY TABLE IF EXISTS fe_dm.kb_tmp;
CREATE TEMPORARY TABLE fe_dm.kb_tmp AS
SELECT b.business_name,
       a.product_id,
       IFNULL(COUNT(CASE WHEN a.shelf_fill_flag = 1 THEN a.shelf_id END) / c.active_shelf,0) AS kb_rate
FROM fe_dwd.dwd_shelf_product_day_all a
JOIN fe_dwd.dwd_shelf_base_day_all b ON a.shelf_id = b.shelf_id AND b.shelf_status = 2 AND b.revoke_status = 1 AND b.shelf_type != 7
LEFT JOIN
(SELECT business_name,
        COUNT(shelf_id)active_shelf
FROM fe_dwd.dwd_shelf_base_day_all
WHERE shelf_status = 2
AND revoke_status = 1
AND shelf_type != 7 
GROUP BY business_name
)c ON b.business_name = c.business_name
GROUP BY b.business_name,a.product_id;
-- 前8周每日前置仓+大仓库存
DROP TEMPORARY TABLE IF EXISTS fe_dm.day_stock_product_tmp;
CREATE TEMPORARY TABLE fe_dm.day_stock_product_tmp AS
SELECT stock_date,
       business_area,
       product_bar,
       SUM(stock1+stock2) stock
FROM
(-- 每日大仓库存
SELECT DATE(p.FPRODUCEDATE)stock_date,
       p.business_area,
       p.product_bar,
       IFNULL(SUM(p.QUALITYQTY),0)stock1,
       0 AS stock2
FROM fe_dwd.dwd_pj_outstock2_day p
WHERE p.FPRODUCEDATE >= @sub_8week
AND p.FPRODUCEDATE < @sdate
GROUP BY DATE(p.FPRODUCEDATE),p.business_area,p.product_bar
UNION ALL 
-- 每日前置仓库存
SELECT DATE(d.check_date)stock_date,
       d.business_area,
       d.product_code2,
       0 AS stock1,
       SUM(d.available_stock)stock2
FROM fe_dm.dm_prewarehouse_stock_detail d
WHERE d.check_date >= @sub_8week
AND d.check_date < @sdate
GROUP BY DATE(d.check_date),d.business_area,d.product_code2
)a
GROUP BY stock_date,business_area,product_bar;
-- 前8周有库存天数
DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_product_tmp;
CREATE TEMPORARY TABLE fe_dm.stock_product_tmp AS
SELECT business_area,
       p.product_id,
       COUNT(DISTINCT CASE WHEN stock > 0 THEN stock_date END)stock_days
FROM fe_dm.day_stock_product_tmp t
JOIN fe_dwd.dwd_product_base_day_all p ON t.product_bar = p.product_code2
GROUP BY business_area,p.product_id;
-- 连续8周被标记为“非常不好卖”或“难卖”的商品
DROP TEMPORARY TABLE IF EXISTS fe_dm.hard_sell_tmp;
CREATE TEMPORARY TABLE fe_dm.hard_sell_tmp AS
SELECT f.business_area,
       f.product_id,
       '不好卖' AS sale_level,
       COUNT(sdate)amount
FROM fe_dm.dm_area_product_sale_flag f
JOIN fe_dwd.dwd_product_base_day_all p ON f.product_id = p.product_id
WHERE f.sdate >= @sub_8week
AND f.sdate < @sdate
AND !ISNULL(f.business_area)
AND f.sale_level IN('非常不好卖','难卖')
AND p.sub_type_id NOT IN(15,43) -- 剔除口香糖、咖啡
GROUP BY f.business_area,f.product_id
HAVING amount >= 8;
-- 上月原有品
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_bad_product;
CREATE TEMPORARY TABLE fe_dm.area_bad_product AS
SELECT h.business_area,
       h.product_id,
       h.product_fe,
       h.product_name,
       IF(s.sale_level IS NOT NULL,1,0)is_hard_sell,
       h.product_type product_type_last
FROM fe_dwd.dwd_pub_product_dim_sserp_his h
LEFT JOIN fe_dm.hard_sell_tmp s ON h.business_area = s.business_area AND h.product_id = s.product_id
WHERE h.version = @version_id1
AND h.product_type = '原有'; 
-- 地区商品近8周gmv及排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.product_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.product_rank_tmp AS
SELECT a.business_area,
       a.product_id,
       a.is_hard_sell,
       a.gmv,
       a.product_type_last,
       @rank := IF(@city = business_area,@rank + 1,1) rank,
       @city := business_area
FROM
(
SELECT p.business_area,
       p.product_id,
       p.product_fe,
       p.product_name,
       p.is_hard_sell,
       sale.gmv,
       p.product_type_last
FROM fe_dm.area_bad_product p
LEFT JOIN
(SELECT business_name,
        product_id,
        SUM(gmv)gmv
FROM fe_dm.dm_area_product_dgmv
WHERE sdate >= @sub_8week 
AND sdate < @sdate
GROUP BY business_name,product_id
)sale ON p.business_area = sale.business_name AND p.product_id =sale.product_id
ORDER BY p.business_area,gmv DESC
)a,(SELECT @rank := 0,@city := NULL)r;
-- 地区最大排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.area_rank_tmp AS
SELECT business_area,
       MAX(rank)max_rank
FROM fe_dm.product_rank_tmp
GROUP BY business_area;
-- 淘汰及时率明细  每日更新，每月1日截存上月数据。
DELETE FROM fe_dm.dm_op_out_product_intime_rate WHERE month_id = @y_m;
INSERT INTO fe_dm.dm_op_out_product_intime_rate
(`month_id`
,`business_area`
,`product_id`
,`kb_rate`
,`stock_days`
,`is_hard_sell`
,`gmv`
,`rank`
,`vs_version`
,`product_type_last`
,`product_type_now`
,`is_last`
,`should_out`
,`load_time`
)
SELECT @y_m month_id,
       h.business_area,
       h.product_id,
       k.kb_rate,
       p.stock_days,
       h.is_hard_sell,
       h.gmv,
       h.rank,
       CONCAT(@version_id1,'vs',@version_id2)vs_version,
       h.product_type_last, -- 上月的商品类型
       h2.product_type product_type_now,-- 本月的商品类型
       IF(h.rank >= ((9/10) * a.max_rank),1,0)is_last,-- 是否排名地区后1/10
       IF(p.stock_days >= 28 AND h.is_hard_sell = 1 AND h.rank >= ((9/10) * a.max_rank) AND k.kb_rate >= 0.6,1,0)should_out, -- 连续8周难卖且gmv排名地区后1/10的应该淘汰
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.product_rank_tmp h
LEFT JOIN fe_dm.stock_product_tmp p ON h.business_area = p.business_area AND h.product_id = p.product_id
LEFT JOIN fe_dm.kb_tmp k ON h.business_area = k.business_name AND h.product_id = k.product_id
LEFT JOIN fe_dm.area_rank_tmp a ON h.business_area = a.business_area
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp_his h2 ON h.business_area = h2.business_area AND h.product_id = h2.product_id AND h2.version = @version_id2
ORDER BY h.business_area,h.gmv DESC;
-- 当前原有品近8周gmv排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.current_normal_product_tmp;
CREATE TEMPORARY TABLE fe_dm.current_normal_product_tmp AS
SELECT a.version,
       a.business_area,
       a.product_id,
       a.is_hard_sell,
       a.gmv,
       a.product_type,
       @rank := IF(@city = business_area,@rank + 1,1) rank,
       @city := business_area
FROM
(
SELECT h.version,
       h.business_area,
       h.product_id,
       IF(s.sale_level IS NOT NULL,1,0)is_hard_sell,
       h.product_type,
       sa.gmv
FROM fe_dwd.dwd_pub_product_dim_sserp h
LEFT JOIN fe_dm.hard_sell_tmp s ON h.business_area = s.business_area AND h.product_id = s.product_id
LEFT JOIN
(SELECT business_name,
        product_id,
        SUM(gmv)gmv
FROM fe_dm.dm_area_product_dgmv
WHERE sdate >= @sub_8week 
AND sdate < @sdate
GROUP BY business_name,product_id
)sa ON h.business_area = sa.business_name AND h.product_id =sa.product_id
WHERE h.product_type = '原有'
ORDER BY h.business_area,gmv DESC
)a,(SELECT @rank := 0,@city := NULL)r;
-- 当前原有品地区最大排名
DROP TEMPORARY TABLE IF EXISTS fe_dm.current_max_rank_tmp;
CREATE TEMPORARY TABLE fe_dm.current_max_rank_tmp AS
SELECT business_area,
       MAX(rank)max_rank
FROM fe_dm.current_normal_product_tmp
GROUP BY business_area;
-- 当前原有品建议淘汰清单 每日更新，每周一截存上周数据
if weekday(@sdate)=0 then  -- 周一 则结存上周的数据，日期为上周日，并且删除上周一到上周日的数据
DELETE FROM fe_dm.dm_op_out_product_suggest WHERE sdate >= subdate(@sdate,interval 7 day) or sdate = @week_end ;
INSERT INTO fe_dm.dm_op_out_product_suggest
(`sdate`
,`version`
,`business_area`
,`product_id`
,`kb_rate`
,`stock_days`
,`is_hard_sell`
,`gmv`
,`rank`
,`product_type`
,`is_last`
,`should_out`
,`load_time`
)
SELECT @week_end sdate,
       h.version,
       h.business_area,
       h.product_id,
       k.kb_rate,
       p.stock_days,
       h.is_hard_sell,
       h.gmv,
       h.rank,
       h.product_type,
       IF(h.rank >= ((9/10) * a.max_rank),1,0)is_last,-- 是否排名地区后1/10
       IF(p.stock_days >= 28 AND h.is_hard_sell = 1 AND h.rank >= ((9/10) * a.max_rank) AND k.kb_rate >= 0.6,1,0)should_out,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.current_normal_product_tmp h
LEFT JOIN fe_dm.stock_product_tmp p ON h.business_area = p.business_area AND h.product_id = p.product_id
LEFT JOIN fe_dm.kb_tmp k ON h.business_area = k.business_name AND h.product_id = k.product_id
LEFT JOIN fe_dm.current_max_rank_tmp a ON h.business_area = a.business_area
ORDER BY h.business_area,h.gmv DESC; 
else -- 非周一，则每天保留前一天的数据
DELETE FROM fe_dm.dm_op_out_product_suggest WHERE sdate >= @week_start ;
INSERT INTO fe_dm.dm_op_out_product_suggest
(`sdate`
,`version`
,`business_area`
,`product_id`
,`kb_rate`
,`stock_days`
,`is_hard_sell`
,`gmv`
,`rank`
,`product_type`
,`is_last`
,`should_out`
,`load_time`
)
SELECT @sdate1 sdate,
       h.version,
       h.business_area,
       h.product_id,
       k.kb_rate,
       p.stock_days,
       h.is_hard_sell,
       h.gmv,
       h.rank,
       h.product_type,
       IF(h.rank >= ((9/10) * a.max_rank),1,0)is_last,-- 是否排名地区后1/10
       IF(p.stock_days >= 28 AND h.is_hard_sell = 1 AND h.rank >= ((9/10) * a.max_rank) AND k.kb_rate >= 0.6,1,0)should_out,
       CURRENT_TIMESTAMP AS load_time
FROM fe_dm.current_normal_product_tmp h
LEFT JOIN fe_dm.stock_product_tmp p ON h.business_area = p.business_area AND h.product_id = p.product_id
LEFT JOIN fe_dm.kb_tmp k ON h.business_area = k.business_name AND h.product_id = k.product_id
LEFT JOIN fe_dm.current_max_rank_tmp a ON h.business_area = a.business_area
ORDER BY h.business_area,h.gmv DESC; 
end if;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_out_product_intime_rate_and_suggest',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华（唐进）@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_out_product_intime_rate','dm_op_out_product_intime_rate_and_suggest','朱星华（唐进）');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_out_product_suggest','dm_op_out_product_intime_rate_and_suggest','朱星华（唐进）');
 
END