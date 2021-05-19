CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_sc_preware_fill_seven_day_eight`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
 # 前置仓每日出库数据，因为出库的状态在一个补货周期内会有变化，因此每次更新10天的数据；
SET @sdate = SUBDATE(CURDATE(),1);
DELETE FROM fe_dwd.dwd_preware_outbound_daily WHERE out_date >= DATE_SUB(CURDATE(),INTERVAL 17 DAY);
INSERT INTO fe_dwd.dwd_preware_outbound_daily
(out_date,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
FNAME,
F_BGJ_POPRICE,
fill_type,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM,
send_nogroup,
shelf_fill_reason
)
SELECT
fo.adjust_send_time AS out_date,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t5.shelf_code,
t5.shelf_name,
fo.supplier_id_adjust AS warehouse_id,
t3.product_code2,
t3.product_name,
fo.product_id,
t3.fname_type,
t3.F_BGJ_POPRICE,
fo.fill_type,
fo.ACTUAL_APPLY_NUM,
fo.ACTUAL_SEND_NUM,
fo.ACTUAL_SIGN_NUM,
fo.ACTUAL_FILL_NUM,
send_nogroup,
fo.shelf_fill_reason
FROM
(SELECT CASE WHEN a.fill_type IN (9,10) THEN DATE(a.apply_time)
WHEN a.fill_type IN (1,2,8,15) AND a.send_time IS NOT NULL THEN DATE(a.send_time)
WHEN a.fill_type IN (1,2,8,15) AND a.send_time IS NULL THEN DATE(a.apply_time)
END AS 'adjust_send_time',
IF(a.fill_type = 15,a.shelf_id,a.supplier_id) AS supplier_id_adjust,
a.product_id,
a.fill_type,
SUM(a.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
SUM(a.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM,
SUM(a.ACTUAL_SIGN_NUM) AS ACTUAL_SIGN_NUM,
SUM(a.ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM,
SUM(IF(ex.shelf_fill_reason = 1 OR ISNULL(ex.shelf_fill_reason) ,a.ACTUAL_SEND_NUM,0)) AS send_nogroup,
ex.shelf_fill_reason
FROM fe_dwd.dwd_fill_day_inc_recent_two_month a
LEFT JOIN fe_dwd.dwd_sf_product_fill_order_extend ex
ON a.order_id = ex.order_id
-- AND ex.shelf_fill_reason != 1
AND ex.data_flag = 1
WHERE 
-- a.supplier_type = 9
-- AND a.fill_type IN (1,2,8,9,10,15)
-- AND a.order_status IN (2,3,4)
((a.supplier_type = 9 AND a.fill_type IN (1,2,8,9,10) AND a.order_status IN (2,3,4)) 
OR (a.supplier_type = 2 AND a.fill_type = 15 AND a.order_status = 8))
-- AND a.data_flag =1
AND a.apply_time >= DATE_SUB(CURDATE(),INTERVAL 40 DAY)
GROUP BY adjust_send_time,
supplier_id_adjust,
a.fill_type,
ex.shelf_fill_reason,
a.product_id) fo
JOIN fe_dwd.dwd_product_base_day_all t3    ##product_code2，FE码
ON fo.product_id = t3.product_id
AND fo.adjust_send_time >= DATE_SUB(CURDATE(),INTERVAL 17 DAY) AND fo.adjust_send_time < CURDATE()
JOIN fe_dwd.dwd_shelf_base_day_all t5   ##区域
ON fo.supplier_id_adjust = t5.shelf_id
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t5.business_name = w.business_area
AND t5.shelf_type = 9
AND w.to_preware = 1
;
#前置仓每周出库，同样更新最近1周的数据
DELETE FROM fe_dwd.dwd_preware_outbound_weekly WHERE week_monday >= DATE_SUB(@sdate,INTERVAL WEEKDAY(@sdate)+7 DAY); #上周及本周的数据更新；
INSERT INTO fe_dwd.dwd_preware_outbound_weekly
(week_monday,
out_week,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
FNAME,
F_BGJ_POPRICE,
fill_type,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM
)
SELECT DATE_SUB(out_date,INTERVAL WEEKDAY(out_date) DAY) AS week_monday,
CONCAT(DATE_FORMAT(out_date,'%Y-%u'),'周') AS out_week,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
FNAME,
F_BGJ_POPRICE,
fill_type,
SUM(ACTUAL_APPLY_NUM) AS week_apply,
SUM(ACTUAL_SEND_NUM) AS week_send,
SUM(ACTUAL_SIGN_NUM) AS week_sign,
SUM(ACTUAL_FILL_NUM) AS week_fill
FROM fe_dwd.dwd_preware_outbound_daily
WHERE out_date >=  DATE_SUB(@sdate,INTERVAL WEEKDAY(@sdate)+7 DAY)
GROUP BY DATE_FORMAT(out_date,'%Y-%u'),
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2
,fill_type;
#前置仓每月出库
DELETE FROM fe_dwd.dwd_preware_outbound_monthly WHERE out_month = DATE_FORMAT(@sdate,'%Y-%m');
INSERT INTO fe_dwd.dwd_preware_outbound_monthly
(out_month,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
FNAME,
F_BGJ_POPRICE,
fill_type,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM,
send_nopre,
send_nogroup,
shelf_fill_reason
)
SELECT DATE_FORMAT(out_date,'%Y-%m') AS out_month,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
FNAME,
F_BGJ_POPRICE,
fill_type,
SUM(ACTUAL_APPLY_NUM) AS week_apply,
SUM(ACTUAL_SEND_NUM) AS week_send,
SUM(ACTUAL_SIGN_NUM) AS week_send,
SUM(ACTUAL_FILL_NUM) AS week_fill,
SUM(IF(fill_type IN (1,2,8,9),ACTUAL_SEND_NUM,0)) AS send_nopre,
SUM(IF(fill_type IN (1,2,8,9) AND (shelf_fill_reason = 1 OR ISNULL(shelf_fill_reason)),ACTUAL_SEND_NUM,0)) AS send_nogroup,
shelf_fill_reason
FROM fe_dwd.dwd_preware_outbound_daily
WHERE out_date >= DATE_FORMAT(@sdate,'%Y-%m-01')
AND out_date <= LAST_DAY(@sdate)
GROUP BY DATE_FORMAT(@sdate,'%Y-%m'),
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2
,fill_type
,shelf_fill_reason
;
#前置仓每日入库数据，因为是是上架已经是流程结束，中间不会发生上架状态变化，因此只更新前一日的入库数据即可；
DELETE FROM fe_dwd.dwd_preware_fill_daily WHERE fill_date >= DATE_SUB(CURDATE(),INTERVAL 14 DAY);
INSERT INTO fe_dwd.dwd_preware_fill_daily
(fill_date,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
product_type,
sales_flag,
FNAME,
F_BGJ_POPRICE,
supplier_id,
supplier_type,
fill_type,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM
)
SELECT
fi.fill_date,
w.region_area,
w.business_area,
w.warehouse_number,
w.warehouse_name,
t4.shelf_code,
t4.shelf_name,
fi.shelf_id AS warehouse_id,
t3.product_code2,
t3.product_name,
fi.product_id,
p.product_type,
sf.sales_flag,
t3.FNAME_type,
t3.F_BGJ_POPRICE,
fi.supplier_id,
fi.supplier_type,
fi.fill_type,
fi.ACTUAL_APPLY_NUM,
fi.ACTUAL_SEND_NUM,
fi.ACTUAL_SIGN_NUM,
fi.ACTUAL_FILL_NUM
FROM
(SELECT
  DATE(a.fill_time) AS fill_date,
  a.shelf_id,
  a.product_id,
  a.fill_type,
  a.supplier_id,
  a.supplier_type,
  SUM(a.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
  SUM(a.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM,
  SUM(a.ACTUAL_SIGN_NUM) AS ACTUAL_SIGN_NUM,
  SUM(a.ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM  
FROM
  fe_dwd.dwd_fill_day_inc_recent_two_month a
  JOIN fe_dwd.dwd_shelf_base_day_all s
    ON a.shelf_id = s.shelf_id
    AND s.shelf_type = 9
    AND s.DATA_FLAG = 1
  AND a.fill_type IN (1,2,4,8,10)
  AND a.order_status IN (3,4)
  AND a.fill_time IS NOT NULL
--   AND a.data_flag =1
--   AND b.data_flag =1
  AND a.fill_time >= DATE_SUB(CURDATE(),INTERVAL 14 DAY) 
  AND a.fill_time < CURDATE()
GROUP BY 
  DATE(a.fill_time),
   a.shelf_id,
   a.supplier_id,
   a.fill_type,
   a.product_id
UNION ALL
SELECT
--  w.region_area,
--   w.business_area,
         DATE(a.fill_time) AS sdate
--         , b.ORDER_ID
        , a.shelf_id
        , a.product_id
        , a.fill_type
        , t.SOURCE_SHELF_ID AS supplier_id
        , a.supplier_type
        , -- s2.SHELF_NAME AS supplier_name,
 SUM(a.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
  SUM(a.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM,
  SUM(a.ACTUAL_SIGN_NUM) AS ACTUAL_SIGN_NUM,
  SUM(a.ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM  
--          ACTUAL_APPLY_NUM
--         , ACTUAL_SEND_NUM
--         , ACTUAL_SIGN_NUM
--         , ACTUAL_FILL_NUM
    FROM
    fe_dwd.dwd_fill_day_inc_recent_two_month a
    JOIN fe_dwd.dwd_shelf_base_day_all ds
    ON a.shelf_id = ds.shelf_id
    AND ds.shelf_type = 9
            AND a.fill_type = 12
            AND a.order_status IN (3, 4)
            AND a.fill_time IS NOT NULL
--             AND a.data_flag = 1
--             AND b.data_flag = 1
            AND a.fill_time >= DATE_SUB(CURDATE(),INTERVAL 14 DAY) 
            AND a.fill_time < CURDATE()
         JOIN fe_dwd.dwd_sf_shelf_goods_transfer t
            ON a.ORDER_ID = t.TARGET_ORDER_ID
            AND t.DATA_FLAG = 1
        JOIN fe_dwd.dwd_shelf_base_day_all s
            ON t.SOURCE_SHELF_ID = s.shelf_id
            AND s.data_flag = 1
   GROUP BY DATE(a.fill_time),
   a.shelf_id,
   t.SOURCE_SHELF_ID,
   a.fill_type,
   a.product_id
) fi
JOIN fe_dwd.dwd_product_base_day_all t3    ##product_code2，FE码
ON fi.product_id = t3.product_id
JOIN fe_dwd.dwd_shelf_base_day_all t4        ## 货架名称，编码
ON fi.shelf_id = t4.shelf_id
AND t4.data_flag =1
JOIN fe_dwd.dwd_pub_warehouse_business_area  w   ### 仓库编码
ON t4.business_name = w.business_area
AND w.to_preware = 1
LEFT JOIN fe_dwd.dwd_pub_product_dim_sserp p 
ON t4.business_name = p.business_area
AND fi.product_id = p.product_id
LEFT JOIN fe_dwd.dwd_shelf_product_day_all sf
ON fi.supplier_id = sf.shelf_id
AND fi.product_id = sf.product_id
AND fi.fill_type = 12
-- AND sf.data_flag = 1
;
# 已更新到2019年的近14天出库量（包括当天）
DELETE FROM fe_dwd.dwd_preware_outbound_forteen_day WHERE sdate = @sdate;
INSERT INTO fe_dwd.dwd_preware_outbound_forteen_day
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
fname,
F_BGJ_POPRICE,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM,
send_nopre,
send_noholiday
)
SELECT @sdate
, region_area
, business_area
, warehouse_number
, warehouse_name
, shelf_code
, shelf_name
, f2.warehouse_id
, f2.product_code2
, f2.product_name
, f2.product_id
, fname
, F_BGJ_POPRICE
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 14 DAY),f2.ACTUAL_APPLY_NUM,0)) AS ACTUAL_APPLY_NUM
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 14 DAY),f2.ACTUAL_SEND_NUM,0)) AS ACTUAL_SEND_NUM 
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 14 DAY),f2.ACTUAL_SIGN_NUM,0)) AS ACTUAL_SIGN_NUM
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 14 DAY),f2.ACTUAL_FILL_NUM,0)) AS ACTUAL_FILL_NUM
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 14 DAY) AND fill_type IN (1,2,8,9),f2.ACTUAL_SEND_NUM,0)) AS send_nopre
,SUM(IF(d.sdate IS NOT NULL AND fill_type IN (1,2,8,9),f2.send_nogroup,0)) AS send_noholiday
FROM fe_dwd.dwd_preware_outbound_daily f2 
LEFT JOIN 
(SELECT t.sdate, @i :=  @i +1 AS i
FROM fe_dwd.dwd_pub_work_day t,
(SELECT @i := 0) a
WHERE t.sdate >= DATE_SUB(CURDATE(), INTERVAL 21 DAY)
AND t.sdate < CURDATE()
AND t.holiday = ''
AND @i < 14
ORDER BY t.sdate DESC
) d
ON f2.out_date = d.sdate
AND d.i <= 14
WHERE f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 21 DAY) AND f2.out_date < CURDATE()
GROUP BY f2.warehouse_id,f2.product_id
;
#近7天出库
DELETE FROM fe_dm.dm_sc_preware_outbound_seven_day WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_outbound_seven_day
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
fname,
F_BGJ_POPRICE,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM,
send_nopre,
send_noholiday
)
SELECT @sdate
, region_area
, business_area
, warehouse_number
, warehouse_name
, shelf_code
, shelf_name
, f2.warehouse_id
, f2.product_code2
, f2.product_name
, f2.product_id
, fname
, F_BGJ_POPRICE
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 7 DAY),f2.ACTUAL_APPLY_NUM,0)) AS ACTUAL_APPLY_NUM
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 7 DAY),f2.ACTUAL_SEND_NUM,0)) AS ACTUAL_SEND_NUM 
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 7 DAY),f2.ACTUAL_SIGN_NUM,0)) AS ACTUAL_SIGN_NUM
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 7 DAY),f2.ACTUAL_FILL_NUM,0)) AS ACTUAL_FILL_NUM
,SUM(IF(f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 7 DAY) AND fill_type IN (1,2,8,9),f2.ACTUAL_SEND_NUM,0)) AS send_nopre
,SUM(IF(d.sdate IS NOT NULL AND fill_type IN (1,2,8,9),f2.send_nogroup,0)) AS send_noholiday
FROM fe_dwd.dwd_preware_outbound_daily f2 
LEFT JOIN 
(SELECT t.sdate, @i :=  @i +1 AS i
FROM fe_dwd.dwd_pub_work_day t,
(SELECT @i := 0) a
WHERE t.sdate >= DATE_SUB(CURDATE(), INTERVAL 21 DAY)
AND t.sdate < CURDATE()
AND t.holiday = ''
AND @i < 7
ORDER BY t.sdate DESC
) d
ON f2.out_date = d.sdate
AND d.i <= 7
WHERE f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 16 DAY) AND f2.out_date < CURDATE()
GROUP BY f2.warehouse_id,f2.product_id
;
# 近3天出库
DELETE FROM fe_dm.dm_sc_preware_outbound_three_day WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_outbound_three_day
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
fname,
F_BGJ_POPRICE,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM,
send_nopre
)
SELECT @sdate AS sdate,region_area,business_area,warehouse_number,warehouse_name,shelf_code,shelf_name,f2.warehouse_id,
f2.product_code2,f2.product_name,f2.product_id,fname,F_BGJ_POPRICE,
SUM(f2.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
SUM(f2.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM,
SUM(f2.ACTUAL_SIGN_NUM) AS ACTUAL_SIGN_NUM,
SUM(f2.ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM,
SUM(IF(fill_type IN (1,2,8,9),ACTUAL_SEND_NUM,0)) AS send_nopre
FROM fe_dwd.dwd_preware_outbound_daily f2
WHERE f2.out_date >= DATE_SUB(CURDATE(),INTERVAL 3 DAY) AND f2.out_date < CURDATE()
GROUP BY f2.warehouse_id,f2.product_id; 
#近7天入库
DELETE FROM fe_dm.dm_sc_preware_fill_seven_day WHERE sdate = @sdate;
INSERT INTO fe_dm.dm_sc_preware_fill_seven_day
(sdate,
region_area,
business_area,
warehouse_number,
warehouse_name,
shelf_code,
shelf_name,
warehouse_id,
product_code2,
product_name,
product_id,
fname,
F_BGJ_POPRICE,
ACTUAL_APPLY_NUM,
ACTUAL_SEND_NUM,
ACTUAL_SIGN_NUM,
ACTUAL_FILL_NUM,
fill_nopre
)
SELECT @sdate AS sdate,region_area,business_area,warehouse_number,warehouse_name,shelf_code,shelf_name,f2.warehouse_id,
f2.product_code2,f2.product_name,f2.product_id,fname,F_BGJ_POPRICE,
SUM(f2.ACTUAL_APPLY_NUM) AS ACTUAL_APPLY_NUM,
SUM(f2.ACTUAL_SEND_NUM) AS ACTUAL_SEND_NUM,
SUM(f2.ACTUAL_SIGN_NUM) AS ACTUAL_SIGN_NUM,
SUM(f2.ACTUAL_FILL_NUM) AS ACTUAL_FILL_NUM,
SUM(IF(fill_type IN (1,2,4,8,12),ACTUAL_FILL_NUM,0)) AS send_nopre
FROM fe_dwd.dwd_preware_fill_daily f2
WHERE f2.fill_date >= DATE_SUB(CURDATE(),INTERVAL 7 DAY) AND f2.fill_date < CURDATE()
GROUP BY f2.warehouse_id,f2.product_id; 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_sc_preware_fill_seven_day_eight',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('吴婷@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_en_order_item_60','dm_sc_preware_fill_seven_day_eight','吴婷');
COMMIT;
    END