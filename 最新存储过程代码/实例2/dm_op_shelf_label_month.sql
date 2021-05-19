CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_label_month`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SELECT @month_id := DATE_FORMAT(SUBDATE(CURRENT_DATE,INTERVAL 1 DAY),'%Y-%m'),
       @month_start := CONCAT(@month_id,'-01'),
       @month_end := IF(CURRENT_DATE > LAST_DAY(@month_start),ADDDATE(LAST_DAY(@month_start),1),CURRENT_DATE),
       @last_month_start :=  SUBDATE(@month_start,INTERVAL 1 MONTH),
       @last_month_id := DATE_FORMAT(@last_month_start,'%Y-%m'),
       @last_month_end := ADDDATE(LAST_DAY(@last_month_start),1);
#以下为补全数据的日期设置	   
#SELECT @sdate := sdate,
#       @month_id := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 1 DAY),'%Y-%m'),
#       @month_start := CONCAT(@month_id,'-01'),
#       @month_end := IF(@sdate > LAST_DAY(@month_start),ADDDATE(LAST_DAY(@month_start),1),@sdate),
#       @last_month_start :=  SUBDATE(@month_start,INTERVAL 1 MONTH),
#       @last_month_id := DATE_FORMAT(@last_month_start,'%Y-%m'),
#       @last_month_end := ADDDATE(LAST_DAY(@last_month_start),1);
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
-- 货架关联明细
DROP TEMPORARY TABLE IF EXISTS fe_dm.bind_item_tmp;
CREATE TEMPORARY TABLE fe_dm.bind_item_tmp (PRIMARY KEY (main_shelf_id,shelf_id))
SELECT IFNULL(a.main_shelf_id,a.shelf_id)main_shelf_id,
       b.shelf_type main_shelf_type,-- 主货架类型
       a.relation_flag,
       a.shelf_id,
       a.shelf_type_desc bind_shelf_type,-- 关联货架类型
       CASE WHEN a.shelf_type IN (1,3) THEN 1 -- 货架(四层、五层)
       ELSE a.shelf_type END AS shelf_type
FROM fe_dwd.dwd_shelf_base_day_all a
LEFT JOIN fe_dwd.dwd_shelf_base_day_all b ON IFNULL(a.main_shelf_id,a.shelf_id) = b.shelf_id
ORDER BY main_shelf_id ASC;
-- 货架关联类型(四层或五层关联冰箱是一货一冰，其他关联类型不算此种类型)
DROP TEMPORARY TABLE IF EXISTS fe_dm.bind_type_tmp;
CREATE TEMPORARY TABLE fe_dm.bind_type_tmp (PRIMARY KEY(main_shelf_id))
SELECT main_shelf_id,
       CASE WHEN shelfs = 1 AND fridges = 1 AND SUM(other_shelf) = 0 THEN '一货一冰'
            WHEN shelfs = 1 AND fridges = 0 AND SUM(other_shelf) = 0 THEN '单货架'
            WHEN shelfs = 0 AND fridges = 1 AND SUM(other_shelf) = 0 THEN '单冰箱'
            WHEN (shelfs + fridges + other_shelf) >= 2 THEN '其他关联类型'
       ELSE '其他单货架类型' END AS bind_type
FROM
(
SELECT main_shelf_id,
       COUNT(CASE WHEN shelf_type = 1 THEN shelf_id END)shelfs,
       COUNT(CASE WHEN shelf_type = 2 THEN shelf_id END)fridges,
       COUNT(CASE WHEN shelf_type IN(3,4,5,6,7,8,9) THEN shelf_id END)other_shelf
FROM fe_dm.bind_item_tmp
GROUP BY main_shelf_id
)a
GROUP BY main_shelf_id;
-- 货架信息      
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.activate_time,
       a.revoke_time,
       IF(a.shelf_type_desc = '虚拟货架' AND a.type_name LIKE '%柜%','智能柜',shelf_type_desc)shelf_type,
       a.shelf_status_desc shelf_status,
       i.item_name revoke_status,
       inner_flag,
       manager_type,
       CASE WHEN activate_time < @month_start AND (revoke_time >= @month_end OR revoke_time IS NULL) THEN '留存' -- 存量：统计月1日前激活且当月未撤架
            WHEN activate_time >= @month_start AND (revoke_time >= @month_end OR revoke_time IS NULL) THEN '新装' -- 统计月新装：统计月激活且当月未撤架
            WHEN activate_time >= @month_start AND (revoke_time >= @month_start AND revoke_time < @month_end) THEN '新装撤架' -- 统计月激活且当月撤架
            WHEN revoke_time >= @month_start AND revoke_time < @month_end THEN '撤架' -- 撤架时间在统计月内
       ELSE NULL END AS operate_type,-- 存量或增量判定
       b.days, -- 货架关闭天数
       IF(b.days >= 5,1,0)close_over5, -- 关闭天数是否>=5
       d.bind_type,-- 货架关联类型
       IFNULL(a.main_shelf_id,a.shelf_id)main_shelf_id,-- 主货架id，如果为空则为其本身
       -- IF(activate_time <= @month_start,@month_start,DATE(activate_time))start_date,
       -- IF(revoke_time IS NOT NULL,IF(revoke_time >= @month_end,@month_end,ADDDATE(DATE(revoke_time),1)),@month_end)end_date,
       -- IF(activate_time <= @last_month_start,@last_month_start,DATE(activate_time))last_start_date,
       -- IF(revoke_time IS NOT NULL,IF(revoke_time >= @last_month_end,@last_month_end,ADDDATE(DATE(revoke_time),1)),@last_month_end)last_end_date,
       DATEDIFF(IF(revoke_time IS NOT NULL,IF(revoke_time >= @month_end,@month_end,ADDDATE(revoke_time,1)),@month_end),IF(activate_time <= @month_start,@month_start,activate_time))active_natural_days,-- 本月运营自然日
       (SELECT COUNT(CASE WHEN if_work_day = 1 THEN sdate END)FROM fe_dwd.dwd_pub_work_day WHERE sdate >= IF(activate_time <= @month_start,@month_start,DATE(activate_time)) AND sdate < IF(revoke_time IS NOT NULL,IF(revoke_time >= @month_end,@month_end,ADDDATE(DATE(revoke_time),1)),@month_end))work_day,-- 本月运营工作日
       (SELECT COUNT(CASE WHEN if_work_day = 1 THEN sdate END) + 0.5*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 7 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 1 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN holiday != '' THEN sdate END)work_day FROM fe_dwd.dwd_pub_work_day WHERE sdate >= IF(activate_time <= @month_start,@month_start,DATE(activate_time)) AND sdate < IF(revoke_time IS NOT NULL,IF(revoke_time >= @month_end,@month_end,ADDDATE(DATE(revoke_time),1)),@month_end))work_convert_day, -- 本月运营折算工作日
       (SELECT COUNT(CASE WHEN if_work_day = 1 THEN sdate END) + 0.5*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 7 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 1 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN holiday != '' THEN sdate END)work_day FROM fe_dwd.dwd_pub_work_day WHERE sdate >= IF(activate_time <= @last_month_start,@last_month_start,DATE(activate_time)) AND sdate < IF(revoke_time IS NOT NULL,IF(revoke_time >= @last_month_end,@last_month_end,ADDDATE(DATE(revoke_time),1)),@last_month_end))last_work_convert_day -- 上月运营折算工作日
FROM fe_dwd.dwd_shelf_base_day_all a
LEFT JOIN fe_dm.dm_op_area_shelf_open_close_times b ON a.shelf_id = b.shelf_id AND b.month_id = @month_id   -- d_op_area_shelf_open_close_times
LEFT JOIN fe_dm.bind_type_tmp d ON IFNULL(a.main_shelf_id,a.shelf_id) = d.main_shelf_id
LEFT JOIN fe_dwd.dwd_pub_dictionary i ON a.revoke_status = i.item_value AND i.dictionary_id = 50
WHERE activate_time < @month_end 
AND (revoke_time IS NULL OR revoke_time >= @month_start); -- 激活在统计月最后一天前或撤架时间>=本月1日
-- 统计月有撤架申请记录
DROP TEMPORARY TABLE IF EXISTS fe_dm.revoke_tmp;
CREATE TEMPORARY TABLE fe_dm.revoke_tmp (PRIMARY KEY (shelf_id))
SELECT r.shelf_id,
       MAX(add_time)max_revoke_time
FROM fe_dwd.dwd_sf_shelf_revoke r   -- sf_shelf_revoke
JOIN fe_dm.shelf_tmp s ON r.shelf_id = s.shelf_id
WHERE add_time >= @month_start
AND add_time < @month_end
AND data_flag = 1
GROUP BY r.shelf_id;
-- 货架本月销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.sale_tmp;
CREATE TEMPORARY TABLE fe_dm.sale_tmp (PRIMARY KEY (shelf_id))
SELECT shelf_id,
       SUM(gmv)gmv
FROM 
(
SELECT shelf_id,
       (gmv + payment_money) gmv
FROM fe_dm.dm_shelf_mgmv -- fjr_shelf_mgmv
WHERE month_id = @month_id
UNION ALL
SELECT shelf_id,
       SUM(total)gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @month_start
AND pay_date < @month_end
AND refund_status = '无'
GROUP BY shelf_id
)a
GROUP BY shelf_id;
-- 货架上月销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.last_sale_tmp;
CREATE TEMPORARY TABLE fe_dm.last_sale_tmp (PRIMARY KEY (shelf_id))
SELECT shelf_id,
       SUM(gmv)gmv
FROM 
(
SELECT shelf_id,
       (gmv + payment_money) gmv
FROM fe_dm.dm_shelf_mgmv -- fjr_shelf_mgmv
WHERE month_id = @last_month_id
UNION ALL
SELECT shelf_id,
       SUM(total)gmv
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @last_month_start
AND pay_date < @last_month_end
AND refund_status = '无'
GROUP BY shelf_id
)a
GROUP BY shelf_id;
DELETE FROM fe_dm.dm_op_shelf_label_month WHERE month_id = @month_id;
INSERT INTO fe_dm.dm_op_shelf_label_month
(
month_id,
business_name,
shelf_id,
activate_time,
revoke_time,
shelf_type,
shelf_status,
revoke_status,
inner_flag,
manager_type,
days,
bind_type,
main_shelf_id,
active_natural_days,
work_day,
work_convert_day,
last_work_convert_day,
gmv,
gmv_last,
operate_type,
load_time
)
SELECT @month_id month_id,
       a.business_name,
       a.shelf_id,
       a.activate_time,
       a.revoke_time,
       a.shelf_type,
       a.shelf_status,
       a.revoke_status,
       a.inner_flag,
       a.manager_type,
       a.days,
       a.bind_type,
       a.main_shelf_id,
       a.active_natural_days,
       a.work_day,
       a.work_convert_day,
       a.last_work_convert_day,
       c.gmv,
       d.gmv gmv_last,
       CASE WHEN a.operate_type = '留存' AND b.max_revoke_time IS NOT NULL THEN '留存-有撤架申请'
            WHEN a.operate_type = '留存' AND a.close_over5 = 1 THEN '留存-关闭天数≥5天'
            WHEN a.operate_type = '留存' AND c.gmv IS NULL THEN '留存-全月0销'
            WHEN a.operate_type = '留存' AND c.gmv < 100 THEN '留存-月gmv<100' 
            WHEN a.operate_type = '留存' AND a.activate_time <= @last_month_start AND ((IFNULL(c.gmv,0)/a.work_convert_day) -(IFNULL(d.gmv,0)/a.last_work_convert_day))/(IFNULL(d.gmv,0)/a.last_work_convert_day) < -0.5 THEN '销售下滑超50%'  -- gmv(含补付款)/当月折算工作日对比上月下滑超过50%
            WHEN a.operate_type = '留存' THEN '普通留存'
       ELSE a.operate_type END AS operate_type,
       @timestamp AS load_time
FROM fe_dm.shelf_tmp a
LEFT JOIN fe_dm.revoke_tmp b ON a.shelf_id = b.shelf_id
LEFT JOIN fe_dm.sale_tmp c ON a.shelf_id = c.shelf_id
LEFT JOIN fe_dm.last_sale_tmp d ON a.shelf_id = d.shelf_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_label_month',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_label_month','dm_op_shelf_label_month','朱星华');
  COMMIT;	
END