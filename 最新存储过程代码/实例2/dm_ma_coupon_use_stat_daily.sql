CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_coupon_use_stat_daily`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate=CURRENT_DATE;
DELETE FROM fe_dm.`dm_ma_coupon_use_stat_daily` WHERE sdate>=SUBDATE(CURDATE(),15) OR sdate<SUBDATE(CURDATE(),90) ;
INSERT INTO fe_dm.`dm_ma_coupon_use_stat_daily`
    (sdate, coupon_id, coupon_name, coupon_usage, cost_dept, business_type, discount_type, reach_amount, discount_amount, discount, use_num)
SELECT DATE(a2.used_time) used_date,a1.coupon_id,a1.coupon_name,a1.coupon_usage
    ,CASE  a1.cost_dept WHEN 1 THEN '市场组' WHEN 2 THEN '运营组' WHEN 3 THEN '采购组' WHEN 4 THEN '大客户组' WHEN 5 THEN 'BD' WHEN 6 THEN '经规组'ELSE '其他' END cost_dept
    ,CASE a1.business_type WHEN 1 THEN '优惠券推送' WHEN 2 THEN '活动推送' WHEN 3 THEN '商品促销' WHEN 4	THEN '新品上架' ELSE '其他'END business_type
    ,CASE a1.discount_type WHEN 1 THEN '满减' WHEN  2 THEN '立减' WHEN 3 THEN '折扣' ELSE '其他' END discount_type
    ,a1.reach_amount,IF(a1.discount_type IN (1,2),a1.discount_amount,NULL) discount_amount,IF(a1.discount_type=3,a1.discount_amount,NULL) discount
    ,SUM(1) use_num
FROM fe_dwd.dwd_sf_coupon_model a1
JOIN fe_dwd.dwd_sf_coupon_use a2 ON a1.coupon_id=a2.coupon_id AND a2.data_flag=1
WHERE a2.used_time >=SUBDATE(@sdate,15) AND a2.used_time<@sdate AND a1.data_flag=1
GROUP BY used_date,a1.coupon_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_coupon_use_stat_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_coupon_use_stat_daily','dm_ma_coupon_use_stat_daily','纪伟铨');
END