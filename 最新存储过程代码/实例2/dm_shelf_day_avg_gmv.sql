CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_shelf_day_avg_gmv`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @month_id := DATE_FORMAT(SUBDATE(CURRENT_DATE,1),'%Y-%m');
SET @month_first_day := CONCAT(@month_id, '-01');
SET @month_last_day := IF(CURRENT_DATE > LAST_DAY(@month_first_day),ADDDATE(LAST_DAY(@month_first_day),1),CURRENT_DATE);
DELETE FROM fe_dm.dm_shelf_day_avg_gmv
WHERE month_id = @month_id;
INSERT INTO fe_dm.dm_shelf_day_avg_gmv
SELECT @month_id,
       s.business_name,
       s.shelf_id,
       di.item_name shelf_type,
       de.item_name shelf_status,
       DATE_FORMAT(s.activate_time,'%Y-%m-%d') activate_time,
       DATE_FORMAT(s.revoke_time,'%Y-%m-%d') revoke_time,
       IFNULL(after_pay.payment_money,0) after_pay,   
       IFNULL(sale.gmv,0) gmv,                        
       IFNULL(after_pay.payment_money,0) + IFNULL(sale.gmv,0) total, -- 补付款+gmv
       (SELECT COUNT(CASE WHEN if_work_day = 1 THEN sdate END) + 0.5*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 7 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 1 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN holiday != '' THEN sdate END)work_day FROM fe_dwd.dwd_pub_work_day WHERE sdate >= @month_first_day AND sdate < @month_last_day) days,
       (IFNULL(after_pay.payment_money,0) + IFNULL(sale.gmv,0)) / (SELECT COUNT(CASE WHEN if_work_day = 1 THEN sdate END) + 0.5*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 7 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN if_work_day = 0 AND DAYOFWEEK(sdate) = 1 AND holiday = '' THEN sdate END) + 0.4*COUNT(CASE WHEN holiday != '' THEN sdate END)work_day FROM fe_dwd.dwd_pub_work_day WHERE sdate >= @month_first_day AND sdate < @month_last_day) day_gmv,
   CURRENT_TIMESTAMP() AS load_time
FROM fe_dwd.dwd_shelf_base_day_all s
LEFT JOIN fe_dwd.dwd_pub_dictionary di ON s.shelf_type = di.item_value AND di.dictionary_id = '8'
LEFT JOIN fe_dwd.dwd_pub_dictionary de ON s.shelf_status = de.item_value AND de.dictionary_id = '9'
LEFT JOIN -- 货架补付款
(SELECT shelf_id,
        SUM(IFNULL(AFTER_PAYMENT_MONEY,0)) payment_money
FROM fe_dwd.dwd_shelf_day_his  -- fjr_shelf_dgmv
WHERE sdate >= @month_first_day
AND sdate <  @month_last_day
GROUP BY shelf_id
) after_pay ON s.shelf_id = after_pay.shelf_id
LEFT JOIN -- 剔除大额订单的货架GMV
(SELECT a.shelf_id,
        SUM(a.gmv) gmv
FROM
(
SELECT shelf_id,
       order_id,
       SUM(quantity_act * sale_price) gmv   
FROM fe_dwd.dwd_pub_order_item_recent_two_month
WHERE pay_date >= @month_first_day
AND pay_date < @month_last_day
GROUP BY shelf_id,order_id
HAVING gmv < 100
)a
GROUP BY a.shelf_id
) sale ON sale.shelf_id = s.shelf_id
WHERE s.shelf_type IN (1,2,3,5,6,7)  
AND s.activate_time <= @month_first_day
AND s.shelf_status IN (2,3,5)
AND (s.revoke_time IS NULL OR s.revoke_time > LAST_DAY(@month_first_day));
    
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_shelf_day_avg_gmv',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('唐进（朱星华）@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_day_avg_gmv','dm_shelf_day_avg_gmv','朱星华');
  COMMIT;	
END