DELIMITER $$

USE `sh_process`$$

DROP PROCEDURE IF EXISTS `dm_ma_sp_plc`$$

CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_sp_plc`(IN p_sdate DATE)
BEGIN
#新增存储过程
-- =============================================
-- Author:	市场  业务方(罗晖)
-- Create date: 2020-3-19
-- Modify date: 2020-9-8
-- Description: 货架商品生命周期每日更新
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #默认前一天
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)); #周一日期
SET @sweek_end= ADDDATE(@sweek,6) ;#周日日期
# 备份数据(清空并插入备份数据)
SET @pt0=MOD(YEARWEEK(@sdate),100);
SET @pt1=MOD(YEARWEEK(SUBDATE(@sdate,7*2)),100);
SET @sql=CONCAT('alter table  fe_dm.dm_ma_sp_plc_backup  truncate partition pweek',@pt0,' ,pweek',@pt1);
PREPARE ext_str FROM @sql;
EXECUTE ext_str;
INSERT INTO fe_dm.dm_ma_sp_plc_backup
    (sdate, DETAIL_ID, SHELF_ID, PRODUCT_ID
    , fill_day_first, up_day_last, sale_day_first, stop_day, observe_days, fill_qty_first, sale_qty_alarm, stock_day_observe, sale_qty_observe
    , day_1, day_2, day_3, day_4, day_5, plc, plc_day, lats_plc, lats_plc_day, last_plc_term, last_plc_term_day)
SELECT @sdate,DETAIL_ID, SHELF_ID, PRODUCT_ID
    , fill_day_first, up_day_last, sale_day_first, stop_day, observe_days, fill_qty_first, sale_qty_alarm, stock_day_observe, sale_qty_observe
    , day_1, day_2, day_3, day_4, day_5, plc, plc_day, lats_plc, lats_plc_day, last_plc_term, last_plc_term_day
FROM fe_dm.dm_ma_sp_plc
;
#插入当天上架的货架商品
REPLACE INTO fe_dm.dm_ma_sp_plc
    ( DETAIL_ID,SHELF_ID, PRODUCT_ID
    , fill_day_first, up_day_last, fill_qty_first,sale_qty_alarm
    , plc, plc_day,day_1,lats_plc)
SELECT  a1.DETAIL_ID, a1.shelf_id, a1.product_id
    ,a1.FIRST_FILL_TIME , DATE(a2.FILL_TIME_min), IFNULL(ACTUAL_FILL_NUM_min,0), IF(ACTUAL_FILL_NUM_min BETWEEN 1 AND 4,ACTUAL_FILL_NUM_min*2,10) sale_qty_alarm
    ,1,@sdate,@sdate,0
FROM fe_dwd.dwd_shelf_product_day_all a1 #当天
STRAIGHT_JOIN fe_dwd.dwd_shelf_base_day_all a4 ON a4.shelf_id=a1.SHELF_ID  AND shelf_type NOT IN (4,9) #筛选相关货架的
STRAIGHT_JOIN fe_dm.dm_op_shelf_product_fill_last_time a2 ON a2.detail_id=a1.DETAIL_ID #当天
WHERE ((FIRST_FILL_TIME=@sdate )
        OR (FIRST_FILL_TIME IS NOT NULL AND a2.FILL_TIME_min=@sdate)
    )
;
#更新近期数据
    #更新近期数据
UPDATE fe_dm.dm_ma_sp_plc a1
LEFT JOIN  fe_dwd.dwd_shelf_product_day_all_recent_32 a2 ON a2.DETAIL_ID=a1.DETAIL_ID AND a2.sdate=@sdate #当天销售
LEFT JOIN  fe_dwd.dwd_shelf_product_day_all_recent_32 a4 ON a4.DETAIL_ID=a1.DETAIL_ID AND a4.sdate=SUBDATE(@sdate,1) #前一天期末库存
LEFT JOIN fe_dm.dm_ma_sp_stopfill a3 ON a3.DETAIL_ID=a1.DETAIL_ID AND a3.stop_status=0 #停补
SET lats_plc=plc
    ,lats_plc_day=plc_day
    ,a1.stock_day_observe=a1.stock_day_observe+IF(DATEDIFF(@sdate,day_1)<observe_days AND a4.stock_quantity>0 AND plc<5,1,0)
    ,a1.sale_qty_observe=a1.sale_qty_observe+IF(DATEDIFF(@sdate,day_1)<observe_days AND plc<5,IFNULL(a2.sal_qty_act,0),0)
    ,a1.stop_day=a3.stop_date
WHERE a1.lats_plc<5
;
    #若上架60天内有库存（含无库存有销售）天数＜7天且未完成销量阈值，观察期延长至90天
UPDATE fe_dm.dm_ma_sp_plc a1
SET observe_days=90
WHERE a1.plc=2
  AND DATEDIFF(@sdate,day_1)=59 #经历60天
  AND sale_qty_observe < sale_qty_alarm #销售数量未达标
  AND stock_day_observe<7 #有库存天数小于7
  AND observe_days=60 #观察天数为60
;
#更新生命周期
UPDATE fe_dm.dm_ma_sp_plc a1
LEFT JOIN  # 近期货架商品日均销gmv
    (SELECT a1.DETAIL_ID
        ,SUM(IF(gmv_avg>=0.5,1,0)) cnt_gmv_05
        ,SUM(IF(gmv_avg<0.25,1,0)) cnt_gmv_025
    FROM fe_dm.dm_ma_sp_salestockinfo_daily_32 a1
    STRAIGHT_JOIN fe_dm.dm_ma_sp_plc a2 ON a2.DETAIL_ID=a1.DETAIL_ID AND a2.plc IN (3,4)
    WHERE sdate IN (@sdate,SUBDATE(@sdate,7))
    GROUP BY DETAIL_ID)
    a2 ON a2.DETAIL_ID=a1.DETAIL_ID
SET a1.plc=
    CASE
        WHEN a1.stop_day IS NOT NULL THEN 5 #衰退期
        WHEN DATEDIFF(@sdate,day_1) BETWEEN 0 AND 28 AND sale_qty_observe=0
            THEN 1 #导入期
        WHEN DATEDIFF(@sdate,day_1) BETWEEN 0 AND observe_days-1
            AND sale_qty_observe BETWEEN 1 AND sale_qty_alarm-1
            THEN 2 #成长期
        WHEN lats_plc=4 AND a2.cnt_gmv_05>=1
            THEN 3 #衰退期转成熟期
        WHEN (lats_plc=3 AND DATEDIFF(@sdate,lats_plc_day)>=29 AND (a2.cnt_gmv_025>=2 OR a2.DETAIL_ID IS NULL) )
            OR (lats_plc=4 AND DATEDIFF(@sdate,lats_plc_day) BETWEEN 0 AND 30) #在衰退期未满足转换条件则待够30天
                 #之前是衰退期或者在成熟期待够28天
            THEN 4 #衰退期
        WHEN sale_qty_observe >= sale_qty_alarm AND (day_4 IS NULL OR lats_plc=3)
            THEN 3 #成长期满足条件转成熟期
        ELSE 5 END
WHERE a1.lats_plc<5
;
    #生命周期日期及上次生命周期状态
UPDATE fe_dm.dm_ma_sp_plc a1
SET day_2= IF(day_2 IS NULL AND plc IN (2,3),@sdate, day_2)
    ,day_3= IF(day_3 IS NULL AND plc=3,@sdate, day_3)
    ,day_4= IF(day_4 IS NULL AND plc=4,@sdate, day_4)
    ,day_5= IF(day_5 IS NULL AND plc=5,@sdate, day_5)
    ,last_plc_term_day=IF(plc<>lats_plc,lats_plc_day,last_plc_term_day)
    ,last_plc_term=IF(plc<>lats_plc,lats_plc,last_plc_term)
    ,plc_day=IF(plc=lats_plc,plc_day,@sdate)
WHERE lats_plc<5
;
#截存标签
#更新生命周期变动的历史数据
UPDATE fe_dm.dm_ma_sp_plc_his a1
JOIN fe_dm.dm_ma_sp_plc a2 ON a2.DETAIL_ID=a1.DETAIL_ID AND a2.plc_day=@sdate
SET a1.day_end =SUBDATE(@sdate,1)
WHERE a2.plc != a1.plc #plc变化时
    AND a1.day_end='9999-12-31'
;
    #插入新的变动记录
INSERT INTO  fe_dm.dm_ma_sp_plc_his
( DETAIL_ID, day_start, day_end, plc,if_first_day)
SELECT DETAIL_ID, @sdate day_start, '9999-12-31' day_end, plc,day_1=plc_day if_first_day
FROM fe_dm.dm_ma_sp_plc
WHERE plc_day=@sdate
;

CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_sp_plc',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user,@sdate), @stime);
END$$

DELIMITER ;