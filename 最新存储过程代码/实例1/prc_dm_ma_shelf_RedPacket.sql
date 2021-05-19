CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_shelf_RedPacket`(IN p_date DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @date0=p_date; #目前固定每周五跑周四跑数据
SET @date1 = ADDDATE(@date0,1)
    ,@week_monday= SUBDATE(@date0,IF(DAYOFWEEK(@date0)=1,7,DAYOFWEEK(@date0)-1)-1 )   ;
#删除历史数据
DELETE FROM feods.dm_ma_shelfRedPacket_shelf WHERE (sdate>=@date0 AND sdate<@date1) OR sdate<SUBDATE(@date0,7*4);
DELETE FROM feods.dm_ma_shelfRedPacket_activity WHERE (sdate>=@date0 AND sdate<@date1) OR sdate<SUBDATE(@date0,7*4);
DELETE FROM feods.dm_ma_shelfRedPacket_activity_compare3week WHERE (actiyity_date>=@date0 AND actiyity_date<@date1) OR actiyity_date<SUBDATE(@date0,7*4);
#创建临时数据
DROP TEMPORARY TABLE IF EXISTS feods.tmp_activity_scope_shelf;   # 定向货架抢红包货架ID及活动相关信息
CREATE TEMPORARY TABLE feods.tmp_activity_scope_shelf(INDEX(shelf_id)) AS
    SELECT activity_id,activity_name,platform,activity_type
        ,a1.start_time,a1.end_time,a2.shelf_id
        ,a2.shelf_scope_id
        ,SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(config_json,'taskTarget',-1),',',1),':',-1)+0 taskTarget
        ,SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(config_json,'taskType',-1),',',1),':',-1)+0  tasktype
    FROM fe_activity.sf_activity a1
    JOIN fe.sf_shelf_scope_detail a2
        ON a2.shelf_scope_id=SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(
                SUBSTRING_INDEX(SUBSTRING_INDEX(config_json,'shelfScopeIds',-1),',',1),':',-1),'\"',-2),'\"',1) #这个不固定可能会变动
               AND a2.data_flag=1
    WHERE a1.end_time>=@date0 AND  a1.end_time<@date1
        AND platform=1 AND activity_type=9 AND a1.data_flag=1;
DROP TEMPORARY TABLE IF EXISTS feods.tmp_user_shelf_order; # 用户当天购买货架
CREATE TEMPORARY TABLE feods.tmp_user_shelf_order(INDEX(user_id)) AS
    SELECT user_id,a1.shelf_id
    FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
    JOIN feods.tmp_activity_scope_shelf a2 ON a2.shelf_id=a1.shelf_id
    WHERE order_date>=@date0 AND order_date<@date1
    GROUP BY user_id;
DROP TEMPORARY TABLE IF EXISTS feods.shelf_user_prize_temp; # 相关货架抢红包用户数及发放E币金额
CREATE TEMPORARY TABLE feods.shelf_user_prize_temp AS
    SELECT IFNULL(a3.shelf_id,a4.last_shelf_id) shelf_id,a1.user_id,a1.prize_amount,a1.activity_id
    FROM fe_activity.sf_prize_record a1
    JOIN fe_activity.sf_activity a2 ON a2.activity_id=a1.activity_id
        AND a2.end_time>=@date0 AND  a2.end_time<@date1 AND a2.activity_type=9 AND a2.data_flag=1
    LEFT JOIN feods.tmp_user_shelf_order  a3 ON a3.user_id=a1.user_id
    LEFT JOIN feods.d_op_su_u_stat a4 ON a4.user_id=a1.user_id
    WHERE a1.add_time>=@date0 AND a1.add_time<@date1
;
#插入数据
    # 插入货架维度数据
INSERT INTO feods.dm_ma_shelfRedPacket_shelf
        (sdate, activity_id,activity_date, activity_name, shelf_scope_id, shelf_id, task_type, task_target, task_achieve
        , redPacket_users, prize_amount
        , gmv_d, amount_d, users_d, orders_d, gmv_ld, amount_ld, users_ld, orders_ld
        ,gmv_w,amount_w,users_w,orders_w,gmv_lw,amount_lw,users_lw,orders_lw
        )
SELECT @date0 sdate,a1.activity_id,@date0 activity_date,a1.activity_name,a1.shelf_scope_id,a1.shelf_id,a1.tasktype,a1.taskTarget,a3.user_num task_achieve
    ,a2.user_num,a2.prize_amount
    ,a3.GMV,a3.amount,a3.user_num,a3.order_num
    ,a4.GMV,a4.amount,a4.user_num,a4.order_num
    ,a5.GMV,a5.amount,a5.user_num,a5.order_num
    ,a6.GMV,a6.amount,a6.user_num,a6.order_num
FROM feods.tmp_activity_scope_shelf a1
LEFT JOIN(SELECT shelf_id,COUNT(DISTINCT user_id) user_num,SUM(prize_amount) prize_amount  FROM  feods.shelf_user_prize_temp GROUP BY shelf_id) a2 ON a2.shelf_id=a1.shelf_id
LEFT JOIN feods.d_ma_shelf_sale_daily a3 ON a3.sdate=@date0  AND a3.SHELF_ID=a1.shelf_id
LEFT JOIN feods.d_ma_shelf_sale_daily a4 ON a4.sdate=SUBDATE(@date0,7)  AND a4.SHELF_ID=a1.shelf_id
LEFT JOIN feods.d_ma_shelf_sale_weekly a5 ON a5.sweek=@week_monday AND a5.SHELF_ID=a1.shelf_id
LEFT JOIN feods.d_ma_shelf_sale_weekly a6 ON a6.sweek=SUBDATE(@week_monday,7) AND a6.SHELF_ID=a1.shelf_id
;   # 插入活动维度数据
INSERT INTO feods.dm_ma_shelfRedPacket_activity
    (sdate, activity_id
    ,activity_date, activity_name, shelf_scope_id, task_type, task_target
    , redPacket_users, prize_amount, shelf_num, shelf_num_achieve
    , gmv_d, amount_d, users_d, orders_d, gmv_ld, amount_ld, users_ld, orders_ld
    , gmv_w, amount_w, orders_w, gmv_lw, amount_lw, orders_lw)
SELECT sdate, a1.activity_id
    ,@date0 ,activity_name, shelf_scope_id, task_type, task_target
    ,a2.user_num redPacket_users, a2.prize_amount,SUM(1) shelf_num,SUM(IF(task_achieve>=task_target,1,0)) shelf_num_achieve
    ,SUM(gmv_d) gmv_d,SUM(amount_d) amount_d,SUM(users_d) users_d,SUM(orders_d) orders_d,SUM(gmv_ld) gmv_ld,SUM(amount_ld) amount_ld,SUM(users_ld) users_ld,SUM(orders_ld) orders_ld
    ,SUM(gmv_w) gmv_w,SUM(amount_w) amount_w,SUM(orders_w) orders_w,SUM(gmv_lw) gmv_lw,SUM(amount_lw) amount_lw,SUM(orders_lw) orders_lw
FROM feods.dm_ma_shelfRedPacket_shelf a1
LEFT JOIN(SELECT activity_id,COUNT(DISTINCT user_id) user_num,SUM(prize_amount) prize_amount  FROM  feods.shelf_user_prize_temp GROUP BY activity_id) a2 ON a2.activity_id=a1.activity_id
WHERE sdate=@date0
GROUP BY sdate,activity_id
;   #插入三周对比活动维度数据
INSERT INTO feods.dm_ma_shelfRedPacket_activity_compare3week
    (sdate, activity_id, activity_name,task_target, actiyity_date, gmv_d,users_d, gmv_w)
SELECT a2.sdate, a1.activity_id, a1.activity_name,a1.taskTarget,@date0 actiyity_date
     ,SUM(a3.GMV) gmv_d,SUM(a3.user_num)  users_d,SUM(a4.GMV) gmv_w
FROM feods.tmp_activity_scope_shelf a1
JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate>=SUBDATE(@date0,7*3) AND a2.sdate<=@date0 AND DAYOFWEEK(a2.sdate)=DAYOFWEEK(@date0)
LEFT JOIN feods.d_ma_shelf_sale_daily a3 ON a3.sdate=a2.sdate AND a3.SHELF_ID=a1.shelf_id
LEFT JOIN feods.d_ma_shelf_sale_weekly a4 ON a4.sweek=ADDDATE(a2.sdate,-IF(DAYOFWEEK(a2.sdate)=1,7,DAYOFWEEK(a2.sdate)-1) +1) AND a4.SHELF_ID=a1.shelf_id
GROUP BY  a2.sdate,a1.activity_id;
# 更新周用户数
DROP TEMPORARY TABLE IF EXISTS feods.temp_week_user;
CREATE TEMPORARY TABLE feods.temp_week_user AS
    SELECT ADDDATE(DATE(a1.PAY_DATE),-IF(DAYOFWEEK(a1.PAY_DATE)=1,7,DAYOFWEEK(a1.PAY_DATE)-1) +1+3) week4,a2.activity_id
         ,COUNT(DISTINCT user_id) user_num
    FROM fe_dwd.dwd_order_item_refund_day a1
    JOIN feods.tmp_activity_scope_shelf a2 ON a2.shelf_id=a1.shelf_id
    WHERE a1.PAY_DATE>=SUBDATE(@week_monday,7*3) AND a1.PAY_DATE<=ADDDATE(@week_monday,6) AND a1.quantity_act>0
    GROUP BY week4,a2.activity_id;
DROP TEMPORARY TABLE IF EXISTS feods.temp_week_user2;
CREATE TEMPORARY TABLE feods.temp_week_user2 AS SELECT * FROM feods.temp_week_user;
UPDATE feods.dm_ma_shelfRedPacket_activity a1
LEFT JOIN feods.temp_week_user a2 ON a2.week4=a1.sdate AND a2.activity_id=a1.activity_id
LEFT JOIN feods.temp_week_user2 a3 ON a3.week4=SUBDATE(a1.sdate,7) AND a2.activity_id=a1.activity_id
SET a1.users_w=a2.user_num,a1.users_lw=a3.user_num
WHERE a1.sdate=@date0;
UPDATE feods.dm_ma_shelfRedPacket_activity_compare3week a1
JOIN feods.temp_week_user a2 ON a2.week4=a1.sdate AND a2.activity_id=a1.activity_id
SET a1.users_w=a2.user_num
WHERE a1.actiyity_date=@date0;
    # 更新增长
UPDATE feods.dm_ma_shelfRedPacket_activity_compare3week a1
JOIN feods.dm_ma_shelfRedPacket_activity_compare3week a2 ON a2.sdate=SUBDATE(a1.sdate,7) AND a2.activity_id=a1.activity_id
SET a1.gmv_d_chg= (a1.gmv_d-a2.gmv_d)/a2.gmv_d,a1.gmv_w_chg= (a2.gmv_w-a1.gmv_w)/a2.gmv_w
    ,a1.users_d_chg=(a1.users_d-a2.users_d)/a2.users_d,a1.users_w_chg=(a1.users_w-a2.users_w)/a2.users_w
WHERE a1.actiyity_date=@date0;
#记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_shelf_RedPacket',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
    END