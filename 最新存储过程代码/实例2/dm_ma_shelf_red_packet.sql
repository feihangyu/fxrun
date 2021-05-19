CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_red_packet`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场业务方(刘燕)
-- Create date: 2020-3-19
-- Modify date:
-- Description: 货架抢红包业务数据
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @date0=p_sdate;  #周一执行,传入上周日期
SET @sweek= SUBDATE(@date0,IF(DAYOFWEEK(@date0)=1,7,DAYOFWEEK(@date0)-1)-1 )  ;
#删除数据
DELETE FROM fe_dm.dm_ma_shelfredpacket_activity WHERE sdate=@sweek ;
DELETE FROM fe_dm.dm_ma_shelfredpacket_shelf WHERE sdate=@sweek OR sdate<SUBDATE(@sweek,7*100);
#创建临时数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_activity;   # 活动信息
CREATE TEMPORARY TABLE fe_dm.tmp_activity(INDEX(activity_id)) AS
    SELECT activity_id,activity_name
        ,platform,activity_type,a1.start_time,a1.end_time
        ,SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX
            (config_json,'shelfScopeIds',-1),',',1),':',-1),'\"',-2),'\"',1) shelf_scope_id
        ,SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(config_json,'taskTarget',-1),',',1),':',-1)+0 task_Target
        ,SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(config_json,'taskType',-1),',',1),':',-1)+0  task_type # 1 下单人数 8订单数
    FROM fe_dwd.dwd_sf_activity a1
    WHERE a1.end_time>=@sweek AND  a1.end_time<ADDDATE(@sweek,7)
        AND a1.platform=1 AND a1.activity_type=9 AND a1.data_flag=1
    ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_activity_scope_shelf;   # 定向货架抢红包货架ID及活动相关信息
CREATE TEMPORARY TABLE fe_dm.tmp_activity_scope_shelf(INDEX(shelf_id)) AS
    SELECT a1.*,a2.shelf_id
    FROM fe_dm.tmp_activity a1
    JOIN fe_dwd.dwd_sf_shelf_scope_detail a2
        ON a2.shelf_scope_id=a1.shelf_scope_id  AND a2.data_flag=1
    #WHERE a1.end_time>=@sweek AND  a1.end_time<adddate(@sweek,7)
    ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_shelf_order; # 用户当天购买货架
CREATE TEMPORARY TABLE fe_dm.tmp_user_shelf_order(INDEX(user_id)) AS
    SELECT user_id,a1.shelf_id
    FROM fe_dwd.dwd_order_item_refund_day a1
    JOIN fe_dm.tmp_activity_scope_shelf a2 ON a2.shelf_id=a1.shelf_id
    WHERE PAY_DATE>=@sweek AND PAY_DATE<ADDDATE(@sweek,7)
    GROUP BY user_id;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_shelf_user_prize; # 相关货架抢红包用户数及发放E币金额
CREATE TEMPORARY TABLE fe_dm.tmp_shelf_user_prize(INDEX (shelf_id)) AS
SELECT a1.activity_id,a1.shelf_id,COUNT(DISTINCT user_id) redPacket_users,SUM(prize_amount) prize_amount
FROM
    (SELECT IFNULL(a3.shelf_id,a4.last_shelf_id) shelf_id,a1.user_id,a1.prize_amount,a1.activity_id
    FROM fe_dwd.dwd_sf_prize_record a1
    JOIN fe_dm.tmp_activity a2 ON a2.activity_id=a1.activity_id
    LEFT JOIN fe_dm.tmp_user_shelf_order  a3 ON a3.user_id=a1.user_id
    LEFT JOIN fe_dm.dm_op_su_u_stat a4 ON a4.user_id=a1.user_id
    WHERE a1.add_time>=@sweek AND a1.add_time<ADDDATE(@sweek,7)
    ) a1
GROUP BY a1.activity_id,a1.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_kpi ; #近四周订单
CREATE TEMPORARY TABLE fe_dm.tmp_kpi(INDEX(shelf_id,activity_id),INDEX(activity_id),INDEX(sweek)) AS
SELECT sweek,shelf_id,activity_id
    ,SUM(pay_amount) pay_amount,SUM(GMV) gmv,COUNT(DISTINCT user_id) users,COUNT(DISTINCT order_id) orders
FROM
    (SELECT order_id,a1.shelf_id,a1.product_id,user_id,a1.PAY_DATE,a2.activity_id
         ,SUBDATE(DATE(PAY_DATE),IF(DAYOFWEEK(PAY_DATE)=1,6,DAYOFWEEK(PAY_DATE)-2)) sweek
        ,a1.pay_amount_product-IFNULL(refund_amount,0) pay_amount
        ,a1.quantity_act*a1.`SALE_PRICE`  GMV
    FROM fe_dm.tmp_activity_scope_shelf a2
    JOIN fe_dwd.dwd_order_item_refund_day a1 ON a1.shelf_id=a2.shelf_id
    WHERE #订单支付时间在三周内
          a1.PAY_DATE>=DATE_SUB(@sweek,INTERVAL 3 WEEK)  AND a1.PAY_DATE<ADDDATE(@sweek,7)
        # 订单发生日期在活动天数内
        AND a1.PAY_DATE>=ADDDATE(a2.start_time,7*FLOOR(DATEDIFF(a1.PAY_DATE,a2.start_time)/7))
        AND a1.PAY_DATE<=ADDDATE(a2.end_time,7*FLOOR(DATEDIFF(a1.PAY_DATE,a2.start_time)/7))
    ) a1
GROUP BY sweek,shelf_id,activity_id
    ;
#插入数据
    # 插入货架维度数据
INSERT INTO fe_dm.dm_ma_shelfRedPacket_shelf
        (sdate, activity_id
        , activity_name, start_time, end_time, activity_days, shelf_scope_id, shelf_id, task_type, task_target, task_achieve, redPacket_users, prize_amount
        , gmv_w, amount_w, users_w, orders_w, gmv_lw, amount_lw, users_lw, orders_lw, gmv_llw, amount_llw, users_llw, orders_llw,gmv_lllw, amount_lllw, users_lllw, orders_lllw
        )
SELECT @sweek, a1.activity_id  , activity_name, start_time, end_time,DATEDIFF(DATE(end_time),DATE(start_time)) days, shelf_scope_id
    , a1.shelf_id, task_type, task_target
    ,CASE WHEN a1.task_type=1 THEN SUM(DISTINCT IF(a3.sweek=@sweek,a3.users,0)) ELSE SUM(DISTINCT IF(a3.sweek=@sweek,a3.orders,0)) END  task_achieve
    ,IFNULL(redPacket_users,0) redPacket_users,IFNULL(a2.prize_amount,0) prize_amount
    ,SUM(IF(a3.sweek=@sweek,GMV,0))  gmv_w,SUM(IF(a3.sweek=@sweek,pay_amount,0))  amount_w,SUM(IF(a3.sweek=@sweek,a3.users,0))  users_w,SUM(IF(a3.sweek=@sweek,a3.orders,0)) orders_w
    ,SUM(IF(a3.sweek=SUBDATE(@sweek,7),GMV,0))  gmv_lw,SUM(IF(a3.sweek=SUBDATE(@sweek,7),a3.pay_amount,0))  amount_lw,SUM(IF(a3.sweek=SUBDATE(@sweek,7),a3.users,0))  users_lw,SUM(IF(a3.sweek=SUBDATE(@sweek,7),a3.orders,0)) orders_lw
    ,SUM(IF(a3.sweek=SUBDATE(@sweek,14),GMV,0))  gmv_llw,SUM(IF(a3.sweek=SUBDATE(@sweek,14),a3.pay_amount,0))  pay_amount_llw,SUM(IF(a3.sweek=SUBDATE(@sweek,14),a3.users,0))  users_llw,SUM(IF(a3.sweek=SUBDATE(@sweek,14),a3.orders,0))  users_llw
    ,SUM(IF(a3.sweek=SUBDATE(@sweek,21),GMV,0))  gmv_llllw,SUM(IF(a3.sweek=SUBDATE(@sweek,21),a3.pay_amount,0))  amount_llllw,SUM(IF(a3.sweek=SUBDATE(@sweek,21),a3.users,0))  users_llllw,SUM(IF(a3.sweek=SUBDATE(@sweek,21),a3.orders,0)) orders_llllw
FROM fe_dm.tmp_activity_scope_shelf a1
LEFT JOIN fe_dm.tmp_shelf_user_prize a2 ON a2.shelf_id=a1.shelf_id AND a2.activity_id=a1.activity_id
LEFT JOIN fe_dm.tmp_kpi a3 ON a3.shelf_id=a1.shelf_id AND a3.activity_id=a1.activity_id
GROUP BY a1.activity_id,a1.shelf_id
;
    # 插入活动维度数据
INSERT INTO fe_dm.dm_ma_shelfRedPacket_activity
    (sdate, activity_id
    , activity_name, start_time, end_time, activity_days, shelf_scope_id, task_type, task_target, redPacket_users, prize_amount #, shelf_num, shelf_num_achieve
    , gmv_w, amount_w, users_w, orders_w, gmv_lw, amount_lw, users_lw, orders_lw, gmv_llw, amount_llw, users_llw, orders_llw, gmv_lllw, amount_lllw, users_lllw, orders_lllw)
SELECT
    @sweek sdate, a1.activity_id
    ,activity_name, start_time, end_time,DATEDIFF(DATE(end_time),DATE(start_time))+1 activity_days, shelf_scope_id, task_type
    ,task_target,IFNULL(redPacket_users,0) redPacket_users,IFNULL(a2.prize_amount,0) prize_amount
    ,SUM(IF(a3.sweek=@sweek,GMV,0))  gmv_w,SUM(IF(a3.sweek=@sweek,pay_amount,0))  amount_w,SUM(IF(a3.sweek=@sweek,a3.users,0))  users_w,SUM(IF(a3.sweek=@sweek,a3.orders,0)) orders_w
    ,SUM(IF(a3.sweek=SUBDATE(@sweek,7),GMV,0))  gmv_lw,SUM(IF(a3.sweek=SUBDATE(@sweek,7),a3.pay_amount,0))  amount_lw,SUM(IF(a3.sweek=SUBDATE(@sweek,7),a3.users,0))  users_lw,SUM(IF(a3.sweek=SUBDATE(@sweek,7),a3.orders,0)) orders_lw
    ,SUM(IF(a3.sweek=SUBDATE(@sweek,14),GMV,0))  gmv_llw,SUM(IF(a3.sweek=SUBDATE(@sweek,14),a3.pay_amount,0))  pay_amount_llw,SUM(IF(a3.sweek=SUBDATE(@sweek,14),a3.users,0))  users_llw,SUM(IF(a3.sweek=SUBDATE(@sweek,14),a3.orders,0)) users_llw
    ,SUM(IF(a3.sweek=SUBDATE(@sweek,21),GMV,0))  gmv_llllw,SUM(IF(a3.sweek=SUBDATE(@sweek,21),a3.pay_amount,0))  amount_llllw,SUM(IF(a3.sweek=SUBDATE(@sweek,21),a3.users,0))  users_llllw,SUM(IF(a3.sweek=SUBDATE(@sweek,21),a3.orders,0)) orders_llllw
FROM fe_dm.tmp_activity a1
LEFT JOIN(SELECT activity_id,SUM(prize_amount) prize_amount,SUM(redPacket_users) redPacket_users FROM fe_dm.tmp_shelf_user_prize GROUP BY  activity_id)a2 ON  a2.activity_id=a1.activity_id
LEFT JOIN fe_dm.tmp_kpi a3 ON  a3.activity_id=a1.activity_id
GROUP BY a1.activity_id
;
UPDATE fe_dm.dm_ma_shelfRedPacket_activity a1
JOIN (SELECT activity_id,COUNT(1) shelf_num,SUM(IF(task_achieve>=task_target,1,0)) shelf_num_achieve
    FROM fe_dm.dm_ma_shelfredpacket_shelf
    WHERE sdate=@sweek
    GROUP BY activity_id
        ) a2 ON a2.activity_id=a1.activity_id
SET a1.shelf_num=a2.shelf_num  ,a1.shelf_num_achieve=a2.shelf_num_achieve
WHERE a1.sdate=@sweek
;
# 记录运行时间
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_red_packet',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
END