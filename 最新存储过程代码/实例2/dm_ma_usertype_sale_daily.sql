CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_usertype_sale_daily`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场  业务方(罗辉)
-- Create date: 2020-3-19
-- Modify date:
-- Description:
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
#删除数据
DELETE FROM fe_dm.dm_ma_usertype_sale_daily WHERE sdate=@sdate ;
#临时数据
    #本日订单
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_order;
CREATE TEMPORARY TABLE fe_dm.tmp_order(INDEX(user_id)) AS
    SELECT a1.PAY_DATE,a1.order_id,a1.shelf_id,a1.user_id
         ,o_discount_amount,COUPON_AMOUNT,IFNULL(third_discount_amount,0) third_discount_amount
         ,sale_price,quantity_act,pay_amount_product,sale_price*quantity_act gmv
    FROM fe_dwd.dwd_order_item_refund_day a1
    WHERE PAY_DATE>=@sdate AND PAY_DATE<ADDDATE(@sdate,1)
    UNION ALL
    SELECT a1.PAY_DATE,a1.PAYMENT_ID,IFNULL(a1.real_shelf_id,a1.SHELF_ID) shelf_id,a1.USER_ID
        ,0,0,0
        ,a1.PAYMENT_MONEY,1 quantity_act,a1.PAYMENT_MONEY pay_amount_product,0 gmv
    FROM fe_dwd.dwd_sf_after_payment a1
    WHERE a1.PAYMENT_DATE>@sdate AND a1.PAYMENT_DATE<ADDDATE(@sdate,1)
;   #本日用户
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_users;
CREATE TEMPORARY TABLE fe_dm.tmp_users(INDEX(user_id)) AS
    SELECT DISTINCT user_id FROM fe_dm.tmp_order
;   #用户近八周销售
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_ueser_sale;
CREATE TEMPORARY TABLE fe_dm.tmp_ueser_sale(INDEX(user_id)) AS
    SELECT a1.user_id
        ,SUM(IF(sdate=SUBDATE(@sweek,7),sale_qty,0)) sw1
        ,SUM(IF(sdate >=SUBDATE(@sweek,7*4),sale_qty,0)) sw1_4
        ,SUM(IF(sdate<SUBDATE(@sweek,7*4),sale_qty,0)) sw5_8
    FROM fe_dm.tmp_users  a1
    JOIN fe_dm.dm_ma_user_sale_weekly a2 ON a2.sdate>=SUBDATE(@sweek,7*8) AND a2.sdate<@sweek AND a2.user_id=a1.user_id
    GROUP BY a1.user_id
;   #用户生命周期
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_ulc;
CREATE TEMPORARY TABLE fe_dm.tmp_ulc(INDEX(user_id)) AS
    SELECT  a1.user_id
        # 用户生命周期大类:(1:导入期,2:成长期,3:成熟期,4:流失期,5:休眠期)
        ,CASE WHEN IFNULL(a3.min_order_date,CURDATE()) >=@sweek THEN 1
            WHEN a3.min_order_date BETWEEN SUBDATE(@sweek,1) AND @sweek THEN 2
            WHEN a2.sw1_4>0 THEN 3
            WHEN a2.sw5_8>0 THEN 5
            WHEN a4.CREATE_DATE>=@sdate THEN 6 #新用户
            ELSE 4 END user_life_cycle_genera
        ,IFNULL(sw1,0) gmv_last
    FROM fe_dm.tmp_users a1
    LEFT JOIN fe_dm.tmp_ueser_sale a2 ON a2.user_id=a1.user_id
    JOIN fe_dm.dm_op_su_u_stat a3 ON a3.user_id=a1.user_id
    JOIN fe_dwd.dwd_user_day_inc a4 ON a4.user_id=a1.user_id
;
#插入数据
    # 总GMV	总无优惠产生的GMV 总实收	总购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,1,SUM(gmv) GMV,SUM(pay_amount_product) pay_amount_product,COUNT(DISTINCT user_id) users
     ,SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)) no_dct_gmv
FROM fe_dm.tmp_order a1
;
    # 新用户GMV	新用户无优惠产生的GMV	新用户实收	新用户购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,2
     ,IFNULL(SUM(gmv),0) GMV,IFNULL(SUM(pay_amount_product),0) pay_amount_product,COUNT(DISTINCT a1.user_id) users
     ,IFNULL(SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)),0) no_dct_gmv
FROM fe_dm.tmp_order a1
JOIN fe_dm.tmp_ulc a2 ON a2.user_id=a1.user_id AND a2.user_life_cycle_genera=6
;
    # 成长期近1周有购买用户GMV	成长期近1周有购买用户无优惠产生的GMV	成长期近1周有购买用户实收	成长期近1周有购买用户购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,3
     ,IFNULL(SUM(gmv),0) GMV,IFNULL(SUM(pay_amount_product),0) pay_amount_product,COUNT(DISTINCT a1.user_id) users
     ,IFNULL(SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)),0) no_dct_gmv
FROM fe_dm.tmp_order a1
JOIN fe_dm.tmp_ulc a2 ON a2.user_id=a1.user_id
WHERE a2.gmv_last>0 AND a2.user_life_cycle_genera=2
;
    # 成熟期近1周有购买用户GMV	成熟期近1周有购买用户无优惠产生的GMV  成熟期近1周有购买用户实收	成熟期近1周有购买用户购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,4
     ,IFNULL(SUM(gmv),0) GMV,IFNULL(SUM(pay_amount_product),0) pay_amount_product,COUNT(DISTINCT a1.user_id) users
     ,IFNULL(SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)),0) no_dct_gmv
FROM fe_dm.tmp_order a1
JOIN fe_dm.tmp_ulc a2 ON a2.user_id=a1.user_id
WHERE a2.gmv_last>0 AND a2.user_life_cycle_genera=3
;
    # 成熟期回流用户GMV	成熟期回流用户无优惠产生的GMV	成熟期回流用户实收	成熟期回流用户购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,5
     ,IFNULL(SUM(gmv),0) GMV,IFNULL(SUM(pay_amount_product),0) pay_amount_product,COUNT(DISTINCT a1.user_id) users
     ,IFNULL(SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)),0) no_dct_gmv
FROM fe_dm.tmp_order a1
JOIN fe_dm.tmp_ulc a2 ON a2.user_id=a1.user_id
WHERE gmv_last=0 AND a2.user_life_cycle_genera=3
;
# 休眠期回流用户GMV	休眠期回流用户无优惠产生的GMV	休眠期回流用户实收	休眠期回流用户购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,6
     ,IFNULL(SUM(gmv),0) GMV,IFNULL(SUM(pay_amount_product),0) pay_amount_product,COUNT(DISTINCT a1.user_id) users
     ,IFNULL(SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)),0) no_dct_gmv
FROM fe_dm.tmp_order a1
JOIN fe_dm.tmp_ulc a2 ON a2.user_id=a1.user_id
WHERE gmv_last=0 AND a2.user_life_cycle_genera=5
;
# 流失期回流用户GMV	流失期回流用户无优惠产生的GMV	流失期回流用户实收	流失期回流用户购买人数
INSERT INTO fe_dm.dm_ma_usertype_sale_daily
    (sdate, user_type, GMV, pay_amount, users, no_dct_gmv)
SELECT @sdate,7
     ,IFNULL(SUM(gmv),0) GMV,IFNULL(SUM(pay_amount_product),0) pay_amount_product,COUNT(DISTINCT a1.user_id) users
     ,IFNULL(SUM(IF(a1.o_discount_amount+COUPON_AMOUNT+third_discount_amount=0,gmv,0)),0) no_dct_gmv
FROM fe_dm.tmp_order a1
JOIN fe_dm.tmp_ulc a2 ON a2.user_id=a1.user_id
WHERE gmv_last=0 AND a2.user_life_cycle_genera=4
;
#记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_usertype_sale_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_usertype_sale_daily','dm_ma_usertype_sale_daily','纪伟铨');
END