CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_ma_users_all_weekly`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场  业务方(叶楚煊)
-- Create date: 2020-3-19
-- Modify date:
-- Description: 每周各种用户数统计 4m 30s
-- =============================================
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;# 默认输入当天
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)); #当周一
SET @sweek_end=ADDDATE(@sweek,6); #当周日
#删除数据
DELETE FROM fe_dm.dm_ma_users_all_weekly WHERE sdate=@sweek ;
#临时数据
DROP TEMPORARY  TABLE IF EXISTS fe_dm.tmp_user_sale_weekly; #用户近9周销售 2 m 9 s 384 ms
CREATE TEMPORARY TABLE fe_dm.tmp_user_sale_weekly(INDEX(user_id)) AS
    SELECT user_id
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*1),buy_rate,0)) ow1
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*2),buy_rate,0)) ow2
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*3),buy_rate,0)) ow3
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*4),buy_rate,0)) ow4
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*5),buy_rate,0)) ow5
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*6),buy_rate,0)) ow6
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*7),buy_rate,0)) ow7
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*8),buy_rate,0)) ow8
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*9),buy_rate,0)) ow9
         ,SUM(buy_rate) o9w
         ,SUM(IF(sdate=SUBDATE(@sweek_end,7*1),gmv,0)) gmv_1
    FROM feods.zs_user_week_sale
    WHERE sdate BETWEEN SUBDATE(@sweek_end,7*9) AND SUBDATE(@sweek_end,7)
    GROUP BY user_id
    ;
DROP TEMPORARY  TABLE IF EXISTS fe_dm.tmp_user_info; #用户标签 1 m 54 s 219 ms
CREATE TEMPORARY TABLE fe_dm.tmp_user_info AS
    SELECT a1.user_id
        ,CASE WHEN a2.user_id IS NULL AND CREATE_DATE>=SUBDATE(@sweek,28) THEN 1
            WHEN a3.min_order_date BETWEEN SUBDATE(@sweek,7) AND @sweek THEN 2
            WHEN DATE(a4.REVOKE_TIME)<@sweek OR a2.user_id IS NULL THEN 5
            WHEN ow1+ow2+ow3+ow4>0 THEN 3
            ELSE 4 END ulc
        ,CASE WHEN IFNULL(ow3+ow4+ow5+ow6,0)=0 AND CREATE_DATE>=SUBDATE(@sweek,28) THEN 1
            WHEN a3.min_order_date BETWEEN SUBDATE(@sweek,7*3) AND SUBDATE(@sweek,7*2) THEN 2
            WHEN DATE(a4.REVOKE_TIME)<SUBDATE(@sweek,7*2) OR a2.user_id IS NULL  OR ow1=o9w THEN 5
            WHEN ow3+ow4+ow5+ow6>0 THEN 3
            ELSE 4 END ulc_llw
        ,CASE WHEN IFNULL(gmv_1/ow1,0) <5 THEN 5 WHEN gmv_1/ow1 <10 THEN 10
            WHEN gmv_1/ow1 <15 THEN 15 WHEN gmv_1/ow1 <20 THEN 20
            ELSE 21 END kdj
        ,CASE WHEN ow1>0 THEN 0 WHEN ow2>0 THEN 1
            WHEN ow3>0 THEN 2 WHEN ow4>0 THEN 3
            WHEN ow5>0 THEN 4 WHEN ow6>0 THEN 5
            WHEN ow7>0 THEN 6 WHEN ow8>0 THEN 7
            WHEN ow9>0 THEN 8
            ELSE 9 END buy_9w
        ,CASE WHEN ow2>0 THEN 1
            WHEN ow3>0 THEN 2 WHEN ow4>0 THEN 3
            WHEN ow5>0 THEN 4 WHEN ow6>0 THEN 5
            WHEN ow7>0 THEN 6 WHEN ow8>0 THEN 7
            WHEN ow9>0 THEN 8
            ELSE 9 END buy_8w
        ,case when CREATE_DATE>=SUBDATE(@sweek,7) then 1 when CREATE_DATE>=subdate(@sweek,7*2) then 2 else 0 end reg_week
        ,ow3=0 AND ow2>0 return_llw
    FROM fe_dwd.dwd_user_day_inc a1
    LEFT JOIN fe_dm.tmp_user_sale_weekly a2 ON a2.user_id=a1.user_id
    LEFT JOIN feods.d_op_su_u_stat a3 ON a3.user_id=a1.user_id
    LEFT JOIN fe_dwd.dwd_shelf_base_day_all a4 ON a4.shelf_id=a3.last_shelf_id
    WHERE a1.CREATE_DATE<@sweek;
#插入数据 17 s 472 ms
INSERT INTO fe_dm.dm_ma_users_all_weekly
    (sdate
    , users_introdution, users_growth, users_mature, users_quiescent, users_loss #生命周期
    , users_kdj5, users_kdj10, users_kdj15, users_kdj20, users_kdj20up #上周客单价
    , users_nobuy_0, users_nobuy_1, users_nobuy_2, users_nobuy_3, users_nobuy_4, users_nobuy_5, users_nobuy_6, users_nobuy_7, users_nobuy_8 #近8周购买情况
    ,rate_return_nobuy_1,rate_return_nobuy_2,rate_return_nobuy_3,rate_return_nobuy_4,rate_return_nobuy_5,rate_return_nobuy_6,rate_return_nobuy_7,rate_return_nobuy_8
    , users_reg_buy_lw, users_reg_lw
    , users_ulc3_llw_buy_lw, users_ulc3_llw
    , users_return_llw_buy_lw, users_return_llw
    ,use_egbuy_llwr_buy_lw,user_regbuy_llw
    )
SELECT @sweek
    ,SUM(ulc=1) users_introdution
    ,SUM(ulc=2) users_growth
    ,SUM(ulc=3) users_mature
    ,SUM(ulc=4) users_quiescent
    ,SUM(ulc=5) users_loss
    ,SUM(kdj=5) users_kdj5
    ,SUM(kdj=10) users_kdj10
    ,SUM(kdj=15) users_kdj15
    ,SUM(kdj=20) users_kdj20
    ,SUM(kdj=21) users_kdj20up
    ,SUM(buy_9w=0) users_nobuy_0
    ,SUM(buy_9w=1) users_nobuy_1
    ,SUM(buy_9w=2) users_nobuy_2
    ,SUM(buy_9w=3) users_nobuy_3
    ,SUM(buy_9w=4) users_nobuy_4
    ,SUM(buy_9w=5) users_nobuy_5
    ,SUM(buy_9w=6) users_nobuy_6
    ,SUM(buy_9w=7) users_nobuy_7
    ,SUM(buy_9w>7) users_nobuy_8
    ,SUM(buy_8w=2 AND buy_9w=0)/SUM(buy_8w=2) rate_return_nobuy_1
    ,SUM(buy_8w=3 AND buy_9w=0)/SUM(buy_8w=3) rate_return_nobuy_2
    ,SUM(buy_8w=4 AND buy_9w=0)/SUM(buy_8w=4) rate_return_nobuy_3
    ,SUM(buy_8w=5 AND buy_9w=0)/SUM(buy_8w=5) rate_return_nobuy_4
    ,SUM(buy_8w=6 AND buy_9w=0)/SUM(buy_8w=6) rate_return_nobuy_5
    ,SUM(buy_8w=7 AND buy_9w=0)/SUM(buy_8w=7) rate_return_nobuy_6
    ,SUM(buy_8w=8 AND buy_9w=0)/SUM(buy_8w=8) rate_return_nobuy_7
    ,SUM(buy_8w=9 AND buy_9w=0)/SUM(buy_8w=9) rate_return_nobuy_8
    ,SUM(reg_week=1 AND buy_9w=0) users_reg_buy_lw
    ,SUM(reg_week=1) users_reg_lw
    ,SUM(ulc_llw=3 AND buy_9w=0) users_ulc3_llw_buy_lw
    ,SUM(ulc_llw=3) users_ulc3_llw
    ,SUM(return_llw=1 AND buy_9w=0) users_return_llw_buy_lw
    ,SUM(return_llw=1) users_return_llw
    ,SUM(reg_week=2 AND buy_8w=1 and buy_9w=0) use_egbuy_llwr_buy_lw
    ,SUM(reg_week=2 AND buy_8w=1) user_regbuy_llw
FROM fe_dm.tmp_user_info
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_ma_users_all_weekly',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('纪伟铨@', @user, @timestamp)
  );
COMMIT;
    END