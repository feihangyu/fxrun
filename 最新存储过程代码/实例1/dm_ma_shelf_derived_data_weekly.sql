CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_ma_shelf_derived_data_weekly`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场
-- Create date: 2020-3-19
-- Modify date:
-- Description: 货架周衍生数据
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每周运行一次输入当天时间
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
#删除数据
DELETE FROM fe_dm.dm_ma_shelf_derived_data_weekly WHERE sdate=@sweek OR sdate<SUBDATE(@sweek,7*200);
#临时数据
SET @rank_num=0
    ,@shelfs=(SELECT COUNT(1) FROM fe_dwd.dwd_shelf_base_day_all a1 WHERE a1.SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
        AND DATE(a1.ACTIVATE_TIME)<@sweek AND DATE(IFNULL(a1.REVOKE_TIME,CURDATE()))>=SUBDATE(@sweek,7)) ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_life_cycle; #用户生命周期
CREATE TEMPORARY TABLE fe_dm.tmp_user_life_cycle(INDEX(shelf_id)) AS
SELECT shelf_id
        ,SUM(IF(user_life_cycle_genera=1,1,0)) users_introdution
        ,SUM(IF(user_life_cycle_genera=2,1,0)) users_growth
        ,SUM(IF(user_life_cycle_genera=3,1,0)) users_mature
        ,SUM(IF(user_life_cycle_genera=4,1,0)) users_loss
        ,SUM(IF(user_life_cycle_genera=5,1,0)) users_quiescent
    FROM feods.zs_shelf_member_flag_history
    WHERE sdate=@sweek
    GROUP BY shelf_id;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_shelf_week;#货架周数据
CREATE TEMPORARY TABLE fe_dm.tmp_shelf_week(INDEX(shelf_id)) AS
    SELECT *
        ,(@rank_num:=@rank_num+1)/@shelfs rank_rate
    FROM
        (SELECT a1.SHELF_ID
            ,order_num,user_num,user_num_last
            ,(user_num-user_num_last)/user_num_last users_chg
            ,user_num_reorder/user_num  reorder_rate
            ,a4.users_mature/(a4.users_growth+a4.users_mature+a4.users_quiescent)   users_quanlity
            ,a4.users_quiescent/(a4.users_growth+a4.users_mature+a4.users_quiescent) users_potential
            ,GMV/order_num   shelf_value
            ,order_num/user_num orders_per_user
            ,ROUND(CASE WHEN scope LIKE '%-%人' THEN (SUBSTRING_INDEX(SUBSTRING_INDEX(scope,'人',1) ,'-',1)+SUBSTRING_INDEX(SUBSTRING_INDEX(scope,'人',1) ,'-',-1)) /2
                 WHEN scope LIKE '%于%' THEN REPLACE(SUBSTRING_INDEX(scope,'于',-1),'人','')
                 ELSE REPLACE(scope,'-',0) END) scope
            ,a3.users,a4.users_introdution, a4.users_growth,a4. users_mature, a4.users_quiescent, a4.users_loss
        FROM fe_dwd.dwd_shelf_base_day_all a1
        left JOIN feods.d_op_su_s_stat a3 ON a3.shelf_id=a1.SHELF_ID
        LEFT JOIN feods.d_ma_shelf_sale_weekly a2 ON a2.sweek=SUBDATE(@sweek,7) AND a2.shelf_id=a1.shelf_id
        LEFT JOIN fe_dm.tmp_user_life_cycle a4 ON a4.shelf_id=a1.shelf_id
        WHERE  a1.SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
            AND DATE(a1.ACTIVATE_TIME)<@sweek AND DATE(IFNULL(a1.REVOKE_TIME,CURDATE()))>=SUBDATE(@sweek,7)
        ORDER BY a2.order_num DESC
        ) a1;

#插入数据
INSERT INTO fe_dm.dm_ma_shelf_derived_data_weekly
    (sdate, shelf_id
    ,users_introdution, users_growth, users_mature, users_quiescent, users_loss
    , users_potential, users_saturability, users_quanlity, reorder_rate, users_active, users_chg, shelf_value, orders_per_user, users_order, users_permeate)
SELECT @sweek,a2.shelf_id
    ,IFNULL(a2.users_introdution,0), IFNULL(a2.users_growth,0), IFNULL(a2.users_mature,0), IFNULL(a2.users_quiescent,0), IFNULL(a2.users_loss,0)
    ,CASE WHEN a2.users_potential<0.2 THEN 1 WHEN a2.users_potential<0.4 THEN 2 WHEN a2.users_potential>=0.4 THEN 3 ELSE 0 END users_potential
    ,CASE WHEN a2.users/a2.scope<0.5 THEN 1  WHEN a2.users/a2.scope<0.8 THEN 2 WHEN a2.users/a2.scope>=0.8 THEN 3 ELSE 0 END users_saturability
    ,CASE WHEN a2.users_quanlity<0.6 THEN 1 WHEN a2.users_quanlity<0.8 THEN 2 WHEN a2.users_quanlity>=0.8 THEN 3 ELSE 0 END users_quanlity
    ,CASE WHEN a2.reorder_rate<0.25 THEN 1 WHEN a2.reorder_rate<0.5 THEN 2 WHEN a2.reorder_rate>=0.5 THEN 3 ELSE 0 END reorder_rate
    ,CASE WHEN a2.rank_rate>=0.7 THEN 1 WHEN a2.rank_rate>=0.3 THEN 2 ELSE 3 END users_active
    ,CASE WHEN a2.users_chg<-1 THEN 1 WHEN a2.users_chg<-0.3 THEN 2 WHEN a2.users_chg<0.3 THEN 3 WHEN a2.users_chg<1 THEN 4
        WHEN a2.users_chg>=1 OR (a2.user_num>0 AND a2.user_num_last=0)  THEN 5 ELSE 0 END users_chg
    ,CASE WHEN a2.shelf_value<5 THEN 1 WHEN a2.shelf_value<10 THEN 2 WHEN a2.shelf_value<15 THEN 3 WHEN a2.shelf_value<20 THEN 4
        WHEN a2.shelf_value>=20 THEN 5 ELSE 0 END shelf_value
    ,CASE WHEN a2.orders_per_user<=1.3 THEN 1 WHEN a2.orders_per_user<=3 THEN 2 WHEN a2.orders_per_user>3 THEN 3 ELSE 0 END orders_per_user
    ,CASE WHEN a2.user_num >=100 THEN 7 WHEN  a2.user_num >=50 THEN 6 WHEN  a2.user_num>=41 THEN 5 WHEN  a2.user_num >=31 THEN 4
        WHEN  a2.user_num>=21 THEN 3 WHEN  a2.user_num>=11 THEN 2 ELSE 1 END users_order
    ,CASE WHEN a2.user_num/a2.scope >=0.7 THEN 9 WHEN a2.user_num/a2.scope  >=0.6 THEN 8 WHEN a2.user_num/a2.scope  >=0.5 THEN 7 WHEN a2.user_num/a2.scope >=0.4 THEN 6
        WHEN a2.user_num/a2.scope >=0.3 THEN 5 WHEN a2.user_num/a2.scope  >=0.2 THEN 4 WHEN a2.user_num/a2.scope >=0.1 THEN 3 WHEN a2.user_num/a2.scope  >=0.05
        THEN 2 WHEN a2.user_num/a2.scope  >=0 THEN 1 ELSE 0 END users_permeate
FROM fe_dm.tmp_shelf_week a2

;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_ma_shelf_derived_data_weekly',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('纪伟铨@', @user, @timestamp)
  );
  COMMIT;
END