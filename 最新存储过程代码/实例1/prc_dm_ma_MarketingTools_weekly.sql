CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_MarketingTools_weekly`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));
#删除数据
DELETE FROM fe_dm.dm_ma_MarketingTools_weekly WHERE sdate=@sweek ;
#临时数据
    #用户生命周期
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_user_life_cycle;
CREATE TEMPORARY TABLE fe_dm.temp_user_life_cycle(INDEX(user_id)) AS
    SELECT user_id,user_life_cycle_genera
    FROM feods.zs_shelf_member_flag_history
    WHERE sdate=@sweek;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_user_sale;
CREATE TEMPORARY TABLE fe_dm.temp_user_sale(INDEX(user_id)) AS
    select user_id,sum(buy_rate) buy_rate,sum(if(sdate=ADDDATE(@sweek,6),buy_rate,0)) buy_rate_fw
    from feods.zs_user_week_sale a1
    where sdate between subdate(@sweek,1) and ADDDATE(@sweek,6)
    group by user_id;
    #营销工具使用用户数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_tool;
CREATE TEMPORARY TABLE fe_dm.tmp_user_tool(INDEX(user_id)) AS
    #邀请活动(1.邀请新用户,2.邀请好友拼单,3.邀请好友拆礼包)
    SELECT DISTINCT a1.inviter_user_id user_id,a1.invite_type tools_type_id
        , a2.invite_status # 3邀请成功,其他未知
        ,1 AS join_type # 1邀请 2被邀请
    FROM fe_activity.sf_activity_invitation a1
    JOIN fe_activity.sf_activity_invitation_detail a2 ON a1.invite_id = a2.invite_id
    WHERE a1.add_time >= @sweek AND  a1.add_time < ADDDATE(@sweek,7)
    AND a1.data_flag = 1 AND a1.invite_type IN(1,2,3)
    UNION ALL
    SELECT DISTINCT a2.invitee_user_id user_id,a1.invite_type
       , a2.invite_status,2 AS join_type
    FROM fe_activity.sf_activity_invitation a1
    JOIN fe_activity.sf_activity_invitation_detail a2 ON a1.invite_id = a2.invite_id
    WHERE a1.add_time >= @sweek AND a1.add_time < ADDDATE(@sweek,7)
    AND a1.data_flag = 1 AND a1.invite_type IN(1,2,3)
    UNION ALL #分享红包 (4)
    SELECT DISTINCT a.member_id user_id,4 tools_type_id,1 invite_status,IF(b.link_id IS NOT  NULL,1,2) join_type
    FROM fe_activity.sf_activity_order_coupon_record a
    LEFT JOIN fe_activity.sf_activity_order_link b ON b.link_id=a.link_id AND b.creator_id=a.member_id
    WHERE a.create_time >= @sweek AND a.create_time < ADDDATE(@sweek,7)
    UNION ALL
    SELECT DISTINCT a.USER_ID,4 tools_type_id,3 invite_status,2 join_type
    FROM fe.sf_coupon_record  a
    JOIN fe.sf_coupon b ON a.COUPON_ID=b.COUPON_ID
    WHERE   a.COUPON_TIME >= @sweek AND a.COUPON_TIME < ADDDATE(@sweek,7)
        AND a.COUPON_CHANNEL = 3 AND a.COUPON_STATUS=3
    UNION ALL#免单用户(5)
    SELECT DISTINCT a1.user_id,5 tools_type_id,3 invite_status,1 join_type
    FROM fe_activity.sf_prize_record a1
    WHERE a1.activity_type=5 AND a1.DATA_FLAG=1
        AND a1.add_time>=@sweek AND a1.add_time < ADDDATE(@sweek,2) AND DAYOFWEEK(add_time)=2
    ;
#插入数据
INSERT INTO fe_dm.dm_ma_MarketingTools_weekly
    (sdate, tools_type_id, user_type_activity, users_join,users_buy)
SELECT @sweek,IFNULL(tools_type_id,6)  tools_type_id,user_life_cycle_genera
     ,COUNT(DISTINCT t1.user_id) users_join
     ,COUNT(DISTINCT if(invite_status=3 AND buy_rate_fw>0,user_id,null ))  users_buy
FROM
    (SELECT t1.*,t3.buy_rate,t3.buy_rate_fw,IFNULL(t2.user_life_cycle_genera,0) user_life_cycle_genera
    FROM fe_dm.tmp_user_tool t1
    left JOIN fe_dm.temp_user_life_cycle t2 ON t2.user_id=t1.user_id
    LEFT JOIN fe_dm.temp_user_sale t3 ON  t3.user_id=t1.user_id
    ) t1
GROUP BY user_life_cycle_genera,tools_type_id
#WITH ROLLUP HAVING  user_life_cycle_genera IS NOT NULL
;
INSERT INTO fe_dm.dm_ma_MarketingTools_weekly
    (sdate, tools_type_id, user_type_activity, users_join,users_invite,users_buy)
SELECT @sweek,IFNULL(tools_type_id,6)  tools_type_id,6 user_life_cycle_genera
     ,count(distinct if(join_type=1,user_id,null)) users_join
     ,count(distinct if(join_type=2,user_id,null)) users_invite
     ,COUNT(DISTINCT if(invite_status=3 AND buy_rate_fw>0,user_id,null )) users_buy
FROM
    (SELECT t1.*,t3.buy_rate,t3.buy_rate_fw
    FROM fe_dm.tmp_user_tool t1
    LEFT JOIN fe_dm.temp_user_sale t3 ON  t3.user_id=t1.user_id
    where t1.tools_type_id in (1,3,4) #邀请新用户, 邀请好友拆礼包,分享红包
    ) t1
GROUP BY tools_type_id
;
#更新名称
    # 用户生命周期大类:(1:导入期,2:成长期,3:成熟期,4:休眠期,5:流失期)
UPDATE fe_dm.dm_ma_MarketingTools_weekly a1
SET a1.tools_type_name=CASE a1.tools_type_id WHEN 1 THEN '邀请新用户' WHEN 2 THEN '邀请好友拼单' WHEN 3 THEN '邀请好友拆礼包'
    WHEN 4 THEN '分享红包' WHEN 5 THEN '免单用户' ELSE'汇总' END
  ,a1.user_type_activity_name=CASE a1.user_type_activity WHEN 1 THEN '导入期' WHEN 2 THEN '成长期' WHEN 3 THEN '成熟期'
    WHEN 4 THEN '休眠期' WHEN 5 THEN '流失期' when 6 then '汇总'  ELSE '无' END
WHERE a1.sdate=@sweek;
-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_MarketingTools_weekly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END