CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_user_flag1`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate=CURDATE();
SET sql_mode='NO_ENGINE_SUBSTITUTION';
TRUNCATE TABLE feods.zs_shelf_member_flag
;
# 获得客户购近24周购买数据
SET @time_36 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.tmp_user_sale; #最近24周销售 2m24s
CREATE TEMPORARY TABLE  feods.tmp_user_sale(INDEX(user_id)) AS
    SELECT user_id
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 1 WEEK)  THEN buy_rate ELSE 0 END) AS last_num  -- 近一周
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 5 WEEK)  THEN buy_rate ELSE 0 END) AS 5_num  -- 近五周
        ,SUM(CASE WHEN b.sdate<DATE_SUB(@sdate,INTERVAL 5 WEEK)  THEN buy_rate ELSE 0 END) AS 19_num  -- 近19周
        ,IFNULL(SUM(buy_rate),0) AS 24_num #近24周订单
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 1 WEEK)  THEN gmv ELSE 0 END) AS last_gmv
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 7 WEEK)  THEN gmv ELSE 0 END ) gmv
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 7 WEEK)  THEN buy_rate ELSE 0 END ) buy_rate
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 7 WEEK)  THEN coupon_order ELSE 0 END ) coupon_order
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 7 WEEK)  THEN discount_order ELSE 0 END ) discount_order
        ,SUM(CASE WHEN b.sdate>=DATE_SUB(@sdate,INTERVAL 7 WEEK)  THEN share_order ELSE 0 END ) share_order
    FROM feods.zs_user_week_sale b
    WHERE sdate>= DATE_SUB(@sdate,INTERVAL 24 WEEK) AND sdate<@sdate
    GROUP BY user_id
;
DROP TEMPORARY TABLE IF EXISTS feods.tmp_user_info; #用户购买信息 2m17s
CREATE TEMPORARY TABLE  feods.tmp_user_info(INDEX(user_id),INDEX(MOBILE_PHONE)) AS
    SELECT a1.user_id
           ,a1.BIRTHDAY,a1.MOBILE_PHONE
               ,REG_CHANNEL
               ,case when a1.OPEN_TYPE is null then 'wechat' else ifnull(substring_index(OPEN_TYPE,'/',1),OPEN_TYPE) end OPEN_TYPE
               ,a1.if_sfer,a1.manager_type,a2.last_shelf_id,a2.max_order_date
           ,CASE WHEN IFNULL(a2.gmv,0)=0 THEN 1
               WHEN (a2.min_order_date>=DATE_SUB(@sdate,INTERVAL 7 DAY) AND a2.min_order_date<@sdate) THEN 2
               ELSE 3 END AS user_type_buy_time_new   -- 用户购买次数-新
           ,CASE WHEN a.last_num>0 THEN 1 ELSE 0 END AS if_buy_this_week                                    #  这周是否购买（周五凌晨5点更新）,1-购买了,0-没有购买
           ,CASE WHEN (a2.min_order_date>=DATE_SUB(@sdate,INTERVAL 7 DAY) AND a2.min_order_date<@sdate) THEN 1    -- 首次购买日期 在近一周内
                 WHEN (CREATE_DATE>=DATE_SUB(@sdate,INTERVAL 7 DAY) AND CREATE_DATE<@sdate AND IFNULL(a2.min_order_date,CURDATE())>=CURDATE() ) THEN 8         -- 上周新增但未购买的用户
                 WHEN (a.5_num>0 AND a.19_num=0) THEN 2 #购买回流用户: 前5周购买，前6-24周未购买
                 WHEN IFNULL(a.24_num,0)=0 THEN 3 #流失期:前24周未购买用户
                 WHEN a.5_num=0 AND a.24_num>0 THEN 4 #沉默:前5周未购买用户,前6-24周有购买
                 WHEN a.5_num>=3 THEN 5 #前5周下单3次及3次以上的用户
                 WHEN a.5_num=2 THEN 6  #前5周下单2次的用户
                 WHEN a.5_num=1 THEN 7  #前5周下单1次的用户
                 END AS user_type_activity   -- 用户活跃类型
          ,a.last_gmv
    FROM fe_dwd.dwd_user_day_inc a1
    LEFT JOIN feods.tmp_user_sale a ON a.user_id =a1.user_id
    LEFT JOIN feods.d_op_su_u_stat a2 ON a2.user_id=a1.user_id #用户统计表
    WHERE a1.CREATE_DATE<@sdate
;
SET @time_38 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag1','@time_36--@time_38',@time_36,@time_38);
SET @time_45 := CURRENT_TIMESTAMP();
INSERT INTO feods.zs_shelf_member_flag #6m41s
    (sdate
    ,user_id
    ,gender                            -- 性别
    ,age_level                         -- 年龄层
    ,age                               -- 年龄
    ,user_xingzuo                      -- 星座
    ,MOBILE_PHONE                      -- 手机号
    ,REG_CHANNEL                       -- 注册渠道
    ,user_type_buy_new                 -- 注册/购买-新
    ,member_level                      -- 用户等级
    ,ext6                              -- 是否顺丰员工
    ,if_shelf_admin                    -- 是否顺丰店主
    ,reg_client                        -- 客户端类型
    ,shelf_id                          -- 货架id
    ,last_buy_time                     -- 最近一次购买时间
    ,user_type_buy_time_new            -- 用户购买次数-新
    ,if_buy_this_week                  -- 这周是否购买
    ,user_type_activity                -- 用户活跃类型
    ,gmv_last                          -- 上周gmv
    ,ext3 #用户ID基偶数
    ,if_wechat_subscribe #是否关注微信公众号
    )
SELECT
    CURRENT_DATE() AS sdate
    ,a.user_id
    ,COALESCE(c.SEX,d.sex,3) AS gender
    ,CASE WHEN a.BIRTHDAY IS NULL THEN 99
          WHEN YEAR(a.BIRTHDAY) BETWEEN '1960' AND '1969' THEN 1
          WHEN YEAR(a.BIRTHDAY) BETWEEN '1970' AND '1979' THEN 2
          WHEN YEAR(a.BIRTHDAY) BETWEEN '1980' AND '1989' THEN 3
          WHEN YEAR(a.BIRTHDAY) BETWEEN '1990' AND '1999' THEN 4
          WHEN YEAR(a.BIRTHDAY) BETWEEN '2000' AND '2009' THEN 5
          ELSE 88
     END AS age_level
    ,FLOOR(DATEDIFF(@sdate,a.BIRTHDAY)/365) AS age
    ,CASE WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0321' AND '0420' THEN 1
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0421' AND '0521' THEN 2
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0522' AND '0621' THEN 3
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0622' AND '0722' THEN 4
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0723' AND '0822' THEN 5
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0823' AND '0923' THEN 6
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0924' AND '1023' THEN 7
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '1024' AND '1122' THEN 8
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '1123' AND '1221' THEN 9
          WHEN ((DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '1222' AND '1231') OR (DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0101' AND '0120')) THEN 10
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0120' AND '0219' THEN 11
          WHEN DATE_FORMAT(a.BIRTHDAY,'%m%d') BETWEEN '0220' AND '0320' THEN 12
          ELSE 99
     END AS user_xingzuo
    ,a.MOBILE_PHONE
    ,a.REG_CHANNEL
    ,CASE WHEN a.MOBILE_PHONE IS NOT NULL THEN 1
          WHEN a.MOBILE_PHONE IS NULL THEN 2
        END AS user_type_buy_new
    ,b2.member_level
    ,a.if_sfer
    ,IF(a.manager_type<>'非店主',0,1) AS if_shelf_admin
    ,case when OPEN_TYPE='wechat' then  1
        when OPEN_TYPE='ccb' then  2
        when OPEN_TYPE='union' then  3
        when OPEN_TYPE='cmb' then  4
        when OPEN_TYPE='SFIM' then  5
        when OPEN_TYPE='XMF' then  6
        when OPEN_TYPE='ST_PAY' then  7
        when OPEN_TYPE='HE_BAO_PAY' then  8
        else 0 end reg_client   -- ifnull(g.OPEN_TYPE,1) AS reg_client
    ,a.last_shelf_id
    ,a.max_order_date
    ,a.user_type_buy_time_new          -- 用户购买次数-新
    ,a.if_buy_this_week                -- 这周是否购买
    ,a.user_type_activity              -- 用户活跃类型
    ,a.last_gmv                 -- 上周gmv
    ,IF(MOD(RIGHT(a.user_id,1),2)=0,2,1) #用户ID基偶数
    ,IF(a2.SUBSCRIBE=1,1,0) if_wechat_subscribe
FROM  feods.tmp_user_info a
LEFT JOIN fe.pub_user_integral_growth b2 ON a.user_id=b2.user_id                                     -- 用户积分成长值表
LEFT JOIN feods.zs_user_receive_tech_new c ON (c.mobile_phone=a.mobile_phone AND c.sex!=3)             -- 收件
LEFT JOIN feods.zs_user_send_tech_new d ON (d.mobile_phone=a.mobile_phone AND d.sex!=3)                -- 寄件
LEFT JOIN fe.pub_user_wechat a2 ON a2.user_id=a.user_id  #微信客户端
GROUP BY a.user_id
;
SET @time_46 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag1','@time_45--@time_46',@time_45,@time_46);
-- 建临时表统计 客单价  优惠券敏感度  商品折扣敏感度  分享红包敏感度
SET @time_49 := CURRENT_TIMESTAMP();
DROP TEMPORARY TABLE IF EXISTS feods.tmp_user_sale_7w ;
CREATE TEMPORARY TABLE feods.tmp_user_sale_7w(INDEX(user_id)) AS
    SELECT  user_id
        ,CASE WHEN IFNULL(order_rate,0)<4 THEN 1
             WHEN order_rate<8 THEN 2
             WHEN order_rate<12 THEN 3
             ELSE 4 END AS user_type_buy_kds_new -- 用户购买客单价-新
        ,CASE WHEN IFNULL(coupon_rate,0)<0.5 THEN 1
             WHEN coupon_rate<0.9 THEN 2
             ELSE 3 END AS coupon_type -- 优惠券敏感度
        ,CASE WHEN IFNULL(discount_rate,0)<0.5 THEN 1
             WHEN discount_rate<0.9 THEN 2
             ELSE 3 END AS discount_type -- 商品折扣敏感度
        ,CASE WHEN IFNULL(share_rate,0)<0.3 THEN 1
             WHEN share_rate<0.6 THEN 2
             ELSE 3 END AS share_type -- 分享红包敏感度
    FROM
        (SELECT user_id
            ,ROUND(gmv/buy_rate,1) AS order_rate
            ,ROUND(coupon_order/buy_rate,1)AS coupon_rate
            ,ROUND(discount_order/buy_rate,1)AS discount_rate
            ,ROUND(share_order/buy_rate,1)AS share_rate
        FROM feods.tmp_user_sale) a1
    ;
UPDATE feods.zs_shelf_member_flag b
JOIN feods.tmp_user_sale_7w a ON a.user_id=b.user_id
SET b.user_type_buy_kds_new=a.user_type_buy_kds_new
    ,b.coupon_type=a.coupon_type
    ,b.discount_type=a.discount_type
    ,b.share_type=a.share_type
;
SET @time_68 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag1','@time_66--@time_68',@time_49,@time_68);
SET @time_76 := CURRENT_TIMESTAMP();
-- 更新 是否撤架 省市区代码 公司行业&名称
UPDATE feods.zs_shelf_member_flag AS b
JOIN  fe_dwd.dwd_shelf_base_day_all a
    ON a.shelf_id=b.shelf_id
SET b.if_revoke=IF(a.SHELF_STATUS=3,1,0) ,
    b.PROVINCE=a.PROVINCE,
    b.CITY=a.CITY,
    b.DISTRICT=a.DISTRICT,
    b.shelf_status=a.SHELF_STATUS,
    b.company_name=a.company_name,
    b.company_type=a.BELONG_INDUSTRY;
SET @time_78 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag1','@time_76--@time_78',@time_76,@time_78);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('prc_d_ma_user_flag1',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
END