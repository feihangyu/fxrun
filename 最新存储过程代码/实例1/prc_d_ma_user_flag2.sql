CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_user_flag2`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate=CURRENT_DATE;
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)); #周一
SET @time_1 := CURRENT_TIMESTAMP();
#获取用户研究表临时信息
DROP TEMPORARY TABLE  IF EXISTS feods.tmp_user_info;
CREATE TEMPORARY TABLE feods.tmp_user_info(INDEX (user_id)) AS
    SELECT user_id
        ,user_life_cycle,week_order_qty,CREATE_DATE,kdj,last_week_order_qty,stock_value_5,stock_value,STOCK_QUANTITY,if_coupon,OPEN_TYPE
        ,CASE WHEN (sh_process.func_if_leay_year(CURDATE())=1 OR (sh_process.func_if_leay_year(CURDATE())=0 AND DATE_FORMAT(BIRTHDAY,'%m-%d')<>'02-29'))
                AND DATE_FORMAT(a1.BIRTHDAY,'%m-%d')>=DATE_FORMAT(CURDATE(),'%m-%d')
                THEN CONCAT(YEAR(CURDATE()),'-',DATE_FORMAT(BIRTHDAY,'%m-%d'))
              WHEN DATE_FORMAT(a1.BIRTHDAY,'%m-%d')<DATE_FORMAT(CURDATE(),'%m-%d') AND (sh_process.func_if_leay_year(DATE_ADD(CURDATE(),INTERVAL 1 YEAR ))=1
                OR (sh_process.func_if_leay_year(DATE_ADD(CURDATE(),INTERVAL 1 YEAR ))=0 AND DATE_FORMAT(BIRTHDAY,'%m-%d')<>'02-29'))
                THEN CONCAT(YEAR(CURDATE())+1,'-',DATE_FORMAT(BIRTHDAY,'%m-%d'))
              ELSE NULL END birthday_year
    FROM feods.user_research a1;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag2','@time_1--@time_2',@time_1,@time_2);
UPDATE feods.zs_shelf_member_flag a
JOIN feods.tmp_user_info b  ON b.user_id=a.user_id
SET
    a.user_life_cycle_genera = (CASE WHEN user_life_cycle='新用户' THEN 1 #'导入期'
        WHEN user_life_cycle='当周初次购买用户' THEN 2 #'成长期'
        WHEN user_life_cycle IN ('持续高频','持续低频','单调上涨','单调下降','高频波动','低频波动','回流用户') THEN 3 #'成熟期'
        WHEN user_life_cycle IN ('撤架流失用户','8周无购买流失用户') THEN 4 #'流失期'
        WHEN user_life_cycle='沉默用户' THEN 5 #'休眠期'
        ELSE 0 END )
    #recent_buy_status 最近购买状况  近4周购买状况(1:近1周有购买,2:近1周未购买,3:近2周未购买...)
    ,a.recent_buy_status = (CASE WHEN SUBSTRING_INDEX(b.week_order_qty,',',-1)>0 THEN 1 #近一周有购买
            WHEN SUBSTRING_INDEX(b.week_order_qty,',',-2)>0 THEN 2 #近一周未购买
            WHEN SUBSTRING_INDEX(b.week_order_qty,',',-3)>0  THEN 3 #近二周未购买
            WHEN SUBSTRING_INDEX(b.week_order_qty,',',-4)>0 THEN 4 #近三周未购买
            WHEN SUBSTRING_INDEX(b.week_order_qty,',',-4)=0 THEN 5 #近四周未购买
            ELSE 6 END  )
    #if_new_register   是否注册一个月内
    ,a.if_new_register=IF(TIMESTAMPDIFF(MONTH,CREATE_DATE,CURDATE())<1,1,0)
    #pct_level 客单价层级
    ,a.pct_level=(CASE WHEN b.kdj>=20 THEN 4
        WHEN b.kdj>=15 AND b.kdj<20 THEN 3
        WHEN b.kdj>=10 AND b.kdj<15 THEN 2
        WHEN b.kdj>=5 AND b.kdj<10 THEN 1
        ELSE 0 END)
    ,a.last_week_order_qty_level =IF(b.last_week_order_qty>7,8,last_week_order_qty)
    ,a.birthday_if_this_week =IF(b.birthday_year BETWEEN CURDATE() AND DATE_ADD(CURDATE(),INTERVAL 6 DAY),1,0)
    ,a.birthday_weekday = IF(DAYOFWEEK(b.birthday_year)=1,7,DAYOFWEEK(b.birthday_year)-1)
    #if_unsalable_user 严重滞销货架
    ,a.if_unsalable_user= IF(b.stock_value_5/b.stock_value>0.5,1,0)
    #if_full_stock_user 库存充足货架
    ,a.if_full_stock_user=IF(b.STOCK_QUANTITY>=120,1,0)
    ,a.if_coupon=b.if_coupon
    #,a.if_wechat_subscribe=IF(b.OPEN_TYPE IS  NULL,1,0)
     # ext2  用户类型 (1:8周无购买流失用户,2: 持续高频,3: 单调上涨,4:单调下降,5:沉默用户,6:高频波动,7:低频波动,8:持续低频,9:当周初次购买用户,10:撤架流失用户,11:新用户,0:其他)
    ,a.ext2=(CASE b.user_life_cycle WHEN  '8周无购买流失用户' THEN 1 WHEN '持续高频' THEN 2 WHEN '单调上涨' THEN 3 WHEN '单调下降' THEN 4
            WHEN '沉默用户' THEN 5 WHEN '高频波动' THEN 6 WHEN '低频波动' THEN  7 WHEN'持续低频' THEN 8 WHEN '当周初次购买用户' THEN 9
            WHEN '撤架流失用户' THEN 10 WHEN '新用户' THEN 11  ELSE 0 END )
    ;
	
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag2','@time_2--@time_3',@time_2,@time_3);
#保留近8周截存
SET @spartition=CONCAT('pweek',MOD(YEARWEEK(@sweek),100));
SET @spartition_delete= CONCAT('pweek',MOD(YEARWEEK(SUBDATE(@sweek,7*9)),100));
SET @sql1=CONCAT('alter table feods.zs_shelf_member_flag_history truncate partition ',@spartition);
SET @sql2=CONCAT('alter table feods.zs_shelf_member_flag_history truncate partition ',@spartition_delete);
    #先删除数据
PREPARE sql_str FROM @sql1;EXECUTE sql_str;
PREPARE sql_str FROM @sql2;EXECUTE sql_str;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag2','@time_3--@time_4',@time_3,@time_4);
    #插入数据
INSERT INTO feods.zs_shelf_member_flag_history
(sdate,user_id,shelf_id,user_life_cycle_genera,user_type_activity)
SELECT @sweek,user_id,shelf_id,user_life_cycle_genera,user_type_activity
FROM feods.zs_shelf_member_flag
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info('prc_d_ma_user_flag2','@time_4--@time_5',@time_4,@time_5);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('prc_d_ma_user_flag2',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
END