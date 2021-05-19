CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_pub_user_integral_result`()
BEGIN
	SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
	SET @yesterday := SUBDATE(CURDATE(), 1);
	-- 获取积分的临时表
	DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_get_integral;
	CREATE TEMPORARY TABLE fe_dm.tmp_get_integral(INDEX(sdate)) AS
	SELECT DATE(from_time) AS sdate, 
	COUNT(DISTINCT user_id ) AS get_integral_user_num, 
	SUM(increase) AS accumulated_get_integral
	FROM fe_dwd.dwd_pub_user_integral_record
	WHERE increase_type=1 # 增加积分
	AND from_time >= @yesterday
	GROUP BY DATE(from_time);
	-- 消耗积分的临时表
	DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_consume_integral;
	CREATE TEMPORARY TABLE fe_dm.tmp_consume_integral(INDEX(sdate)) AS
	SELECT DATE(from_time) AS sdate,
	COUNT(DISTINCT user_id ) AS consume_integral_user_num,
	SUM(increase)*(-1) AS accumulated_consume_integral
	FROM fe_dwd.dwd_pub_user_integral_record
	WHERE increase_type=2 # 消耗积分
	AND from_time >= @yesterday
	GROUP BY DATE(from_time);
	
	
		
	-- 积分消耗(积分兑换)--临时表
        DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_exchange_integral;
        CREATE TEMPORARY TABLE fe_dm.tmp_exchange_integral(INDEX(sdate)) AS
        SELECT DATE(from_time)AS sdate, 
        COUNT(DISTINCT user_id ) AS  exchange_user_num,
        SUM(increase)*(-1) AS exchange_consume_integral
        FROM fe_dwd.dwd_pub_user_integral_record
        WHERE increase_type=2 #消耗积分
        AND from_type =-1
        AND from_time  >= @yesterday
        GROUP BY DATE(from_time);
	
	-- 积分消耗(积分抽奖)--临时表
        DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_lottery_integral;
        CREATE TEMPORARY TABLE fe_dm.tmp_lottery_integral(INDEX(sdate)) AS
        SELECT DATE(from_time)AS sdate, 
        COUNT(DISTINCT user_id ) AS  lottery_user_num,
        SUM(increase)*(-1) AS lottery_consume_integral
        FROM fe_dwd.dwd_pub_user_integral_record
        WHERE increase_type=2 #消耗积分
        AND from_type =6
        AND from_time  >= @yesterday
        GROUP BY DATE(from_time);
        -- 积分兑换获取优惠券转化--临时表
        DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_integral_exchange_coupon;
        CREATE TEMPORARY TABLE fe_dm.tmp_integral_exchange_coupon(INDEX(sdate)) AS
        SELECT DATE(so.order_date)AS sdate,
        COUNT(DISTINCT so.user_id) AS exchange_coupon_user_num
        FROM fe_dwd.dwd_pub_order_item_recent_one_month so   
        JOIN fe_dwd.dwd_sf_coupon_use sc ON sc.order_id = so.order_id
        JOIN fe_dwd.dwd_sf_user_present sup ON sup.present_obj_id = sc.coupon_id
        WHERE sc.coupon_status= 3  #已使用
        AND so.ORDER_STATUS IN (2,7)
        AND sup.present_type=1 # 优惠券来源积分兑换领取
        AND so.order_date >= @yesterday
        GROUP BY DATE(so.order_date);
        -- 积分抽奖获取优惠券转化--临时表
        DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_integral_lottery_coupon;
        CREATE TEMPORARY TABLE fe_dm.tmp_integral_lottery_coupon(INDEX(sdate)) AS
        SELECT DATE(so.order_date)AS sdate,
        COUNT(DISTINCT so.user_id) AS lottery_coupon_user_num
        FROM fe_dwd.dwd_pub_order_item_recent_one_month so   
        JOIN fe_dwd.dwd_sf_coupon_use sc ON sc.order_id = so.order_id
        WHERE sc.coupon_status= 3  #已使用
        AND so.ORDER_STATUS IN (2,7) # 订单状态(2:已付款;7:出货成功;)
        AND sc.coupon_channel =13 # 优惠券来源积分抽奖领取
        AND so.order_date >= @yesterday
        GROUP BY DATE(so.order_date);
	
	
	
	# 删除昨天的数据
	DELETE FROM fe_dm.dm_ma_user_integral_statistic WHERE sdate >= @yesterday;
	INSERT INTO fe_dm.dm_ma_user_integral_statistic(sdate)  VALUES (@yesterday);    
	
	-- 更新获取积分
	UPDATE fe_dm.dm_ma_user_integral_statistic target JOIN fe_dm.tmp_get_integral source
	SET target.get_integral_user_num=source.get_integral_user_num,  target.accumulated_get_integral=source.accumulated_get_integral
	WHERE target.sdate = source.sdate;
	-- 更新消耗积分
	UPDATE fe_dm.dm_ma_user_integral_statistic target JOIN fe_dm.tmp_consume_integral source
	SET target.consume_integral_user_num=source.consume_integral_user_num,  target.accumulated_consume_integral=source.accumulated_consume_integral
	WHERE target.sdate = source.sdate;
	-- 更新积分余额
	UPDATE fe_dm.dm_ma_user_integral_statistic SET integral_balance = accumulated_get_integral-accumulated_consume_integral;
	
	-- 更新积分消耗--积分兑换
	UPDATE fe_dm.dm_ma_user_integral_statistic target JOIN fe_dm.tmp_exchange_integral source
	SET target.exchange_user_num=source.exchange_user_num,  
	target.exchange_consume_integral=source.exchange_consume_integral
	WHERE target.sdate = source.sdate;
	-- 更新积分消耗--积分抽奖
	UPDATE fe_dm.dm_ma_user_integral_statistic target JOIN fe_dm.tmp_lottery_integral source
	SET target.lottery_user_num=source.lottery_user_num,  
	target.lottery_consume_integral=source.lottery_consume_integral
	WHERE target.sdate = source.sdate;
	-- 积分抽奖获取优惠券转化
	UPDATE fe_dm.dm_ma_user_integral_statistic target JOIN fe_dm.tmp_integral_lottery_coupon source
	SET target.lottery_coupon_user_num=source.lottery_coupon_user_num
	WHERE target.sdate = source.sdate;
	-- 积分兑换获取优惠券转化
	UPDATE fe_dm.dm_ma_user_integral_statistic target JOIN fe_dm.tmp_integral_exchange_coupon source
	SET target.exchange_coupon_user_num=source.exchange_coupon_user_num
	WHERE target.sdate = source.sdate;
	
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_pub_user_integral_result',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱慧敏@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_user_integral_statistic','dm_pub_user_integral_result','朱慧敏');
  COMMIT;	
END