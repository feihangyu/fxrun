CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_group_activity_order_day`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
#活动订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_temp_result;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_temp_result(KEY(order_user_id),KEY(order_id))
AS
SELECT 
DATE_FORMAT(d.order_date,'%Y%m') the_month,#月份
d.order_date,#订单日期
d.order_user_id,#用户ID
a.activity_id,#活动ID
a.activity_name,#活动名称
o.order_id,#订单ID
a.start_time,#活动开始时间
a.end_time,#活动结束时间
IFNULL(o.order_amount,0) order_amount,#订单金额
s.activity_user_num#参与活动人数
FROM fe_dwd.dwd_group_order_refound_address_day d
LEFT OUTER JOIN fe_activity.sf_activity_invitation_order o ON o.order_id=d.order_id
LEFT JOIN fe_activity.sf_activity_invitation i ON o.invite_id=i.invite_id
LEFT JOIN fe_activity.sf_activity a ON a.activity_id=i.activity_id
LEFT JOIN fe_activity.sf_activity_group_stat s ON s.activity_id=a.activity_id
WHERE o.data_flag=1 AND i.data_flag=1 AND a.data_flag=1
#增量抽取一天的数据
AND d.order_date >= SUBDATE(CURDATE(), INTERVAL 1 DAY) 
AND d.order_date < CURDATE();
#当月的首次下单信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_first_order_refound_temp;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_first_order_refound_temp(KEY(order_user_id),KEY(order_id))
AS
SELECT 
order_user_id,
order_id,
DATE_FORMAT(d.order_date,'%Y%m') the_month, 
MIN(d.order_date) min_order_date
FROM fe_dwd.dwd_group_order_refound_address_day d 
#增量抽取一天的数据
WHERE d.order_date >= SUBDATE(CURDATE(), INTERVAL 1 DAY) 
AND d.order_date < CURDATE()
GROUP BY order_user_id,DATE_FORMAT(d.order_date,'%Y%m');
#插入数据
DELETE FROM fe_dwd.dwd_group_activity_order_day WHERE  order_date >= SUBDATE(CURDATE(), INTERVAL 1 DAY) AND order_date < CURDATE();
INSERT INTO fe_dwd.dwd_group_activity_order_day(
the_month,order_date,min_order_date,is_first_order,activity_id,
activity_name,start_time,end_time,order_user_id,order_id,order_amount,activity_user_num
)
SELECT
r.the_month,#月份
r.order_date,#订单日期
t.min_order_date,#本月首次下单时间
CASE WHEN t.min_order_date>r.start_time THEN '是'
     ELSE '否' END AS is_first_order,#本月首次下单是否在活动期间
r.activity_id,#活动ID
r.activity_name,#活动名称
r.start_time,#活动开始时间
r.end_time,#活动结束时间
r.order_user_id,#用户ID
r.order_id,#优惠券订单ID
r.order_amount, #GMV金额
r.activity_user_num #参与活动人数
FROM fe_dwd.dwd_group_order_refound_temp_result r
LEFT JOIN fe_dwd.dwd_group_first_order_refound_temp t ON r.the_month=t.the_month AND r.order_user_id=t.order_user_id
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_group_activity_order_day',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('郑志省@', @user, @timestamp)
  );
  
COMMIT;	
END