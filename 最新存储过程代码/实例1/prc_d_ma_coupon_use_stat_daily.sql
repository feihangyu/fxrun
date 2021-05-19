CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_coupon_use_stat_daily`()
begin
SET @run_date:= CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
delete from feods.d_ma_coupon_use_stat_daily where sdate>=subdate(curdate(),15) or sdate<subdate(curdate(),90) ;
insert into feods.d_ma_coupon_use_stat_daily
    (sdate, coupon_id, coupon_name, coupon_usage, cost_dept, business_type, discount_type, reach_amount, discount_amount, discount, use_num)
select date(a2.used_time) used_date,a1.coupon_id,a1.coupon_name,a1.coupon_usage
    ,case  a1.cost_dept when 1 then '市场组' when 2 then '运营组' when 3 then '采购组' when 4 then '大客户组' when 5 then 'BD' when 6 then '经规组'else '其他' end cost_dept
    ,case a1.business_type when 1 then '优惠券推送' when 2 then '活动推送' when 3 then '商品促销' when 4	then '新品上架' else '其他'end business_type
    ,case a1.discount_type when 1 then '满减' when  2 then '立减' when 3 then '折扣' else '其他' end discount_type
    ,a1.reach_amount,if(a1.discount_type in (1,2),a1.discount_amount,null) discount_amount,if(a1.discount_type=3,a1.discount_amount,null) discount
    ,sum(1) use_num
from fe.sf_coupon_model a1
join fe.sf_coupon_use a2 on a1.coupon_id=a2.coupon_id and a2.data_flag=1
where a2.used_time >=subdate(curdate(),15) and a2.used_time<curdate() and a1.data_flag=1
group by used_date,a1.coupon_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'prc_d_ma_coupon_use_stat_daily',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('纪伟铨@',@user,@timestamp)
);
END