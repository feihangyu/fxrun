CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_sale_monthly`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
set @sdate=p_sdate;
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
SET @date0=IF(DAY(@sdate)<5,DATE_SUB(@smonth,INTERVAL 1 MONTH ),@smonth );
DELETE FROM feods.d_ma_shelf_sale_monthly WHERE (smonth>=@date0 AND smonth<=@smonth)  OR smonth<date_sub(@smonth,INTERVAL 40 MONTH ) ;
# 插入当月数据
INSERT INTO feods.d_ma_shelf_sale_monthly
    ( smonth, SHELF_ID  ,shelf_type,manager_id,city_name,workday_num,workday_num_lm
    , sale_num, GMV, amount, after_pay_amount, DISCOUNT_AMOUNT, COUPON_AMOUNT, order_num,user_num,refund_amount
    ,user_num_reorder,GMV_lm,amount_lm)
SELECT  DATE_FORMAT(a1.sdate,'%Y-%m-01') smonth, SHELF_ID,shelf_type,manager_id,city_name,workday_num,workday_num_lm
    , sale_num_m, GMV_m, amount_m, after_pay_amount_m, DISCOUNT_AMOUNT_m, COUPON_AMOUNT_m, order_num_m,user_num_m,refund_amount_m
    ,user_num_reorder_m,GMV_lm,amount_lm
FROM feods.d_ma_shelf_sale_daily a1
WHERE a1.sdate IN
    (SELECT MAX(sdate) FROM feods.d_ma_shelf_sale_daily WHERE sdate>=@date0  AND sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH ) AND sdate<CURDATE()  GROUP BY MONTH(sdate) )
;
#上月数据
update feods.d_ma_shelf_sale_monthly a1
join feods.d_ma_shelf_sale_monthly a2 on a2.SHELF_ID=a1.SHELF_ID and a2.smonth=date_sub(a1.smonth,interval 1 month )
set a1.order_num_lm=a2.order_num,a1.user_num_lm=a2.user_num
where a1.smonth=@smonth or a1.smonth=date_sub(@smonth,interval 1 month )
;

-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('prc_d_ma_shelf_sale_monthly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
END