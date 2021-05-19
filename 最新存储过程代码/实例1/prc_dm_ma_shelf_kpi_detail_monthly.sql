CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_shelf_kpi_detail_monthly`(IN p_sdate DATE)
BEGIN

SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01') ;
#删除数据
DELETE FROM feods.dm_ma_shelf_kpi_detail_monthly WHERE sdate=@smonth OR sdate<DATE_SUB(@smonth,INTERVAL 24 MONTH);
#插入数据
#插入数据
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_sale;
CREATE TEMPORARY TABLE feods.temp_shelf_sale(INDEX(shelf_id)) AS
    SELECT shelf_id
        ,SUM(quantity_act*sale_price) gmv
        ,SUM(IF(a3.if_work_day=0,quantity_act*sale_price,0)) gmv_holiday
        ,SUM(IF(TIME(a1.order_date) >='19:00' OR TIME(order_date)<'07:00' ,quantity_act*sale_price,0)) gmv_overtime
        ,SUM(IF(sale_price>=5,quantity_act*sale_price,0))  gmv_5above
        ,SUM(IF(a4.SECOND_TYPE_ID=1,quantity_act*sale_price,0)) gmv_sec_type1 ,SUM(IF(a4.SECOND_TYPE_ID=2,quantity_act*sale_price,0)) gmv_sec_type2
        ,SUM(IF(a4.SECOND_TYPE_ID=4,quantity_act*sale_price,0)) gmv_sec_type4,SUM(IF(a4.SECOND_TYPE_ID=5,quantity_act*sale_price,0)) gmv_sec_type5
        ,SUM(IF(a4.SECOND_TYPE_ID=6,quantity_act*sale_price,0)) gmv_sec_type6
        ,COUNT(DISTINCT a1.user_id) users,COUNT(DISTINCT IF(a2.gender>0,a1.user_id,NULL) ) users_gender
        ,COUNT(DISTINCT IF(a2.gender=2,a1.user_id,NULL) ) users_women
    FROM fe_dwd.dwd_order_item_refund_day a1
    JOIN fe_dwd.dwd_user_day_inc a2 ON a2.user_id=a1.user_id
    JOIN fe_dwd.dwd_pub_work_day a3 ON a3.sdate=DATE(a1.order_date)
    JOIN fe_dwd.dwd_product_base_day_all a4 ON a4.PRODUCT_ID=a1.product_id
    WHERE a1.PAY_DATE>=@smonth AND a1.PAY_DATE<DATE_ADD(@smonth,INTERVAL 1 MONTH )
        AND a1.quantity_act>0
    GROUP BY shelf_id;
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_sale2;
CREATE TEMPORARY TABLE feods.temp_shelf_sale2(INDEX(shelf_id)) AS
    SELECT shelf_id,SUM(PAY_AMOUNT) amount,SUM(after_pay_amount) after_pay_amount
    FROM
        (SELECT shelf_id,PAY_AMOUNT,0 after_pay_amount
        FROM fe_dwd.dwd_order_item_refund_day
        WHERE PAY_DATE>=@smonth AND PAY_DATE<DATE_ADD(@smonth,INTERVAL 1 MONTH )
            AND quantity_act>0
        GROUP BY order_id
        UNION ALL
        SELECT shelf_id,0 PAY_AMOUNT,payment_money after_pay_amount
        FROM feods.fjr_shelf_dgmv a1
        WHERE a1.sdate>=@smonth AND a1.sdate<DATE_ADD(@smonth,INTERVAL 1 MONTH )
        ) a1
    GROUP BY shelf_id;

INSERT INTO feods.dm_ma_shelf_kpi_detail_monthly
    (sdate, shelf_id
    , gmv, amount, after_pay_amount, gmv_holiday, gmv_overtime, gmv_5above
    , users, users_gender, users_women
    , gmv_sec_type1, gmv_sec_type2, gmv_sec_type4, gmv_sec_type5, gmv_sec_type6)
SELECT @smonth,a1.shelf_id
    ,ifnull(a2.gmv,0),a1.amount,a1.after_pay_amount ,ifnull(a2.gmv_holiday,0) ,ifnull(a2.gmv_overtime,0) ,ifnull(a2.gmv_5above,0)
    ,ifnull(a2.users,0) ,ifnull(a2.users_gender,0) ,ifnull(a2.users_women,0)
    ,ifnull(a2.gmv_sec_type1,0) ,ifnull(a2.gmv_sec_type2,0) ,ifnull(a2.gmv_sec_type4,0) ,ifnull(a2.gmv_sec_type5,0) ,ifnull(a2.gmv_sec_type6,0)
FROM feods.temp_shelf_sale2 a1
LEFT JOIN feods.temp_shelf_sale a2 ON a2.shelf_id=a1.shelf_id
;

-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_shelf_kpi_detail_monthly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END