CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_ma_coupon_shelf_daily`(IN p_sdate DATE,IN p_update_flag TINYINT)
BEGIN
    #每4小时运行一次,11点前更新昨天数据, 11:02到16:02更新今天数据(2)
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @update_flag=
    CASE WHEN p_update_flag>0 THEN p_update_flag
        WHEN CURRENT_TIME BETWEEN '07:20' AND '07:35' THEN 1 #更新昨天数据
        WHEN CURRENT_TIME BETWEEN '11:00:00' AND '16:10:00' THEN 2 #更新今天数据
        ELSE 0 END ;
SET @date0=CASE @update_flag WHEN 1 THEN SUBDATE(@sdate,1) WHEN 2 THEN @sdate ELSE 0 END;
SET @date1=ADDDATE(@date0,1);
IF @update_flag>0 THEN #在限定时段才运行
#删除数据
DELETE FROM fe_dm.dm_ma_coupon_shelf_daily WHERE sdate=@date0 OR sdate<SUBDATE(@date0,60);
#插入数据
INSERT INTO fe_dm.dm_ma_coupon_shelf_daily
    (sdate, shelf_id, coupon_id, coupon_name
    , coupon_usage, coupon_type, cost_dept, business_type, discount_type, discount_amount, reach_amount, receive_type
    , issue_start_time, issue_end_time, valid_end_time
    , deliver_num, used_num, deliver_shelfs, used_shelfs, coupon_amount,gmv)
SELECT @date0 sdate,a2.shelf_id
    ,a1.coupon_id,coupon_name,coupon_usage
    ,CASE a1.coupon_type WHEN 1 THEN '通用券'  WHEN 2 THEN '商品券'  WHEN 3 THEN '品类券'  WHEN 4 THEN '尝鲜券' ELSE '其他' END coupon_type
    ,CASE  a1.cost_dept WHEN 1 THEN '市场组' WHEN 2 THEN '运营组' WHEN 3 THEN '采购组' WHEN 4 THEN '大客户组' WHEN 5 THEN 'BD' WHEN 6 THEN '经规组'
        ELSE '其他' END cost_dept
    ,CASE a1.business_type WHEN 1 THEN '优惠券推送' WHEN 2 THEN '活动推送' WHEN 3 THEN '商品促销' WHEN 4	THEN '新品上架' ELSE '其他'END business_type
    ,CASE a1.discount_type WHEN 1 THEN '满减' WHEN  2 THEN '立减' WHEN 3 THEN '折扣' ELSE '其他' END discount_type
    ,a1.discount_amount,a1.reach_amount,IFNULL(a4.ITEM_NAME,'其他') receive_type,issue_start_time,issue_end_time,IFNULL(a1.valid_end_time,a1.valid_day)
    ,SUM(IF(a2.received_time>=@date0 AND a2.received_time<@date1,1,0)) deliver_num
    ,SUM(IF(a2.used_time>=@date0 AND a2.used_time<@date1 and a5.ORDER_ID is not null,1,0)) used_num
    ,COUNT(DISTINCT a2.shelf_id) deliver_shelfs,COUNT(DISTINCT a5.shelf_id) used_shelfs
    ,SUM(a5.COUPON_AMOUNT) coupon_amount
    ,IFNULL(SUM(ifnull(a5.PRODUCT_TOTAL_AMOUNT,0)+IFNULL(a5.DISCOUNT_AMOUNT,0)+IFNULL(a5.COUPON_AMOUNT,0)),0) gmv
FROM fe.sf_coupon_model a1
JOIN fe.sf_coupon_scope_delivery a11 ON a11.coupon_id=a1.coupon_id
JOIN fe.sf_coupon_use a2 ON a2.coupon_id=a1.coupon_id
LEFT JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a2.shelf_id
LEFT JOIN fe.pub_dictionary_item a4 ON a4.ITEM_VALUE=a11.receive_type AND a4.DICTIONARY_ID=29
LEFT JOIN fe.sf_order a5 ON a5.ORDER_ID=a2.order_id AND a5.ORDER_STATUS IN (2,6,7) AND a5.DATA_FLAG=1
WHERE (a2.received_time>=@date0 AND a2.received_time<@date1) OR (a2.used_time>=@date0 AND a2.used_time<@date1)
GROUP BY a2.shelf_id, a1.coupon_id
;
END IF;
-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('dm_ma_coupon_shelf_daily',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END