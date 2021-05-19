CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_kpi_data_daily`(IN p_date DATE)
BEGIN
SET @run_date:= CURRENT_DATE(),@user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_date;
SET @date0= DATE_FORMAT(@sdate,'%Y-%m-01') ,@date1 =ADDDATE(@sdate,1)  ;

DELETE FROM fe_dm.dm_ma_kpi_data_daily  WHERE (sdate>=@date0 AND sdate<@date1) OR sdate<DATE_SUB(@date0,INTERVAL 6 MONTH );
#自贩机订单单用户5分钟内去重
SET @rank_user_id=0,@rank_datetime=CURRENT_TIMESTAMP(),@rank_num=1,@shelf_id=0;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_order7;
CREATE TEMPORARY TABLE fe_dm.temp_order7 (INDEX (SHELF_ID)) AS
    SELECT DISTINCT sdate,SHELF_ID,USER_ID,rank_num
    FROM
         (SELECT ORDER_ID,SHELF_ID, USER_ID, sdate
             ,CASE WHEN @rank_user_id=USER_ID AND PAY_DATE>ADDTIME(@rank_datetime,'00:05:00') AND shelf_id=@shelf_id
                 THEN @rank_num :=@rank_num+1 ELSE @rank_num:=1 END  rank_num
             ,@rank_datetime :=PAY_DATE rank_datetime
             ,@rank_user_id :=USER_ID rank_user_id
             ,@shelf_id :=shelf_id shelf_id_temp
        FROM
            (SELECT a1.ORDER_ID,a1.USER_ID,a1.SHELF_ID,DATE(a1.PAY_DATE) sdate,a1.PAY_DATE
            FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
            JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a1.SHELF_ID=a2.SHELF_ID AND a2.SHELF_TYPE=7
            WHERE a1.PAY_DATE>=@date0 AND  a1.PAY_DATE<@date1
            ORDER BY a1.USER_ID,a1.PAY_DATE) a
        ) aa;
#所有货架销售计算
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_shelf_sale;
CREATE TEMPORARY TABLE fe_dm.temp_shelf_sale AS
    SELECT t1.sdate,t1.SHELF_ID
         ,SUM(GMV) GMV,SUM(pay_amount) pay_amount,SUM(AFTER_PAYMENT_MONEY) after_pay_amount
         ,IF(t1.SHELF_TYPE=7,t2.order_num,SUM(t1.orders)) order_num,SUM(user_num) user_num
         ,SUM(batchorder_gmv) batchorder_gmv,SUM(batchorder_amount) batchorder_amount,SUM(batchorder_num) batchorder_num
    FROM
        (SELECT #普通货架订单
            sdate, t1.SHELF_ID,t2.SHELF_TYPE
            ,gmv,t1.pay_amount,t1.orders,t1.users user_num
            ,0 batchorder_gmv,0 batchorder_amount,0 batchorder_num, t1.AFTER_PAYMENT_MONEY
        FROM fe_dwd.dwd_shelf_day_his t1
        JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.SHELF_ID=t1.SHELF_ID
        WHERE sdate>=@date0 AND sdate<@date1 AND IFNULL(t1.gmv,0)+IFNULL(t1.AFTER_PAYMENT_MONEY,0)>0
        UNION ALL #批量电商订单 ,a1.TOTAL_PRICE gmv,SUM(a2.bank_actual_price) amount
        SELECT DATE(APPLY_TIME) sdate ,a1.SUPPLIER_ID shelf_id,a3.shelf_type
            ,0 GMV,0 pay_amount,0 order_num,0 user_num
            ,SUM(a1.TOTAL_PRICE) batchorder_gmv,SUM(a2.bank_actual_price) batchorder_amount,COUNT(1) batchorder_num,0 after_pay_amount
        FROM fe.sf_product_fill_order a1
        JOIN fe.sf_product_fill_order_extend a2 ON a2.order_id = a1.ORDER_ID
        JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.SHELF_ID=a1.SUPPLIER_ID
        WHERE a1.APPLY_TIME>=@date0 AND a1.APPLY_TIME<@date1
            AND a1.order_status = 11 AND  a1.sales_bussniess_channel = 1
            AND a1.sales_order_status = 3 AND a1.sales_audit_status = 2 AND a1.fill_type =13
            AND a2.bank_actual_price>0 # 实收金额大于0
        GROUP BY sdate,a1.SUPPLIER_ID
            ) t1
    LEFT JOIN
        (SELECT sdate,shelf_id,COUNT(1) order_num FROM fe_dm.temp_order7 GROUP BY sdate,shelf_id )t2
            ON t2.sdate=t1.sdate AND t2.SHELF_ID=t1.SHELF_ID
    GROUP BY t1.sdate,t1.SHELF_ID;
#汇总插入
    #日期（按天取）	地区	投放场景	终端状态	终端类型	GMV	实收	补付款	批量订单GMV	批量订单实收
INSERT INTO fe_dm.dm_ma_kpi_data_daily
    (sdate, business_area,city_name, set_position, shelf_status1, shelf_type1
    , GMV, amount, after_pay_amount, order_num,user_num, shelfnum_sale
    , batchorder_gmv, batchorder_amount, batchorder_num)
SELECT t1.sdate,IFNULL(t2.business_name,'other') business_area,IFNULL(t2.CITY_NAME,'other') city_name
    ,CASE t4.cooperation_type WHEN 5 THEN '内部-分点部' WHEN 1 THEN '内部-办公室' ELSE '外部' END set_position
    ,CASE WHEN t2.ACTIVATE_TIME>=@date0 AND IFNULL(t2.REVOKE_TIME,'2099-12-31')<DATE_ADD(@date0,INTERVAL 1 MONTH) THEN '新增当月撤架'
         WHEN t2.ACTIVATE_TIME>=@date0 THEN '新增继续运营'
         WHEN t2.ACTIVATE_TIME<@date0 AND IFNULL(t2.REVOKE_TIME,'2099-12-31')<DATE_ADD(@date0,INTERVAL 1 MONTH) THEN '留存-当月撤架'
         ELSE '留存-继续运营' END shelf_status1
    ,CASE WHEN t2.SHELF_TYPE IN (1,3,4,8)  THEN '货架' WHEN t2.SHELF_TYPE IN(2,5) THEN '冰箱冰柜' WHEN t2.SHELF_TYPE IN (6) THEN '智能货柜' WHEN t2.SHELF_TYPE IN (7) THEN '自动贩卖机' ELSE t2.SHELF_TYPE END shelf_type1
    ,SUM(t1.gmv) gmv,SUM(pay_amount) pay_amount,SUM(after_pay_amount)  after_pay_amount,SUM(order_num) order_num,SUM(t1.user_num) user_num
     ,COUNT(DISTINCT t1.shelf_id) shelfnum_sale
    ,SUM(batchorder_gmv) batchorder_gmv,SUM(batchorder_amount) batchorder_amount,SUM(batchorder_num) batchorder_num
FROM fe_dm.temp_shelf_sale t1
JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.shelf_id=t1.SHELF_ID
LEFT JOIN feods.dm_ma_shelfInfo_extend t4 ON t4.shelf_id=t1.shelf_id
GROUP BY t1.sdate,business_area,city_name,set_position,shelf_status1,shelf_type1
;




-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_kpi_data_daily',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END