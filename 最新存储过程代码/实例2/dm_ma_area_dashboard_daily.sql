CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_area_dashboard_daily`( IN p_if_history TINYINT)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @stime := CURRENT_TIMESTAMP();
SET @sdate=CURRENT_DATE;
SET @update_flag=CASE WHEN p_if_history=1 OR CURTIME() BETWEEN '02:05' AND '02:35' THEN 1 WHEN CURTIME() BETWEEN '08:02:00' AND '22:50:00' THEN 2 ELSE 0 END;
SET @smonth= DATE_FORMAT(@sdate,'%Y-%m-01');
SET @date0=CASE WHEN  @update_flag=1 THEN SUBDATE(@sdate,15) ELSE @sdate END;
SELECT @update_flag;
### 每10分钟更新今日数据,如果时间在7:00 至 7:05之间 则更新近15天数据
IF @update_flag>0 THEN
DELETE  FROM fe_dm.dm_ma_area_dashboard_daily WHERE sdate>=@date0 OR sdate<SUBDATE(@sdate,100)  ;
    # 插入订单数据
INSERT INTO fe_dm.dm_ma_area_dashboard_daily
    (sdate, business_name, workday_num
    ,sale_num, GMV, amount, after_pay_amount, discount_amount, coupon_amount, order_num,shelfnum_sale
    )
SELECT sdate,IFNULL(business_name,'other') business_name,workday_num
     ,SUM(sale_num) sale_num, SUM(GMV),SUM(amount) amount,SUM(after_pay_amount) after_pay_amount,SUM(discount_amount) discount_amount,SUM(coupon_amount) coupon_amount,SUM(order_num) order_num
     ,SUM( IF(sale_num>0 AND a2.SHELF_TYPE NOT IN (2,4,5) ,1,0) ) shelfnum_sale
FROM fe_dm.dm_ma_shelf_sale_daily a1
LEFT JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.SHELF_ID
WHERE sdate>=@date0
GROUP BY sdate,business_name
;
    # 更热日用户数
UPDATE fe_dm.dm_ma_area_dashboard_daily t1
JOIN
    (SELECT sdate,b.business_name,COUNT(DISTINCT USER_ID) user_num
    FROM
        (SELECT a1.SHELF_ID, a1.USER_ID,DATE(ORDER_DATE) sdate
        FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
        WHERE a1.ORDER_DATE >= @date0
            AND a1.ORDER_STATUS IN (2, 6, 7) AND quantity_act>0
        UNION ALL
        SELECT IFNULL(a1.real_shelf_id, a1.SHELF_ID) SHELF_ID, user_id,DATE(PAYMENT_DATE) sdate
        FROM fe_dwd.dwd_sf_after_payment a1
        WHERE a1.PAYMENT_DATE >=@date0
            AND a1.PAYMENT_STATUS = 5
        ) a
    LEFT JOIN fe_dwd.dwd_shelf_base_day_all b ON b.shelf_id=a.SHELF_ID
    GROUP BY sdate,b.business_name
    )t2 ON t2.sdate=t1.sdate AND t2.business_name=t1.business_name
SET t1.user_num=t2.user_num
WHERE t1.sdate>=@date0
;
    # 更新运营终端数量 新增终端数 撤架终端数
UPDATE fe_dm.dm_ma_area_dashboard_daily t1
JOIN
    (SELECT a2.sdate,a1.business_name
	    ,COUNT(1) shelfnum_operating
	    ,COUNT(CASE WHEN DATE(a1.ACTIVATE_TIME)=a2.sdate THEN 1  END )  shelfnum_new
	    ,COUNT(CASE WHEN DATE(a1.REVOKE_TIME)=a2.sdate THEN 1 END  ) shelfnum_revoke
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate BETWEEN @date0 AND @sdate
                                       AND DATE(a1.ACTIVATE_TIME)<=a2.sdate AND IFNULL(REVOKE_TIME,CURRENT_DATE)>=a2.sdate
    WHERE  a1.SHELF_STATUS NOT IN (10,1) AND a1.SHELF_TYPE NOT IN (9,2,4,5)
    GROUP BY sdate,a1.business_name
    ) t2 ON t2.sdate=t1.sdate AND t2.business_name=t1.business_name
SET t1.shelfnum_operating=t2.shelfnum_operating,t1.shelfnum_new=t2.shelfnum_new,t1.shelfnum_revoke=t2.shelfnum_revoke
WHERE t1.sdate>=@date0
;
# 更新当日终端生命周期维度货架商品数据
UPDATE fe_dm.dm_ma_area_dashboard_daily t1
JOIN
    (SELECT business_name
        ,SUM(IF(if_valid =1,1,0)) productnum_valid
        ,SUM(IF(if_valid =1 AND if_sale=1 AND if_out=0 AND SALES_FLAG IN(1,2,3),1,0)) productnum_valid_qualified
        ,SUM(IF(if_valid =1 AND if_sale=1 AND if_out=0 AND SALES_FLAG IN(1,2,3) AND STOCK_QUANTITY>0,1,0)) productnum_valid_qualified_stored
    FROM fe_dm.dm_ma_shelf_product_temp
    GROUP BY business_name
    ) t2 ON t2.business_name=t1.business_name
SET t1.productnum_valid=t2.productnum_valid,t1.productnum_valid_qualified=t2.productnum_valid_qualified,t1.productnum_valid_qualified_stored=t2.productnum_valid_qualified_stored
WHERE t1.sdate=@sdate
;
# 当日终端生命周期维度货架库存等数据
UPDATE
    (SELECT a1.business_name
            ,SUM(IF(a2.pid IS NOT NULL,1,0)) shelfnum_valid
            ,SUM(IF(a2.pid IS NOT NULL AND a4.stock_type='其他',1,0)) shelfnum_valid_quantity_satisfied
            ,SUM(IF(a1.REVOKE_STATUS IN (1,4) AND a4.stock_quantity=0,1,0 )) shelfnum_0stored
            ,SUM(IF(a1.REVOKE_STATUS IN (1,4) AND a1.CLOSE_TYPE=6 AND a1.WHETHER_CLOSE=2 AND a4.stock_quantity>0,1,0 ))shelfnum_closed
    FROM fe_dwd.dwd_shelf_base_day_all a1
    LEFT JOIN fe_dm.dm_shelf_flag a2 ON a2.shelf_id=a1.SHELF_ID AND a2.ext4 >0 #货架生命周期维度
    LEFT JOIN fe_dm.dm_fill_shelf_action_history a4 ON a4.sdate=@sdate AND a4.shelf_id=a1.SHELF_ID
    WHERE  a1.SHELF_STATUS NOT IN (10,1) AND a1.SHELF_TYPE NOT IN (9,2,4,5)
    GROUP BY business_name
    ) t2
JOIN fe_dm.dm_ma_area_dashboard_daily t1 ON t1.sdate=@sdate AND t1.business_name=t2.business_name
SET t1.shelfnum_valid=t2.shelfnum_valid,t1.shelfnum_valid_quantity_satisfied=t2.shelfnum_valid_quantity_satisfied
  ,t1.shelfnum_0stored=t2.shelfnum_0stored,t1.shelfnum_closed=t2.shelfnum_closed
;
END IF;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_area_dashboard_daily',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_area_dashboard_daily','dm_ma_area_dashboard_daily','纪伟铨');
 
END