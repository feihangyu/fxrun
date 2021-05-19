CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_sale_weekly`(IN p_sdate DATE)
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; #每天默认传入前一天
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)) #当周一
    ,@sweek_end= SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)-6) #当周日
    ;
#删除历史数据
DELETE FROM fe_dm.dm_ma_shelf_sale_weekly WHERE sdate=@sweek OR (sdate<SUBDATE(@sweek,7*5) AND GMV=0) ;
# 插入当周数据
INSERT INTO fe_dm.dm_ma_shelf_sale_weekly
    (sdate, SHELF_ID
    , sale_num, GMV, amount, after_pay_amount, DISCOUNT_AMOUNT, COUPON_AMOUNT, order_num, refund_amount,GMV_wd,after_pay_amount_wd)
SELECT @sweek , a1.SHELF_ID
    ,SUM(sale_num) sale_num,SUM(GMV), SUM(amount) amount ,SUM(after_pay_amount) after_pay_amount,SUM(DISCOUNT_AMOUNT) DISCOUNT_AMOUNT
    ,SUM(COUPON_AMOUNT) COUPON_AMOUNT,SUM(order_num) order_num,SUM(refund_amount) refund_amount
    ,SUM(IF(a2.if_work_day=1 ,a1.GMV,0)) GMV_wd,SUM(IF(a2.if_work_day=1,a1.after_pay_amount,0)) after_pay_amount_wd
FROM fe_dm.dm_ma_shelf_sale_daily a1
JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate=a1.sdate
WHERE a1.sdate>=@sweek AND  a1.sdate<ADDDATE(@sweek,7)
    AND a1.sdate<CURDATE()
GROUP BY SHELF_ID
;   # 更新本周用户数,复购用户数
UPDATE fe_dm.dm_ma_shelf_sale_weekly t1
JOIN
    (SELECT SHELF_ID,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT shelf_id,user_id,COUNT(DISTINCT order_id) orders
	    FROM fe_dwd.dwd_pub_order_item_recent_two_month
	    WHERE PAY_DATE>=@sweek AND PAY_DATE<ADDDATE(@sweek,7)
            AND PAY_DATE<CURDATE()
        GROUP BY shelf_id, user_id
        )t
    GROUP BY SHELF_ID
    ) t2 ON   t1.SHELF_ID=t2.SHELF_ID
SET t1.user_num=t2.user_num,t1.user_num_reorder=t2.user_num_reorder
WHERE  t1.sdate=@sweek
;   # 更新本周天数
UPDATE
    (SELECT shelf_id,SUM(1) days ,SUM(IF(a2.if_work_day=1,1,0)) days_wd
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_work_day a2 ON  a2.sdate>=@sweek AND a2.sdate<=@sdate
        AND a2.sdate>=DATE(a1.ACTIVATE_TIME) AND a2.sdate<=IFNULL(DATE(a1.REVOKE_TIME),CURDATE())
    WHERE  SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
    GROUP BY shelf_id
    )t1
JOIN fe_dm.dm_ma_shelf_sale_weekly  t2 ON t2.sdate=@sweek AND t2.SHELF_ID=t1.shelf_id
SET t2.days=t1.days ,t2.days_wd=t1.days_wd
WHERE 1=1
;    #更新上周数据
UPDATE fe_dm.dm_ma_shelf_sale_weekly a1
JOIN fe_dm.dm_ma_shelf_sale_weekly a2 ON a2.sdate=SUBDATE(@sweek,7) AND a2.SHELF_ID=a1.SHELF_ID
SET a1.gmv_last=a2.GMV,a1.order_num_last=a2.order_num,a1.user_num_last=a2.user_num
    ,a1.user_num_reorder_lw=a2.user_num_reorder
WHERE a1.sdate=@sweek
;    #更新货架库存信息
UPDATE
    (SELECT a1.SHELF_ID
         ,SUM(STOCK_QUANTITY) STOCK_QUANTITY,SUM(STOCK_QUANTITY*SALE_PRICE) STOCK_value
         ,SUM(1) SKU,SUM(IF(a1.SALES_FLAG IN (1,2),1,0)) SKU_boom
    FROM fe_dwd.dwd_shelf_product_day_all a1
    WHERE a1.STOCK_QUANTITY>0
    GROUP BY a1.SHELF_ID  ) t1
JOIN fe_dm.dm_ma_shelf_sale_weekly t2 ON t2.sdate=@sweek AND t2.SHELF_ID=t1.SHELF_ID
SET t2.stock_quantity= t1.STOCK_QUANTITY, t2.STOCK_value =t1.STOCK_value,t2.sku =t1.SKU, t2.SKU_boom=t1.SKU_boom
WHERE 1=1
;
# 输入日期为周日时更新运营kpi2首页数据
IF DAYOFWEEK(@sdate)=1 THEN
    #更新运营kpi2首页
    DELETE FROM fe_dm.dm_op_kpi2_monitor WHERE sdate = @sweek AND indicate_type = 'w'  AND indicate_id = 101;
    INSERT INTO fe_dm.dm_op_kpi2_monitor
        (sdate,indicate_type,indicate_id,indicate_name,indicate_value)
    SELECT
        @sweek sdate,'w' indicate_type,101 indicate_id
       ,'dm_ma_shelf_sale_weekly.gmv' indicate_name
       ,ROUND(SUM(gmv) / SUM(gmv_last) - 1, 6) indicate_value
    FROM  fe_dm.dm_ma_shelf_sale_weekly a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2
        ON a2.shelf_id=a1.SHELF_ID AND a2.ACTIVATE_TIME<=SUBDATE(@sweek,7) AND IFNULL(DATE(a2.REVOKE_TIME),CURRENT_DATE)>=ADDDATE(@sweek,7)
    WHERE a1.sdate=@sweek;
        #更新运营kpi2地区首页
    DELETE FROM fe_dm.dm_op_kpi2_monitor_area WHERE sdate = @sweek AND indicate_type = 'w' AND indicate_id = 101;
    INSERT INTO fe_dm.dm_op_kpi2_monitor_area
        ( sdate, business_name, indicate_type, indicate_id, indicate_name, indicate_value)
    SELECT
        @sweek sdate,business_name,'w' indicate_type,101 indicate_id,
        'dm_ma_shelf_sale_weekly.gmv' indicate_name,
        ROUND(SUM(gmv) / SUM(gmv_last) - 1, 6) indicate_value
    FROM fe_dm.dm_ma_shelf_sale_weekly a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2
        ON a2.shelf_id=a1.SHELF_ID AND a2.ACTIVATE_TIME<=SUBDATE(@sweek,7) AND IFNULL(DATE(a2.REVOKE_TIME),CURRENT_DATE)>=ADDDATE(@sweek,7)
    WHERE a1.sdate=@sweek
    GROUP BY business_name;
END IF;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_sale_weekly',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user),@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_sale_weekly','dm_ma_shelf_sale_weekly','纪伟铨');
END