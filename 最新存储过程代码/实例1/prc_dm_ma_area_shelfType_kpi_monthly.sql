CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_area_shelfType_kpi_monthly`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate; SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
#删除数据
DELETE  FROM feods.dm_ma_area_shelfType_kpi_monthly WHERE sdate=@smonth OR sdate<DATE_SUB(@smonth,INTERVAL 10 MONTH );
#插入数据
INSERT INTO  feods.dm_ma_area_shelfType_kpi_monthly
    (sdate, business_area, shelf_type, gmv, orders,shelfs_sale)
SELECT @smonth,a2.business_name,IF(a2.shelf_type IN (6,7,8) ,a3.ITEM_NAME,'货架冰箱') shelf_type1
    ,SUM(gmv ) gmv,SUM(order_num) ordes,sum(if(sale_num>0,1,0)) shelfs_sale
FROM feods.d_ma_shelf_sale_monthly a1
JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.SHELF_ID
JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.ITEM_VALUE=a2.shelf_type AND DICTIONARY_ID=8
WHERE smonth=@smonth
GROUP BY business_name,shelf_type1;
    # 更新本周用户数,复购用户数
UPDATE
    (SELECT business_name,shelf_type1
          ,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT a2.business_name,IF(a2.shelf_type IN (6,7,8) ,a3.ITEM_NAME,'货架冰箱') shelf_type1,user_id
              ,COUNT(DISTINCT order_id) orders
	    FROM fe_dwd.dwd_order_item_refund_day a1
        JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
        JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.ITEM_VALUE=a2.shelf_type AND DICTIONARY_ID=8
	    WHERE PAY_DATE>=@smonth AND PAY_DATE<DATE_ADD(@smonth,INTERVAL 1 MONTH)
        GROUP BY a2.business_name,shelf_type1,user_id
        )t
    GROUP BY business_name,shelf_type1
    ) t2
JOIN feods.dm_ma_area_shelfType_kpi_monthly t1 ON  t1.business_area=t2.business_name AND t1.shelf_type=t2.shelf_type1
SET t1.users=t2.user_num,t1.users_reorder=t2.user_num_reorder
WHERE  t1.sdate=@smonth
;
    #更新货架数
UPDATE
    (SELECT a1.business_name,IF(a1.shelf_type IN (6,7,8) ,a3.ITEM_NAME,'货架冰箱') shelf_type1
        ,SUM(1) shelfs
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.ITEM_VALUE=a1.shelf_type AND DICTIONARY_ID=8
    WHERE  SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
      AND   DATE(ACTIVATE_TIME)<=LAST_DAY(@smonth) AND IFNULL(DATE(REVOKE_TIME),CURRENT_DATE)>=@smonth
    GROUP BY  a1.business_name,shelf_type1) t2
JOIN feods.dm_ma_area_shelfType_kpi_monthly t1 ON  t1.business_area=t2.business_name AND t1.shelf_type=t2.shelf_type1
SET t1.shelfs=t2.shelfs
WHERE t1.sdate=@smonth;

 #更新数据
-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_area_shelfType_kpi_monthly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END