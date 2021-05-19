CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_area_shelfType_kpi_weekly`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2));
#删除数据
DELETE  FROM feods.dm_ma_area_shelfType_kpi_weekly WHERE sdate=@sweek OR sdate<SUBDATE(@sweek,7*100);
#插入数据
INSERT INTO  feods.dm_ma_area_shelfType_kpi_weekly
    (sdate, business_area, shelf_type, gmv,amount, orders,shelfs_sale
    )
SELECT @sweek,a2.business_name,IF(a2.shelf_type IN (6,7,8) ,a2.shelf_type_desc,'货架冰箱') shelf_type1
    ,SUM(gmv ) gmv,SUM(amount),SUM(order_num) ordes,sum(if(sale_num>0,1,0)) shelfs_sale
FROM feods.d_ma_shelf_sale_weekly a1
JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.SHELF_ID
WHERE sweek=@sweek
GROUP BY business_name,shelf_type1;
    #更新 用户生命周期
UPDATE
    (SELECT a2.business_name,IF(a2.shelf_type IN (6,7,8) ,a2.shelf_type_desc,'货架冰箱') shelf_type1
        ,SUM(IF(user_life_cycle_genera=1,1,0)) users_introdution
        ,SUM(IF(user_life_cycle_genera=2,1,0)) users_growth
        ,SUM(IF(user_life_cycle_genera=3,1,0)) users_mature
        ,SUM(IF(user_life_cycle_genera=4,1,0)) users_loss
        ,SUM(IF(user_life_cycle_genera=5,1,0)) users_quiescent
    FROM feods.zs_shelf_member_flag_history a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.SHELF_ID
    WHERE a1.sdate=@sweek
    GROUP BY a2.business_name,shelf_type1 ) a1
JOIN feods.dm_ma_area_shelfType_kpi_weekly a2 ON a2.sdate=@sweek AND a2.business_area=a1.business_name AND a2.shelf_type=a1.shelf_type1
SET a2.users_introdution=a1.users_introdution
    ,a2.users_growth=a1.users_growth
    ,a2.users_mature=a1.users_mature
    ,a2.users_loss=a1.users_loss
    ,a2.users_quiescent=a1.users_quiescent
;
    # 更新本周用户数,复购用户数
UPDATE
    (SELECT business_name,shelf_type1
          ,COUNT(1) user_num,SUM(IF(orders>1,1,0)) user_num_reorder
    FROM
        (SELECT a2.business_name,IF(a2.shelf_type IN (6,7,8) ,a3.ITEM_NAME,'货架冰箱') shelf_type1
              ,user_id,COUNT(DISTINCT order_id) orders
	    FROM fe_dwd.dwd_order_item_refund_day a1
        JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
        JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.ITEM_VALUE=a2.shelf_type AND DICTIONARY_ID=8
	    WHERE PAY_DATE>=@sweek AND PAY_DATE<ADDDATE(@sweek,7)
        GROUP BY a2.business_name,shelf_type1,user_id
        )t
    GROUP BY business_name,shelf_type1
    ) t2
JOIN feods.dm_ma_area_shelfType_kpi_weekly t1 ON t1.sdate=@sweek AND t1.business_area=t2.business_name AND t1.shelf_type=t2.shelf_type1
SET t1.users=t2.user_num,t1.users_reorder=t2.user_num_reorder
WHERE 1=1
;
    #更新货架数
UPDATE
    (SELECT a1.business_name,IF(a1.shelf_type IN (6,7,8) ,a3.ITEM_NAME,'货架冰箱') shelf_type1
        ,SUM(1) shelfs
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN fe_dwd.dwd_pub_dictionary a3 ON a3.ITEM_VALUE=a1.shelf_type AND DICTIONARY_ID=8
    WHERE  SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9)
      AND   DATE(ACTIVATE_TIME)<ADDDATE(@sweek,7)  AND IFNULL(DATE(REVOKE_TIME),CURRENT_DATE)>=@sweek
    GROUP BY  a1.business_name,shelf_type1) t2
JOIN feods.dm_ma_area_shelfType_kpi_weekly t1 ON  t1.business_area=t2.business_name AND t1.shelf_type=t2.shelf_type1
SET t1.shelfs=t2.shelfs
WHERE t1.sdate=@sweek;



 #更新数据
-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_area_shelfType_kpi_weekly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END