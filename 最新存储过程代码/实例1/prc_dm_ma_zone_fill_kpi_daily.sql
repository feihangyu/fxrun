CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_zone_fill_kpi_daily`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
#删除数据
DELETE FROM feods.dm_ma_zone_fill_kpi_daily WHERE sdate=@sdate OR sdate<SUBDATE(@sdate,62);
#插入数据
    #销售数据
DROP TEMPORARY TABLE IF EXISTS feods.temp_sale ;
CREATE TEMPORARY TABLE feods.temp_sale(INDEX(business_name,zone_name)) AS
    SELECT business_name,IFNULL(zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
        ,SUM(a1.gmv+IFNULL(AFTER_PAYMENT_MONEY,0)) gmv ,SUM(pay_amount) pay_amount,SUM(orders) orders
        ,COUNT(DISTINCT IF(sal_qty>0,a1.shelf_id,NULL)) shelfs_saling
    FROM fe_dwd.dwd_shelf_day_his a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
    LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a1.shelf_id
    WHERE sdate=@sdate
    GROUP BY business_name,zone_name,shelf_type1,if_fill
;    #用户数
DROP TEMPORARY TABLE IF EXISTS feods.temp_users ;
CREATE TEMPORARY TABLE feods.temp_users(INDEX(business_name,zone_name)) AS
    SELECT business_name,IFNULL(zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
        ,COUNT(DISTINCT user_id) users
    FROM fe_dwd.dwd_order_item_refund_day a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
    LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a1.shelf_id
    WHERE PAY_DATE>=@sdate AND PAY_DATE<ADDDATE(@sdate,1)
    GROUP BY business_name,zone_name,shelf_type1,if_fill
;   #运营终端数 运营终端(去次货架)
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelfs ;
CREATE TEMPORARY TABLE feods.temp_shelfs(INDEX(business_name,zone_name)) AS
    SELECT business_name,IFNULL(zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
        ,COUNT(1) shelfs,SUM(IF(a2.MAIN_SHELF_ID IS NULL,1,0)) shelfs_main
    FROM fe_dwd.dwd_shelf_base_day_all a2
    LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a2.shelf_id
    WHERE DATE(ACTIVATE_TIME)<=@sdate AND IFNULL(DATE(REVOKE_TIME),CURDATE())>=@sdate
        AND SHELF_STATUS IN (2,3,4,5) AND  a2.shelf_type NOT IN (9)
    GROUP BY business_name,zone_name,shelf_type1,if_fill
;   #库存满足终端
DROP TEMPORARY TABLE IF EXISTS feods.temp_stock_satisfy ;
CREATE TEMPORARY TABLE feods.temp_stock_satisfy(INDEX(business_name,zone_name)) AS
SELECT business_name,IFNULL(zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
    ,SUM(IF(a1.stock_type='其他',1,0)) stcok_num
FROM fe_dwd.dwd_shelf_base_day_all a2
JOIN feods.zs_buhuo_shelf_action_history a1
    ON a1.sdate=@sdate and  a1.shelf_id=a2.shelf_id
LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a2.shelf_id
where    a2.shelf_type IN (1,3) AND a2.SHELF_STATUS IN (2,3,4,5) and a2.MAIN_SHELF_ID is null
    and  DATE(a2.ACTIVATE_TIME)<=@sdate AND IFNULL(DATE(a2.REVOKE_TIME),CURDATE())>=@sdate
GROUP BY business_name,zone_name,shelf_type1,if_fill;

INSERT INTO feods.dm_ma_zone_fill_kpi_daily
    (sdate, business_area, zone_name, shelf_type_name, if_fill
    , shelfs_operating, shelfs_saling, shelfs_operating_main, shelfs_stcok_satisfy, GMV, amount, orders, users)
SELECT @sdate sdate,a1.business_name, a1.zone_name, a1.shelf_type1, a1.if_fill
     , a1.shelfs, shelfs_saling, a1.shelfs_main, a4.stcok_num, GMV, pay_amount, orders, users
FROM feods.temp_shelfs a1
LEFT JOIN feods.temp_sale a2
    ON a2.business_name=a1.business_name AND a2.zone_name=a1.zone_name AND a2.shelf_type1=a1.shelf_type1 AND a2.if_fill=a1.if_fill
LEFT JOIN feods.temp_users a3
     ON a3.business_name=a1.business_name AND a3.zone_name=a1.zone_name AND a3.shelf_type1=a1.shelf_type1 AND a3.if_fill=a1.if_fill
LEFT JOIN feods.temp_stock_satisfy  a4
     ON a4.business_name=a1.business_name AND a4.zone_name=a1.zone_name AND a4.shelf_type1=a1.shelf_type1 AND a4.if_fill=a1.if_fill
;

#记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_zone_fill_kpi_daily',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END