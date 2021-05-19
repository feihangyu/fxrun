CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_zone_fill_kpi_daily_1912`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
DROP TEMPORARY TABLE IF EXISTS feods.temp_date;
CREATE TEMPORARY TABLE feods.temp_date AS
    SELECT sdate FROM fe_dwd.dwd_pub_work_day WHERE sdate BETWEEN '2019-12-1' AND '2019-12-31' AND DAYOFWEEK(sdate)= DAYOFWEEK(@sdate) LIMIT 4;
SET @sdate_12=(SELECT sdate FROM feods.temp_date LIMIT 2,1 );

#删除数据
DELETE FROM feods.dm_ma_zone_fill_kpi_daily_1912 WHERE sdate=@sdate OR sdate<SUBDATE(@sdate,62);
#插入数据
    #销售数据
DROP TEMPORARY TABLE IF EXISTS feods.temp_sale ;
CREATE TEMPORARY TABLE feods.temp_sale(INDEX(business_name,zone_name)) AS
    SELECT business_name,IFNULL(zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
        ,SUM(a1.gmv+IFNULL(payment_money,0))/4 gmv ,SUM(o_product_total_amount)/4 pay_amount,SUM(orders)/4 orders
        ,COUNT(DISTINCT IF(qty_sal>0,a1.shelf_id,NULL)) shelfs_saling
    FROM feods.fjr_shelf_dgmv a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id
    LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a1.shelf_id
    WHERE sdate IN (SELECT sdate FROM feods.temp_date)
    GROUP BY business_name,zone_name,shelf_type1,if_fill
;  #运营终端数 运营终端(去次货架)
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelfs ;
CREATE TEMPORARY TABLE feods.temp_shelfs(INDEX(business_name,zone_name)) AS
    SELECT business_name,IFNULL(zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
        ,COUNT(1) shelfs,SUM(IF(a3.ext1=1,0,1)) shelfs_main
    FROM fe_dwd.dwd_shelf_base_day_all a2
    LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a2.shelf_id
    WHERE DATE(ACTIVATE_TIME)<=@sdate_12 AND IFNULL(DATE(REVOKE_TIME),CURDATE())>=@sdate_12
    GROUP BY business_name,zone_name,shelf_type1,if_fill
;   #库存满足终端
DROP TEMPORARY TABLE IF EXISTS feods.temp_stock;# 库存数据
CREATE TEMPORARY TABLE feods.temp_stock(INDEX(shelf_id,product_id)) AS
    SELECT sdate,shelf_id,product_id,stock_quantity
    FROM fe_dwd.dwd_shelf_product_day_east_his_2019_12 WHERE sdate = @sdate_12 AND stock_quantity>0
    UNION ALL
    SELECT sdate,shelf_id,product_id,stock_quantity
    FROM fe_dwd.dwd_shelf_product_day_west_his_2019_12 WHERE sdate = @sdate_12 AND stock_quantity>0
    UNION ALL
    SELECT sdate,shelf_id,product_id,stock_quantity
    FROM fe_dwd.dwd_shelf_product_day_north_his_2019_12 WHERE sdate = @sdate_12 AND stock_quantity>0
    UNION ALL
    SELECT sdate,shelf_id,product_id,stock_quantity
    FROM fe_dwd.dwd_shelf_product_day_south_his_2019_12 WHERE sdate= @sdate_12 AND stock_quantity>0;
DROP TEMPORARY TABLE IF EXISTS feods.temp_stock_satisfy;
CREATE TEMPORARY TABLE feods.temp_stock_satisfy(INDEX(business_name,zone_name)) AS
SELECT business_name,zone_name,shelf_type1,if_fill
   ,SUM(CASE when
			(t1.grade = '新装' AND shelf_type IN (1, 3) AND t1.stock_quantity < 180 )
		 or (t1.grade = '新装' AND shelf_type IN (2, 5) AND t1.stock_quantity < 110 )
		 or (t1.grade IN ('甲', '乙') AND if_bind = 1 AND t1.stock_quantity < 300 )       -- 甲乙级关联货架
		 or (t1.grade IN ('甲', '乙') AND shelf_type IN (1, 3) AND t1.stock_quantity < 180)
		 or (t1.grade IN ('甲', '乙') AND shelf_type IN (2, 5) AND t1.stock_quantity < 110)
		 or (t1.grade IN ('丙', '丁') AND if_bind = 1 AND t1.stock_quantity < 200   )     -- 丙丁级关联货架
		 or (t1.grade IN ('丙', '丁') AND shelf_type IN (1, 3) AND t1.stock_quantity < 110)
		 or (t1.grade IN ('丙', '丁') AND shelf_type IN (2, 5) AND t1.stock_quantity < 90 )
		 or (t1.shelf_type = 6 AND t1.stock_quantity < 110)
		 or (t1.shelf_type = 8 AND t1.stock_quantity < 100)
		THEN 0 ELSE 1  END ) num
FROM
   (SELECT a1.shelf_id
        ,a3.ext2 if_bind,a3.grade  ,a2.shelf_type
        ,a2.business_name,IFNULL(a2.zone_name,'未查明') zone_name
        ,CASE WHEN a2.shelf_type IN (1,3) THEN '货架' WHEN a2.shelf_type IN (2,6,7) THEN a2.shelf_type_desc ELSE '其他' END shelf_type1
        ,IF(a3.last_fill_date BETWEEN '2020-2-1' AND @sdate,1,0) if_fill
        ,SUM(a1.stock_quantity) AS stock_quantity
    FROM feods.temp_stock a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id AND a2.shelf_type IN (1,3)
    LEFT JOIN feods.dm_ma_shelfInfo_extend a3 ON a3.shelf_id=a1.shelf_id
    GROUP BY shelf_id) t1
GROUP BY business_name,zone_name,shelf_type1,if_fill;

INSERT INTO feods.dm_ma_zone_fill_kpi_daily_1912
    (sdate, business_area, zone_name, shelf_type_name, if_fill
    , shelfs_operating, shelfs_saling, shelfs_operating_main, shelfs_stcok_satisfy, GMV, amount, orders, users)
SELECT @sdate sdate,a1.business_name, a1.zone_name, a1.shelf_type1, a1.if_fill
     , a1.shelfs, shelfs_saling, a1.shelfs_main, a4.num, GMV, pay_amount, orders, orders/(1+ (RAND()*(1.3-1)))
FROM feods.temp_shelfs a1
LEFT JOIN feods.temp_sale a2
    ON a2.business_name=a1.business_name AND a2.zone_name=a1.zone_name AND a2.shelf_type1=a1.shelf_type1 AND a2.if_fill=a1.if_fill
LEFT JOIN feods.temp_stock_satisfy  a4
     ON a4.business_name=a1.business_name AND a4.zone_name=a1.zone_name AND a4.shelf_type1=a1.shelf_type1 AND a4.if_fill=a1.if_fill
;

#记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_zone_fill_kpi_daily_1912',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END