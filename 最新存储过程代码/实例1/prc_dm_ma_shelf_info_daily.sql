CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_shelf_info_daily`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场  业务方(罗辉)
-- Create date: 2020-3-19
-- Modify date:
-- Description:
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @weekend=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)+1);
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');

#删除数据
DELETE FROM fe_dm.dm_ma_shelf_info_daily WHERE (sdate=@sdate) OR (sdate < SUBDATE(@sdate,150));
#临时数据
    #字段:  日期√ 货架ID√ 终端类型√ 终端生命周期√  12月货架销售等级√ 商品库存量√ 库存sku量√  货架库存是否满足√ SKU库存是否满足√  优质商品库存量  优质商品SKU  4周内补货次数√
SET @shelf_id=0,@cur_date=CURDATE(),@row=1;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_shelf_bind_info; #主货架分段绑定日期
CREATE TEMPORARY TABLE fe_dm.temp_shelf_bind_info(INDEX (shelf_id)) AS
    SELECT shelf_id,row_num,MIN(sdate) bind_date,MAX(sdate) UNBIND_date
    FROM
        (SELECT t.*
             ,CASE WHEN @shelf_id<>shelf_id THEN @row:=1
                WHEN @shelf_id=shelf_id AND sdate >= @cur_date AND sdate<=ADDDATE(@cur_date,1) THEN @row:=@row
                ELSE @row:=@row+1 END row_num
             ,@shelf_id :=shelf_id shelf_id2,@cur_date:=sdate sdate2
        FROM
             (SELECT a1.shelf_id,a2.sdate
             FROM
                 (SELECT MAIN_SHELF_ID shelf_id,DATE(add_time) add_date
                       ,IF(SHELF_HANDLE_STATUS=10,DATE(IFNULL(UNBIND_TIME,LAST_UPDATE_TIME)),CURDATE()) UNBIND_date
                FROM fe.sf_shelf_relation_record
                WHERE SHELF_HANDLE_STATUS IN (9,10) AND IFNULL(UNBIND_TIME,CURDATE())>=@sdate
                     AND DATA_FLAG=1
                )a1
            JOIN fe_dwd.dwd_pub_work_day  a2 ON a2.sdate BETWEEN a1.add_date AND a1.UNBIND_date
            ORDER BY a1.shelf_id, a2.sdate
            ) t
        ) tt
    GROUP BY shelf_id,row_num;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_shelf_bind_info_sec; #次货架分段绑定日期
CREATE TEMPORARY TABLE fe_dm.temp_shelf_bind_info_sec(INDEX (shelf_id)) AS
    SELECT shelf_id,row_num,MIN(sdate) bind_date,MAX(sdate) UNBIND_date
    FROM
        (SELECT t.*
             ,CASE WHEN @shelf_id<>shelf_id THEN @row:=1
                WHEN @shelf_id=shelf_id AND sdate >= @cur_date AND sdate<=ADDDATE(@cur_date,1) THEN @row:=@row
                ELSE @row:=@row+1 END row_num
             ,@shelf_id :=shelf_id shelf_id2,@cur_date:=sdate sdate2
        FROM
             (SELECT a1.shelf_id,a2.sdate
             FROM
                 (SELECT SECONDARY_SHELF_ID shelf_id,DATE(add_time) add_date
                       ,IF(SHELF_HANDLE_STATUS=10,DATE(IFNULL(UNBIND_TIME,LAST_UPDATE_TIME)),CURDATE()) UNBIND_date
                FROM fe.sf_shelf_relation_record
                WHERE SHELF_HANDLE_STATUS IN (9,10) AND IFNULL(UNBIND_TIME,CURDATE())>=@sdate
                     AND DATA_FLAG=1
                )a1
            JOIN fe_dwd.dwd_pub_work_day  a2 ON a2.sdate BETWEEN a1.add_date AND a1.UNBIND_date
            ORDER BY a1.shelf_id, a2.sdate
            ) t
        ) tt
    GROUP BY shelf_id,row_num;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_shelf;  #当日运营货架信息
CREATE TEMPORARY TABLE fe_dm.temp_shelf(INDEX(shelf_id)) AS
    SELECT a1.shelf_id
         ,CASE WHEN a1.SHELF_TYPE IN (1,3,4,8)  THEN '货架' WHEN a1.SHELF_TYPE IN(2,5) THEN '冰箱冰柜'
             WHEN a1.SHELF_TYPE IN (6) THEN '智能货柜' WHEN a1.SHELF_TYPE IN (7) THEN '自动贩卖机' ELSE a1.SHELF_TYPE END shelf_type2
         ,a1.business_name
         ,CASE a2.ext4 WHEN 1 THEN '核心货架' WHEN 2 THEN'高潜力货架' WHEN 3 THEN'新终端' ELSE '其他' END  shelf_life_cycle #1:核心货架 ,2:高潜力货架,3:新终端,0:其他
         ,a1.grade
        ,CASE WHEN a5.shelf_id IS NOT NULL THEN '主货架' WHEN a6.shelf_id IS NOT NULL THEN '次货架' ELSE NULL END  bind_type
        ,a1.shelf_type
    FROM fe_dwd.dwd_shelf_base_day_all a1
    LEFT JOIN feods.zs_shelf_flag_his a2 ON a2.sdate='2020-1-1' AND a2.shelf_id=a1.shelf_id
    LEFT JOIN feods.zs_shelf_flag_his a3 ON a3.sdate=@sdate AND a3.shelf_id=a1.shelf_id
    LEFT JOIN feods.dm_ma_shelfInfo_extend a4 ON a4.shelf_id=a1.shelf_id
    LEFT JOIN fe_dm.temp_shelf_bind_info a5 ON a5.shelf_id=a1.shelf_id AND @sdate BETWEEN a5.bind_date AND a5.UNBIND_date
    LEFT JOIN fe_dm.temp_shelf_bind_info_sec a6 ON a6.shelf_id=a1.shelf_id AND @sdate BETWEEN a6.bind_date AND a6.UNBIND_date
    WHERE DATE(a1.ACTIVATE_TIME)<=@sdate AND DATE(IFNULL(a1.REVOKE_TIME,CURDATE()))>=@sdate
      AND a1.SHELF_STATUS IN (2,3,4,5) AND  a1.shelf_type NOT IN (9);
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_stock; #货架商品库存
CREATE TEMPORARY TABLE fe_dm.temp_stock(INDEX(shelf_id,product_id)) AS
    SELECT sdate,shelf_id,product_id,stock_quantity
    FROM fe_dwd.dwd_shelf_product_day_all_recent_32 WHERE sdate=SUBDATE(@sdate,1)  AND stock_quantity>0
    ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_area_product; #优质商品
CREATE TEMPORARY TABLE fe_dm.temp_area_product(INDEX(business_area,product_id)) AS
    SELECT a1.business_area,a1.product_id,IFNULL(a2.product_sale_level,a1.product_sale_level) product_sale_level2
    FROM feods.fjr_product_list_manager_week a1
    LEFT JOIN feods.fjr_product_list_manager_week a2 ON a2.week_end='2019-12-29' AND a2.business_area=a1.business_area AND a2.product_id=a1.product_id
    WHERE a1.week_end=@weekend AND a2.product_type='原有'
      AND ( (a1.product_sale_level IN ('好卖','热卖','非常好卖')) OR (a2.product_sale_level IN ('好卖','热卖','非常好卖')) ) ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_fill_times; # 4周内补货次数
CREATE TEMPORARY TABLE fe_dm.temp_fill_times(INDEX(SHELF_ID)) AS
    SELECT SHELF_ID,COUNT(DISTINCT order_id) fill_times
    FROM fe_dwd.dwd_fill_day_inc_recent_two_month
    WHERE FILL_TIME>=SUBDATE(@sdate,27) AND FILL_TIME<ADDDATE(@sdate,1)
        AND FILL_TYPE IN (1,2,3,4,7,8,9) AND order_status = 4
    GROUP BY SHELF_ID;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_stock_satisfy; # 库存满足率
CREATE TEMPORARY TABLE fe_dm.temp_stock_satisfy(INDEX(shelf_id)) AS
SELECT shelf_id,shelf_type2, business_name, shelf_life_cycle, grade, bind_type, shelf_type
   ,CASE WHEN
			(t1.grade = '新装' AND shelf_type IN (1, 3) AND t1.stock_quantity < 180 )
		 OR (t1.grade = '新装' AND shelf_type IN (2, 5) AND t1.stock_quantity < 110 )
		 OR (t1.grade IN ('甲', '乙') AND bind_type = '主货架' AND t1.stock_quantity < 300 )       -- 甲乙级关联货架
		 OR (t1.grade IN ('甲', '乙') AND shelf_type IN (1, 3) AND t1.stock_quantity < 180)
		 OR (t1.grade IN ('甲', '乙') AND shelf_type IN (2, 5) AND t1.stock_quantity < 110)
		 OR (t1.grade IN ('丙', '丁') AND bind_type = '主货架' AND t1.stock_quantity < 200   )     -- 丙丁级关联货架
		 OR (t1.grade IN ('丙', '丁') AND shelf_type IN (1, 3) AND t1.stock_quantity < 110)
		 OR (t1.grade IN ('丙', '丁') AND shelf_type IN (2, 5) AND t1.stock_quantity < 90 )
		 OR (t1.shelf_type = 6 AND t1.stock_quantity < 110)
		 OR (t1.shelf_type = 8 AND t1.stock_quantity < 100)
		THEN 0 ELSE 1  END if_shelf_stock
    , CASE
      WHEN (shelf_type IN (1, 3) AND t1.stock_sku < 30)
        OR (shelf_type IN (2, 5) AND stock_sku < 10)
        OR (shelf_type = 8 AND stock_sku < 15)
        THEN 0 ELSE 1 END AS if_sku_stock
FROM
   (SELECT a1.shelf_id, shelf_type2, business_name, shelf_life_cycle, grade, bind_type, shelf_type
        ,IFNULL(SUM(a2.stock_quantity),0) AS stock_quantity,COUNT(DISTINCT a2.product_id) stock_sku
    FROM fe_dm.temp_shelf  a1
    LEFT JOIN fe_dm.temp_stock a2 ON a2.shelf_id=a1.shelf_id
    GROUP BY shelf_id) t1;
#插入数据
INSERT INTO fe_dm.dm_ma_shelf_info_daily
    (sdate, shefl_id
    , shelf_type2,bind_type, shelf_life_cycle, shelf_grade_12, stock_qty, stock_qty_quality, stock_sku, stock_sku_quality, if_shelf_stock, if_sku_stock, fill_times_4week)
SELECT @sdate,a1.shelf_id,shelf_type2,a1.bind_type,a1.shelf_life_cycle,a1.grade
    ,IFNULL(SUM(a2.stock_quantity),0) stock_quantity,0 stock_qty_quality  #SUM(IF(a3.product_id IS NOT NULL,a2.stock_quantity,0)) stock_qty_quality
    ,COUNT(DISTINCT a2.product_id) stock_sku,0 stock_sku_quality #COUNT(DISTINCT IF(a3.product_id IS NOT NULL,a2.product_id,NULL) ) stock_sku_quality
    ,if_shelf_stock, if_sku_stock,a4.fill_times
FROM fe_dm.temp_stock_satisfy a1
LEFT JOIN fe_dm.temp_stock a2 ON a2.shelf_id=a1.shelf_id
LEFT JOIN fe_dm.temp_area_product a3 ON a3.business_area=a1.business_name AND a3.product_id=a2.product_id
LEFT JOIN fe_dm.temp_fill_times a4 ON a4.SHELF_ID=a1.shelf_id
GROUP BY a1.shelf_id
;
#更新运营状态
UPDATE   fe_dm.dm_ma_shelf_info_daily a2
JOIN fe_dwd.dwd_shelf_base_day_all a1 ON a1.shelf_id=a2.shefl_id
SET a2.running_status= CASE WHEN DATE(a1.ACTIVATE_TIME)<@smonth AND IFNULL(DATE(a1.REVOKE_TIME),CURRENT_DATE)>@sdate THEN'留存-运营'
             WHEN DATE(a1.ACTIVATE_TIME)<@smonth    AND IFNULL(DATE(a1.REVOKE_TIME),CURRENT_DATE) BETWEEN @smonth AND @sdate THEN'留存-撤架'
             WHEN DATE(a1.ACTIVATE_TIME) BETWEEN @smonth AND @sdate  AND IFNULL(DATE(a1.REVOKE_TIME),CURRENT_DATE) >@sdate THEN'新增-留存'
             WHEN DATE(a1.ACTIVATE_TIME) BETWEEN @smonth AND @sdate  AND IFNULL(DATE(a1.REVOKE_TIME),CURRENT_DATE) BETWEEN @smonth AND @sdate  THEN'新增-撤架'
             ELSE '其他' END
WHERE a2.sdate>=@smonth AND a2.sdate<=@sdate;
#记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_shelf_info_daily',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END