CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_shelfInfo_extend`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
    #插入数据
INSERT INTO feods.dm_ma_shelfInfo_extend
    (shelf_id)
SELECT a1.shelf_id
FROM fe_dwd.dwd_shelf_base_day_all a1
LEFT JOIN feods.dm_ma_shelfInfo_extend a2 ON a2.shelf_id=a1.shelf_id
WHERE  a2.shelf_id IS NULL AND  a1.SHELF_STATUS=2 ;
    #更新销售时间
UPDATE
    (SELECT DISTINCT SHELF_ID
    FROM feods.d_ma_shelf_sale_daily a1
    WHERE sdate>=@sdate AND sdate<ADDDATE(@sdate,1) AND GMV>0 ) a1
JOIN feods.dm_ma_shelfInfo_extend a2 ON a2.shelf_id=a1.SHELF_ID
SET a2.last_sale_date=@sdate
    ,a2.first_sale_date=ifnull(a2.first_sale_date,@sdate);
    # 更新补货时间
UPDATE
    (select SHELF_ID,date(max(FILL_TIME)) max_filltime,date(min(FILL_TIME)) min_filltime
    from fe_dwd.dwd_fill_day_inc
    where apply_time>=subdate(@sdate,30) and apply_time<adddate(@sdate,1)
        and FILL_TYPE IN (1,2,3,4,7,8,9) AND order_status = 4
    group by SHELF_ID) a1
JOIN feods.dm_ma_shelfInfo_extend a2 ON a2.shelf_id=a1.SHELF_ID
SET a2.first_fill_date=ifnull(a2.first_fill_date,a1.min_filltime)
    ,a2.last_fill_date=if(ifnull(a2.last_fill_date,subdate(@sdate,20))<a1.max_filltime,a1.max_filltime,a2.last_fill_date);


#插入数据
-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_shelfInfo_extend',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END