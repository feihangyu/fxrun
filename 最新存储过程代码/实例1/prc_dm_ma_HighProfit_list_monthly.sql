CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_HighProfit_list_monthly`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	市场  罗晖
-- Create date: 2020-3-19
-- Modify date:
-- Description:
-- =============================================
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
SET @sdate=p_sdate;
SET @smonth=DATE_FORMAT( DATE_SUB(@sdate,INTERVAL 1 MONTH) ,'%Y-%m-01');
SET @smonth_str=DATE_FORMAT( DATE_SUB(@sdate,INTERVAL 1 MONTH) ,'%Y-%m');
SET @sweekend=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)+1); #上周日
#删除
DELETE FROM fe_dm.dm_ma_HighProfit_list_monthly WHERE sdate=@smonth OR sdate<DATE_SUB(@smonth,INTERVAL 72 MONTH );
#临时数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_profit;
CREATE TEMPORARY TABLE fe_dm.tmp_profit(INDEX(business_area,SECOND_TYPE_ID)) AS #原有品上月利润率
    SELECT a1.business_area,a2.SECOND_TYPE_ID,a1.product_id
         ,IF(a3.product_id IS NOT NULL,1,0) if_origin,ROUND(SUM(`profit`)/SUM(`GMV`),6)  profit_rate2
    FROM feods.`d_sc_profit_monthly_shelf_product` a1
    JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.product_id
    LEFT JOIN feods.fjr_product_list_manager_week a3 ON a3.week_end=@sweekend AND a3.business_area=a1.business_area AND a3.product_id=a1.product_id AND a3.product_type='原有'
    WHERE a1.`stat_month` = @smonth_str
    GROUP BY a1.business_area,a1.product_id
    ORDER BY a1.business_area,a2.SECOND_TYPE_ID,profit_rate2 DESC;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_num; #地区二级分类数量
CREATE TEMPORARY TABLE fe_dm.tmp_num(INDEX(business_area,SECOND_TYPE_ID)) AS
    SELECT `business_area`,SECOND_TYPE_ID,COUNT(1) ROWS FROM fe_dm.tmp_profit WHERE if_origin=1 GROUP BY `business_area`,SECOND_TYPE_ID ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_profit_line;
SET @rank_by='',@rank_num=0;
CREATE TEMPORARY TABLE fe_dm.tmp_profit_line(INDEX(business_area,SECOND_TYPE_ID)) AS # 地区二级分类高毛利线
    SELECT  business_area,SECOND_TYPE_ID, profit_rate2 profit_line
    FROM
        (SELECT  business_area,SECOND_TYPE_ID, profit_rate2
        FROM
            (SELECT  t1.business_area, product_id,t1.SECOND_TYPE_ID, profit_rate2,t2.rows
                ,IF(@rank_by = CONCAT(t1.business_area,t1.SECOND_TYPE_ID) ,@rank_num:=@rank_num+1,@rank_num:=1) rank_num
                ,@rank_by:= CONCAT(t1.business_area,t1.SECOND_TYPE_ID) rank_by
            FROM fe_dm.tmp_profit t1
            JOIN fe_dm.tmp_num t2 ON t2.business_area=t1.business_area AND t2.SECOND_TYPE_ID=t1.SECOND_TYPE_ID
            WHERE t1.if_origin=1
            ) t1
        WHERE  t1.rank_num/ROWS<=0.25
        ORDER BY business_area,SECOND_TYPE_ID,profit_rate2 DESC) t1
    GROUP BY business_area,SECOND_TYPE_ID
    ;
#插入最终数据
INSERT INTO fe_dm.dm_ma_HighProfit_list_monthly
    (sdate, business_area, product_id, profit_rate)
SELECT @smonth, t1.business_area, t1.product_id, profit_rate2
FROM fe_dm.tmp_profit t1
JOIN fe_dm.tmp_profit_line t2 ON t2.business_area=t1.business_area AND t2.SECOND_TYPE_ID=t1.SECOND_TYPE_ID
WHERE t1.profit_rate2>=t2.profit_line
;
-- 记录日志
CALL sh_process.`sp_sf_dw_task_log`('prc_dm_ma_HighProfit_list_monthly',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END