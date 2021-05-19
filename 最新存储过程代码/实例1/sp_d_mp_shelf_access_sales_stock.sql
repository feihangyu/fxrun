CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_shelf_access_sales_stock`()
BEGIN
-- =============================================
-- Author:	财务部
-- Create date: 
-- Modify date: 
-- Description:	
-- 	货架进销存结果及期初数据校验
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
set @this_month:= DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 DAY),'%Y%m');
SET @last_month:= DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 2 month),'%Y%m');
-- 以PC后台的货架进销存统计表为主表跑财务的货架进销存结果
if day(current_date) = 1 then
DELETE FROM feods.D_MP_shelf_system_temp_table_main WHERE STAT_DATE = @this_month;
INSERT INTO feods.D_MP_shelf_system_temp_table_main
(
 STAT_DATE 
,business_area         
,product_id            
,product_name                      
,last_remain_qty 
,last_remain_amount        
,actual_shelf_qty      
,sale_qty              
,due_stock_qty         
,eorror_qty            
,actual_stock_qty      
)
SELECT 
 REPLACE(t.`STAT_MONTH`,'-','') AS STAT_DATE     -- 统计日期 
,c.business_name                              -- 地区名称                 
,p.`PRODUCT_CODE2`                            -- 商品编号                 
,p.`PRODUCT_NAME`                             -- 商品名称                 
,0 AS last_remain_qty     -- 上期结余库存
,0 AS last_remain_amount  -- 上期结余库存金额
,SUM(t.CURR_FILL_NUM) AS actual_shelf_qty       -- 本期实际上架量 
,SUM(t.CURR_SALE_NUM) AS sale_qty               -- 本期销售量         
,SUM(t.CURR_SHOULD_STOCK) AS due_stock_qty      -- 本期应有库存   
,SUM((IFNULL(t.CURR_ACTUAL_STOCK,0) - IFNULL(t.CURR_SHOULD_STOCK,0))) AS eorror_qty    -- 本期损耗量 
,SUM(t.CURR_ACTUAL_STOCK) AS actual_stock_qty   -- 本期实际库存                             
FROM fe.sf_statistics_product_inventory t
  , fe.`sf_product` p
  ,feods.`fjr_city_business` c
  WHERE t.`STAT_MONTH`= DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 DAY),'%Y-%m')
    AND t.`PRODUCT_ID`= p.`PRODUCT_ID`
    AND  SUBSTRING_INDEX(SUBSTRING_INDEX(t.REGION_NAME, ',', 2),',',-1)=c.city_name
--     AND (p.`PRODUCT_CODE2` LIKE 'FE%' OR p.product_code2 LIKE 'SH%')
GROUP BY  c.business_name
         ,p.`PRODUCT_CODE2`;
end if;
--  利用上月的汇总结果表更新上月的货架进销存表的加权平均采购价 
UPDATE feods.D_MP_shelf_system_temp_table_main t
   SET t.weighted_average_price = (
   SELECT s.weighted_average_price FROM feods.csl_finance_instock_sales_outstock_table s
   WHERE s.`BUSINESS_AREA` = t.`business_area`
     AND s.`product_id` = t.`product_id`
     AND s.`STAT_DATE`= @last_month)
 WHERE t.`STAT_DATE`= @last_month
 AND (t.`business_area`,t.`product_id`) IN (SELECT s.`BUSINESS_AREA`,s.`product_id` FROM  feods.csl_finance_instock_sales_outstock_table s WHERE s.`STAT_DATE`= @last_month);
--  --  利用上月的汇总结果表更新上月的货架进销存表的上期期末库存金额
UPDATE feods.D_MP_shelf_system_temp_table_main t
SET t.stock_amount = t.`actual_stock_qty` * t.weighted_average_price
WHERE t.`STAT_DATE`= @last_month;
 
     
 -- 本期期初数量更新
 UPDATE feods.D_MP_shelf_system_temp_table_main t
 LEFT JOIN 
 (SELECT
   n.`business_area`,
   n.`product_id`,
   n.`actual_stock_qty`
  FROM
  feods.D_MP_shelf_system_temp_table_main n
 WHERE n.`STAT_DATE`= @last_month) s
 ON s.`BUSINESS_AREA` = t.`business_area`
 AND s.`product_id` = t.`product_id`
 SET t.last_remain_qty = s.actual_stock_qty
 WHERE t.`STAT_DATE` = @this_month
;
-- 本期期初金额更新
 UPDATE feods.D_MP_shelf_system_temp_table_main t
 LEFT JOIN 
 (SELECT
   n.`business_area`,
   n.`product_id`,
   n.`stock_amount`
  FROM
  feods.D_MP_shelf_system_temp_table_main n
 WHERE n.`STAT_DATE`= @last_month) s
 ON s.`BUSINESS_AREA` = t.`business_area`
 AND s.`product_id` = t.`product_id`
 SET t.last_remain_amount = s.stock_amount
 WHERE t.`STAT_DATE` = @this_month
;
delete from feods.`D_MP_finance_statement_log` where smonth = @this_month and item_type=1;
insert into feods.`D_MP_finance_statement_log`(
 smonth                 
,item_type           
,this_month_qty            
,last_month_qty            
,this_month_amount         
,last_month_amount         
)
select
 @this_month smonth,
 1 as item_type,
 sum(if(a.`STAT_DATE`= @this_month,a.last_remain_qty,0)) this_month_qty,
 SUM(IF(a.`STAT_DATE`= @last_month,a.actual_stock_qty,0)) last_month_qty,
 SUM(IF(a.`STAT_DATE`= @this_month,a.last_remain_amount,0)) this_month_amount,
 SUM(IF(a.`STAT_DATE`= @last_month,a.stock_amount,0)) last_month_amount
from
 feods.D_MP_shelf_system_temp_table_main a
where a.STAT_DATE in (@this_month,@last_month);
update feods.`D_MP_finance_statement_log` g
set g.result = if(g.this_month_qty = g.last_month_qty and g.this_month_amount = g.last_month_amount,'相符','不相符')
where g.smonth = @this_month;
-- 5月货架进销存应财务要求计算应有库存并放在实际库存列
SELECT
 t.STAT_DATE                             
,t.business_area                             
,t.product_id                      
,t.product_name                          
,t.last_remain_qty                       
,t.last_remain_amount                    
,t.actual_shelf_qty            
,t.sale_qty                            
,t.due_stock_qty                     
,t.eorror_qty                          
,IFNULL(t.last_remain_qty,0)+IFNULL(t.`actual_shelf_qty`,0)-IFNULL(t.`sale_qty`,0) AS actual_stock_qty                  
,t.weighted_average_price              
,t.stock_amount
FROM feods.`D_MP_shelf_system_temp_table_main` t 
WHERE t.`STAT_DATE` = 202005
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_d_mp_shelf_access_sales_stock',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
END