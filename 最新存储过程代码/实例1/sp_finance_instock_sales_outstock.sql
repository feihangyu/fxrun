CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_finance_instock_sales_outstock`()
BEGIN
-- =============================================
-- Author:	财务
-- Create date: 
-- Modify date: 
-- Description:	
-- 	财务前置站进销存结果表（每月1号的0时）
-- 
-- =============================================
set @run_date:= current_date;
set @user:= current_user;
set @timestamp:= current_timestamp;
SET @this_month:= DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 DAY),'%Y%m');
SET @last_month:= DATE_FORMAT(DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 2 MONTH),'%Y%m');
if day(current_date)=1 then
  delete
  from
    feods.D_MP_Lead_warehouse_temp_table_main where STAT_DATE = @this_month;
  set @date_top:= DATE_SUB(DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY),INTERVAL 1 MONTH);
  set @date_end:= DATE_ADD(CURRENT_DATE,INTERVAL -DAY(CURRENT_DATE)+1 DAY);
  INSERT INTO feods.D_MP_Lead_warehouse_temp_table_main(
    STAT_DATE,
    PRODUCT_ID,
    PRODUCT_NAME,
    BUSINESS_AREA,
    instock_qty,
    transferred_shelf_qty,
    total_stock_qty
  )
  SELECT
--      应有库存=期初数量+本期入库数量-本期转入货架数量
--      本期货损数量=实际库存-应有库存  
     DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 MONTH),'%Y%m') AS STAT_DATE,
     t6.product_code2 AS '商品FE码',
     t6.product_name AS '商品名称',
     t4.business_name AS '区域',
    IFNULL(SUM(t9.instock_qty), 0) AS '本期入库数量',
    IFNULL(SUM(t2.transferred_shelf_qty),0) AS '本期转入货架数量',
    IFNULL(SUM(t1.total_stock_qty), 0) AS '实际库存'
   FROM
    (SELECT
      a.warehouse_id,
      a.product_id,
      SUM(a.total_stock) AS total_stock_qty
    FROM
      feods.`pj_prewarehouse_stock_detail` a
    WHERE a.check_date = DATE_SUB(@date_end,INTERVAL 1 DAY) 
    GROUP BY a.warehouse_id,
      a.product_id) t1   -- 前置站库存结余表作为主表
    LEFT JOIN
      (SELECT
        b.SUPPLIER_ID,
        a.PRODUCT_ID,
        SUM(a.ACTUAL_SEND_NUM) AS transferred_shelf_qty
      FROM
        fe.sf_product_fill_order_item a,
        fe.sf_product_fill_order b
      WHERE a.order_id = b.order_id
      AND b.SUPPLIER_ID IN
        (SELECT DISTINCT
          p.warehouse_id
        FROM
          fe.sf_prewarehouse_stock_detail p)
        AND b.fill_type IN (1, 2, 8, 9, 10, 15)
        AND b.fill_time >= @date_top
        AND b.fill_time < @date_end
         AND b.order_status IN (2, 3, 4)
      GROUP BY b.SUPPLIER_ID,
        a.PRODUCT_ID) t2        -- 前置站转出部分的计算
      ON t1.warehouse_id = t2.SUPPLIER_ID
      AND t1.product_id = t2.product_id
    LEFT JOIN
      (SELECT
        b.SHELF_ID,
        a.PRODUCT_ID,
        SUM(ACTUAL_SEND_NUM) AS instock_qty
      FROM
        fe.sf_product_fill_order_item a,
        fe.sf_product_fill_order b
      WHERE a.order_id = b.order_id
       AND b.SHELF_ID IN
        (SELECT DISTINCT
          h.warehouse_id
        FROM
          fe.sf_prewarehouse_stock_detail h)
        AND b.fill_type IN (1, 2, 4, 12, 10)
        AND b.fill_time >= @date_top
        AND b.fill_time < @date_end
        AND b.order_status IN (2, 3, 4)
      GROUP BY b.SHELF_ID,
        a.PRODUCT_ID) t9      -- 前置站转入部分的计算 
      ON t1.warehouse_id = t9.SHELF_ID
      AND t1.product_id = t9.product_id
    LEFT JOIN fe.sf_shelf t3
      ON t1.warehouse_id = t3.shelf_id
    LEFT JOIN feods.`fjr_city_business` t4
      ON t3.city = t4.city
    LEFT JOIN fe.sf_product t6
      ON t1.product_id = t6.product_id
  GROUP BY t4.business_name,
  t6.product_code2
  ;
end if;
--  利用上月的汇总结果表更新上月的前置仓进销存表的加权平均采购价
UPDATE feods.`D_MP_Lead_warehouse_temp_table_main` t
   SET t.weighted_average_price = (
   SELECT s.weighted_average_price FROM feods.csl_finance_instock_sales_outstock_table s
   WHERE s.`BUSINESS_AREA` = t.`business_area`
     AND s.`product_id` = t.`product_id`
     AND s.`STAT_DATE`= @last_month)
 WHERE t.`STAT_DATE`= @last_month
 AND (t.`business_area`,t.`product_id`) IN (SELECT s.`BUSINESS_AREA`,s.`product_id` FROM  feods.csl_finance_instock_sales_outstock_table s WHERE s.`STAT_DATE`= @last_month);
--  利用上月的汇总结果表更新上月的前置仓进销存表的上期期末库存金额
UPDATE feods.`D_MP_Lead_warehouse_temp_table_main` t
SET t.stock_amount = t.total_stock_qty * t.weighted_average_price
WHERE t.`STAT_DATE`= @last_month;
-- 本期期初数量更新
UPDATE feods.`D_MP_Lead_warehouse_temp_table_main` t
LEFT JOIN (
SELECT
 n.`BUSINESS_AREA`,
 n.`PRODUCT_ID`,
 n.`total_stock_qty`
FROM
  feods.`D_MP_Lead_warehouse_temp_table_main` n
WHERE n.`STAT_DATE`= @last_month
) s
ON s.`BUSINESS_AREA` = t.`business_area`
AND s.`product_id` = t.`product_id`
SET t.initial_qty = s.total_stock_qty
WHERE t.`STAT_DATE` = @this_month;
-- 本期期初金额更新
UPDATE feods.`D_MP_Lead_warehouse_temp_table_main` t
LEFT JOIN (
SELECT
 n.`BUSINESS_AREA`,
 n.`PRODUCT_ID`,
 n.stock_amount
FROM
  feods.`D_MP_Lead_warehouse_temp_table_main` n
WHERE n.`STAT_DATE`= @last_month
) s
ON s.`BUSINESS_AREA` = t.`business_area`
AND s.`product_id` = t.`product_id`
SET t.initial_amount = s.stock_amount
WHERE t.`STAT_DATE` = @this_month;
-- 更新应有库存
 UPDATE feods.D_MP_Lead_warehouse_temp_table_main t 
 SET t.aval_stock_qty = IFNULL(initial_qty,0)+ IFNULL(instock_qty,0)- IFNULL(transferred_shelf_qty,0)
 WHERE t.`STAT_DATE`= @this_month;
 
 -- 更新货损库存
 UPDATE feods.D_MP_Lead_warehouse_temp_table_main t 
 SET t.actual_sendto_shelf_qty = IFNULL(t.total_stock_qty,0) - IFNULL(t.aval_stock_qty,0)
 WHERE t.`STAT_DATE`= @this_month;
-- 更新财务进销存结果对比日志,方便查看期初期末是否对上
DELETE FROM feods.`D_MP_finance_statement_log` WHERE smonth = @this_month AND item_type=2;
INSERT INTO feods.`D_MP_finance_statement_log`(
 smonth                 
,item_type           
,this_month_qty            
,last_month_qty            
,this_month_amount         
,last_month_amount         
)
SELECT
 @this_month AS smonth,
 2 AS item_type,
 SUM(IF(a.`STAT_DATE`= @this_month,a.initial_qty,0)) this_month_qty,
 SUM(IF(a.`STAT_DATE`= @last_month,a.total_stock_qty,0)) last_month_qty,
 SUM(IF(a.`STAT_DATE`= @this_month,a.initial_amount,0)) this_month_amount,
 SUM(IF(a.`STAT_DATE`= @last_month,a.stock_amount,0)) last_month_amount
FROM
 feods.D_MP_Lead_warehouse_temp_table_main a
WHERE a.STAT_DATE IN (@this_month,@last_month);
UPDATE feods.`D_MP_finance_statement_log` g
SET g.result = IF(g.this_month_qty = g.last_month_qty AND g.this_month_amount = g.last_month_amount,'相符','不相符')
WHERE g.smonth = @this_month;
COMMIT;
-- 执行日志
CALL sh_process.`sp_sf_dw_task_log` (
  'sp_finance_instock_sales_outstock',
  DATE_FORMAT(@run_date, '%Y-%m-%d'),
  CONCAT('caisonglin@', @user, @timestamp)
);
END