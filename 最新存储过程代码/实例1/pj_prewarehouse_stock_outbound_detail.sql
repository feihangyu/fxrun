CREATE DEFINER=`feprocess`@`%` PROCEDURE `pj_prewarehouse_stock_outbound_detail`()
    SQL SECURITY INVOKER
BEGIN
    DECLARE l_test VARCHAR(1);
    DECLARE l_row_cnt INT;
    DECLARE CODE CHAR(5) DEFAULT '00000';
    DECLARE done INT;
    
	DECLARE l_table_owner   VARCHAR(64);
	DECLARE l_city          VARCHAR(64);
    DECLARE l_task_name     VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1
			CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
			CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                       # CALL feods.sp_event_task_log(l_task_name,l_state_date_hour,3);
		END; 
		
    SET l_task_name = 'pj_prewarehouse_stock_outbound_detail'; 
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();   
DELETE FROM feods.pj_prewarehouse_stock_outbound_detail WHERE DATE(CHECK_DATE)= DATE_SUB(CURDATE(),INTERVAL 1 DAY);
  INSERT INTO  feods.pj_prewarehouse_stock_outbound_detail
(CHECK_DATE, 
REGION_AREA,
BUSINESS_AREA,
CITY_NAME,
WAREHOUSE_NUMBER,
WAREHOUSE_NAME,
SHELF_CODE,
WAREHOUSE_ID,
SHELF_NAME,
PRODUCT_ID,
PRODUCT_CODE2,
PRODUCT_NAME,
PURCHASE_PRICE,
SF_CODE,
REAL_NAME,
FREEZE_STOCK,
AVAILABLE_STOCK,
TOTAL_STOCK,
AVAILABLE_AMOUNT,
FILL_TYPE,
OUTBOUND_QUANTITY,
OUTBOUND_AMOUNT,
OUTBOUND_DAYS 
)
SELECT
  DATE_SUB(CURDATE(),INTERVAL 1 DAY) AS 'CHECK_DATE',
  t9.REGION_AREA,  
  t9.BUSINESS_AREA,
  t4.CITY_NAME ,
  t9.WAREHOUSE_NUMBER,
  t9.WAREHOUSE_NAME,
  t3.SHELF_CODE,
  t1.WAREHOUSE_ID,
  t3.SHELF_NAME,
  t1.PRODUCT_ID,
  t6.PRODUCT_CODE2,
  CONCAT('"',t6.PRODUCT_NAME,'"') AS 'PRODUCT_NAME',
  t8.F_BGJ_POPRICE AS 'PURCHASE_PRICE', 
  t5.SF_CODE,
  t5.REAL_NAME,
  t1.FREEZE_STOCK,
  t1.AVAILABLE_STOCK,
  t1.TOTAL_STOCK,
  (t1.AVAILABLE_STOCK * t8.F_BGJ_POPRICE) AS 'AVAILABLE_AMOUNT',
  t2.FILL_TYPE ,
  t2.近15天出库量 AS OUTBOUND_QUANTITY,
  t2.近15天出库金额 AS OUTBOUND_AMOUNT,
  t2.近15天出库天数 AS OUTBOUND_DAYS
FROM
  (SELECT
    a.warehouse_id,
    a.product_id,
    SUM(a.freeze_stock) AS freeze_stock,
    SUM(a.available_stock) AS available_stock,
    SUM(
      a.freeze_stock + a.available_stock
    ) AS total_stock
  FROM
    fe.sf_prewarehouse_stock_detail a
    LEFT JOIN fe.sf_shelf_product_detail b
      ON a.warehouse_id = b.shelf_id
      AND a.product_id = b.product_id
  GROUP BY a.warehouse_id,
    a.product_id) t1
  LEFT JOIN
    (SELECT
      b.SUPPLIER_ID,
      a.PRODUCT_ID,
      b.FILL_TYPE,
      SUM(a.ACTUAL_SEND_NUM) AS '近15天出库量',
      SUM(a.ACTUAL_SEND_NUM * a.`PURCHASE_PRICE`) AS '近15天出库金额',
      COUNT(DISTINCT DATE(b.fill_time)) AS '近15天出库天数'
     FROM
      fe.sf_product_fill_order_item a
      LEFT JOIN fe.sf_product_fill_order b
        ON a.order_id = b.order_id
    WHERE b.SUPPLIER_ID IN
      (SELECT DISTINCT
        warehouse_id
      FROM
        fe.sf_prewarehouse_stock_detail)
      AND b.fill_type IN (8, 9, 10, 1, 2)
      AND b.FILL_TIME > DATE_SUB(date(CURDATE()),INTERVAL 15 DAY)
      AND b.order_status IN (2, 3, 4)
    GROUP BY b.SUPPLIER_ID,
      a.PRODUCT_ID, b.`FILL_TYPE`) t2
    ON t1.warehouse_id = t2.SUPPLIER_ID
    AND t1.product_id = t2.product_id
    LEFT JOIN fe.sf_shelf t3
    ON t1.warehouse_id = t3.shelf_id
  LEFT JOIN feods.zs_city_business t4
    ON SUBSTRING_INDEX(
      SUBSTRING_INDEX(t3.AREA_ADDRESS, ',', 2),
      ',',
      - 1
    ) = t4.city_name
   LEFT JOIN fe.pub_shelf_manager t5
     ON t3.manager_id = t5.manager_id
   LEFT JOIN fe.sf_product t6
     ON t1.product_id = t6.product_id
   LEFT JOIN 
(SELECT *
FROM feods.wt_warehouse_business_area 
WHERE pid IN
(SELECT MAX(pid) 
FROM feods.wt_warehouse_business_area
GROUP BY business_area)) t9
ON t4.business_area = t9.business_area
LEFT JOIN 
(SELECT DISTINCT m.FNUMBER,m.F_BGJ_POPRICE  
FROM sserp.T_BD_MATERIAL m) t8
ON t6.product_code2 = t8.FNUMBER
WHERE  t6.data_flag = 1
AND t5.data_flag = 1;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'pj_prewarehouse_stock_outbound_detail',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
 
COMMIT;
    END