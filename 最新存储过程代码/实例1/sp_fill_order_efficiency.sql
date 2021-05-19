CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_fill_order_efficiency`()
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
		
    SET l_task_name = 'sp_fill_order_efficiency'; 
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 由于补货订单的状态会发生变化，因此每次都更新7天的数据，一般情况下，前置仓补货7天一个周期的较多，且如果申请补货，一般7天内将会完成申请-上架的流程；
# 申请补货当天前14天出库量,中间表
DROP TEMPORARY TABLE IF EXISTS feods.d_sc_preware_apply_tmp;
CREATE TEMPORARY TABLE feods.d_sc_preware_apply_tmp
( KEY idx_sdate_preware_product(sdate,warehouse_id,product_id))
AS 
SELECT DATE(apply_time) AS sdate,   #### 申请日期
    a.shelf_id AS warehouse_id,
    a.PRODUCT_ID,
    a.order_id,
    b.order_status,
    b.supplier_type,
    b.fill_type,
    b.fill_time AS fill_time,
    a.actual_apply_num,
    a.actual_send_num,
    a.actual_sign_num,
    a.actual_fill_num,
    b.surplus_reason,
    b.sale_faulty_type
  FROM
    fe.sf_product_fill_order_item a
    JOIN fe.sf_product_fill_order b
      ON a.order_id = b.order_id
  WHERE b.shelf_id  IN (SELECT shelf_id FROM fe_dwd.`dwd_shelf_base_day_all` WHERE shelf_type = 9 AND data_flag =1)
    AND b.apply_time >= DATE_SUB(CURDATE(),INTERVAL 7 DAY)
    AND b.apply_time < CURDATE()
    AND b.supplier_type IN (2,9) #### 包括从大仓发往前置仓，前置仓发往前置仓的订单
    AND b.order_status IN (1,2,3,4,5,6,7,8) #### 订单状态不包括取消订单，其余都有
    AND b.fill_type IN (1,2,8,10)  ##人工、系统触发、要货以及前置站调前置站
    AND a.data_flag =1
    AND b.data_flag =1
    ; 
 
# 结果表 
 DELETE FROM feods.pj_fill_order_efficiency WHERE apply_time >= DATE_SUB(CURDATE(),INTERVAL 7 DAY);
 INSERT INTO feods.pj_fill_order_efficiency
 (apply_time,
  business_area ,
  region_area ,
  warehouse_name,
  warehouse_number, 
  order_id ,
  warehouse_id ,
  shelf_code, 
  shelf_name ,
  PRODUCT_ID,
  product_code2,
  product_name,
  available_stock,
  qualityqty,
  product_type, 
  forteen_bef_out,
  famine,
  fill_time,
  actual_apply_num,
  actual_send_num,
  actual_sign_num,
  actual_fill_num,
  order_status, 
  surplus_reason,
  sale_faulty_type,
  supplier_type, 
  fill_type
 )
 SELECT t1.sdate AS apply_time
, w.business_area
, w.region_area
, w.warehouse_name
, w.warehouse_number
, t1.order_id
, t1.warehouse_id
, s.shelf_code
, s.shelf_name
, p.PRODUCT_ID
, p.product_code2
, p.product_name
, t2.available_stock
, t3.qualityqty
, IFNULL(t2.product_type,t3.product_type) AS product_type
, t2.actual_send_forteen_qty 
,IF(IFNULL(t2.available_stock,0)/(t2.avg_send_num) <= 2 AND IFNULL(t2.product_type,t3.product_type) IN ('原有','新增'),"严重缺货",NULL) AS '严重缺货',
  t1.fill_time,
  t1.actual_apply_num,
  t1.actual_send_num,
  t1.actual_sign_num,
  t1.actual_fill_num,
  CASE t1.order_status WHEN 1 THEN "已申请" 
  WHEN 2 THEN "已发货"
  WHEN 3 THEN "已签收"
  WHEN 4 THEN "已上架"
  WHEN 5 THEN "退货中"
  WHEN 7 THEN "待调整"
  WHEN 8 THEN "已退货"
  END AS '订单状态',
  t1.surplus_reason,
  t1.sale_faulty_type,
  CASE t1.supplier_type WHEN 2 THEN "仓库" 
  WHEN 9 THEN "前置仓"
  END AS "发货方" ,
  CASE t1.fill_type WHEN 1 THEN "人工申请"
  WHEN 2 THEN "系统触发"
  -- WHEN 3 THEN "初始商品包订单"
  -- WHEN 4 THEN "撤架转移订单"
  -- WHEN 5 THEN "撤架负数订单"
  -- WHEN 6 THEN "调货调出订单"
  -- WHEN 7 THEN "调货调入订单"
  WHEN 8 THEN "要货订单"
  -- when 9 then "前置仓调能量站"
  WHEN 10 THEN "前置仓调前置仓"
  END  AS '订单类型'
    FROM feods.d_sc_preware_apply_tmp t1
  LEFT JOIN 
  (SELECT ADDDATE(sdate,1) AS asdate,sdate,warehouse_id,product_id,product_type,actual_send_forteen_qty,avg_send_num,available_stock
  FROM feods.`d_sc_preware_daily_report` 
  WHERE sdate >= SUBDATE(CURDATE(),8)) t2
  ON t1.sdate = t2.asdate
  AND t1.warehouse_id = t2.warehouse_id
  AND t1.product_id = t2.product_id
  JOIN fe_dwd.`dwd_shelf_base_day_all` s
  ON t1.warehouse_id = s.shelf_id
  AND s.data_flag =1
  JOIN fe_dwd.`dwd_product_base_day_all` p
  ON t1.product_id = p.product_id 
  JOIN fe_dwd.`dwd_pub_warehouse_business_area` w
  ON s.business_name = w.business_area
  AND w.to_preware = 1
  LEFT JOIN 
  (SELECT ADDDATE(t.`FPRODUCEDATE`,1) AS sdate,t.`FPRODUCEDATE`,t.`BUSINESS_AREA`,t.`WAREHOUSE_NUMBER`,t.`WAREHOUSE_NAME`,t.`PRODUCT_BAR`,t.`QUALITYQTY`,t.`PRODUCT_TYPE`
   FROM feods.`PJ_OUTSTOCK2_DAY` t
   WHERE t.`FPRODUCEDATE`>= SUBDATE(CURDATE(),8)) t3
   ON t1.sdate = t3.sdate
   AND w.`WAREHOUSE_NUMBER` = t3.warehouse_number
   AND p.`PRODUCT_CODE2` = t3.product_bar
 ;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_fill_order_efficiency',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END