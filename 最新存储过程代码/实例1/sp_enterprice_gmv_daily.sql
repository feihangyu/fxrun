CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_enterprice_gmv_daily`()
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
		
    SET l_task_name = 'sp_enterprice_gmv_daily'; 
-- SET @sdate1 = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
-- SET @sdate2 = DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY),'%Y-%m-01'); 
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
DELETE FROM feods.pj_enterprice_gmv_daily WHERE sdate = DATE_SUB(CURDATE(),INTERVAL 1 DAY);
# 插入每日GMV
INSERT INTO feods.pj_enterprice_gmv_daily
(sdate
,shelf_id
,shelf_name
,group_customer_id
,group_name
,GMV
,quantity 
,shelf_type
,if_bind
,bind_cnt
)
SELECT DATE_SUB(CURDATE(),INTERVAL 1 DAY) AS sdate
,o.shelf_id
,s.shelf_name
,c.`group_customer_id`
,c.group_name
,SUM(oi.quantity * oi.sale_price) AS GMV
,SUM(oi.quantity) AS quantity  
,CASE s.SHELF_TYPE WHEN 1 THEN '四层标准货架'
WHEN 3 THEN "五层防鼠货架"
END AS shelf_type
,IF(r.bind_cnt IS NULL,0,1) AS if_bind
,IFNULL(r.bind_cnt,0) AS bind_cnt
FROM fe_group.sf_group_customer c
JOIN fe_group.sf_group_emp e
ON c.group_customer_id = e.group_customer_id
JOIN fe.sf_order o
ON e.customer_user_id = o.user_id
JOIN fe.sf_order_item oi
ON o.order_id = oi.ORDER_ID
JOIN fe.sf_shelf s
ON o.SHELF_ID = s.SHELF_ID
LEFT JOIN 
(SELECT s.shelf_id,COUNT(r.SECONDARY_SHELF_ID) AS bind_cnt
FROM fe.sf_shelf s
JOIN fe.sf_shelf_relation_record r
ON s.shelf_id = r.main_shelf_id
WHERE s.data_flag =1
AND r.data_flag = 1
AND r.UNBIND_TIME IS NULL
GROUP BY s.shelf_id
) r
ON s.shelf_id = r.shelf_id
WHERE o.order_date >= DATE_SUB(CURDATE(),INTERVAL 1 DAY)
AND o.order_date < CURDATE()
AND o.ORDER_STATUS = 2
AND c.group_name NOT IN('丰e足食店主管理组','丰e足食风控组','丰e足食经营管理组','丰e足食市场组','丰e足食物流管理组')
AND s.data_flag =1
GROUP BY DATE(o.order_date),o.shelf_id;
# 月度累计
UPDATE feods.pj_enterprice_gmv_daily a1
JOIN 
(SELECT DATE_SUB(CURDATE(),INTERVAL 1 DAY) AS sdate
,o.shelf_id
,c.`group_customer_id`
,c.group_name
,SUM(oi.quantity * oi.sale_price) AS GMV
,SUM(oi.quantity) AS quantity  
FROM fe_group.sf_group_customer c
JOIN fe_group.sf_group_emp e
ON c.group_customer_id = e.group_customer_id
JOIN fe.sf_order o
ON e.customer_user_id = o.user_id
JOIN fe.sf_order_item oi
ON o.order_id = oi.ORDER_ID
WHERE o.order_date >= DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY),'%Y-%m-01')
AND o.order_date < CURDATE()
AND o.ORDER_STATUS = 2
AND c.group_name NOT IN('丰e足食店主管理组','丰e足食风控组','丰e足食经营管理组','丰e足食市场组','丰e足食物流管理组')
GROUP BY o.shelf_id) a2
ON a1.sdate = a2.sdate
AND a1.shelf_id = a2.shelf_id
SET a1.`month_accumulate_gmv` = a2.GMV
,a1.`month_accumulate_qty` = a2.quantity; 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_enterprice_gmv_daily',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END