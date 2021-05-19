CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_user_suspect`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
#SET @ptime :=p_time; #获取执行时间  
#16-8   8.30发送   8.10 调度
#8-12   14发送     12.10调度
#12-16  17发送     16.10调度
SET @ptime :=NOW() ;
IF 
HOUR(@ptime)=8 
THEN 
SET @ptime1 :=CONCAT(  SUBDATE( CURRENT_DATE, 1 ), ' 16:00:00' ),
    @ptime2 :=CONCAT( CURRENT_DATE, ' 08:00:00' );
ELSEIF
HOUR(@ptime)=12
THEN
SET @ptime1 :=CONCAT( CURRENT_DATE, ' 08:00:00' ),
    @ptime2 :=CONCAT( CURRENT_DATE, ' 12:00:00' );
ELSEIF
HOUR(@ptime)=16
THEN
SET @ptime1 :=CONCAT( CURRENT_DATE, ' 12:00:00' ),
    @ptime2 :=CONCAT( CURRENT_DATE, ' 16:00:00' );
ELSE 
SET @ptime1 :=CONCAT( CURRENT_DATE, ' 00:00:00' ),
    @ptime2 :=CONCAT( CURRENT_DATE, ' 00:00:00' );
END IF;
### 区间未付款订单
DROP TEMPORARY TABLE IF EXISTS  fe_dm.temp_order1; 
CREATE TEMPORARY TABLE fe_dm.temp_order1 (INDEX (user_id,order_date)) AS
	SELECT
		user_id,
		order_id,
		order_status,
		MAX( order_date ) AS order_date,  #取最后
		gmv 
	FROM
		(
		SELECT DISTINCT
			a.user_id,
			order_id,
			order_status,
			order_date,
			PRODUCT_TOTAL_AMOUNT + DISCOUNT_AMOUNT + COUPON_AMOUNT AS gmv
		FROM
			fe.sf_order a
			LEFT JOIN fe.pub_member b ON a.user_id = b.member_id
			LEFT JOIN fe.pub_user_open uo ON a.user_id = uo.user_id 
			AND uo.data_flag = 1 
		WHERE
			order_date > @ptime1 #CONCAT(  SUBDATE( CURRENT_DATE, 1 ), ' 16:00:00' ) 
			AND order_date <= @ptime2 #CONCAT( CURRENT_DATE, ' 08:00:00' ) -- 这此处修改时间
			AND a.order_type = 1
			AND a.data_flag = 1 
			AND order_status !=2#未付款
		ORDER BY
			order_date DESC 
		) b 
	GROUP BY
		user_id;
 #### 区间付款订单		
DROP TEMPORARY TABLE IF EXISTS  fe_dm.temp_order2;
CREATE TEMPORARY TABLE fe_dm.temp_order2  AS 
	SELECT DISTINCT 
		user_id,
		order_date
	FROM
		(
		SELECT DISTINCT
			a.user_id,
			order_id,
			order_status,
			order_date,
			PRODUCT_TOTAL_AMOUNT + DISCOUNT_AMOUNT + COUPON_AMOUNT AS gmv
		FROM
			fe.sf_order a
			LEFT JOIN fe.pub_member b ON a.user_id = b.member_id
			LEFT JOIN fe.pub_user_open uo ON a.user_id = uo.user_id 
			AND uo.data_flag = 1 
		WHERE
			order_date > @ptime1 #CONCAT(  SUBDATE(  CURRENT_DATE, 1 ), ' 16:00:00' ) 
			AND order_date <= ADDDATE(@ptime2, INTERVAL 5 MINUTE) #CONCAT( CURRENT_DATE, ' 08:00:05' )  #增加5分钟 
			AND a.order_type = 1
			AND a.data_flag = 1 
			AND order_status =2#付款
		ORDER BY
			order_date DESC 
		) b ;
### 过滤掉5分钟之内支付成功的用户
DROP TEMPORARY TABLE IF EXISTS  fe_dm.temp_order3; 
CREATE TEMPORARY TABLE fe_dm.temp_order3  AS
SELECT DISTINCT a.*,IFNULL(b.user_id,0) FROM fe_dm.temp_order1  a
LEFT JOIN 			
 fe_dm.temp_order2  b
ON a.USER_ID=b.user_id 
# AND TIMESTAMPDIFF(SECOND,a.ORDER_DATE,b.ORDER_DATE)<=300 AND TIMESTAMPDIFF(SECOND,a.ORDER_DATE,b.ORDER_DATE)>=0 
#需求改为过滤掉周期内已支付
WHERE IFNULL(b.user_id,0)=0;
INSERT INTO   fe_dm.dm_user_suspect  
(
SELECT 
0,a.order_id,a.user_id,a.order_date,a.gmv, @ptime
FROM 
fe_dm.temp_order3 a WHERE a.user_id 
NOT IN 
(SELECT user_id FROM fe_dm.dm_user_suspect 
WHERE 
#order_date>= date_sub(curdate(),INTERVAL WEEKDAY(curdate()) DAY)
order_date> DATE_SUB(CURDATE(),INTERVAL 7 DAY)   #需求经确认改为7天之内不发送
)
);
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'prc_dm_user_suspect',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('feihangyu@', @user, @timestamp)
  );
  
COMMIT;	
END