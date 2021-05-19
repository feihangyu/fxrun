CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_d_mp_ssf_payment`(in_sdate DATE)
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
		
    SET l_task_name = 'sp_d_mp_ssf_payment'; 
# 包含了顺手付 及 升腾支付
   
SET @sdate1 =  DATE_add(in_sdate,INTERVAL 1 DAY);
SET @sdate = in_sdate;
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
delete from feods.`d_mp_ssf_payment` where sdate = @sdate;
insert into feods.`d_mp_ssf_payment`
( `sdate` ,
  `order_date`,
  `pay_time`,
  `order_id`,
  `ssf_pay_id`,
  `pay_order_id`,
  `ssf_pay_type`,
  `payment_type_gateway`,
  `pay_amount`,
  `shelf_type`,
  `business_type`
) 
SELECT @sdate,o.order_date,p.pay_time,
p.`ORDER_ID` AS order_id,
p.`PAY_ID` AS pay_id,
p.order_pay_id AS pay_order_id,
  CASE p.pay_type WHEN 2 THEN "微信顺手付"
  WHEN 3 THEN "建行龙支付"
  WHEN 6 THEN "云闪付"
  WHEN 10 THEN '顺手付一码付'
  WHEN 5 THEN '顺银支付'
  WHEN 13 THEN "升腾支付"
  END AS 'ssf_pay_type',
o.payment_type_gateway,
p.pay_amount,
 CASE s.shelf_type WHEN 1 THEN "四层标准货架"
  WHEN 2 THEN "冰箱"
  WHEN 3 THEN "五层防鼠货架"
  WHEN 4 THEN "虚拟货架"
  WHEN 5 THEN "冰柜"
  WHEN 6 THEN "智能货柜"
  WHEN 7 THEN "自动贩卖机"
  WHEN 8 THEN "校园货架"
  WHEN 9 THEN "前置仓"
  END AS "shelf_type",
  CASE p.order_type WHEN 'topUpBalance' THEN "e币充值"
  WHEN 'afterPayment' THEN "货架补款"
  WHEN 'goodsOrder' THEN "货架实收(不包括智能货架和退款)"
  WHEN "smartShelf" THEN '货架收款(智能货柜)'
  WHEN 9 THEN '货架收款(智能货柜9)'
  END AS 'business_type'
FROM 
(SELECT *
FROM fe.sf_order_pay 
WHERE PAY_TIME >= @sdate AND pay_time < @sdate1
AND PAY_STATE = 2 
-- aND order_type IN ("topUpBalance","afterPayment","goodsOrder","smartShelf",9)
AND `PAY_TYPE` IN (2,3,5,6,10,13)) p
LEFT JOIN fe.`sf_order` o
ON p.order_id = o.order_id
JOIN fe.`sf_shelf` s
ON p.shelf_id = s.shelf_id
AND s.data_flag =1
# 企业充值
UNION ALL
SELECT 
@sdate
-- ,r.group_customer_id AS '公司ID'
-- ,sc.group_name AS '公司名称'
,r.order_date AS '订单日期'
,r.pay_date AS '支付日期'
,r.recharge_order_id AS recharge_order_id
,r.gateway_order_id AS gateway_order_id
,b.trade_id AS trade_id
,CASE WHEN r.pay_type='SspB2cPay' THEN 'web网银支付' WHEN r.pay_type='transfer' THEN '线下支付' END "支付方式"
,pay_type AS payment_type_gateway
,l.amounts 
,"" AS shelf_type
,CASE WHEN r.recharge_type=1 THEN '企业充值实付金额' 
WHEN r.recharge_type=2 THEN '赠送金额'
WHEN r.recharge_type=3 THEN '内部福利' END AS recharge_type
FROM fe_group.sf_group_recharge_order r
JOIN fe_group.`sf_group_customer`  sc ON sc.group_customer_id = r.group_customer_id
LEFT JOIN fe_group.sf_group_wallet_log_business b ON (b.business_type = 1 AND b.business_id = r.recharge_order_id AND b.data_flag=1)
LEFT JOIN fe_group.sf_group_wallet_log l ON (b.trade_id = l.trade_id AND l.data_flag=1)
WHERE order_status=2
-- AND l.amounts>=1
AND r.data_flag=1
AND r.pay_type != 'transfer'
AND r.order_date >= @sdate AND r.order_date < @sdate1
#企业订单
UNION ALL
SELECT 
@sdate,
p.update_time AS order_date,
p.pay_time,
p.`ORDER_ID` AS order_id,
p.`PAY_ID` AS pay_id,
p.order_pay_id AS pay_order_id,
  CASE p.pay_type WHEN 2 THEN "微信顺手付"
  WHEN 3 THEN "建行龙支付"
  WHEN 6 THEN "云闪付"
  WHEN 10 THEN '顺手付一码付'
  WHEN 5 THEN '顺银支付'
  WHEN 13 THEN "升腾支付"
  END AS 'ssf_pay_type',
"" AS payment_type_gateway,
p.pay_amount,
"" AS "shelf_type",
"企业商城订单" AS 'business_type'
FROM  fe_pay.sf_order_pay_2 p
WHERE PAY_TIME >= @sdate AND pay_time < @sdate1
AND PAY_STATE = 2 
-- aND order_type IN ("topUpBalance","afterPayment","goodsOrder","smartShelf",9)
AND `PAY_TYPE` IN (2,3,5,6,10,13)
AND order_type = 'shopOrder';
-- ------------------------------------------------------------------------------------------
-- 更新昨日的招行一卡通支付的订单数据
DELETE FROM feods.`D_MP_CMBC_payment` WHERE sdate = @sdate;
insert into feods.`D_MP_CMBC_payment`(
 sdate            
,order_date       
,pay_time         
,order_id         
,CMBC_pay_type    
,CMBC_pay_id       
,GMV               
,pay_amount       
,third_amount     
,shelf_type
)
SELECT
  @sdate,
  t3.order_date,
  t3.pay_date,
  t2.order_id,
  t3.payment_type_name,
  t3.GATEWAY_ORDER_ID,
  t2.zh_gmv AS '招行GMV',
  t3.zh_AMOUNT AS '招行实收',
  t3.zh_third_AMOUNT AS '招行优惠金额',
  t5.item_name AS shelf_type
FROM
  (SELECT
    b.`ORDER_ID`,
    SUM(a.QUANTITY * a.SALE_PRICE) AS zh_gmv
  FROM
    fe.sf_order_item a
    LEFT JOIN fe.sf_order b
      ON a.`ORDER_ID` = b.`ORDER_ID`
    LEFT JOIN fe_group.sf_group_wallet_log_business k
      ON b.`ORDER_ID` = k.`business_id`
    LEFT JOIN fe_group.sf_group_emp e
      ON k.`add_user_id` = e.`emp_user_id`
    LEFT JOIN fe_group.sf_group_customer aa
      ON e.`group_customer_id` = aa.`group_customer_id`
  WHERE b.pay_date >= @sdate
    AND b.pay_date < @sdate1
    AND b.ORDER_STATUS IN (2, 6, 7)
    AND b.PAYMENT_TYPE_NAME = '招行一卡通'
  GROUP BY b.`ORDER_ID`) t2
  INNER JOIN
    (SELECT distinct
      b.`ORDER_ID`,
      b.order_date,
      b.pay_date,
      b.payment_type_name,
      b.GATEWAY_ORDER_ID,
      b.shelf_id,
      SUM(b.PRODUCT_TOTAL_AMOUNT-IFNULL(c.refund_amount, 0)) AS zh_AMOUNT,
      SUM(b.`third_discount_amount`) AS zh_third_AMOUNT
    FROM
      fe.sf_order b
      LEFT JOIN fe_group.sf_group_wallet_log_business g
        ON b.`ORDER_ID` = g.`business_id`
      LEFT JOIN fe_group.sf_group_emp e
        ON g.`add_user_id` = e.`emp_user_id`
      LEFT JOIN fe_group.sf_group_customer aa
        ON e.`group_customer_id` = aa.`group_customer_id`
      LEFT JOIN fe.sf_order_refund_order c
        ON b.ORDER_ID = c.order_id
        AND c.refund_status = 5
    WHERE b.pay_date >= @sdate
      AND b.pay_date < @sdate1
      AND b.ORDER_STATUS IN (2, 6, 7)
      AND b.PAYMENT_TYPE_NAME = '招行一卡通'
    GROUP BY b.`ORDER_ID`) t3
    ON t2.order_id = t3.order_id
  INNER JOIN fe.`sf_shelf` t4
    ON t3.shelf_id = t4.shelf_id
  INNER JOIN
    (SELECT
      m.`ITEM_VALUE`,
      m.`ITEM_NAME`
    FROM
      fe.`pub_dictionary_item` m
    WHERE m.`DICTIONARY_ID` IN (8)) t5
    ON t4.shelf_type = t5.item_value;
-- ------------------------------------------------------------------------------------------
-- 更新昨日的e币支付的货架明细数据
delete from feods.D_MP_epay_shelf_detail where sdate = @sdate;
insert into feods.D_MP_epay_shelf_detail (
 sdate                  
,shelf_id                 
,pay_amount             
,shelf_type)
SELECT
 @sdate,
 t.shelf_id,
 t.pay_amount,
 m.item_name
FROM
(SELECT
  g.`SHELF_ID`,
  SUM(g.`AMOUNTS`) AS pay_amount
FROM
  fe.`user_member_wallet_log` g
WHERE g.`DATA_FLAG`= 1
AND g.`TRADE_TYPE` = 2
AND g.`TRADE_STATUS` = 1
AND g.`FROM_TYPE` = 2
AND g.`PAY_DATE` >= @sdate
AND g.`PAY_DATE` < @sdate1
AND g.`SHELF_ID` IS NOT NULL
GROUP BY g.`SHELF_ID`) t
JOIN fe.`sf_shelf` f
ON t.shelf_id = f.shelf_id
JOIN fe.`pub_dictionary_item` m
ON f.shelf_type = m.item_value
AND m.`DICTIONARY_ID` = 8;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_d_mp_ssf_payment',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
COMMIT;
    END