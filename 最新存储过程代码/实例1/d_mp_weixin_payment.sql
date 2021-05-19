CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_mp_weixin_payment`(in_sdate DATE)
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
		
    SET l_task_name = 'd_mp_weixin_payment'; 
   
SET @sdate =  DATE_SUB(in_sdate,INTERVAL 1 DAY);
SET @sdate1 = in_sdate;
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DELETE FROM feods.d_mp_weixin_payment WHERE sdate = @sdate;
INSERT INTO feods.d_mp_weixin_payment
(sdate,
order_date,
pay_time,
order_id,
weixin_pay_type,
payment_type_gateway,
weixin_pay_id,
pay_amount,
shelf_type,
business_type
)
SELECT @sdate
, o.order_date
, p.pay_time
, p.`ORDER_ID`
,CASE WHEN p.PAY_TYPE = 1 THEN '微信支付'
WHEN p.PAY_TYPE = 8 THEN '微信委托付款'
WHEN  p.PAY_TYPE = 21 THEN '微信小程序支付'
END  AS 'weixin_pay_type',
o.payment_type_gateway,
p.`PAY_ID` AS weixin_pay_id,
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
  WHEN 'requirementOrder' THEN "自贩机收款正"
  END AS 'business_type'
FROM 
(SELECT *
FROM fe.sf_order_pay
WHERE PAY_TIME >= @sdate AND PAY_TIME < @sdate1
AND PAY_STATE = 2 
-- aND order_type IN ("topUpBalance","afterPayment","goodsOrder","smartShelf",9)
AND `PAY_TYPE` IN (1,8,21)) p
LEFT JOIN fe.`sf_order` o
ON p.order_id = o.order_id
JOIN fe.`sf_shelf` s
ON p.shelf_id = s.shelf_id
AND s.data_flag =1
WHERE (o.PAYMENT_TYPE_GATEWAY IN ('WeiXinPayJSAPI','WeiXinContractPay','WX_PAY_FACE') OR ISNULL(o.PAYMENT_TYPE_GATEWAY))
-- # 自贩机退款对冲，微信后台有，但系统查找无记录
-- UNION 
-- SELECT @sdate
-- , NULL AS order_date
-- , p.pay_time
-- , p.`ORDER_ID`
-- ,CASE WHEN p.PAY_TYPE = 1 THEN '微信支付'
-- WHEN p.PAY_TYPE = 8 THEN '微信委托付款'
-- WHEN  p.PAY_TYPE = 21 THEN '微信小程序支付'
-- END  AS 'weixin_pay_type',
-- NULL AS payment_type_gateway,
-- p.`PAY_ID` AS weixin_pay_id,
-- -p.pay_amount AS pay_amount,
--  CASE s.shelf_type WHEN 1 THEN "四层标准货架"
--   WHEN 2 THEN "冰箱"
--   WHEN 3 THEN "五层防鼠货架"
--   WHEN 4 THEN "虚拟货架"
--   WHEN 5 THEN "冰柜"
--   WHEN 6 THEN "智能货柜"
--   WHEN 7 THEN "自动贩卖机"
--   WHEN 8 THEN "校园货架"
--   WHEN 9 THEN "前置仓"
--   END AS "shelf_type",
--  "自贩机退款负" AS 'business_type'
-- FROM 
-- (SELECT *
-- FROM fe.sf_order_pay
-- WHERE PAY_TIME >= @sdate AND PAY_TIME < @sdate1
-- AND PAY_STATE = 2 
-- AND order_type = 'requirementOrder'
-- -- aND order_type IN ("topUpBalance","afterPayment","goodsOrder","smartShelf",9)
-- AND `PAY_TYPE` IN (1,8,21)) p
-- JOIN fe.`sf_shelf` s
-- ON p.shelf_id = s.shelf_id
-- AND s.data_flag =1
#企业福利
UNION ALL
SELECT @sdate,NULL AS order_date,p.pay_time,
p.`ORDER_ID`,
CASE WHEN p.PAY_TYPE = 1 THEN '微信支付'
WHEN p.PAY_TYPE = 8 THEN '微信委托付款'
WHEN  p.PAY_TYPE = 21 THEN '微信小程序支付'
END  AS 'weixin_pay_type',
"" AS payment_type_gateway,
p.`PAY_ID` AS weixin_pay_id,
p.pay_amount,
"" AS "shelf_type",
CASE order_type 
WHEN 'fengeqifu' THEN "企业福利" 
WHEN 'scanOrder' THEN "企业扫码支付"
WHEN 'requirementOrder' THEN '福利商城消费'
END AS 'business_type'
FROM  fe_pay.sf_order_pay_2 p
WHERE PAY_TIME >= @sdate AND PAY_TIME < @sdate1
AND PAY_STATE = 2 
-- aND order_type IN ("topUpBalance","afterPayment","goodsOrder","smartShelf",9)
AND `PAY_TYPE` IN (1,8,21) 
AND order_type IN ('fengeqifu','scanOrder','requirementOrder')
# 企业业务实收
UNION ALL
SELECT @sdate,go.order_date,ga.pay_time,
ga.order_id,
CASE WHEN ga.pay_type=1 THEN '微信支付' END AS payment_type_name,
'' AS PAYMENT_TYPE_GATEWAY,
ga.three_transaction_id AS '微信流水号', 
pay_amount AS amount,
'' AS '货架类型',
'企业业务实收' AS '业务类型'
FROM fe_goods.`sf_group_order_pay` ga
JOIN fe_goods.`sf_group_order` go
ON ga.order_id = go.order_id
AND  ga.pay_type =1 AND pay_state = 2 #微信，已支付
AND ga.pay_time >= @sdate AND ga.pay_time < @sdate1
# 企业业务退款
UNION ALL
SELECT @sdate,gr.apply_time,gr.finish_time,ga.order_id,
CASE WHEN ga.pay_type=1 THEN '微信支付' END AS payment_type_name,'' AS PAYMENT_TYPE_GATEWAY,
ga.three_transaction_id AS '微信流水号', -gr.refund_amount AS amount,
'' AS '货架类型',
CASE WHEN DATE(gr.apply_time) = DATE(gr.finish_time) THEN '企业退款(当日到账)' 
ELSE '企业退款(之前申请当日到账)'
END AS '业务类型'
FROM fe_goods.`sf_group_order_refund` gr
JOIN fe_goods.`sf_group_order_pay` ga
ON gr.order_id = ga.order_id
WHERE gr.data_flag = 1 AND gr.finish_time >= @sdate AND gr.finish_time < @sdate1
AND ga.pay_type =1
#企业业务退款（跨天）
UNION ALL
SELECT @sdate,gr.apply_time,gr.finish_time,ga.order_id ,
CASE WHEN ga.pay_type=1 THEN '微信支付' END AS payment_type_name,'' AS PAYMENT_TYPE_GATEWAY,
ga.three_transaction_id AS '微信流水号', -gr.refund_amount AS amount,
'' AS '货架类型',
'企业退款（当日申请未到账）' AS '业务类型'
FROM fe_goods.`sf_group_order_refund` gr
JOIN fe_goods.`sf_group_order_pay` ga
ON gr.order_id = ga.order_id
WHERE gr.data_flag = 1 
AND gr.apply_time >= @sdate AND gr.apply_time < @sdate1
AND gr.finish_time >= @sdate1
AND ga.pay_type = 1
#饿了么退款
UNION ALL
SELECT 
@sdate
,t4.`pay_time`
,t1.`refund_date`
,t3.order_id
,CASE WHEN t4.pay_type=1 THEN '微信支付' END AS payment_type_name
,'' AS PAYMENT_TYPE_GATEWAY
,t4.three_transaction_id AS '微信流水号'
,(-t4.`pay_amount`) AS pay_amount
,'' AS '货架类型'
,CONCAT(t1.`third_app_id`,"企业退款") AS '业务类型'
FROM fe_goods.sf_group_order_refund_third t1
JOIN fe_goods.sf_group_order_pay_third t2
ON t1.`transaction_order_id` = t2.`transaction_order_id`
AND t1.data_flag = 1
AND t2.data_flag = 1
JOIN fe_goods.`sf_group_order` t3
ON t2.`service_order_id` = t3.`order_id`
JOIN fe_goods.`sf_group_order_pay` t4
ON t3.`order_id` = t4.`order_id`
AND t4.`pay_type` = 1 
WHERE t1.`refund_date` >= @sdate AND t1.`refund_date` < @sdate1 
# 货架退款（已到账）
UNION ALL
SELECT  @sdate,ro.apply_time,ro.finish_time,rs.ORDER_ID,rs.`PAYMENT_TYPE_NAME`,rs.`PAYMENT_TYPE_GATEWAY`,rs.gateway_order_id,
- ro.refund_amount AS amount,
 CASE s.shelf_type WHEN 1 THEN "四层标准货架"
  WHEN 2 THEN "冰箱"
  WHEN 3 THEN "五层防鼠货架"
  WHEN 4 THEN "虚拟货架"
  WHEN 5 THEN "冰柜"
  WHEN 6 THEN "智能货柜"
  WHEN 7 THEN "自动贩卖机"
  WHEN 8 THEN "校园货架"
  WHEN 9 THEN "前置仓"
  END AS "货架类型",
CASE WHEN DATE(ro.apply_time) = DATE(ro.finish_time) THEN '货架退款(当日到账)' 
ELSE "货架退款(之前申请当日到账)"
END AS '业务类型'
FROM fe.`sf_order_refund_order`  ro
JOIN fe.`sf_order` rs 
ON rs.order_id = ro.order_id
AND ro.finish_time >= @sdate AND ro.finish_time < @sdate1 
AND ro.data_flag = 1 
AND ro.refund_status=5
AND rs.`ORDER_STATUS` IN (2,6,7)
AND rs.`PAYMENT_TYPE_NAME` IN ('微信委托扣款','顺手付微信支付','微信支付')
JOIN fe.`sf_shelf` s
ON ro.shelf_id = s.shelf_id
AND s.data_flag = 1
-- # 货架退款(跨天)
-- UNION ALL
-- SELECT  @sdate,ro.apply_time,ro.finish_time,rs.ORDER_ID,rs.`PAYMENT_TYPE_NAME`,rs.`PAYMENT_TYPE_GATEWAY`,rs.gateway_order_id,
-- - ro.refund_amount AS amount,
--  CASE s.shelf_type WHEN 1 THEN "四层标准货架"
--   WHEN 2 THEN "冰箱"
--   WHEN 3 THEN "五层防鼠货架"
--   WHEN 4 THEN "虚拟货架"
--   WHEN 5 THEN "冰柜"
--   WHEN 6 THEN "智能货柜"
--   WHEN 7 THEN "自动贩卖机"
--   WHEN 8 THEN "校园货架"
--   WHEN 9 THEN "前置仓"
--   END AS "货架类型",
-- '货架退款(当日申请未到账)' AS '业务类型'
-- FROM fe.`sf_order_refund_order`  ro
-- JOIN fe.`sf_order` rs 
-- ON rs.order_id = ro.order_id
-- AND ro.apply_time >= @sdate AND ro.apply_time < @sdate1
-- AND ro.finish_time >= @sdate1
-- AND ro.data_flag = 1 
-- AND ro.refund_status=5
-- AND rs.`ORDER_STATUS` IN (2,6,7)
-- AND rs.`PAYMENT_TYPE_NAME` IN ('微信委托扣款','顺手付微信支付','微信支付')
-- JOIN fe.`sf_shelf` s
-- ON ro.shelf_id = s.shelf_id
-- AND s.data_flag = 1
UNION ALL
#自动售卖机退款中
SELECT @sdate,ro.apply_time,ro.finish_time,rs.ORDER_ID,rs.`PAYMENT_TYPE_NAME`,rs.`PAYMENT_TYPE_GATEWAY`,rs.gateway_order_id AS '微信流水号',
- ro.refund_amount AS amount,
 CASE s.shelf_type WHEN 1 THEN "四层标准货架"
  WHEN 2 THEN "冰箱"
  WHEN 3 THEN "五层防鼠货架"
  WHEN 4 THEN "虚拟货架"
  WHEN 5 THEN "冰柜"
  WHEN 6 THEN "智能货柜"
  WHEN 7 THEN "自动贩卖机"
  WHEN 8 THEN "校园货架"
  WHEN 9 THEN "前置仓"
  END AS "货架类型",
'自动售卖机退款中' AS '业务类型'
FROM fe.`sf_order_refund_order`  ro
JOIN fe.`sf_order` rs 
ON rs.order_id = ro.order_id
AND ro.apply_time >= @sdate AND ro.apply_time < @sdate1
AND ro.data_flag = 1 
AND ro.refund_status = 4
AND rs.`ORDER_STATUS` IN (2,6,7)
AND rs.`PAYMENT_TYPE_NAME` IN ('微信委托扣款','顺手付微信支付','微信支付')
JOIN fe.`sf_shelf` s
ON ro.shelf_id = s.shelf_id
AND s.data_flag = 1;
-- -- # 智能货柜首次支付失败，后来支付成功，但是pay表没有更新支付时间的场景
-- DROP TEMPORARY TABLE IF EXISTS feods.d_mp_shelf8_crossday_tmp;
-- CREATE TEMPORARY TABLE feods.d_mp_shelf8_crossday_tmp
-- (KEY idx_order_payid(order_id,weixin_pay_id))
-- AS 
-- SELECT @sdate
-- , o.order_date
-- , o.pay_date
-- , p.`ORDER_ID`
-- ,
-- CASE WHEN p.PAY_TYPE = 1 THEN '微信支付'
-- WHEN p.PAY_TYPE = 8 THEN '微信委托付款'
-- WHEN  p.PAY_TYPE = 21 THEN '微信小程序支付'
-- END  AS 'weixin_pay_type',
-- o.payment_type_gateway,
-- p.`PAY_ID` AS weixin_pay_id,
-- p.pay_amount,
--  CASE s.shelf_type WHEN 1 THEN "四层标准货架"
--   WHEN 2 THEN "冰箱"
--   WHEN 3 THEN "五层防鼠货架"
--   WHEN 4 THEN "虚拟货架"
--   WHEN 5 THEN "冰柜"
--   WHEN 6 THEN "智能货柜"
--   WHEN 7 THEN "自动贩卖机"
--   WHEN 8 THEN "校园货架"
--   WHEN 9 THEN "前置仓"
--   END AS "shelf_type",
--  "智能柜首次支付失败现到账" AS 'business_type'
-- FROM fe_dwd.`dwd_shelf_base_day_all` s
-- JOIN fe.`sf_order` o
-- ON s.shelf_id = o.shelf_id
-- AND s.shelf_type = 6
-- AND s.data_flag =1
-- AND o.order_status IN (2,6,7)
-- AND o.`PAY_DATE` >= @sdate
-- AND o.pay_date < @sdate1
-- JOIN fe.`sf_order_pay` p
-- ON p.`ORDER_ID` = o.`ORDER_ID`
-- AND p.`PAY_STATE` =2
-- AND p.`PAY_TYPE` IN (1,8,21)
-- AND p.`UPDATE_TIME` >= @sdate
-- AND p.`UPDATE_TIME` < @sdate1
-- AND p.`PAY_TIME` < @sdate
-- ;
-- DELETE  t2
-- FROM feods.d_mp_weixin_payment t1
-- JOIN feods.d_mp_shelf8_crossday_tmp t2
-- WHERE t1.order_id = t2.order_id
-- AND t1.weixin_pay_id = t2.weixin_pay_id
-- AND t1.pay_amount = t2.pay_amount
-- AND t1.sdate >= DATE_FORMAT(@sdate,"%Y-%m-01")
-- AND t1.sdate < @sdate
-- ;
-- INSERT INTO feods.d_mp_weixin_payment
-- (sdate,
-- order_date,
-- pay_time,
-- order_id,
-- weixin_pay_type,
-- payment_type_gateway,
-- weixin_pay_id,
-- pay_amount,
-- shelf_type,
-- business_type
-- )
-- SELECT *
-- FROM feods.d_mp_shelf8_crossday_tmp
-- ;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'd_mp_weixin_payment',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('wuting@', @user, @timestamp)
  );
COMMIT;
    END