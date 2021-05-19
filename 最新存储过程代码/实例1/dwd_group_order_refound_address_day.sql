CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_group_order_refound_address_day`()
BEGIN
   SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   SET @month_3_date = SUBDATE(@end_date,INTERVAL 90 DAY);
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @week_flag := (@w = 6);
   SET @timestamp := CURRENT_TIMESTAMP();
-- 先删除昨日的数据 
DELETE FROM fe_dwd.dwd_group_order_refound_address_day WHERE pay_time >= @start_date;
 -- 全量添加最新退款的表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_1_1 ;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_1_1  AS
SELECT ss.order_item_id,
  TRIM(sr.gateway_pay_id) AS gateway_pay_id_rufound,  -- 支付网关的退款单ID 
  IFNULL(sr.refund_amount, 0) AS refund_amount -- 退款金额              
FROM fe_goods.sf_group_order_refund_item ss 
JOIN fe_goods.sf_group_order_refund_pay sr
ON ss.refund_order_id = sr.refund_order_id 
AND sr.data_flag = 1
AND sr.state=2
AND ss.data_flag = 1
;
CREATE INDEX dwd_group_order_refound_address_day_1_1
ON fe_dwd.dwd_group_order_refound_address_day_1_1 (order_item_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_order_refound_address_day_1`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_1  AS 
SELECT
  CASE
	WHEN c.pay_type = 1
	THEN '微信支付'
	WHEN c.pay_type = 2
	THEN '手工线下支付'
	WHEN c.pay_type = 3
	THEN '月结付款'
	WHEN c.pay_type = 4
	THEN 'E币支付'
	WHEN c.pay_type = 5
	THEN '顺银支付'
	WHEN c.pay_type = 6
	THEN '顺手付云闪付'
	WHEN c.pay_type = 7
	THEN '招行一卡通'
	WHEN c.pay_type =8
	THEN '微信委托扣款'
	WHEN c.pay_type = 9
	THEN '餐卡支付'
	WHEN c.pay_type = 10
	THEN '顺手付一码付'
	WHEN c.pay_type = 11
	THEN '企业代扣'
	WHEN c.pay_type = 12
	THEN '小蜜蜂积分支付'
	WHEN c.pay_type = 13
	THEN '升腾支付'
	WHEN c.pay_type = 14
	THEN '兑换卡兑换'
	WHEN c.pay_type = 15
	THEN '中国移动和包支付'
	WHEN c.pay_type = 16
	THEN '组合支付'
	WHEN c.pay_type = 22
	THEN '微信H5支付'
	WHEN c.pay_type = 23
	THEN '微信刷脸支付'
	WHEN c.pay_type = 24
	THEN '顺丰丰侠支付'
	WHEN c.pay_type = 26
	THEN '云闪付直连免密支付'
	WHEN c.pay_type = 27
	THEN '招行免密支付'
	WHEN c.pay_type = 29
	THEN '劳保支付'
	WHEN c.pay_type = 30
	THEN '慰问支付'
	WHEN c.pay_type = 31
	THEN '万翼支付'	
  END AS pay_type_desc,  -- 支付类型
  c.pay_type,
  b.order_type order_type_number,  
 CASE
    WHEN b.order_type = 1
    THEN '实物订单'
    WHEN b.order_type = 2
    THEN '虚拟订单'
    WHEN b.order_type = 3
    THEN '第三方充值订单'
    WHEN b.order_type = 4
    THEN '欧非卡密商品订单'
    WHEN b.order_type = 5
    THEN '饿了么订单'
    WHEN b.order_type = 6
    THEN '网易严选订单'
    WHEN b.order_type = 7
    THEN '顺丰优选订单'
    WHEN b.order_type = 8
    THEN '美餐订单'
    WHEN b.order_type = 9
    THEN '生活缴费'
    WHEN b.order_type = 10
    THEN '拼团订单'
    WHEN b.order_type = 11
    THEN '滴滴订单'
    WHEN b.order_type = 12
    THEN '京东'
    WHEN b.order_type = 13
    THEN '口碑到店'
    WHEN b.order_type = 14
    THEN '票牛'
    WHEN b.order_type = 15
    THEN '本来生活'
    WHEN b.order_type = 16
    THEN '天虹现金券'
    WHEN b.order_type = 17
    THEN '库盒'
    WHEN b.order_type = 18
    THEN '饿了么团餐'
    WHEN b.order_type = 19
    THEN '苏宁订单'
  END AS order_type,  -- 订单类型
  b.sale_channel,
  CASE
    WHEN b.order_from = 1
    THEN '销售助手'
    WHEN b.order_from = 3
    THEN '企业采购'
    WHEN b.order_from = 2 AND b.sale_channel = '0' THEN '丰e能量站'
    WHEN b.order_from = 2 AND b.sale_channel = 'PAYQB' THEN '平安壹钱包'
    WHEN b.order_from = 2 AND b.sale_channel = 'QYFL' THEN '企业福利前台'
    WHEN b.order_from = 2 AND b.sale_channel = 'SFIM' THEN '丰声渠道'
    WHEN b.order_from = 2 AND b.sale_channel = 'ST_PAY' THEN '升腾'
    WHEN b.order_from = 2 AND b.sale_channel = 'XMF' THEN '小蜜丰'
    WHEN b.order_from = 2 AND b.sale_channel = 'ZCWL' THEN '中创物流'
    WHEN b.order_from = 2 AND b.sale_channel = 'fengxiang' THEN '丰享'
	WHEN b.order_from = 2 AND b.sale_channel = 'SF_COD' THEN '顺丰cod'
    WHEN b.order_from = 2 AND b.sale_channel = 'zxcy' THEN '正心诚意'
     WHEN b.order_from = 2 AND b.sale_channel = 'ZD' THEN '中电'
      WHEN b.order_from = 2 AND b.sale_channel = '1001' THEN '中小月结'
      WHEN b.order_from = 2 AND b.sale_channel = 'SF_FX' THEN '丰侠'
      WHEN b.order_from = 2 AND b.sale_channel = 'SYHNQ' THEN '速运湖南区兑换卡消费'
      WHEN b.order_from = 2 AND b.sale_channel = 'YKTQD' THEN '亿咖通渠道'
    WHEN b.order_from = 2 AND b.sale_channel = 'YKTKJQD' THEN '浙江亿咖通科技有限公司'
	WHEN b.order_from = 2 AND b.sale_channel = 'BJDC' THEN '福利商城'
	WHEN b.order_from = 2 AND b.sale_channel = 'ZXCYDX' THEN '正心诚意-电信'
	WHEN b.order_from = 2 AND b.sale_channel = 'ZXCY_ZXGH' THEN '正心诚意-正心关怀'
	WHEN b.order_from = 2 AND b.sale_channel = 'FSQD' THEN '飞书上的企业' 
	WHEN b.order_from = 2 AND b.sale_channel = 'WYYC' THEN '万翼云城' 
	WHEN b.order_from = 2 AND b.sale_channel = 'ZDKQD' THEN '中电科渠道' 
	WHEN b.order_from = 2 AND b.sale_channel = 'FAYD' THEN '福安移动'  
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME
  END AS sale_from,  -- 销售渠道
  CASE
    WHEN b.order_from = 1
    THEN 'bd下单'
    WHEN b.order_from = 2
    THEN '用户自主下单'
    WHEN b.order_from = 3
    THEN '企业用户下单'
  END AS order_from,  -- 订单来源
  b.freight_amount AS  freight_amount , -- '运费_订单',
  a.freight_amount AS  freight_amount_item, -- '运费_明细'
  b.supplyerid AS supplyerid,  -- 供应商ID
  REPLACE(
    REPLACE(
      REPLACE(s.group_name, CHAR(10), ''),
      CHAR(13),
      ''
    ),
    CHAR(9),
    ''
  ) AS group_name, -- 供应商名称
 b.order_date,  -- 订单日期
  b.order_user_id AS order_user_id,  -- 用户ID
  b.group_link_id,
  a.order_id AS order_id,  -- 订单号
  IF(c.parent_order_pay_id =0,c.order_id,c.parent_order_pay_id) parent_order_pay_id,
  a.order_item_id AS order_item_id,  -- 订单明细ID
  c.three_transaction_id AS three_transaction_id,  -- 第三方订单号
  c.gateway_pay_id AS gateway_pay_id ,  -- 支付网关,
  c.order_pay_id,
  h.item_name AS order_status_name,
  a.product_spec_id AS product_spec_id,  -- sku商品编码
  a.product_name,
  a.quantity AS quantity,  -- 销量
  a.purchase_unit_price AS purchase_unit_price,  -- 采购单价
  a.sale_unit_price AS sale_unit_price,  -- 销售单价
  a.origin_sale_unit_price AS origin_sale_unit_price,  -- 原销售单价
  b.order_discount_amount ,                          -- 优惠金额_订单                                                                                                                                                        
  b.coupon_total_amount  AS coupon_total_amount_order,  --  优惠券优惠金额_订单
  a.discount_total_amount AS discount_total_amount,  -- 折扣优惠总金额
  a.coupon_total_amount ,  --  优惠券优惠总金额
  a.real_total_amount AS real_total_amount,  -- 商品实收
  b.order_total_amount AS order_total_amount,  -- 最终订单结算金额
  b.sale_total_amount AS sale_total_amount,   -- 订单销售金额
  b.purchase_total_amount AS purchase_total_amount,  -- 订单采购总价
  c.pay_amount AS pay_amount,                    -- pay_订单实收  
  c.pay_discount_amount AS pay_discount_amount,  -- pay_优惠金额
  CASE
    WHEN c.pay_state = 1
    THEN '未支付'
    WHEN c.pay_state = 2
    THEN '已支付'
  END AS pay_state,  -- 支付状态
  sr.gateway_pay_id_rufound,  -- 支付网关的退款单ID   sr.gateway_pay_id
  IFNULL(sr.refund_amount, 0) AS refund_amount,  -- 退款金额                 sr.refund_amount
  a.cost_percent AS cost_percent,  -- 成本比例，单位
  c.pay_time , -- 支付日期
  b.finish_time,
  b.supply_channel
FROM
  fe_goods.sf_group_order b
  JOIN fe_goods.sf_group_order_item a
    ON a.order_id = b.order_id
  JOIN fe_goods.sf_group_order_pay c  
    ON a.order_id = c.order_id
-- LEFT JOIN fe_goods.sf_group_order_refund_item ss ON (ss.order_item_id = a.order_item_id AND ss.data_flag = 1)  -- 订单退款表  获取明细订单的退款金额
-- LEFT JOIN fe_goods.sf_group_order_refund_pay sr ON (ss.refund_order_id = sr.refund_order_id AND sr.data_flag = 1 AND sr.state=2)
LEFT JOIN 
fe_dwd.dwd_group_order_refound_address_day_1_1 sr 
ON sr.order_item_id = a.order_item_id   
  LEFT JOIN
    (SELECT
      ITEM_VALUE,
      ITEM_NAME
    FROM
      fe.pub_dictionary_item
    WHERE dictionary_id = 192) d  -- 获取销售渠道
    ON b.sale_channel = d.ITEM_VALUE	
  LEFT JOIN fe_group.sf_group_supply s  -- 企业信息表  一个企业id对应一条记录
    ON s.group_id = a.supply_group_id   -- 企业ID=供应商企业ID
	AND s.data_flag =1
  LEFT JOIN
    (SELECT
      item_value,
      item_name
    FROM
      fe.pub_dictionary_item
    WHERE dictionary_id = 227) h   -- 订单状态
    ON b.order_status = h.ITEM_VALUE
WHERE a.data_flag = 1
  AND b.data_flag = 1
  AND c.pay_state = 2
  AND c.data_flag = 1 
  AND c.pay_time >= @start_date 
  AND c.pay_time < @end_date
;
CREATE INDEX idx_dwd_group_order_refound_address_day_1
ON fe_dwd.dwd_group_order_refound_address_day_1 (order_id,group_link_id);
/*
-- 测试的数据删除掉
DELETE FROM fe_dwd.dwd_group_order_refound_address_day_1  WHERE order_item_id = '21921467300900000';
-- 12月8号网易严选出故障，删除脏数据
DELETE FROM fe_dwd.dwd_group_order_refound_address_day_1  WHERE order_item_id = '23204761600670000' AND gateway_pay_id_rufound IS NULL ;
DELETE FROM fe_dwd.dwd_group_order_refound_address_day_1  WHERE order_item_id = '23204780201340007' AND gateway_pay_id_rufound IS NULL ;
*/
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`dwd_group_order_refound_address_day_2`;  
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_2  AS 
SELECT 
a.order_item_id,
  a.group_link_id,
 e.link_name AS link_name,  -- 收货联系人
  e.mobile AS mobile,  -- 收货电话
  e.province AS province,  -- 收货省
  e.city AS city, -- 收货市
  e.district AS district,  -- 收货区
  REPLACE(
    REPLACE(
      REPLACE(e.address, CHAR(10), ''),
      CHAR(13),
      ''
    ),
    CHAR(9),
    ''
  ) AS address,  -- 详细地址
  f.add_time AS add_time,  -- 运单号添加时间
  REPLACE(
    REPLACE(
      REPLACE(
        e1.delivery_link_assign_id,
        CHAR(10),
        ''
      ),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS delivery_link_assign_id,  -- 发货单号
  REPLACE(
    REPLACE(
      REPLACE(f.express_number, CHAR(10), ''),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS express_number,  -- 运单号
  REPLACE(
    REPLACE(
      REPLACE(
        GROUP_CONCAT(f.express_group_name),
        CHAR(10),
        ''
      ),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS express_group_name  -- 物流公司
  FROM
fe_dwd.dwd_group_order_refound_address_day_1 a
    JOIN  fe_goods.sf_group_delivery_link e
    ON a.order_id = e.order_id    
	-- AND a.group_link_id = e.group_link_id
	AND e.data_flag=1 
    LEFT JOIN fe_goods.sf_group_delivery_link_assian e1   -- 收件人商品分派表  一个明细订单有多个运单号  剔除掉这些订单 不发散 获取发货单号
    ON e.delivery_link_id = e1.delivery_link_id
	AND a.order_item_id = e1.order_item_id
	AND e1.data_flag=1
  LEFT JOIN fe_goods.sf_group_delivery_express f  -- 订单人员快递信息表 获取运单号 物流公司 运单号添加时间 
    ON e.delivery_link_id = f.delivery_link_id   -- 收件人信息
	AND f.data_flag =1
	GROUP BY 
a.order_item_id,
  a.group_link_id;
CREATE INDEX idx_dwd_group_order_refound_address_day_2
ON fe_dwd.dwd_group_order_refound_address_day_2 (order_item_id,group_link_id);
INSERT INTO fe_dwd.dwd_group_order_refound_address_day(
pay_type,
pay_type_desc,
order_type_number,
order_type,
sale_channel,
sale_from,
order_from,
link_name,
mobile,
province,
city,
district,
address,
add_time,
delivery_link_assign_id,
express_number,
express_group_name,
freight_amount,
freight_amount_item,
supplyerid,
group_name,
order_date,
order_user_id,
parent_order_pay_id,
order_id,
order_item_id,
three_transaction_id,
gateway_pay_id,
order_pay_id,
order_status_name,
product_spec_id,
product_name,
quantity,
purchase_unit_price,
sale_unit_price,
origin_sale_unit_price,
order_discount_amount ,
coupon_total_amount_order,
discount_total_amount,
coupon_total_amount,
real_total_amount,
order_total_amount,
sale_total_amount,
purchase_total_amount,
pay_amount,
pay_discount_amount,
pay_state,
gateway_pay_id_rufound,
refund_amount,
cost_percent,
pay_time,
finish_time,
supply_channel
)
SELECT
a.pay_type,
a.pay_type_desc,
a.order_type_number,
a.order_type,
a.sale_channel,
a.sale_from,
a.order_from,
b.link_name,
b.mobile,
b.province,
b.city,
b.district,
b.address,
b.add_time,
b.delivery_link_assign_id,
b.express_number,
b.express_group_name,
a.freight_amount,
a.freight_amount_item,
a.supplyerid,
a.group_name,
a.order_date,
a.order_user_id,
a.parent_order_pay_id,
a.order_id,
a.order_item_id,
a.three_transaction_id,
a.gateway_pay_id,
a.order_pay_id,
a.order_status_name,
a.product_spec_id,
a.product_name,
a.quantity,
a.purchase_unit_price,
a.sale_unit_price,
a.origin_sale_unit_price,
a.order_discount_amount ,
a.coupon_total_amount_order,
a.discount_total_amount,
a.coupon_total_amount,
a.real_total_amount,
a.order_total_amount,
a.sale_total_amount,
a.purchase_total_amount,
a.pay_amount,
a.pay_discount_amount,
a.pay_state,
a.gateway_pay_id_rufound,
a.refund_amount,
a.cost_percent,
a.pay_time,
a.finish_time,
a.supply_channel
FROM 
fe_dwd.dwd_group_order_refound_address_day_1 a 
LEFT JOIN
fe_dwd.dwd_group_order_refound_address_day_2 b 
ON a.order_item_id = b.order_item_id    ;
-- AND a.group_link_id = b.group_link_id;
 -- 全量更新最新添加退款的表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_3 ;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_3  AS
SELECT ss.order_item_id,
  TRIM(sr.gateway_pay_id) AS gateway_pay_id_rufound,  -- 支付网关的退款单ID 
  IFNULL(sr.refund_amount, 0) AS refund_amount -- 退款金额              
FROM fe_goods.sf_group_order_refund_item ss 
JOIN fe_goods.sf_group_order_refund_pay sr
ON ss.refund_order_id = sr.refund_order_id 
AND sr.data_flag = 1
AND sr.state=2
AND ss.data_flag = 1
;
CREATE INDEX dwd_group_order_refound_address_day_3
ON fe_dwd.dwd_group_order_refound_address_day_3 (order_item_id);
UPDATE fe_dwd.dwd_group_order_refound_address_day AS b
JOIN fe_dwd.dwd_group_order_refound_address_day_3 AS a 
ON  a.order_item_id = b.order_item_id
SET b.gateway_pay_id_rufound = a.gateway_pay_id_rufound,
b.refund_amount = a.refund_amount,
b.load_time = CURRENT_TIMESTAMP;
  
-- 添加到订单维度的退款金额
-- DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_4 ;
-- CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_4  AS
-- SELECT ss.order_id,
--   SUM(IFNULL(sr.refund_amount, 0)) AS refund_amount_order -- 退款金额              
-- FROM fe_goods.sf_group_order_refund_item ss 
-- JOIN fe_goods.sf_group_order_refund_pay sr
-- ON ss.refund_order_id = sr.refund_order_id 
-- AND sr.data_flag = 1
-- AND sr.state=2
-- AND ss.data_flag = 1
-- GROUP BY ss.order_id
-- ;
 
 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_4 ;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_4  AS
SELECT sr.order_id,
  SUM(IFNULL(sr.refund_amount, 0)) AS refund_amount_order -- 退款金额              
 FROM fe_goods.sf_group_order_refund_pay sr
WHERE sr.data_flag = 1
AND sr.state=2
GROUP BY sr.order_id
; 
 
CREATE INDEX dwd_group_order_refound_address_day_4
ON fe_dwd.dwd_group_order_refound_address_day_4 (order_id);
UPDATE fe_dwd.dwd_group_order_refound_address_day AS b
JOIN fe_dwd.dwd_group_order_refound_address_day_4 AS a 
ON  a.order_id = b.order_id
SET b.refund_amount_order = a.refund_amount_order,
b.load_time = CURRENT_TIMESTAMP;
  
 
-- 第三方退款金额 到订单  到不了明细 饿了么
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_group_order_refound_address_day_5 ;
CREATE TEMPORARY TABLE fe_dwd.dwd_group_order_refound_address_day_5  AS
SELECT o.order_id ,
SUM(IFNULL(rp.refund_amount, 0)) AS refund_amount_order
FROM 
`fe_goods`.sf_group_order_refund_third r 
JOIN `fe_goods`.sf_group_order_refund_pay rp 
ON rp.refund_order_id = r.refund_order_id 
AND rp.data_flag =1
AND rp.state =2 
JOIN `fe_goods`.sf_group_order_pay_third pt 
ON pt.transaction_order_id = r.transaction_order_id 
AND pt.data_flag =1
JOIN `fe_goods`.sf_group_order o 
ON o.order_id = pt.service_order_id 
AND o.data_flag =1
WHERE r.data_flag =1
-- and o.order_id IN (21798455300900000)
GROUP BY o.order_id;
 
 CREATE INDEX dwd_group_order_refound_address_day_5
ON fe_dwd.dwd_group_order_refound_address_day_5 (order_id);
UPDATE fe_dwd.dwd_group_order_refound_address_day AS b
JOIN fe_dwd.dwd_group_order_refound_address_day_5 AS a 
ON  a.order_id = b.order_id
SET b.refund_amount_order = a.refund_amount_order,
b.load_time = CURRENT_TIMESTAMP;
  
  
  
 -- 这几个供应商是测试的 ，需删除
 DELETE FROM fe_dwd.`dwd_group_order_refound_address_day`
 WHERE supplyerid IN (200002140,200000117,200001267,200000165,200000142,200000118)  ;
 
 
  -- 这几个是测试的 ，需删除
--  DELETE FROM fe_dwd.`dwd_group_order_refound_address_day`
--  WHERE order_id IN (21678537700900000,
-- 21919982600900000,
-- 21920526200900000,
-- 22343934200900000,
-- 22344368300900000,
-- 22344843800900000,
-- 22345021400900000,
-- 22377486800800000,
-- 24451894700670000,
-- 24451959600670000,
-- 24452130400800000,
-- 24452450601340000,
-- 24452461100800000,
-- 24452592300800000,
-- 24452633301340000,
-- 24452649300670000,
-- 24456314300670000,
-- 24456327801340000,
-- 24477399701340000)  ;
 
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_group_order_refound_address_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('lishilong@', @user, @timestamp));
 
  COMMIT;
 
 END