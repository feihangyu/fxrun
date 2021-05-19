CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_zd_emp_order`()
BEGIN
 DECLARE t_error INTEGER; 
   DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
   START TRANSACTION;  /*开启事务*/
        SET @user := CURRENT_USER,@timestamp := CURRENT_TIMESTAMP;
	DELETE FROM feods.`d_en_zd_scan_order` WHERE add_time >= CURDATE();  /*防止重跑数据冲突*/
	DELETE FROM feods.`d_en_zd_order` WHERE add_time >= CURDATE();  /*防止重跑数据冲突*/
	SET @sdate = CURDATE();
	IF DAY(@sdate) = 4 THEN  /*每个月3号执行上个月的消费数据*/   
	SET @start_time := DATE_ADD(DATE_ADD(LAST_DAY(@sdate),INTERVAL 1 DAY ),INTERVAL -2 MONTH), @end_time := DATE_ADD(DATE_ADD(LAST_DAY(@sdate),INTERVAL 1 DAY ),INTERVAL -1 MONTH);
	DELETE FROM feods.`d_en_zd_scan_order` WHERE add_time < SUBDATE(@start_time, INTERVAL 6 MONTH) ; /*删除3个月前的数据 只保留3个月明细数据*/
	DELETE FROM feods.`d_en_zd_order` WHERE add_time < SUBDATE(@start_time, INTERVAL 6 MONTH) ;/* 删除3个月前的数据 只保留3个月明细数据*/
	/*中电物业员工信息中间临时表*/
	DROP TABLE IF EXISTS feods.d_en_zd_emp_info;
	CREATE TEMPORARY TABLE feods.d_en_zd_emp_info(KEY(customer_user_id))
	AS 
	SELECT  emp_user_name, 
		emp_user_id ,
		e.group_customer_id,
		c.group_name,
		e.customer_user_id 
	FROM  fe_group.sf_group_emp e 
	JOIN fe_group.sf_group_customer c ON e.group_customer_id = c.group_customer_id 
	WHERE c.group_customer_id = 616
	AND e.data_flag = 1
	AND c.data_flag =1
	AND e.customer_user_id IS NOT NULL
	;	
	INSERT INTO feods.d_en_zd_scan_order
	(
	channel,
	order_id,
	order_date,
	pay_time,
	order_total_amount,
	short_name,
	supplyerid,
	gateway_pay_id,
	three_transaction_id,
	user_id,
	order_user_name,
	emp_user_name,
	emp_user_id,
	group_customer_id,
	group_name,
	pay_type,
	pay_amount,
	pay_state,
	cost_total_rate,
	amount
	)
	/*中电线下二维码销售数据*/  
	SELECT '线下扫码' AS 渠道名称,
	CAST(b.order_id AS CHAR) AS 订单号 ,
	b.order_date AS 订单时间,
	c.pay_time 支付时间,
	b.order_total_amount AS 订单金额,
	s.short_name AS 商户简称,
	b.supplyerid AS 商户id,
	c.gateway_pay_id AS 支付网关pay_id,
	c.three_transaction_id AS 第三方交易流水号,
	b.order_user_id,
	b.order_user_name AS 下单人名称,
	a.emp_user_name,
	a.emp_user_id,
	a.group_customer_id,
	a.group_name, 
	c.pay_time AS 支付时间,
	CASE WHEN c.pay_type=1 THEN '微信支付'
	WHEN c.pay_type=2 THEN '手工线下支付'
	WHEN c.pay_type=3 THEN '月结付款'
	WHEN c.pay_type=4 THEN 'E币支付'
	WHEN c.pay_type=9 THEN '餐卡支付'
	WHEN c.pay_type=12 THEN '小蜜蜂积分支付' 
	WHEN c.pay_type=13 THEN '升腾' 
	END AS 支付类型,
	c.pay_amount AS 支付金额,
	CASE WHEN c.pay_state=1 THEN '未支付' WHEN c.pay_state=2 THEN '已支付' END AS '支付状态',
	b.cost_total_rate AS 服务费率,
	ROUND(b.order_total_amount*b.cost_total_rate/100,2) AS 服务费
	FROM fe_goods.sf_scan_order b 
	JOIN feods.d_en_zd_emp_info a ON a.customer_user_id = b.order_user_id
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	LEFT JOIN fe_group.`sf_group_supply` s ON s.group_id = b.supplyerid
	WHERE b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_state=2 
	AND c.pay_time BETWEEN @start_time AND @end_time
	;	
	 /*中电物业员工货架消费*/
	INSERT INTO feods.`d_en_zd_order`
	(
	order_type,
	order_id,
	pay_id,
	order_name,
	user_id,
	emp_user_name,
	emp_user_id,
	group_customer_id,
	group_name,
	`area`,
	org,
	product_name,
	freight_amount_p,
	spec_desc,
	tax_rate,
	QUANTITY,
	sale_price,
	PURCHASE_PRICE,
	p_amount,
	o_amount,
	freight_amount_o,
	order_date,
	pay_date,
	pay_type,
	pay_state,
	refund_id,
	refund_status,
	refund_amount,
	order_real_amount,
	product_real_amount
	)
	 /*中电物业员工货架消费*/
	SELECT 
	ss.SHELF_NAME AS '货架消费'
	,so.order_id AS '订单号'
	,so.GATEWAY_ORDER_ID AS '支付接口id'
	,so.user_name AS '下单人姓名'
	,so.user_id
	,a.emp_user_name
	,a.emp_user_id
	,a.group_customer_id
	,a.group_name
	,fcb.business_name AS '地区'
	,'' AS '结算组织'
	,soi.product_name AS '商品名称'
	,0 AS '运费'
	,'-' AS '规格'
	,'-' AS '税率'
	,soi.QUANTITY AS '销量'
	,soi.SALE_PRICE AS '销售价'
	,soi.PURCHASE_PRICE AS '采购价'
	,soi.REAL_TOTAL_PRICE AS '商品实收'
	,so.PRODUCT_TOTAL_AMOUNT AS '订单实收'
	,0 AS '订单运费'
	,so.order_date AS '下单时间'
	,so.PAY_DATE AS '支付时间'
	,so.PAYMENT_TYPE_NAME  AS '支付方式'
	,CASE WHEN so.ORDER_STATUS = 2
		THEN '已支付' 
		WHEN so.order_status = 6
		THEN '出货失败'
		WHEN so.order_status = 7
		THEN '出货成功'
		END AS '支付状态'
	,sor.refund_order_id AS '货架退款id'
	,CASE
	WHEN sor.refund_status = 1
	THEN '提交待处理'
	WHEN sor.refund_status = 2
	THEN '审核通过'
	WHEN sor.refund_status = 3
	THEN '退货时需要买家处理'
	WHEN sor.refund_status = 4
	THEN '同意退款退款中'
	WHEN sor.refund_status = 5
	THEN '退款成功'
	WHEN sor.refund_status = 6
	THEN '退款失败'
	WHEN sor.refund_status = 7
	THEN '驳回申请'
	END AS '退款状态'
	,SUM(IFNULL(sor.refund_amount, 0)) AS '退款'
	,CASE   WHEN sor.refund_status = 5
		THEN (
		so.PRODUCT_TOTAL_AMOUNT - IFNULL(sor.refund_amount, 0))
		ELSE so.PRODUCT_TOTAL_AMOUNT
		END AS '订单实收(减掉退款)'
	,ROUND(
	(
	SUM(
	CASE
	  WHEN so.ORDER_STATUS = 2
	  THEN soi.QUANTITY * soi.SALE_PRICE
	  ELSE soi.quantity_shipped * soi.SALE_PRICE
	END
	) / (
	so.PRODUCT_TOTAL_AMOUNT + so.DISCOUNT_AMOUNT + so.COUPON_AMOUNT
	)
	) * (
	CASE
	WHEN sor.refund_status = 5
	THEN (
	  so.PRODUCT_TOTAL_AMOUNT - IFNULL(sor.refund_amount, 0)
	)
	ELSE so.PRODUCT_TOTAL_AMOUNT
	END
	),
	2
	) AS '商品实收(减掉退款)'
	FROM
	fe.sf_order so
	JOIN fe.sf_order_item soi ON so.order_id = soi.order_id
	JOIN feods.d_en_zd_emp_info a ON a.customer_user_id = so.user_id
	LEFT JOIN fe.sf_order_refund_order sor ON (sor.ORDER_ID = so.order_id AND sor.data_flag = 1)
	LEFT JOIN fe.sf_order_refund_item sri ON sri.order_item_id = soi.order_item_id
	JOIN fe.sf_shelf ss ON so.shelf_id = ss.shelf_id
	JOIN feods.`fjr_city_business` fcb ON fcb.city = ss.city
	WHERE so.ORDER_STATUS IN (2, 6, 7)
	AND so.data_flag = 1
	AND soi.data_flag = 1
	AND so.PAY_DATE BETWEEN @start_time AND @end_time
	GROUP BY so.order_id,soi.product_id
	;	
	 /*中电物业员工商城消费*/
	INSERT INTO feods.`d_en_zd_order`
	(
	order_type,
	order_id,
	pay_id,
	order_name,
	user_id,
	emp_user_name,
	emp_user_id,
	group_customer_id,
	group_name,
	`area`,
	org,
	product_name,
	freight_amount_p,
	spec_desc,
	tax_rate,
	QUANTITY,
	sale_price,
	PURCHASE_PRICE,
	p_amount,
	o_amount,
	freight_amount_o,
	order_date,
	pay_date,
	pay_type,
	pay_state,
	refund_id,
	refund_status,
	refund_amount,
	order_real_amount,
	product_real_amount
	)
	SELECT 
	CASE WHEN sgo.order_type = 5 THEN '饿了么'
		    ELSE '福利商城' END AS '类型'
	,sgo.order_id AS '订单号'
	,TRIM(sgp.gateway_pay_id)  AS '支付网关pay_id'
	,sgo.order_user_name AS '下单姓名'
	,sgo.order_user_id
	,a.emp_user_name
	,a.emp_user_id
	,a.group_customer_id
	,a.group_name
	,'-' AS '地区'
	,'-' AS '结算组织'
	,REPLACE(REPLACE(REPLACE(sgi.product_name,CHAR(13),''),CHAR(9),''),CHAR(10),'') AS '商品名称'
	,sgi.freight_amount AS '商品运费'
	,ps.spec_desc AS '规格'
	,ps.tax_rate AS '税率'
	,sgi.quantity AS '销量'
	,sgi.sale_unit_price AS '销售单价'
	,sgi.purchase_unit_price AS '采购价'
	,sgi.real_total_amount AS '商品实收'
	,sgp.pay_amount AS '订单实收'
	,sgo.freight_amount
	,sgo.order_date AS '下单时间'
	,sgp.pay_time
	,CASE WHEN sgp.pay_type=1 THEN '微信支付'
	WHEN sgp.pay_type=2 THEN '手工线下支付'
	WHEN sgp.pay_type=3 THEN '月结付款'
	WHEN sgp.pay_type=4 THEN 'E币支付'
	WHEN sgp.pay_type=9 THEN '餐卡支付'
	WHEN sgp.pay_type=12 THEN '小蜜丰积分支付' END AS '支付方式'
	,CASE WHEN sgp.pay_state=1 THEN '未支付'
	      WHEN sgp.pay_state=2 THEN '已支付' END AS '支付状态'
	,TRIM(sgorp.gateway_pay_id) AS '支付网关的退款单ID'
	,CASE WHEN sgorp.state= 1 THEN '退款中'
	      WHEN sgorp.state= 2 THEN '退款成功'
	      WHEN sgorp.state= 3 THEN '退款异常'
	      WHEN sgorp.state= 4 THEN '退款关闭' END AS '退款状态'
	,IFNULL(sgor.refund_amount, 0) AS '退款金额'
	,CASE WHEN sgorp.state= 2 THEN (sgp.pay_amount - IFNULL(sgor.refund_amount, 0)) 
	      ELSE sgp.pay_amount END AS '订单实收(减掉退款)'
	,CASE WHEN sgorp.state= 2 THEN ((sgi.real_total_amount + sgi.freight_amount)/sgp.pay_amount)* (sgp.pay_amount - IFNULL(sgor.refund_amount, 0)) 
	      ELSE (sgi.real_total_amount + sgi.freight_amount) END AS '商品实收_含运费_减退款'
	FROM fe_goods.sf_group_order sgo
	JOIN feods.d_en_zd_emp_info a ON a.customer_user_id = sgo.order_user_id
	JOIN fe_goods.sf_group_order_item sgi ON sgo.order_id = sgi.order_id   
	JOIN fe_goods.sf_group_order_pay sgp ON sgo.order_id=sgp.order_id
	LEFT JOIN fe_goods.sf_group_order_refund sgor ON sgor.order_id = sgo.order_id
	LEFT JOIN fe_goods.sf_group_order_refund_item sgori ON sgori.order_item_id = sgi.order_item_id
	LEFT JOIN fe_goods.sf_group_order_refund_pay sgorp ON sgorp.order_id = sgo.order_id
	LEFT JOIN fe_goods.sf_group_product_spec ps ON sgi.product_spec_id=ps.spec_id
	WHERE sgo.data_flag = 1
	AND sgi.data_flag = 1
	AND sgp.pay_state=2 
	AND sgp.data_flag= 1
	AND sgp.pay_time BETWEEN @start_time AND @end_time
	GROUP BY sgi.order_id,sgi.product_name,sgi.product_spec_id
	;
	END IF;	
	IF t_error = 1 THEN  
	     ROLLBACK;  
	 ELSE  
	     COMMIT;  
	 END IF;
   /*调用日志*/
  CALL feods.sp_task_log (
    'd_en_zd_emp_order',
    @sdate,
    CONCAT(
      'lnh@',
      @user,@timestamp
    )
  );
  COMMIT;
/*
CREATE TABLE feods.`d_en_zd_order` (
pid BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
  `type` VARCHAR(50) CHARACTER SET utf8 DEFAULT NULL COMMENT '来源',
  `order_id` BIGINT(20) NOT NULL COMMENT '订单编号',
  `pay_id` VARCHAR(100) DEFAULT NULL COMMENT '支付接口返回的ID',
  `order_name` VARCHAR(100) DEFAULT NULL COMMENT '会员名称',
  `user_id` BIGINT(20) NOT NULL COMMENT '会员ID',
  `emp_user_name` VARCHAR(100) DEFAULT NULL COMMENT '姓名',
  `emp_user_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT '员工id',
  `group_customer_id` BIGINT(20) UNSIGNED DEFAULT NULL COMMENT '企业ID',
  `group_name` VARCHAR(200) DEFAULT NULL COMMENT '企业名称',
  `area` VARCHAR(100) NOT NULL COMMENT '地区',
  `org` CHAR(0) COMMENT '结算组织',
  `product_name` VARCHAR(100) NOT NULL COMMENT '商品名称',
  `freight_amount_p` INT(1)  COMMENT '运费' ,
  `spec_desc` VARCHAR(1) COMMENT '规格',
  `tax_rate` VARCHAR(1) COMMENT '税率',
  `QUANTITY` BIGINT(20) NOT NULL COMMENT '购买数量',
  `sale_price` DECIMAL(18,2)COMMENT '销售价',
  `PURCHASE_PRICE` DECIMAL(18,2) COMMENT '采购价',
  `p_amount` DECIMAL(18,2) COMMENT '实际应付金额',
  `o_amount` DECIMAL(18,2) COMMENT '商品总金额',
  `freight_amount_o` INT(1)  COMMENT '订单运费',
  `order_date` DATETIME NOT NULL COMMENT '订单创建日期',
  `pay_date` DATETIME DEFAULT NULL COMMENT '付款日期',
  `pay_type` VARCHAR(100) DEFAULT NULL COMMENT '付款类型名称',
  `pay_state` VARCHAR(4) DEFAULT NULL COMMENT '支付状态',
  `refund_id` BIGINT(20) COMMENT '退款订单ID',
  `refund_status` VARCHAR(9) DEFAULT NULL COMMENT '退款状态',
  `refund_amount` DECIMAL(34,2) DEFAULT NULL COMMENT '退款金额',
  `order_real_amount` DECIMAL(19,2) DEFAULT NULL COMMENT '订单实收减退款',
  `product_real_amount` DECIMAL(60,2) DEFAULT NULL COMMENT '商品实收减退款',
  add_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '添加时间',
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  KEY idx_d_en_zd_order_order_id(order_id),
  KEY idx_d_en_zd_order_user_id(user_id),
  KEY idx_d_en_zd_order_pay_date(pay_date),
  KEY idx_d_en_zd_order_add_time(add_time)
) COMMENT = '中电物业对账(含货架&商城)'
SELECT column_name
FROM information_schema.`COLUMNS`
WHERE table_name = 'd_en_zd_order'
AND table_schema = 'feods'
*/  
  
END