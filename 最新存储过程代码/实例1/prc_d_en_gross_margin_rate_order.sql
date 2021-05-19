CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_gross_margin_rate_order`()
BEGIN
 ## 按周统计毛利
SET @sdate = CURDATE(),@user := CURRENT_USER,@timestamp := CURRENT_TIMESTAMP;
SET @y_date = DAY(SUBDATE(@sdate, INTERVAL 1 DAY)),@yy_date = SUBDATE(@sdate, INTERVAL 1 DAY);
DELETE FROM feods.`d_en_gross_margin_rate_order_week` WHERE add_time >= @sdate;
DELETE FROM feods.`d_en_gross_margin_rate_order_month` WHERE add_time >= @sdate;
IF @y_date MOD 7 = 0 THEN /*按周统计*/
	REPLACE INTO feods.`d_en_gross_margin_rate_order_week`(
	sdate,
	sale_channel,
	gross_margin,
	gross_margin_avg,
	gmv,
	amount,
	purchase_total_amount,
	oorder_num,
	user_num
	)
	SELECT t.日期,
	t.item_name ,
	ELT(INTERVAL(t.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS 毛利率分层,
	AVG(t.毛利率) 平均毛利率,
	SUM(t.gmv) gmv,
	SUM(t.订单实收) AS 实收,
	SUM(t.采购成本) AS 采购总价,
	COUNT(DISTINCT t.order_id) AS 订单数,
	COUNT(DISTINCT t.order_user_id) AS 用户数
	FROM 
	(
	SELECT 
	CASE WHEN b.order_type != 5 THEN ROUND(((b.sale_total_amount + b.freight_amount)- b.purchase_total_amount)/(b.sale_total_amount + b.freight_amount),2)
	     ELSE ROUND((b.sale_total_amount - b.purchase_total_amount)/b.sale_total_amount,2) END 
	 AS 毛利率, 
	CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
	     ELSE b.sale_total_amount END AS gmv,
	c.pay_amount AS 订单实收,
	b.purchase_total_amount AS 采购成本,
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
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME
	    END AS ITEM_NAME,
	CONCAT(MONTH(b.order_date),'月第',CEIL(DAY(b.order_date)/7),'周') AS 日期,
	b.order_id,
	b.order_user_id
	FROM fe_goods.sf_group_order b
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	WHERE c.pay_state = 2 # 已支付
	AND b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_amount >= 0.1
	AND b.purchase_total_amount >= 0.1
	AND b.order_date >= DATE_SUB(@sdate,INTERVAL 7 DAY) 
	AND b.order_date < @sdate
	)t
	GROUP BY  /*t.日期,*/t.item_name,毛利率分层 ; 
	
	REPLACE INTO feods.`d_en_gross_margin_rate_user_week`(
	sdate,
	sale_channel,
	gross_margin,
	gross_margin_avg,
	gmv,
	amount,
	purchase_total_amount,
	oorder_num,
	user_num
	)
	SELECT 
	tt.日期,
	tt.item_name,
	ELT(INTERVAL(tt.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS 毛利率分层,
	AVG(tt.毛利率) AS 平均毛利率,
	SUM(tt.gmv) gmv,
	SUM(tt.订单实收) 订单实收,
	SUM(tt.采购成本) 采购成本,
	SUM(tt.订单数) 订单数,
	COUNT(DISTINCT tt.order_user_id) AS 用户数
	FROM
	(
	SELECT t.order_user_id,
	       t.item_name,t.日期,
	       SUM(t.gmv) AS gmv,
	       SUM(t.订单实收) 订单实收,
	       SUM(t.采购成本) 采购成本,
	       COUNT(DISTINCT t.order_id) 订单数,
	       ROUND((SUM(t.gmv)-SUM(t.采购成本))/SUM(t.gmv),2) AS 毛利率 
	FROM 
	(
	SELECT 
	b.order_user_id,
	CONCAT(MONTH(b.order_date),'月第',CEIL(DAY(b.order_date)/7),'周') 日期,
	CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
	     ELSE b.sale_total_amount END AS gmv,
	c.pay_amount AS 订单实收,
	b.purchase_total_amount AS 采购成本,
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
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME
	    END AS ITEM_NAME,
	b.order_id
	FROM fe_goods.sf_group_order b
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	WHERE c.pay_state = 2 # 已支付
	AND b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_amount >= 0.1 -- 过滤掉测试数据
	AND b.purchase_total_amount >= 0.1  -- 过滤掉测试数据
	AND b.order_date >= DATE_SUB(@sdate,INTERVAL 7 DAY) 
	AND b.order_date < @sdate
	)t
	GROUP BY t.order_user_id, t.item_name 
	)tt
	GROUP BY /*tt.日期,8*/ tt.item_name, 毛利率分层
	;
	
	
ELSEIF  LAST_DAY(DATE_SUB(@sdate, INTERVAL 1 MONTH)) = @yy_date THEN  -- 当昨天是 月的最后一天 则 按月进行 分层统计
        REPLACE INTO feods.d_en_gross_margin_rate_order_month(
	sdate,
	sale_channel,
	gross_margin,
	gross_margin_avg,
	gmv,
	amount,
	purchase_total_amount,
	oorder_num,
	user_num
	)
	SELECT t.日期,
	t.item_name ,
	ELT(INTERVAL(t.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS 毛利率分层,
	AVG(t.毛利率) 平均毛利率,
	SUM(t.gmv) gmv,
	SUM(t.订单实收) AS 实收,
	SUM(t.采购成本) AS 采购总价,
	COUNT(DISTINCT t.order_id) AS 订单数,
	COUNT(DISTINCT t.order_user_id) AS 用户数
	FROM 
	(
	SELECT 
	CASE WHEN b.order_type != 5 THEN ROUND(((b.sale_total_amount + b.freight_amount)- b.purchase_total_amount)/(b.sale_total_amount + b.freight_amount),2)
	     ELSE ROUND((b.sale_total_amount - b.purchase_total_amount)/b.sale_total_amount,2) END 
	 AS 毛利率, 
	CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
	     ELSE b.sale_total_amount END AS gmv,
	c.pay_amount AS 订单实收,
	b.purchase_total_amount AS 采购成本,
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
	    WHEN c.pay_type = 2 THEN '手工线下'
-- 	    WHEN b.order_type = 5 THEN '饿了么'
	    ELSE d.ITEM_NAME END AS ITEM_NAME,
	CONCAT(YEAR(b.order_date),'年',MONTH(b.order_date),'月') AS 日期,
	b.order_id,
	b.order_user_id
	FROM fe_goods.sf_group_order b
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	WHERE c.pay_state = 2 # 已支付
	AND b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_amount >= 0.1
	AND b.purchase_total_amount >= 0.1
	AND b.order_date >= DATE_ADD(DATE_ADD(LAST_DAY(@sdate),INTERVAL 1 DAY ),INTERVAL -2 MONTH) 
	AND b.order_date < @sdate	
	)t
	GROUP BY  /*t.日期,*/t.item_name,毛利率分层 ; 
	## 用户层级毛利按月统计
 	REPLACE INTO feods.`d_en_gross_margin_rate_user_month`(
	sdate,
	sale_channel,
	gross_margin,
	gross_margin_avg,
	gmv,
	amount,
	purchase_total_amount,
	oorder_num,
	user_num
	)
	SELECT 
	tt.日期,
	tt.item_name,
	ELT(INTERVAL(tt.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS 毛利率分层,
	AVG(tt.毛利率) AS 平均毛利率,
	SUM(tt.gmv) gmv,
	SUM(tt.订单实收) 订单实收,
	SUM(tt.采购成本) 采购成本,
	SUM(tt.订单数) 订单数,
	COUNT(DISTINCT tt.order_user_id) AS 用户数
	FROM
	(
	SELECT t.order_user_id,
	       t.item_name,t.日期,
	       SUM(t.gmv) AS gmv,
	       SUM(t.订单实收) 订单实收,
	       SUM(t.采购成本) 采购成本,
	       COUNT(DISTINCT t.order_id) 订单数,
	       ROUND((SUM(t.gmv)-SUM(t.采购成本))/SUM(t.gmv),2) AS 毛利率 
	FROM 
	(
	SELECT 
	b.order_user_id,
	CONCAT(YEAR(b.order_date),'年',MONTH(b.order_date),'月') 日期,
	CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
	     ELSE b.sale_total_amount END AS gmv,
	c.pay_amount AS 订单实收,
	b.purchase_total_amount AS 采购成本,
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
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME
	    END AS ITEM_NAME,
	b.order_id
	FROM fe_goods.sf_group_order b
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	WHERE c.pay_state = 2 # 已支付
	AND b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_amount >= 0.1 -- 过滤掉测试数据
	AND b.purchase_total_amount >= 0.1  -- 过滤掉测试数据
	AND b.order_date >= DATE_ADD(DATE_ADD(LAST_DAY(@sdate),INTERVAL 1 DAY ),INTERVAL -2 MONTH) 
	AND b.order_date < @sdate
	)t
	GROUP BY t.order_user_id, t.item_name 
	)tt
	GROUP BY /*tt.日期,8*/ tt.item_name, 毛利率分层
	;
 	## 月底统计 月末28号之后的数据
 	REPLACE INTO feods.`d_en_gross_margin_rate_order_week`(
	sdate,
	sale_channel,
	gross_margin,
	gross_margin_avg,
	gmv,
	amount,
	purchase_total_amount,
	oorder_num,
	user_num
	)
	SELECT t.日期,
	t.item_name ,
	ELT(INTERVAL(t.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS 毛利率分层,
	AVG(t.毛利率) 平均毛利率,
	SUM(t.gmv) gmv,
	SUM(t.订单实收) AS 实收,
	SUM(t.采购成本) AS 采购总价,
	COUNT(DISTINCT t.order_id) AS 订单数,
	COUNT(DISTINCT t.order_user_id) AS 用户数
	FROM 
	(
	SELECT 
	CASE WHEN b.order_type != 5 THEN ROUND(((b.sale_total_amount + b.freight_amount)- b.purchase_total_amount)/(b.sale_total_amount + b.freight_amount),2)
	     ELSE ROUND((b.sale_total_amount - b.purchase_total_amount)/b.sale_total_amount,2) END 
	 AS 毛利率, 
	CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
	     ELSE b.sale_total_amount END AS gmv,
	c.pay_amount AS 订单实收,
	b.purchase_total_amount AS 采购成本,
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
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME 
	    END AS ITEM_NAME,
	CONCAT(MONTH(b.order_date),'月第',CEIL(DAY(b.order_date)/7),'周') AS 日期,
	b.order_id,
	b.order_user_id
	FROM fe_goods.sf_group_order b
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	WHERE c.pay_state = 2 # 已支付
	AND b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_amount >= 0.1
	AND b.purchase_total_amount >= 0.1
	AND b.order_date >= DATE_SUB(@sdate,INTERVAL (@y_date-28) DAY)
	AND b.order_date < @sdate 
	)t
	GROUP BY  /*t.日期,*/t.item_name,毛利率分层 ; 
	
	REPLACE INTO feods.`d_en_gross_margin_rate_user_week`(
	sdate,
	sale_channel,
	gross_margin,
	gross_margin_avg,
	gmv,
	amount,
	purchase_total_amount,
	oorder_num,
	user_num
	)
	SELECT 
	tt.日期,
	tt.item_name,
	ELT(INTERVAL(tt.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS 毛利率分层,
	AVG(tt.毛利率) AS 平均毛利率,
	SUM(tt.gmv) gmv,
	SUM(tt.订单实收) 订单实收,
	SUM(tt.采购成本) 采购成本,
	SUM(tt.订单数) 订单数,
	COUNT(DISTINCT tt.order_user_id) AS 用户数
	FROM
	(
	SELECT t.order_user_id,
	       t.item_name,t.日期,
	       SUM(t.gmv) AS gmv,
	       SUM(t.订单实收) 订单实收,
	       SUM(t.采购成本) 采购成本,
	       COUNT(DISTINCT t.order_id) 订单数,
	       ROUND((SUM(t.gmv)-SUM(t.采购成本))/SUM(t.gmv),2) AS 毛利率 
	FROM 
	(
	SELECT 
	b.order_user_id,
	CONCAT(MONTH(b.order_date),'月第',CEIL(DAY(b.order_date)/7),'周') 日期,
	CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
	     ELSE b.sale_total_amount END AS gmv,
	c.pay_amount AS 订单实收,
	b.purchase_total_amount AS 采购成本,
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
    WHEN c.pay_type = 2 THEN '手工线下'
	ELSE d.ITEM_NAME
	    END AS ITEM_NAME,
	b.order_id
	FROM fe_goods.sf_group_order b
	LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
	JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
	WHERE c.pay_state = 2 # 已支付
	AND b.data_flag = 1
	AND c.data_flag = 1
	AND c.pay_amount >= 0.1 -- 过滤掉测试数据
	AND b.purchase_total_amount >= 0.1  -- 过滤掉测试数据
	AND b.order_date >= DATE_SUB(@sdate,INTERVAL (@y_date-28) DAY) 
	AND b.order_date < @sdate
	)t
	GROUP BY t.order_user_id, t.item_name 
	)tt
	GROUP BY /*tt.日期,8*/ tt.item_name, 毛利率分层
	;
 	
 	
 END IF ;
 
 
#日毛利
DELETE FROM feods.d_en_gross_margin_rate_user_day WHERE sdate=SUBDATE(CURRENT_DATE,INTERVAL 1 DAY);
INSERT INTO feods.d_en_gross_margin_rate_user_day(
sdate,
sale_channel,
gross_margin,
gross_margin_avg,
gmv,
amount,
purchase_total_amount,
oorder_num,
user_num
)
SELECT 
	tt.日期 sdate,#日期
	tt.item_name sale_channel, #渠道
	ELT(INTERVAL(tt.毛利率,0,0.03,0.07,0.15),"0-3%","3-7%","7-15%","15%+") AS gross_margin, #毛利率分层
	AVG(tt.毛利率) AS gross_margin_avg,#平均毛利率
	SUM(tt.gmv) gmv,
	SUM(tt.订单实收) amount,#订单实收
	SUM(tt.采购成本) purchase_total_amount,#采购成本
	SUM(tt.订单数) oorder_num,#订单数
	COUNT(DISTINCT tt.order_user_id) AS user_num#用户数
FROM (
	SELECT t.order_user_id,
	       t.item_name,t.日期,
	       SUM(t.gmv) AS gmv,
	       SUM(t.订单实收) 订单实收,
	       SUM(t.采购成本) 采购成本,
	       COUNT(DISTINCT t.order_id) 订单数,
	       ROUND((SUM(t.gmv)-SUM(t.采购成本))/SUM(t.gmv),2) AS 毛利率 
	FROM (
		SELECT 
		b.order_user_id,
		#CONCAT(YEAR(b.order_date),'年',MONTH(b.order_date),'月') 日期,
		DATE_FORMAT(b.order_date,'%Y-%m-%d') 日期,
		CASE WHEN b.order_type != 5 THEN (b.sale_total_amount + b.freight_amount) 
		     ELSE b.sale_total_amount END AS gmv,
		c.pay_amount AS 订单实收,
		b.purchase_total_amount AS 采购成本,
		CASE WHEN b.order_from = 1 THEN '销售助手'
		WHEN b.order_from = 3 THEN '企业采购'
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
		WHEN b.order_from = 2 AND b.sale_channel = 'BJDC' THEN '北京订餐'
                WHEN b.order_from = 2 AND b.sale_channel = 'ZDKQD' THEN '中电科渠道'
                WHEN b.order_from = 2 AND b.sale_channel = 'WYYC' THEN '万翼云城'
                WHEN b.order_from = 2 AND b.sale_channel = 'FAYD' THEN '福安移动'
		WHEN c.pay_type = 2 THEN '手工线下'
		ELSE d.ITEM_NAME END AS ITEM_NAME,
		b.order_id
		FROM fe_goods.sf_group_order b
		LEFT JOIN (SELECT ITEM_VALUE,ITEM_NAME FROM fe.pub_dictionary_item WHERE dictionary_id=192) d ON b.sale_channel=d.ITEM_VALUE
		JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
		WHERE c.pay_state = 2 # 已支付
		AND b.data_flag = 1 AND c.data_flag = 1
		AND c.pay_amount >= 0.1 -- 过滤掉测试数据
		AND b.purchase_total_amount >= 0.1  -- 过滤掉测试数据
		#每天增量抽取一次数据
		AND b.order_date >= SUBDATE(CURRENT_DATE,INTERVAL 1 DAY) 
		AND b.order_date < CURRENT_DATE
	)t GROUP BY t.order_user_id, t.item_name 
)tt GROUP BY #tt.日期,
tt.item_name, gross_margin;
 
 
  CALL feods.sp_task_log (
    'prc_d_en_gross_margin_rate_order',
    @sdate,
    CONCAT(
      '郑志省@',
      @user,@timestamp
    )
  );
  COMMIT;
END