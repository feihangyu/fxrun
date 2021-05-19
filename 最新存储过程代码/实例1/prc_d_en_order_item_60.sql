CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_order_item_60`()
BEGIN
        SET @sdate := current_date,@user := CURRENT_USER,@timestamp := CURRENT_TIMESTAMP;
        SET @sdate := current_date,@user := CURRENT_USER,@timestamp := CURRENT_TIMESTAMP;
   SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   DELETE FROM feods.`d_en_order_item_60` WHERE pay_time <= SUBDATE(CURDATE(),INTERVAL 62 DAY);  -- 删掉62天前的数据
   DELETE FROM feods.`d_en_order_item_60` WHERE pay_time >= @start_date ;  -- 删掉当天前的数据
	      
	        INSERT INTO feods.`d_en_order_item_60`(
			user_id,
		pay_type,
		sale_channel,
		order_id,
		order_type,
		order_date,
		buy_type,
		sale_total_amount,
		freight_amount,
-- 		real_total_amount,
		purchase_total_amount,
		pay_amount,
		pay_time,
		product_name,
		product_spec_id,
		first_category_name
		 )
SELECT
a.order_user_id AS user_id,
a.pay_type_desc AS pay_type,
a.sale_from  AS sale_channel,
a.order_id,
a.order_type,
a.order_date,
CASE WHEN a.order_date BETWEEN SUBDATE(CURDATE(),INTERVAL 60 DAY) 
AND SUBDATE(CURDATE(),INTERVAL 30 DAY) THEN '1' 
ELSE '2' END AS buy_type,
a.sale_total_amount,
a.freight_amount_item freight_amount,
-- a.real_total_amount,
a.purchase_total_amount,
a.pay_amount,
a.pay_time,
a.product_name,
a.product_spec_id,
b.first_category_desc first_category_name  
FROM fe_dwd.`dwd_group_order_refound_address_day` a
left join fe_dwd.dwd_group_product_base_day b 
on a.product_spec_id=b.spec_id
WHERE a.pay_time >= @start_date 
and a.pay_time < @end_date
AND a.purchase_total_amount >= 1  #去掉产品测试	
;	    
		DROP TABLE IF EXISTS feods.d_en_buy_type_temp;  
		CREATE TEMPORARY TABLE feods.d_en_buy_type_temp(KEY(user_id),KEY(max_date), KEY(sale_channel)) ## 60-30天有购买 30-1天无购买用户中间表
		AS
		SELECT t.user_id, t.sale_channel, GROUP_CONCAT(DISTINCT t.buy_type) type1,MAX(t.order_date) AS max_date
		FROM feods.`d_en_order_item_60` t
		GROUP BY t.user_id,t.sale_channel
		HAVING type1 = '1'
		;
		TRUNCATE TABLE feods.d_en_wastage_item;  -- 
		INSERT INTO feods.d_en_wastage_item(
		user_id,
		sale_channel,
		pay_type,
		order_date,
		product_name,
		first_category_name,
		pay_amount,
		sale_total_amount,
		freight_amount
		)
		SELECT 
		      b.user_id 用户id, 
		      b.sale_channel 渠道, 
		      b.pay_type 支付类型, 
		      b.order_date 最近购买时间, 
		      b.product_name 购买商品, 
		      CASE WHEN b.order_type = '饿了么' THEN b.order_type ELSE b.first_category_name END 商品一级分类, 
		      b.pay_amount 订单实收, 
		      b.sale_total_amount, 
		      b.freight_amount
		FROM feods.d_en_buy_type_temp a
		JOIN feods.`d_en_order_item_60` b ON a.user_id = b.user_id AND a.sale_channel = b.sale_channel AND a.max_date = b.order_date
		;
TRUNCATE TABLE feods.`d_en_last60_buy_users`;
		INSERT INTO feods.`d_en_last60_buy_users`
		(sdate,user_num,nlz_user_num, payqb_user_num,
		fx_user_num, fs_user_num, qyflqt_user_num, st_user_num, 
		xmf_user_num, zchl_user_num, 
		sfcod_user_num, zxcy_user_num, zd_user_num,  zxyj_user_num, 
		 fxia_user_num, syhnq_user_num, 
		sgxx_user_num)
		SELECT 
 SUBDATE(CURDATE(),INTERVAL 1 DAY) AS sdate,
 COUNT(DISTINCT user_id) 近60天购买用户数,
 COUNT(DISTINCT CASE WHEN sale_channel = '丰e能量站' THEN user_id ELSE NULL END) 丰e能量站_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '平安壹钱包' THEN user_id ELSE NULL END) 平安壹钱包_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '丰享' THEN user_id ELSE NULL END) 丰享_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '丰声渠道' THEN user_id ELSE NULL END) 丰声渠道_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '企业福利前台' THEN user_id ELSE NULL END) 企业福利前台_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '升腾' THEN user_id ELSE NULL END) 升腾_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '小蜜丰' THEN user_id ELSE NULL END) 小蜜丰_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '中创物流' THEN user_id ELSE NULL END) 中创物流_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '顺丰cod' THEN user_id ELSE NULL END) 顺丰cod_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '正心诚意' THEN user_id ELSE NULL END) 正心诚意_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '中电' THEN user_id ELSE NULL END) 中电_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '中小月结' THEN user_id ELSE NULL END) 中小月结_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '丰侠' THEN user_id ELSE NULL END) 丰侠_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '速运湖南区兑换卡消费' THEN user_id ELSE NULL END) 速运湖南区兑换卡消费_购买用数,
 COUNT(DISTINCT CASE WHEN sale_channel = '手工线下' THEN user_id ELSE NULL END) 手工线下_购买用数
FROM feods.`d_en_order_item_60`;
 
  CALL feods.sp_task_log (
    'prc_d_en_order_item_60',
    @sdate,
    CONCAT(
      'lnh@',
      @user,@timestamp
    )
  );
  COMMIT;
END