CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ka_cmb_union`()
BEGIN
DECLARE t_error INTEGER; 
DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
START TRANSACTION;
	SET @sdate = CURDATE(),
	@add_user := CURRENT_USER,
	@timestamp := CURRENT_TIMESTAMP;
	REPLACE INTO feods.d_ka_cmb_union(sdate,REGION_AREA,BUSINESS_AREA,
	PAYMENT_TYPE_NAME,shelf_num,gmv,amount,refund_amount,order_num,
	user_num,DISCOUNT_AMOUNT,COUPON_AMOUNT,third_discount_amount)
	 SELECT
	  t1.sdate,
	  t1.REGION_AREA,
	  t1.BUSINESS_AREA,
	  t1.PAYMENT_TYPE_NAME,
	  COUNT(DISTINCT SHELF_ID) shelf_num,
	  SUM(gmv) AS gmv,
	  SUM(t1.PRODUCT_TOTAL_AMOUNT) AS amount,
	  SUM(refund_amount) AS refund_amount,
	  COUNT(DISTINCT order_id) AS order_num,
	  COUNT(DISTINCT user_id) AS user_num,
	  SUM(t1.DISCOUNT_AMOUNT) AS DISCOUNT_AMOUNT,
	  SUM(t1.COUPON_AMOUNT) AS COUPON_AMOUNT,
	  SUM(t1.third_discount_amount) 
	FROM
	  (SELECT
	    o.order_id,
	    o.ORDER_STATUS,
	    o.PAYMENT_TYPE_NAME,
	    o.SHELF_ID,
	    zc.BUSINESS_AREA,
	    zc.REGION_AREA,
	    SHELF_TYPE,
	    SHELF_STATUS,
	    DATE(o.ORDER_DATE) AS sdate,
	    o.USER_ID,
	    o.PRODUCT_TOTAL_AMOUNT - IFNULL(e.refund_amount, 0) AS PRODUCT_TOTAL_AMOUNT,
	    o.DISCOUNT_AMOUNT,
	    o.COUPON_AMOUNT,
	    o.third_discount_amount,
	    SUM(
	      CASE
		WHEN o.ORDER_STATUS = 2
		THEN a.QUANTITY * a.SALE_PRICE
		ELSE a.quantity_shipped * a.SALE_PRICE
	      END
	    ) AS GMV,
	    IFNULL(e.refund_amount, 0) AS refund_amount
	  FROM
	    fe.sf_order_item a
	    JOIN fe.sf_order o
	      ON a.order_id = o.ORDER_ID
	    JOIN fe.sf_shelf s
	      ON s.SHELF_ID = o.SHELF_ID
	    JOIN fe.zs_city_business zc
	      ON SUBSTRING_INDEX(
		SUBSTRING_INDEX(s.AREA_ADDRESS, ',', 2),
		',',
		- 1
	      ) = zc.CITY_NAME
	    LEFT JOIN fe.sf_order_refund_order e
	      ON (
		o.ORDER_ID = e.order_id
		AND e.refund_status = 5
		AND e.data_flag = 1
		AND o.ORDER_STATUS = 6
	      )
	  WHERE o.ORDER_STATUS IN (2, 6, 7)
	    AND o.ORDER_DATE  >= SUBDATE(@sdate,INTERVAL 1 DAY)
	    AND o.ORDER_DATE < @sdate
	    AND PAYMENT_TYPE_NAME IN (
	      '招行一卡通',
	      '顺手付云闪付'
	    )
	  GROUP BY o.order_id
	  ) t1
	GROUP BY t1.sdate,
	  t1.PAYMENT_TYPE_NAME,
	  t1.BUSINESS_AREA  ;
 IF t_error = 1 THEN  
             ROLLBACK;  
         ELSE  
             COMMIT;  
         END IF;
         
  CALL feods.sp_task_log (
    'prc_d_ka_cmb_union',
    @sdate,
    CONCAT(
      'lnh@',
      @user,@timestamp
    )
  );
  COMMIT;
END