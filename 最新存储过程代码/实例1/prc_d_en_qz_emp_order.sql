CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_qz_emp_order`()
BEGIN
 DECLARE t_error INTEGER; 
   DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
   START TRANSACTION;  /*开启事务*/
        SET @user := CURRENT_USER,@timestamp := CURRENT_TIMESTAMP;
	DELETE FROM feods.`d_en_qz_emp_order` WHERE add_time >= CURDATE();  /*防止重跑数据冲突*/
	set @sdate = curdate();
	if day(@sdate) = 8 then  /*每个月3号执行上个月的消费数据*/   
	SET @start_time := DATE_ADD(DATE_ADD(LAST_DAY(@sdate),INTERVAL 1 DAY ),INTERVAL -2 MONTH), @end_time := LAST_DAY(DATE_ADD(@sdate,INTERVAL -1 MONTH));
	DELETE FROM feods.`d_en_qz_emp_order` WHERE add_time < subdate(@sdate, interval 2 month) ; /*删除两个月前的数据 只保留2个月明细数据*/
        /*员工信息中间临时表*/
	DROP TABLE IF EXISTS feods.d_en_qz_emp_info;
	CREATE TEMPORARY TABLE feods.d_en_qz_emp_info(KEY(customer_user_id))
	AS 
	SELECT  emp_user_name, 
		emp_user_id ,
		e.group_customer_id,
		c.group_name,
		e.customer_user_id 
	FROM  fe_group.sf_group_emp e 
	JOIN fe_group.sf_group_customer c ON e.group_customer_id = c.group_customer_id 
	WHERE c.group_customer_id = 4750
	AND e.data_flag = 1
	AND c.data_flag =1
	AND e.customer_user_id IS NOT NULL
	;
	/*企业在绑定企业用户在商城的消费 临时中间表*/ 
	DROP TABLE IF EXISTS feods.de_en_sc_tmp;
	CREATE TEMPORARY TABLE feods.de_en_sc_tmp(KEY(user_id))
	AS
	SELECT
		CASE WHEN sgo.order_type=5 THEN '饿了么订单' ELSE '商城消费' END AS channel,
		sgo.order_id,
		sgo.order_date,
		DATE(sgo.order_date) AS date_r,
		sgo.order_user_id AS user_id,
		sgp.pay_amount AS order_amount,
		t1.emp_user_name ,
		t1.emp_user_id ,
		t1.group_customer_id ,
		t1.group_name 
	 FROM fe_goods.sf_group_order sgo  
	JOIN fe_goods.sf_group_order_pay sgp ON sgo.order_id=sgp.order_id
	JOIN feods.d_en_qz_emp_info t1 ON t1.customer_user_id = sgo.order_user_id
	WHERE sgo.data_flag = 1
	AND sgp.data_flag= 1
	AND sgp.pay_state = 2
	-- AND sgo.order_date >=@start_time
	-- AND sgo.order_date < @enf_time
	AND sgp.pay_time >=@start_time
	AND sgp.pay_time < @end_time
	;
	/*企业在绑定企业用户在货架的消费 临时中间表*/ 
	DROP TABLE IF EXISTS feods.de_en_shelf_qz_tmp;
	CREATE TEMPORARY TABLE feods.de_en_shelf_qz_tmp(KEY(user_id))
	AS 
	SELECT 
		'货架消费' AS channel,
		so.order_id ,
		so.order_date ,
		DATE(so.order_date) date_r,
		so.user_id ,
		so.PRODUCT_TOTAL_AMOUNT AS order_amount,
		t1.emp_user_name ,
		t1.emp_user_id ,
		t1.group_customer_id ,
		t1.group_name 
	FROM
	  fe.sf_order so
	JOIN feods.d_en_qz_emp_info t1 ON t1.customer_user_id = so.user_id 
	WHERE so.data_flag = 1
	and so.`ORDER_STATUS` IN (2,6,7)
	AND so.PAY_DATE >= @start_time
	AND so.PAY_DATE < @end_time
	;
	/*将商城&货架消费中间表 插入都结果表中*/
	INSERT INTO feods.`d_en_qz_emp_order`
	(
		channel,
		order_id,
		order_date,
		date_r,
		user_id,
		order_amount,
		emp_user_name,
		emp_user_id,
		group_customer_id,
		group_name
	)
	SELECT aa.channel,
	       aa.order_id,
	       aa.order_date,
	       aa.date_r,
	       aa.user_id,
	       aa.order_amount,
	       aa.emp_user_name,
	       aa.emp_user_id,
	       aa.group_customer_id,
	       aa.group_name
	FROM feods.de_en_sc_tmp aa
	UNION ALL 
	SELECT bb.channel,
	       bb.order_id,
	       bb.order_date,
	       bb.date_r,
	       bb.user_id,
	       bb.order_amount,
	       bb.emp_user_name,
	       bb.emp_user_id,
	       bb.group_customer_id,
	       bb.group_name
	FROM feods.de_en_shelf_qz_tmp bb
	;
	end if;	
	IF t_error = 1 THEN  
	     ROLLBACK;  
	 ELSE  
	     COMMIT;  
	 END IF;
   /*调用日志*/
  CALL feods.sp_task_log (
    'd_en_qz_emp_order',
    @sdate,
    CONCAT(
      'lnh@',
      @user,@timestamp
    )
  );
  COMMIT;
END