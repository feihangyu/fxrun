CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_fx_new_user_daily_balance`()
BEGIN
 DECLARE t_error INTEGER; 
   DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;
   START TRANSACTION;
        SET @user := CURRENT_USER,@timestamp := CURRENT_TIMESTAMP;
		SET @sdate = CURDATE();
		-- CREATE TABLE feods.`d_en_fx_new_user_daily_balance` (
		--    pid BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
		--   `user_id` BIGINT(20) DEFAULT NULL COMMENT '下单人员ID',
		--   `min_date` DATE DEFAULT NULL COMMENT '首单日期',
		--   `WALLET_ID` BIGINT(20) NOT NULL COMMENT '钱包ID',
		--   `BALANCE` DECIMAL(18,2) DEFAULT '0.00' COMMENT '余额',
		--    add_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '添加时间',
		--    update_time DATETIME DEFAULT CURRENT_TIMESTAMP  ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
		--   KEY idx_user_id(user_id),
		--   UNIQUE KEY uk_user_id_min_date(user_id,min_date)
		-- ) COMMENT = '丰享渠道新用户首单日余额量'
		-- ;
		-- SET @sdate = CURDATE();
		-- insert into feods.`d_en_fx_new_user_daily_balance`(user_id, min_date, WALLET_ID, BALANCE)
		-- SELECT t1.user_id, t1.min_date, t2.WALLET_ID, t2.BALANCE
		-- FROM 
		-- (
		-- SELECT  b.order_user_id AS user_id,b.sale_channel,MIN(b.order_date) AS min_date_time, DATE(MIN(b.order_date)) min_date
		-- FROM fe_goods.sf_group_order b 
		-- JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
		-- WHERE b.data_flag=1 
		-- AND pay_state = 2
		-- AND c.data_flag = 1
		-- AND b.data_flag = 1
		-- AND b.sale_channel = 'fengxiang'
		-- GROUP BY order_user_id -- ,b.sale_channel
		-- HAVING min_date = SUBDATE(CURDATE(), INTERVAL 1 DAY)
		-- )t1
		-- JOIN 
		-- (
		-- SELECT WALLET_ID, BALANCE
		-- FROM fe.`user_member_wallet`
		-- WHERE DATA_FLAG = 1
		-- AND STATUS = 1
		-- )t2 ON t1.user_id = t2.WALLET_ID
		-- ;
		DELETE FROM feods.`d_en_fx_new_user_daily_balance` WHERE add_time >= CURDATE();
		DELETE FROM feods.`d_en_fx_daily_num_user_balance` WHERE add_time >= CURDATE();
		DELETE FROM feods.`d_en_fx_balance` WHERE add_time >= CURDATE();
		DROP TABLE IF EXISTS feods.d_en_fx_new_user_daily_balance_temp;
		CREATE TEMPORARY TABLE feods.d_en_fx_new_user_daily_balance_temp(KEY(user_id))
		AS 
		SELECT  b.order_user_id AS user_id,b.sale_channel,MIN(b.order_date) AS min_date_time, DATE(MIN(b.order_date)) min_date
		FROM fe_goods.sf_group_order b 
		JOIN fe_goods.sf_group_order_pay c ON b.order_id=c.order_id
		WHERE b.data_flag=1 
		AND pay_state = 2
		AND c.data_flag = 1
		AND b.data_flag = 1
		AND b.sale_channel = 'fengxiang'
		GROUP BY order_user_id -- ,b.sale_channel
		HAVING min_date = SUBDATE(CURDATE(), INTERVAL 1 DAY)
		;
		INSERT INTO feods.`d_en_fx_new_user_daily_balance`(user_id, min_date, WALLET_ID, BALANCE)
		SELECT a.user_id, a.min_date,b.WALLET_ID, b.`BALANCE`
		FROM feods.d_en_fx_new_user_daily_balance_temp a
		JOIN fe.`user_member_wallet` b ON a.user_id = b.WALLET_ID
		WHERE b.`DATA_FLAG` = 1
		AND STATUS = 1
		;
		INSERT INTO feods.`d_en_fx_daily_num_user_balance` (sdate, user_num, balances)
		SELECT min_date, COUNT(DISTINCT user_id), SUM(BALANCE)
		FROM feods.`d_en_fx_new_user_daily_balance`
		WHERE min_date = SUBDATE(CURDATE(), INTERVAL 1 DAY)
		;
		
		/*丰享用户E币账户余额截存数据-日截存*/
		DROP TABLE IF EXISTS feods.d_en_fx_emp;
		CREATE TEMPORARY TABLE feods.d_en_fx_emp(KEY(customer_user_id))
		AS /*丰享用户E币账户*/
		SELECT c.`group_customer_id`,c.group_name, e.`customer_user_id` /*AS 消费端id*/, e.`emp_user_name` AS 姓名, e.emp_user_id
		FROM fe_group.sf_group_customer c
		JOIN fe_group.sf_group_emp e ON e.group_customer_id = c.group_customer_id  AND e.data_flag = 1 
		WHERE c.group_name NOT LIKE '%测试%'
		AND e.`emp_user_name` NOT LIKE '%测试%'
		AND e.`customer_user_id` IS NOT NULL
		AND e.`customer_user_id` > 0
		AND c.group_customer_id = 4726
		GROUP BY e.`customer_user_id`
		 ;
                /*E币账户余额截存*/
		INSERT INTO feods.`d_en_fx_balance` (WALLET_ID, BALANCE)
		SELECT b.WALLET_ID, 
		       b.`BALANCE`
		FROM feods.d_en_fx_emp a
		JOIN fe.`user_member_wallet` b ON a.customer_user_id = b.WALLET_ID 
		WHERE b.`DATA_FLAG` = 1
		;
	IF t_error = 1 THEN  
	     ROLLBACK;  
	 ELSE  
	     COMMIT;  
	 END IF;
 
  CALL feods.sp_task_log (
    'prc_d_en_fx_new_user_daily_balance',
    @sdate,
    CONCAT(
      'lnh@',
      @user,@timestamp
    )
  );
  COMMIT;
END