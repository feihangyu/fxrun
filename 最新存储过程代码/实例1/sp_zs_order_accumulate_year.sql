CREATE DEFINER=`liuyi`@`%` PROCEDURE `sp_zs_order_accumulate_year`(IN pi_dutydate      VARCHAR(12), 
                                              OUT po_returnvalue  VARCHAR(12))
    SQL SECURITY INVOKER
BEGIN
    DECLARE l_state_date BIGINT;
    DECLARE l_state_date_hour BIGINT;
    DECLARE l_test VARCHAR(1);
    DECLARE l_row_cnt INT;
    DECLARE CODE CHAR(5) DEFAULT '00000';
    DECLARE done INT;
    declare l_table_cnt int;
    
    DECLARE l_table_owner   VARCHAR(64);
    DECLARE l_city          VARCHAR(64);
    DECLARE l_task_name     VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
                # 异常日志记录模块
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1
			CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
			CALL feods.sp_stat_err_log_info(l_task_name,@x2); 
                        CALL feods.sp_event_task_log(l_task_name,l_state_date,3);
                        SET po_returnvalue = 999;
		END; 
     # 日志变量设置	
    SET po_returnvalue = -10;  -- 存储过程返回值，执行成功为0
    SET l_task_name = 'sp_zs_order_accumulate_year';  -- 存储过程名称
   
    SET l_state_date = CAST(SUBSTRING(pi_dutydate,1,8) AS SIGNED); -- 统计时间
    #SET l_state_date_hour = CAST(SUBSTRING(pi_dutydate,1,10) AS SIGNED);
    SET l_city = pi_city;
    # 日志存储过程
    CALL feods.sp_event_task_log(l_task_name,l_state_date,1);
    CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'开始执行',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
       # 存储过程逻辑 执行sql
           #步骤1：清除中间表
           CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'步骤1：清除中间表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
              
              delete from zs_order_accumulate_year where order_date = str_to_date(SUBSTRING(pi_dutydate,1,8),'%Y-%m-%d');
              
           CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'步骤1：清除中间表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));   
           #步骤2：统计中间表
           CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'步骤2：统计中间表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
	
	   CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'步骤2：统计中间表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
           #步骤3：写入结果表
           CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'步骤3：写入结果表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
              select count(*) into l_table_cnt from feods.zs_order_accumulate_year;
              
              if l_table_cnt = 0 then
		      INSERT INTO feods.zs_order_accumulate_year
				(
				  ORDER_DATE ,
				  BUSINESS_AREA ,
				  PRODUCT_ID ,
				  QUANTITY   ,
				  REAL_TOTAL_PRICE ,
				  GMV ,
				  ORDER_CNT 
				)
				SELECT
				      DATE(o2.ORDER_DATE) AS OrderDate,
				      o4.BUSINESS_AREA,
				      o1.PRODUCT_ID,
				      SUM(o1.QUANTITY) AS QUANTITY,
				      SUM(o1.REAL_TOTAL_PRICE) AS REAL_TOTAL_PRICE,
				      SUM(o1.QUANTITY * o1.SALE_PRICE) AS GMV,
				      COUNT(DISTINCT o1.ORDER_ID) AS ORDER_CNT
				    FROM
				      fe.sf_order_item o1
				      LEFT JOIN fe.sf_order o2
					ON o1.ORDER_ID = o2.ORDER_ID
				      LEFT JOIN fe.sf_shelf o3
					ON o2.SHELF_ID = o3.SHELF_ID
				      LEFT JOIN fe.zs_city_business o4
					ON SUBSTRING_INDEX(
					  SUBSTRING_INDEX(o3.AREA_ADDRESS, ',', 2),
					  ',',
					  - 1
					) = o4.CITY_NAME
				    WHERE o2.ORDER_STATUS = 2
				      AND o2.ORDER_DATE BETWEEN '2019-01-01'
				      AND NOW()
				    GROUP BY o1.PRODUCT_ID,
				      o4.BUSINESS_AREA,
				      DATE(o2.ORDER_DATE)
				      ON  DUPLICATE KEY UPDATE QUANTITY=QUANTITY+VALUES(QUANTITY)
				      ,REAL_TOTAL_PRICE=REAL_TOTAL_PRICE+VALUES(REAL_TOTAL_PRICE)
				      ,GMV=GMV+VALUES(GMV)
				      ,ORDER_CNT=ORDER_CNT+VALUES(ORDER_CNT);
			else
			      INSERT INTO feods.zs_order_accumulate_year
					(
					  ORDER_DATE ,
					  BUSINESS_AREA ,
					  PRODUCT_ID ,
					  QUANTITY   ,
					  REAL_TOTAL_PRICE ,
					  GMV ,
					  ORDER_CNT 
					)
					SELECT
					      DATE(o2.ORDER_DATE) AS OrderDate,
					      o4.BUSINESS_AREA,
					      o1.PRODUCT_ID,
					      SUM(o1.QUANTITY) AS QUANTITY,
					      SUM(o1.REAL_TOTAL_PRICE) AS REAL_TOTAL_PRICE,
					      SUM(o1.QUANTITY * o1.SALE_PRICE) AS GMV,
					      COUNT(DISTINCT o1.ORDER_ID) AS ORDER_CNT
					    FROM
					      fe.sf_order_item o1
					      LEFT JOIN fe.sf_order o2
						ON o1.ORDER_ID = o2.ORDER_ID
					      LEFT JOIN fe.sf_shelf o3
						ON o2.SHELF_ID = o3.SHELF_ID
					      LEFT JOIN fe.zs_city_business o4
						ON SUBSTRING_INDEX(
						  SUBSTRING_INDEX(o3.AREA_ADDRESS, ',', 2),
						  ',',
						  - 1
						) = o4.CITY_NAME
					    WHERE o2.ORDER_STATUS = 2
					      AND o2.ORDER_DATE BETWEEN STR_TO_DATE(SUBSTRING(pi_dutydate,1,8),'%Y-%m-%d')
					      AND NOW()
					    GROUP BY o1.PRODUCT_ID,
					      o4.BUSINESS_AREA,
					      DATE(o2.ORDER_DATE)
					      ON  DUPLICATE KEY UPDATE QUANTITY=QUANTITY+VALUES(QUANTITY)
					      ,REAL_TOTAL_PRICE=REAL_TOTAL_PRICE+VALUES(REAL_TOTAL_PRICE)
					      ,GMV=GMV+VALUES(GMV)
					      ,ORDER_CNT=ORDER_CNT+VALUES(ORDER_CNT);
			  end if;
			 
			      
           CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'步骤3：写入结果表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
       COMMIT;
       SET po_returnvalue = 0;
    CALL feods.sp_task_log(l_task_name,l_state_date,CONCAT(l_state_date,'结束执行',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
    CALL feods.sp_event_task_log(l_task_name,l_state_date,2);
END