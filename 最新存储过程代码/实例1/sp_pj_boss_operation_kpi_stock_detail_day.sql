CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_pj_boss_operation_kpi_stock_detail_day`(IN pi_dutydate  DATE)
    SQL SECURITY INVOKER
BEGIN
-- =============================================
-- Author:	liuyi
-- Create date: 2019/04/10
-- Modify date: 
-- Description:	
-- 	GMV各业务达成保障kpi指标-库存满足率明细
-- 
-- =============================================
    DECLARE l_state_date1 DATE;
    DECLARE l_state_date2 DATE;
    # DECLARE l_state_date1_hour BIGINT;
    DECLARE l_test VARCHAR(1);
    DECLARE l_row_cnt INT;
    DECLARE CODE CHAR(5) DEFAULT '00000';
    DECLARE done INT;
    DECLARE l_workday_flag INT;
    DECLARE l_work_day DATE;
    
    DECLARE l_table_owner   VARCHAR(64);
    DECLARE l_task_name     VARCHAR(64);
    
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
                # 异常日志记录模块
		DECLARE EXIT HANDLER FOR SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1
			CODE = RETURNED_SQLSTATE,@x2 = MESSAGE_TEXT;
			CALL sh_process.sp_stat_err_log_info(l_task_name,@x2); 
                        CALL sh_process.sp_event_task_log(l_task_name,l_state_date1,3);
                     #   SET po_returnvalue = 999;
		END; 
     # 日志变量初始化	
    SET l_task_name = 'sp_pj_boss_operation_kpi_stock_detail_day';  -- 存储过程名称
   
    # 程序变量初始化
    #SET l_state_date = CAST(SUBSTRING(pi_dutydate,1,8) AS SIGNED); -- 统计时间
    SET l_state_date1 = DATE(pi_dutydate);
    SET l_state_date2 = DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY));
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
    #SET l_state_date1_hour = CAST(SUBSTRING(pi_dutydate,1,10) AS SIGNED);
    # 日志存储过程
    CALL sh_process.sp_event_task_log(l_task_name,l_state_date1,1);
    CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'开始执行',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
       # 存储过程逻辑 执行sql
           #步骤1：清除中间表和结果表 统计周期内数据
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤1：清除中间表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
              
              DELETE FROM feods.pj_boss_operation_kpi_stock_detail_day WHERE sdate = l_state_date2;
              
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤1：清除中间表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));   
           #步骤2：统计中间表
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤2：统计中间表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
              
              
              	
	   CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤2：统计中间表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
           #步骤3：写入结果表
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤3：写入结果表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
		# 仓库库存满足率
		INSERT INTO feods.pj_boss_operation_kpi_stock_detail_day
		(  sdate
		  ,business_area           
		  ,product_fe              
		  ,product_name            
		  ,suggest_fill_num        
		  ,stock_num               
		  ,result_rate                        
		  ,result_type 
		)
		SELECT 
		  l_state_date2 
		 ,tab1.business_area
		 ,tab1.product_fe
		 ,tab1.product_name
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) AS stock_num
		 ,CASE WHEN SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum) > 1 THEN 1
		       ELSE IFNULL(SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum),0)
		      END AS result_rate
		 ,1  # 结果类型 1:仓库，2：前置仓
		FROM  
		 ( SELECT fill_sug.business_area
		         ,prod.product_fe
		         ,prod.product_name
		         ,fill_sug.suggest_fill_sum
		     FROM 
			(
			 SELECT  city.business_area
				,foi.product_id
				,SUM(foi.suggest_fill_num) AS  suggest_fill_sum
			   FROM fe.sf_fillorder_requirement fo
			   LEFT JOIN fe.sf_fillorder_requirement_item  foi
			   ON fo.requirement_id = foi.requirement_id
			   LEFT JOIN fe.sf_shelf sh
			   ON fo.shelf_id = sh.shelf_id
			   LEFT JOIN feods.zs_city_business city
			   ON SUBSTRING_INDEX(SUBSTRING_INDEX(sh.AREA_ADDRESS, ',', 2),',',-1)=city.city_name
			WHERE fo.data_flag=1
			  AND fo.total_price >= 150  # 在原基础上，提出当大仓覆盖货架系统触发整单金额<150元（前置仓覆盖货架不受金额影响）数据，该部分数据不考核库存满足率
			  AND fo.add_time >= l_state_date1
			  AND fo.add_time < DATE_ADD(l_state_date1,INTERVAL 1 DAY) 
			GROUP BY city.business_area
				,foi.product_id
		   ) fill_sug
		  , feods.zs_product_dim_sserp prod
		   WHERE prod.product_type  IN ('新增（正式运行）','原有')
		     AND prod.remark ='remark'     # 剔除预淘汰的商品
		     AND fill_sug.product_id = prod.product_id 
		     AND fill_sug.business_area = prod.BUSINESS_AREA
			) tab1
		  LEFT JOIN
		 (
		  SELECT stock.BUSINESS_AREA
			,stock.PRODUCT_BAR
			,SUM(stock.FBASEQTY) AS stock_num
		    FROM feods.PJ_OUTSTOCK2_DAY stock
		   WHERE stock.fproducedate >= DATE_SUB(l_state_date1,INTERVAL 2 DAY)
		     AND stock.fproducedate < DATE_SUB(l_state_date1,INTERVAL 1 DAY)  
		     AND stock.product_type  IN ('新增（正式运行）','原有')
		   GROUP BY stock.BUSINESS_AREA
			,stock.PRODUCT_BAR
		 ) tab2
		 ON tab1.business_area = tab2.BUSINESS_AREA
		 AND tab1.product_fe = tab2.PRODUCT_BAR
		 GROUP BY 
		 tab1.business_area
		 ,tab1.product_fe
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) ;
		 	  
		  COMMIT;
		  
            # 前置仓库存满足率
            INSERT INTO feods.pj_boss_operation_kpi_stock_detail_day
		(  sdate
		  ,business_area           
		  ,product_fe              
		  ,product_name            
		  ,suggest_fill_num        
		  ,stock_num               
		  ,result_rate
		  ,warehouse_id
		  ,result_type 
		)
		SELECT 
		  l_state_date2
		 ,tab1.business_area
		 ,tab1.product_fe
		 ,tab1.product_name
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) AS stock_num
		 ,CASE WHEN SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum) > 1 THEN 1
		       ELSE IFNULL(SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum),0)
		      END AS result_rate
		 ,tab1.warehouse_id
		 ,2 # 结果类型 1:仓库，2：前置仓 
		FROM  
		 (SELECT  fill_sug.business_area
			 ,prod.product_fe
			 ,prod.product_name
			 ,fill_sug.warehouse_id
			 ,fill_sug.suggest_fill_sum
	            FROM
		        (
			 SELECT  city.business_area
				,foi.product_id
				,pre.warehouse_id
				,SUM(foi.suggest_fill_num) AS  suggest_fill_sum
			   FROM fe.sf_fillorder_requirement fo
			   LEFT JOIN fe.sf_fillorder_requirement_item  foi
			   ON fo.requirement_id = foi.requirement_id
			   LEFT JOIN fe.sf_shelf sh
			   ON fo.shelf_id = sh.shelf_id
			   LEFT JOIN feods.zs_city_business city
			   ON SUBSTRING_INDEX(SUBSTRING_INDEX(sh.AREA_ADDRESS, ',', 2),',',-1)=city.city_name
			   LEFT JOIN fe.sf_prewarehouse_shelf_detail pre
			   ON fo.shelf_id = pre.shelf_id
			 WHERE fo.data_flag=1
			  AND fo.add_time >= l_state_date1
			  AND fo.add_time < DATE_ADD(l_state_date1,INTERVAL 1 DAY)
			GROUP BY city.business_area
				,foi.product_id
				,pre.warehouse_id
	             ) fill_sug
		  , feods.zs_product_dim_sserp prod
		   WHERE prod.product_type  IN ('新增（正式运行）','原有')
		     AND prod.remark ='remark'     # 剔除预淘汰的商品
		     AND fill_sug.product_id = prod.product_id 
		     AND fill_sug.business_area = prod.BUSINESS_AREA
		) tab1
		  LEFT JOIN
		 (
		  SELECT stock.BUSINESS_AREA
			,prod.product_fe
			,stock.warehouse_id
			,SUM(stock.AVAILABLE_STOCK) AS stock_num
		    FROM feods.pj_prewarehouse_stock_detail stock
		    LEFT JOIN feods.zs_product_dim_sserp prod
		     ON stock.product_id = prod.product_id 
		   WHERE stock.check_date >= DATE_SUB(l_state_date1,INTERVAL 1 DAY)
		    AND stock.check_date < l_state_date1 
		     AND prod.product_type  IN ('新增（正式运行）','原有')
		     AND prod.remark ='remark'     # 剔除预淘汰的商品
		   GROUP BY stock.BUSINESS_AREA
			,prod.product_fe
			,stock.warehouse_id 
		 ) tab2
		 ON tab1.business_area = tab2.BUSINESS_AREA
		 AND tab1.product_fe = tab2.product_fe
		 AND tab1.warehouse_id = tab2.warehouse_id
		 GROUP BY 
		 tab1.business_area
		 ,tab1.product_fe
		 ,tab1.warehouse_id
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) 
		 ;	 
			      
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤3：写入结果表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
       COMMIT;
       # SET po_returnvalue = 0;
    CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'结束执行',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
    CALL sh_process.sp_event_task_log(l_task_name,l_state_date1,2);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_pj_boss_operation_kpi_stock_detail_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));    
END