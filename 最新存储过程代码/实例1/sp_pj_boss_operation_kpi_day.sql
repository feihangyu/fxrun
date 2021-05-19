CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_pj_boss_operation_kpi_day`(IN pi_dutydate  DATE)
    SQL SECURITY INVOKER
BEGIN
-- =============================================
-- Author:	liuyi
-- Create date: 2019/04/01
-- Modify date: 
-- Description:	
-- 	处理GMV各业务达成保障清单-经规kpi报表
-- 
-- =============================================
    DECLARE l_state_date1 DATE;
    DECLARE l_state_date2 DATE;
    DECLARE l_state_beg_date DATE;
    DECLARE l_state_end_date DATE;
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
     # 日志变量设置	
    SET l_task_name = 'sp_pj_boss_operation_kpi_day';  -- 存储过程名称
   
    #SET l_state_date = CAST(SUBSTRING(pi_dutydate,1,8) AS SIGNED); -- 统计时间
    SET l_state_date1 = DATE(pi_dutydate);
    SET l_state_date2 = DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY));
    SET l_state_beg_date = DATE_ADD(l_state_date1,INTERVAL -DAY(CURDATE())+1 DAY);
    SET l_state_end_date = LAST_DAY(l_state_date1);
    SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
    #SET l_state_date1_hour = CAST(SUBSTRING(pi_dutydate,1,10) AS SIGNED);
    # 日志存储过程
    CALL sh_process.sp_event_task_log(l_task_name,l_state_date1,1);
    CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'开始执行',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
       # 存储过程逻辑 执行sql
           #步骤1：清除中间表和结果表 统计周期内数据
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤1：清除中间表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
              
              DELETE FROM feods.pj_boss_operation_kpi_day WHERE sdate = l_state_date2;
              
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤1：清除中间表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));   
           #步骤2：统计中间表
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤2：统计中间表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
              SELECT if_work_day INTO l_workday_flag FROM feods.fjr_work_days WHERE sdate=l_state_date1;
              
              # 如果是周末统计的日期取最近一个工作日的seq往前减两个工作日，为了取补货上架后GMV和补货上架前GMV的工作日比对，
              # 如果是工作日直接取前第三个工作日数据；只有前面第三个工作日才有数据
              IF l_workday_flag = 0 THEN
                      SELECT t.sdate
                        INTO l_work_day
		        FROM feods.fjr_work_days t 
		       WHERE t.work_day_seq=(SELECT MAX(t.work_day_seq)-2
		                               FROM feods.fjr_work_days t
		                              WHERE  t.sdate <= l_state_date1);
	     ELSE
			SELECT
			  t.sdate
			INTO l_work_day
			FROM
			  feods.fjr_work_days t
			WHERE t.work_day_seq =
			  (SELECT
			    t.work_day_seq - 3
			  FROM
			    feods.fjr_work_days t
			  WHERE t.sdate = l_state_date1);
		 END IF;	
	   CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤2：统计中间表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
           #步骤3：写入结果表
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤3：写入结果表开始',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
		#补货上架后GMV提升率
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		 SELECT
		   DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY)) AS stat_date,
		  1 AS seq,
		  '补货上架后GMV提升率' AS primary_index,
		  tab1.supplier_type AS secondary_index,
		  SUM(IF(A > 0, 1, 0)) / COUNT(DISTINCT tab1.shelf_id) AS res_rate,
		  CONCAT(DATE_FORMAT(tab1.fill_date,'%Y-%m-%d'),'补货上架后GMV提升率') AS comm
		FROM
		  (SELECT
		    CASE
		      WHEN sub.supplier_type = 2
		      THEN '仓库'
		      WHEN sub.supplier_type = 9
		      THEN '前置仓'
		    END AS supplier_type,
		    sub.fill_date,
		    sub.shelf_id,
		    SUM(
		      IF(
			od.work_day_seq >= sub.before_workday_seq
			AND od.work_day_seq < sub.fill_date_seq,
			od.sale_price * od.quantity,
			0
		      )
		    ) AS before_workday_gmv,
		    SUM(
		      IF(
			od.work_day_seq > sub.fill_date_seq
			AND od.work_day_seq <= sub.after_workday_seq,
			od.sale_price * od.quantity,
			0
		      )
		    ) AS after_workday_gmv #,count(distinct if(od.work_day_seq = sub.fill_date_seq,sub.shelf_id,0) )
		    ,
		    SUM(
		      IF(
			od.work_day_seq > sub.fill_date_seq
			AND od.work_day_seq <= sub.after_workday_seq,
			od.sale_price * od.quantity,
			0
		      )
		    ) / SUM(
		      IF(
			od.work_day_seq >= sub.before_workday_seq
			AND od.work_day_seq < sub.fill_date_seq,
			od.sale_price * od.quantity,
			0
		      )
		    ) - 1 AS A
		  FROM
		    (SELECT
		      DATE(a.fill_time) AS fill_date,
		      a.shelf_id,
		      a.supplier_type,
		      b.work_day_seq AS fill_date_seq,
		      b.work_day_seq - 2 AS before_workday_seq,
		      b.work_day_seq + 2 AS after_workday_seq
		    FROM
		      fe.sf_product_fill_order a,
		      feods.fjr_work_days b
		    WHERE a.ORDER_STATUS IN (2, 3, 4)
		      AND a.fill_type IN (1, 2, 8, 9, 10)
		      AND a.supplier_type IN (2, 9)
		      AND a.FILL_TIME >= DATE_SUB(l_state_date1, INTERVAL 7 DAY)
		      AND a.fill_time < l_state_date1
		      AND DATE(a.fill_time) = b.sdate
		      AND b.if_work_day = 1) sub,
		    (SELECT
		      o.shelf_id,
		      o.sale_price,
		      o.quantity,
		      w.sdate,
		      w.work_day_seq
		    FROM
		      feods.sf_order_item_temp o,
		      feods.fjr_work_days w
		    WHERE w.sdate = DATE(o.order_date)
		      AND w.if_work_day = 1
		      AND w.sdate >= DATE_SUB(l_state_date1, INTERVAL 10 DAY)
		      AND w.sdate < l_state_date1) od
		  WHERE sub.shelf_id = od.shelf_id
		  GROUP BY
		    CASE
		      WHEN sub.supplier_type = 2
		      THEN '仓库'
		      WHEN sub.supplier_type = 9
		      THEN '前置仓'
		    END,
		    sub.fill_date,
		    sub.shelf_id) tab1
		WHERE tab1.fill_date = l_work_day
		GROUP BY tab1.supplier_type;
		# 新品GMV
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		 SELECT
		   DATE(DATE_SUB(l_state_date1, INTERVAL 1 DAY)) AS stat_date,
		  2 AS seq,
		  '新品GMV' AS primary_index,
		  '' AS secondary_index,
		  SUM(tab1.sale_price * tab1.quantity) AS gmv,
		  '新品GMV' AS comm
		FROM
		(
		  SELECT city.BUSINESS_AREA
		        ,a.PRODUCT_ID
		        ,a.sale_price
		        ,a.quantity
		    FROM 
			  feods.sf_order_item_temp a,
			  fe.sf_shelf sh,
			  feods.zs_city_business city
	            WHERE a.shelf_id = sh.SHELF_ID
	                  and a.order_status = 2   # 11月05日改回去除自动贩卖机，上一次修改10月16日
			  AND sh.data_flag = 1
			  AND SUBSTRING_INDEX(SUBSTRING_INDEX(sh.AREA_ADDRESS, ',', 2),',',-1)=city.city_name
			  AND a.order_date >= l_state_date2
			  AND a.order_date < l_state_date1
	            ) tab1,
		  (SELECT t.business_area
		    ,t.PRODUCT_ID
		  FROM
		    feods.zs_product_dim_sserp t
		  WHERE t.PRODUCT_TYPE IN (
		      '新增（试运行）',
		      '新增（免费货）'
		    )) sub
		WHERE tab1.PRODUCT_ID = sub.product_id
		  AND tab1.BUSINESS_AREA = sub.BUSINESS_AREA;
		    
		  
		# 次日上架率
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		 SELECT
		 DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY)) AS stat_date,
		 3 AS seq,
		  '次日上架率' AS primary_index,
		  CASE WHEN c.shelf_level IN ('甲级','甲级2','乙级')  THEN '甲乙、新安装货架'
		       ELSE '其它货架' END AS secondary_index,
		  COUNT( DISTINCT IF(DATEDIFF(a.fill_time,a.apply_time) < 2,a.order_id,NULL) )/
		  COUNT(DISTINCT a.order_id) AS morrow_fill_percent,
		  '次日上架率' AS comm
		FROM
		  fe.sf_product_fill_order a
		  LEFT JOIN feods.pj_shelf_level_ab c
		    ON a.shelf_id = c.shelf_id
		WHERE a.order_status IN ( 2, 3, 4)
		  AND a.fill_type IN (1,2,8,9)
		  AND a.data_flag = 1 
		  AND c.smonth=CAST(DATE_FORMAT(l_state_date1,'%Y%m') AS SIGNED)
		  AND a.APPLY_TIME >= l_state_date2
		  AND a.apply_time < l_state_date1
		  GROUP BY   CASE WHEN c.shelf_level IN ('甲级','甲级2','乙级')  THEN '甲乙、新安装货架'
		       ELSE '其它货架' END ;
		  
		 # 仓库库存满足率
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		 SELECT 
		  DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY)) AS stat_date
		  ,4 AS seq
		--  ,IF(SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)>1,1,SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)) AS B_factor
		--  ,AVG(table1.x1) AS A_factor
		 ,'库存满足率' AS primary_index
		 ,'仓库库存满足率' AS secondary_index
		 ,IF(SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)>1,1,SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)) * 0.5 + AVG(table1.x1) * 0.5 AS inventory_fill_rate
		 ,'库存满足率-仓库库存满足率' AS comm
		 FROM 
		 (
		SELECT 
		 tab1.business_area
		 ,tab1.product_fe
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) AS stock_num
		 ,CASE WHEN SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum) > 1 THEN 1
		       ELSE IFNULL(SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum),0)
		      END AS x1
		FROM  
		
		 ( SELECT fill_sug.business_area
		         ,prod.product_fe
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
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) 
		 ) table1;
		 
		# 前置仓库存满足率
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		SELECT 
		DATE(DATE_SUB(l_state_date1, INTERVAL 1 DAY)) AS stat_date
		 , 5 AS seq
		 ,'库存满足率' AS primary_index
		 ,'前置仓库存满足率' AS secondary_index
		,IF(SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)>1,1,SUM(table1.stock_num)/SUM(table1.suggest_fill_sum))  * 0.5 + AVG(table1.x1) * 0.5 AS inventory_fill_rate
		-- ,IF(SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)>1,1,SUM(table1.stock_num)/SUM(table1.suggest_fill_sum)) AS B_factor 
		-- ,AVG(table1.x1) AS A_factor
		,'库存满足率-前置仓库存满足率' AS comm
		 FROM 
		 (
		SELECT 
		 tab1.business_area
		 ,tab1.product_fe
		 ,tab1.warehouse_id
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) AS stock_num
		 ,CASE WHEN SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum) > 1 THEN 1
		       ELSE IFNULL(SUM(tab2.stock_num)/SUM(tab1.suggest_fill_sum),0)
		      END AS x1
		FROM  
		 (SELECT  fill_sug.business_area
			 ,prod.product_fe
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
		  SELECT stock.business_area
			,prod.product_fe
			,stock.warehouse_id
			,SUM(stock.available_stock) AS stock_num
		    FROM feods.pj_prewarehouse_stock_detail stock
		    LEFT JOIN feods.zs_product_dim_sserp prod
		     ON stock.product_id = prod.product_id 
		   WHERE stock.check_date >= DATE_SUB(l_state_date1,INTERVAL 1 DAY)
		    AND stock.check_date <  l_state_date1 
		     AND prod.product_type  IN ('新增（正式运行）','原有')
		     AND prod.remark ='remark'     # 剔除预淘汰的商品
		   GROUP BY stock.business_area
			,prod.product_fe 
			,stock.warehouse_id 
		 ) tab2
		 ON tab1.business_area = tab2.business_area
		 AND tab1.product_fe = tab2.product_fe
		 AND tab1.warehouse_id = tab2.warehouse_id
		 GROUP BY 
		 tab1.business_area
		 ,tab1.product_fe
		 ,tab1.warehouse_id
		 ,tab1.suggest_fill_sum
		 ,IF(tab2.stock_num >= tab1.suggest_fill_sum, tab1.suggest_fill_sum,tab2.stock_num) 
		 ) table1;
		# 当月新装货架GMV累计
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
           SELECT 
		  DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY)) AS stat_date
		 ,6 AS seq
		 ,'当月新装货架GMV累计' AS primary_index
		 ,'' AS secondary_index
		 ,SUM(
		      c.PRODUCT_TOTAL_AMOUNT+c.DISCOUNT_AMOUNT+c.COUPON_AMOUNT
		    ) AS 当月新装货架GMV累计
		 ,'当月新装货架GMV累计' AS comm
		  FROM 
		  (
		  SELECT   DATE_FORMAT(b.ACTIVATE_TIME, '%Y-%m')  AS act_mon,
			 e.BUSINESS_AREA,
			 b.SHELF_ID,
			 b.ACTIVATE_TIME,
			 DATE_ADD(b.ACTIVATE_TIME,INTERVAL 30 DAY) act_order_date
		    FROM   fe.sf_shelf b
			 ,feods.zs_city_business e
		   WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),',',-1)=e.city_name
		  AND b.ACTIVATE_TIME >= l_state_beg_date
		  AND b.ACTIVATE_TIME < DATE_ADD(l_state_end_date,INTERVAL 1 DAY)
		  ) shelf
		  ,fe.sf_order c
		 WHERE shelf.shelf_id = c.shelf_id
		   AND c.order_status = 2
		   AND c.ORDER_DATE >= l_state_beg_date
		   AND c.ORDER_DATE < DATE_ADD(l_state_end_date,INTERVAL 1 DAY) ;
                #所有企业产生货架GMV
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		  SELECT 
		    DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY)) AS stat_date
		  ,7 AS seq
		 ,'所有企业产生货架GMV' AS primary_index
		 ,'' AS secondary_index
		  ,SUM(oi.sale_price * oi.quantity) 当日GMV
		 ,'所有企业产生货架GMV' AS comm
		    FROM fe_group.sf_group_customer c
                    JOIN fe_group.sf_group_emp e
                    ON c.group_customer_id = e.group_customer_id
                    JOIN fe.sf_order o
                    ON e.customer_user_id = o.user_id
                    JOIN fe.sf_order_item oi
                    ON o.order_id = oi.ORDER_ID
                    WHERE o.ORDER_STATUS = 2
                    AND c.group_name NOT IN('丰e足食店主管理组','丰e足食风控组','丰e足食经营管理组','丰e足食市场组','丰e足食物流管理组')  
		    AND o.order_date >= DATE(DATE_SUB(l_state_date1, INTERVAL 1 DAY))
			  AND o.order_date < l_state_date1
		GROUP BY DATE(DATE_SUB(l_state_date1, INTERVAL 1 DAY));		  
	
			  
		# 新装货架30日内货架数
		INSERT INTO feods.pj_boss_operation_kpi_day
		(sdate,
		seq,
		primary_index,
		secondary_index,
		result_rate,
		comm
		)
		  SELECT 
		    DATE(DATE_SUB(l_state_date1,INTERVAL 1 DAY)) AS stat_date
		  ,8 AS seq
		 ,'当月新装货架数量累计' AS primary_index
		 ,'' AS secondary_index
		  ,COUNT(
		    DISTINCT 
		      shelf.shelf_id
		    ) AS '当月新装货架数量累计'
		 ,'当月新装货架数量累计' AS comm
		  FROM 
		  (
                SELECT   DATE_FORMAT(b.ACTIVATE_TIME, '%Y-%m')  AS act_mon,
			 e.BUSINESS_AREA,
			 b.SHELF_ID,
			 b.ACTIVATE_TIME,
			 DATE_ADD(b.ACTIVATE_TIME,INTERVAL 30 DAY) act_order_date
		    FROM   fe.sf_shelf b
			 ,feods.zs_city_business e
		   WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),',',-1)=e.city_name
		  AND b.ACTIVATE_TIME >= l_state_beg_date
		  AND b.ACTIVATE_TIME < DATE_ADD(l_state_end_date,INTERVAL 1 DAY) 
		  ) shelf;		  
		  COMMIT;
			 
			      
           CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'步骤3：写入结果表结束',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
       COMMIT;
       # SET po_returnvalue = 0;
    CALL sh_process.sp_task_log(l_task_name,l_state_date1,CONCAT(l_state_date1,'结束执行',DATE_FORMAT(NOW(),'%Y%m%d %H:%i:%s')));
    CALL sh_process.sp_event_task_log(l_task_name,l_state_date1,2);
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_pj_boss_operation_kpi_day',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
END