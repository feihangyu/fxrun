CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_en_emp_distribute_zxcy`()
BEGIN
   SET @run_date := CURRENT_DATE();
   SET @user := CURRENT_USER();
   SET @timestamp := CURRENT_TIMESTAMP();
   SET @last_month_first_day := DATE_SUB(DATE_SUB(DATE_FORMAT(NOW(),'%y-%m-%d'),INTERVAL EXTRACT(
DAY FROM NOW())-1 DAY),INTERVAL 1 MONTH) ,
@last_month_last_day := DATE_SUB(DATE_SUB(DATE_FORMAT(NOW(),'%y-%m-%d'),INTERVAL EXTRACT(
DAY FROM NOW())-1 DAY),INTERVAL 0 MONTH);
REPLACE INTO `fe_dm`.`dm_en_emp_distribute_zxwh_mid`(
emp_user_id,
emp_user_name,
job_number,
dept,
dept_id,
group_customer_id,
group_name,
`year`,
quar,
mon,
recycled_amount,
distributed_amout,
rank_amount
)
SELECT 	t.emp_user_id,
	t.emp_user_name,
	t.job_number,
	t.dept,
	t.dept_id,
	t.group_customer_id,
	t.group_name,
	`year`, 
	quar,
	mon, 
	SUM(CASE WHEN t.distribute_status = 3 THEN t.distribute_amount ELSE 0 END) AS  recycled_amount,
	SUM(t.distribute_amount) AS distributed_amout,
	SUM(t.distribute_amount) - SUM(CASE WHEN t.distribute_status = 3 THEN t.distribute_amount ELSE 0 END) AS rank_amount
FROM 
(
SELECT		
   YEAR(d.add_time) `year`,
   MONTH(d.add_time) mon,
   QUARTER(d.add_time) quar,
   d.add_time ,
   p.group_customer_id,
   c.group_name ,			
   e.job_number ,
   e.`emp_user_id`,			
   e.emp_user_name ,			
   e.dept ,	
   e.dept_id,	
   d.distribute_status,						
   d.distribute_amount 	
FROM			
    `fe_group`.sf_group_distribute_plan_detail d			
JOIN fe_group.sf_group_distribute_plan p ON p.distribute_plan_id = d.distribute_plan_id			
JOIN fe_group.sf_group_customer c ON c.group_customer_id = p.group_customer_id			
JOIN fe_group.sf_group_emp e ON e.emp_user_id = d.emp_user_id
WHERE d.data_flag = 1
AND p.group_customer_id = 6038  ## 正心诚意公司
AND d.add_time >=  @last_month_first_day  # 上个月第一天
AND d.add_time <  @last_month_last_day # 本月第一天
AND e.dept_id IS NOT NULL ## 
)t
GROUP BY t.emp_user_id,t.dept_id
ORDER BY t.dept_id,rank_amount DESC
;
# ============================================
# 开发用表 
# =============================================
## 个人月度 
REPLACE INTO `fe_dm`.`dm_en_group_distribute_emp_rank`
(
group_customer_id,
       dept_id,
       emp_id,
       `year`,
       unit_type,
       unit_time,
       distributed_amount,
       recycled_amount,
       rank_amount,
       data_flag,
       add_time,
       add_user_id,
       last_update_time,
       last_update_user_id
)
SELECT group_customer_id,
       dept_id,
       emp_user_id,
       `year`,
       1 AS unit_type,
       mon unit_time,
       distributed_amout,
       recycled_amount,
       rank_amount,
       1 AS data_flag,
       NOW()AS  add_time,
       1383377 AS add_user_id,
       NOW()AS last_update_time,
       1383377 AS last_update_user_id
FROM `fe_dm`.`dm_en_emp_distribute_zxwh_mid`
WHERE `year` = YEAR(DATE_SUB(CURDATE(),INTERVAL 1 MONTH)) 
AND mon = MONTH(DATE_SUB(CURDATE(),INTERVAL 1 MONTH))
;
## 个人季度统计
REPLACE INTO `fe_dm`.`dm_en_group_distribute_emp_rank`
(
group_customer_id,
       dept_id,
       emp_id,
       `year`,
       unit_type,
       unit_time,
       distributed_amount,
       recycled_amount,
       rank_amount,
       data_flag,
       add_time,
       add_user_id,
       last_update_time,
       last_update_user_id
)
SELECT group_customer_id,
       dept_id,
       emp_user_id,
       `year`,
       2 AS unit_type,
       quar unit_time,
       SUM(distributed_amout) distributed_amout ,
       SUM(recycled_amount) recycled_amount,
       SUM(rank_amount) rank_amount,
       1 AS data_flag,
       NOW()AS  add_time,
       1383377 AS add_user_id,
       NOW()AS last_update_time,
       1383377 AS last_update_user_id
FROM `fe_dm`.`dm_en_emp_distribute_zxwh_mid`
WHERE `year` = YEAR(DATE_SUB(CURDATE(),INTERVAL 1 QUARTER)) 
AND quar = QUARTER(DATE_SUB(CURDATE(),INTERVAL 1 QUARTER))
GROUP BY emp_user_id
ORDER BY dept_id, rank_amount DESC
;
## 部门 月度统计
REPLACE INTO `fe_dm`.`dm_en_group_distribute_dept_rank`
(
       group_customer_id,
       dept_id,
       `year`,
       unit_type,
       unit_time,
       distributed_amount,
       recycled_amount,
       rank_amount,
       data_flag,
       add_time,
       add_user_id,
       last_update_time,
       last_update_user_id
)
SELECT group_customer_id,
       dept_id,
       `year`,
       1 AS unit_type,
       mon unit_time,
       SUM(distributed_amout) distributed_amout ,
       SUM(recycled_amount) recycled_amount,
       SUM(rank_amount) rank_amount,
       1 AS data_flag,
       NOW()AS  add_time,
       1383377 AS add_user_id,
       NOW()AS last_update_time,
       1383377 AS last_update_user_id
FROM `fe_dm`.`dm_en_emp_distribute_zxwh_mid`
WHERE `year` = YEAR(DATE_SUB(CURDATE(),INTERVAL 1 MONTH)) 
AND mon = MONTH(DATE_SUB(CURDATE(),INTERVAL 1 MONTH))
GROUP BY dept_id
ORDER BY rank_amount DESC
;
## 部门 季度统计
REPLACE INTO `fe_dm`.`dm_en_group_distribute_dept_rank`
(
       group_customer_id,
       dept_id,
       `year`,
       unit_type,
       unit_time,
       distributed_amount,
       recycled_amount,
       rank_amount,
       data_flag,
       add_time,
       add_user_id,
       last_update_time,
       last_update_user_id
)
SELECT group_customer_id,
       dept_id,
       `year`,
       2 AS unit_type,
       quar unit_time,
       SUM(distributed_amout) distributed_amout ,
       SUM(recycled_amount) recycled_amount,
       SUM(rank_amount) rank_amount,
       1 AS data_flag,
       NOW()AS  add_time,
       1383377 AS add_user_id,
       NOW()AS last_update_time,
       1383377 AS last_update_user_id
FROM `fe_dm`.`dm_en_emp_distribute_zxwh_mid`
WHERE `year` = YEAR(DATE_SUB(CURDATE(),INTERVAL 1 QUARTER)) 
AND quar = QUARTER(DATE_SUB(CURDATE(),INTERVAL 1 QUARTER))
GROUP BY dept_id
ORDER BY rank_amount DESC
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dm_en_emp_distribute_zxcy',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('linihe@', @user, @timestamp)
  );
  COMMIT;
END