CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_op2_shelf_grade`()
BEGIN
SET    @sdate := SUBDATE(CURRENT_DATE,1);
SET    @month_id := DATE_FORMAT(@sdate,'%Y-%m');
SET    @month_first_day := CONCAT(@month_id,'-01');
SET    @month_last_day := IF(CURRENT_DATE > LAST_DAY(@month_first_day),ADDDATE(LAST_DAY(@month_first_day),1),CURRENT_DATE);
SET    @next_month := ADDDATE(LAST_DAY(@month_first_day),1);
SET    @sub3_month := SUBDATE(@month_first_day,INTERVAL 3 MONTH);
SET    @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();	   
       
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
CREATE TEMPORARY TABLE feods.shelf_tmp AS
SELECT business_name,
       shelf_id,
       shelf_code,
       shelf_name,                                     -- 货架名称
       shelf_type shelf_type_id,                       -- 货架编码
       di.item_name shelf_type,                        -- 货架状态
       de.item_name shelf_status,                      -- 撤架状态
       DATE(s.activate_time)activate_time,
       d2.item_name revoke_status,
       DATE(revoke_time)revoke_time,
       IFNULL(s.main_shelf_id,shelf_id)main_shelf_id   -- 主货架id(如果为空则为货架id)
FROM fe_dwd.dwd_shelf_base_day_all s
LEFT JOIN fe_dwd.dwd_pub_dictionary di ON s.shelf_type = di.item_value AND di.dictionary_id = '8'
LEFT JOIN fe_dwd.dwd_pub_dictionary de ON s.shelf_status = de.item_value AND de.dictionary_id = '9'
LEFT JOIN fe_dwd.dwd_pub_dictionary d2 ON s.revoke_status = d2.item_value AND d2.dictionary_id = '50'
WHERE s.shelf_type IN(1,2,3,4,5,6,7,8)
AND (DATE(activate_time) < @next_month OR activate_time IS NULL)
AND (revoke_time IS NULL OR revoke_time >= @sub3_month)-- 2020/06/04添加，保留前3个月撤架货架
AND shelf_status != 10;-- 2020/06/04添加,剔除已失效
-- 给临时表添加索引
CREATE INDEX idx_tmp_shelf_id ON feods.shelf_tmp (shelf_id);
SELECT @times := IFNULL(SUM(IF(t.if_work_day, 1, .5)) / SUM(CASE WHEN t.sdate > @sdate THEN 0 WHEN t.if_work_day THEN 1 ELSE .5 END),0) -- 倍数
FROM feods.fjr_work_days t
WHERE t.sdate >= @month_first_day
AND t.sdate < @next_month
AND t.holiday = '';
-- 货架销量
DROP TEMPORARY TABLE IF EXISTS feods.shelf_sale_tmp;
CREATE TEMPORARY TABLE feods.shelf_sale_tmp AS 
SELECT @month_id smonth,                                                                                           
       s.business_name,
       s.shelf_id, 
       s.shelf_name,
       s.main_shelf_id,
       s.shelf_type_id,
       s.shelf_code,
       s.shelf_type,
       s.shelf_status,
       s.revoke_status,
       s.activate_time,
       s.revoke_time,
       IFNULL(a.gmv,0)gmv, -- GMV
       IFNULL(payment_money,0)after_pay, -- 补付款
       IFNULL(a.gmv,0) + IFNULL(payment_money,0)gmv_sum, -- GMV+补款
       ROUND((IFNULL(a.gmv,0) + IFNULL(payment_money,0)) * @times,2) predict_gmv, -- 本月预估含补付款GMV
       b.cur_days, -- 当前实际工作日天数
       b.work_days -- 当月总工作日天数
FROM feods.shelf_tmp s 
LEFT JOIN -- 货架销售数据
    (                                                                       
     SELECT shelf_id,                                                       
            SUM(gmv)gmv,
            SUM(payment_money)payment_money                                 
     FROM feods.fjr_shelf_dgmv                                              
     WHERE sdate >= @month_first_day                                        
     AND sdate < @month_last_day                                            
     GROUP BY shelf_id                                                      
    )a ON a.shelf_id = s.shelf_id                                           
LEFT JOIN -- 工作日天数
   (
    SELECT DATE_FORMAT(sdate,'%Y-%m')work_month,
           COUNT(CASE WHEN sdate >= @month_first_day AND sdate < @month_last_day THEN 1 END)cur_days, -- 截至昨日的工作日天数
           COUNT(*)work_days -- 当月总工作日天数
    FROM feods.fjr_work_days
    WHERE sdate >= @month_first_day
    AND sdate < @next_month
    AND if_work_day = 1
    GROUP BY DATE_FORMAT(sdate,'%Y-%m')
   ) b ON @month_id = b.work_month;
   
-- 给临时表添加索引
CREATE INDEX idx_tmp_main_shelf_id1 ON feods.shelf_sale_tmp (main_shelf_id);
-- 关联货架合计GMV、补付款
DROP TEMPORARY TABLE IF EXISTS feods.relate_shelf_sale_tmp;
CREATE TEMPORARY TABLE feods.relate_shelf_sale_tmp AS  
SELECT main_shelf_id,
       COUNT(*)shelf_amount,
       SUM(gmv_sum)gmv_sum,
       SUM(predict_gmv)predict_gmv
FROM feods.shelf_sale_tmp p
GROUP BY main_shelf_id;
-- 给临时表添加索引
CREATE INDEX idx_tmp_main_shelf_id ON feods.relate_shelf_sale_tmp (main_shelf_id);
-- 货架明细
DROP TEMPORARY TABLE IF EXISTS feods.shelf_total_tmp;
CREATE TEMPORARY TABLE feods.shelf_total_tmp AS  
SELECT p.smonth,                                                                                                                   
       p.business_name,                                                                                                            
       p.shelf_id,                                                                                                                 
       p.shelf_name,                                                                                                               
       p.main_shelf_id,                                                                                                            
       p.shelf_type_id,                                                                                                            
       p.shelf_code,                                                                                                               
       p.shelf_type,                                                                                                               
       p.shelf_status,                                                                                                             
       p.revoke_status,                                                                                                            
       p.activate_time,                                                                                                            
       p.revoke_time,                                                                                                              
       p.gmv, -- GMV                                                                                                               
       p.after_pay, -- 补付款                                                                                                      
       p.gmv_sum, -- GMV+补付款                                                                                                    
       IFNULL(s.gmv_sum,p.gmv_sum)gmv_total, -- 关联总GMV                                                                          
       IFNULL(s.predict_gmv,p.predict_gmv) predict_gmv, -- 预估GMV                                                                 
       IFNULL(s.shelf_amount,1) shelf_amount, -- 关联货架数                                                                        
       ROUND(IFNULL(s.gmv_sum,p.gmv_sum) / IFNULL(s.shelf_amount,1) / p.cur_days,2) shelf_avg_gmv1,-- 实际日架均gmv                
       ROUND(IFNULL(s.predict_gmv,p.predict_gmv) / IFNULL(s.shelf_amount,1) / p.work_days,2) shelf_avg_gmv2 -- 预估日架均gmv       
FROM feods.shelf_sale_tmp p
LEFT JOIN feods.relate_shelf_sale_tmp s ON p.main_shelf_id = s.main_shelf_id;
DELETE FROM feods.d_op_shelf_grade
WHERE month_id = @month_id;	   
INSERT INTO feods.d_op_shelf_grade
(month_id,                              
 business_name,                       
 shelf_id,                            
 shelf_name,                          
 main_shelf_id,                       
 shelf_code,                          
 shelf_type,                          
 shelf_status,                        
 REVOKE_STATUS,                       
 ACTIVATE_TIME,                       
 REVOKE_TIME,                         
 gmv,                                 
 after_pay,                           
 gmv_sum,                             
 gmv_total,                           
 shelf_amount,                        
 shelf_avg_gmv1,                      
 shelf_avg_gmv2,                      
 grade,
 load_time 
)                                              
SELECT smonth AS month_id,
       business_name,
       shelf_id,
       shelf_name,
       main_shelf_id,
       shelf_code,
       shelf_type,
       shelf_status,
       revoke_status,
       activate_time,
       revoke_time,
       gmv,       -- gmv
       after_pay, -- 补付款
       gmv_sum,   -- 含补付款gmv
       gmv_total, -- 主次货架总gmv
       shelf_amount,
       shelf_avg_gmv1,
       shelf_avg_gmv2,
       CASE WHEN shelf_status =  '已失效' OR shelf_type = '虚拟货架' OR shelf_status = '待激活' THEN '-'
            WHEN shelf_status =  '已撤架' THEN '已撤架'
            WHEN DATE_FORMAT(ACTIVATE_TIME,'%Y-%m') = smonth THEN '新装'
            WHEN (shelf_avg_gmv2 < 10 AND (shelf_type_id IN(1,2,3,5,8) OR (shelf_amount > 1))) OR (shelf_type_id = 6 AND shelf_avg_gmv2 < 25) OR (shelf_type_id = 7 AND shelf_avg_gmv2 < 50) THEN '丁'
            WHEN (shelf_avg_gmv2 >= 10 AND shelf_avg_gmv2 < 25 AND((shelf_type_id IN(1,2,3,5,8)) OR shelf_amount > 1)) OR (shelf_type_id = 6 AND shelf_avg_gmv2 >=25 AND shelf_avg_gmv2 < 70) OR(shelf_type_id = 7 AND shelf_avg_gmv2 >= 50 AND shelf_avg_gmv2 < 95) THEN '丙'
            WHEN (shelf_avg_gmv2 >= 25 AND shelf_avg_gmv2 < 40 AND(shelf_amount > 1)) OR(shelf_avg_gmv2 >= 25 AND shelf_avg_gmv2 < 40 AND shelf_type_id IN(1,3,8) AND shelf_amount = 1) OR(shelf_type_id IN(2,5) AND MONTH(@month_first_day) IN ('4','5','6','7','8','9') AND shelf_avg_gmv2 >= 25 AND shelf_avg_gmv2 < 60 AND shelf_amount = 1) OR(shelf_type_id IN(2,5) AND MONTH(@month_first_day) IN ('1','2','3','10','11','12') AND shelf_avg_gmv2 >= 25 AND shelf_avg_gmv2 < 40 AND shelf_amount = 1) OR (shelf_type_id = 6 AND shelf_avg_gmv2 >= 70 AND shelf_avg_gmv2 < 95) OR (shelf_type_id = 7 AND shelf_avg_gmv2 >= 95 AND shelf_avg_gmv2 < 140)THEN '乙'
            WHEN (shelf_avg_gmv2 >= 40 AND shelf_amount > 1) OR (shelf_avg_gmv2 >= 40 AND shelf_type_id IN(1,3,8) AND shelf_amount = 1) OR (shelf_type_id IN(2,5) AND MONTH(@month_first_day) IN ('4','5','6','7','8','9') AND shelf_avg_gmv2 >= 60 AND shelf_amount = 1) OR(shelf_type_id IN(2,5) AND MONTH(@month_first_day) IN ('1','2','3','10','11','12') AND shelf_avg_gmv2 >= 40 AND shelf_amount = 1) OR (shelf_type_id = 6 AND shelf_avg_gmv2 >= 95) OR (shelf_type_id = 7 AND shelf_avg_gmv2 >= 140)THEN '甲'
       END AS grade,
       CURRENT_TIMESTAMP AS load_time
FROM feods.shelf_total_tmp;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'd_op2_shelf_grade',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('唐进(朱星华)@', @user, @timestamp));
 
COMMIT;
END