CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_area_product_shelf_cover_week`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SELECT @week_end := SUBDATE(CURRENT_DATE,WEEKDAY(CURRENT_DATE) + 1),
       @y_m := DATE_FORMAT(@week_end, '%Y-%m'),                    
       @d := DAY(@week_end),                                       
       @month_start := SUBDATE(@week_end, @d - 1),                 
       @month_start1 := SUBDATE(@month_start, INTERVAL 1 MONTH),
       @month_start2 := SUBDATE(@month_start, INTERVAL 2 MONTH),
       @month_start3 := SUBDATE(@month_start, INTERVAL 3 MONTH),
       @y_m1 := DATE_FORMAT(@month_start1, '%Y-%m'),            
       @y_m2 := DATE_FORMAT(@month_start2, '%Y-%m'),            
       @y_m3 := DATE_FORMAT(@month_start3, '%Y-%m'),                       
       @sdate90 := SUBDATE(@week_end,89),                         
       @month_end1 := LAST_DAY(@month_start1),                  
       @month_end2 := LAST_DAY(@month_start2),                  
       @month_end3 := LAST_DAY(@month_start3),                                    
       @mflag1 := @month_end1 >= @month_end2,                      
       @mflag2 := @month_end2 >= @month_end3,                   
       @mflag3 := @month_end3 >= @sdate90;
-- 货架信息
DROP TEMPORARY TABLE IF EXISTS fe_dm.shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.shelf_tmp (PRIMARY KEY (shelf_id))
SELECT a.business_name,
       a.shelf_id,
       a.activate_time,
       b.grade
FROM fe_dwd.`dwd_shelf_base_day_all` a
LEFT JOIN fe_dm.dm_pub_shelf_grade b ON a.shelf_id = b.shelf_id AND b.month_id = @y_m1 -- 取上月货架等级  d_op_shelf_grade
WHERE a.shelf_type IN (1,2,3,6) -- 剔除虚拟货架、冰柜、自贩机、前置仓
AND a.shelf_status = 2
AND a.revoke_status = 1;
-- 地区激活货架数
DROP TEMPORARY TABLE IF EXISTS fe_dm.business_shelf_tmp;
CREATE TEMPORARY TABLE fe_dm.business_shelf_tmp AS
SELECT business_name,
       COUNT(shelf_id)active_shelf, -- 激活货架数
       COUNT(CASE WHEN grade IN ('甲','乙') THEN shelf_id END)good_shelf -- 激活甲乙货架数
FROM fe_dm.shelf_tmp
GROUP BY business_name;
-- 商品覆盖货架情况
DROP TEMPORARY TABLE IF EXISTS fe_dm.business_product_tmp;
CREATE TEMPORARY TABLE fe_dm.business_product_tmp (PRIMARY KEY (business_name,product_id))
SELECT s.business_name,
       d.product_id,
       COUNT(CASE WHEN stock_quantity > 0 THEN d.shelf_id END)sto_shelf,-- 有库存货架数
       COUNT(CASE WHEN shelf_fill_flag = 1 THEN d.shelf_id END)fill_shelf,-- 可补货货架数
       COUNT(CASE WHEN stock_quantity > 0 OR shelf_fill_flag = 1 THEN d.shelf_id END)normal_shelf,-- 有库存或可补货货架数
       COUNT(CASE WHEN grade IN ('甲','乙') AND stock_quantity > 0 THEN d.shelf_id END)sto_good_shelf,-- 有库存甲乙货架数
       COUNT(CASE WHEN grade IN ('甲','乙') AND shelf_fill_flag = 1 THEN d.shelf_id END)fill_good_shelf,-- 可补货甲乙货架数
       COUNT(CASE WHEN grade IN ('甲','乙') AND (stock_quantity > 0 OR shelf_fill_flag = 1) THEN d.shelf_id END)normal_good_shelf,-- 有库存或可补货甲乙货架数
       AVG( DATEDIFF(CURRENT_DATE,first_fill_time))avg_fill_day -- 架均上架天数
FROM fe_dwd.dwd_shelf_product_day_all d
JOIN fe_dm.shelf_tmp s ON d.shelf_id = s.shelf_id
GROUP BY s.business_name,d.product_id;
-- 货架商品近90天有库存天数
SELECT 
    CONCAT(
      "SELECT t.shelf_id,t.product_id,0",
      GROUP_CONCAT(
        CONCAT("+(t.t", DAY(t.sdate), ">0)") SEPARATOR ' '
      ),
      "sto_days FROM fe_dm.dm_op_sp_sal_sto_detail t  
      WHERE t.month_id = @y_m and (0 ",
      GROUP_CONCAT(
        CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
      ),
      ")"
    ) INTO @sql_str
FROM fe_dwd.dwd_pub_work_day t
WHERE t.sdate BETWEEN GREATEST(@sdate90, @month_start)
AND @week_end;
SELECT 
    CONCAT(
      @sql_str,
      IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0",
          GROUP_CONCAT(
            CONCAT("+(t.t", DAY(t.sdate), ">0)") SEPARATOR ' '
          ),
          "sto_days FROM fe_dm.dm_op_sp_sal_sto_detail t 
          WHERE t.month_id = @y_m1 and @mflag1 = 1 and(0 ",
          GROUP_CONCAT(
            CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
          ),
          ")"
        ),
        ""
      )
    ) INTO @sql_str
 FROM fe_dwd.dwd_pub_work_day t
 WHERE t.sdate BETWEEN GREATEST(@sdate90, @month_start1)
 AND @month_end1
 AND @mflag1 = 1;
 
 SELECT 
    CONCAT(
      @sql_str,
      IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0",
          GROUP_CONCAT(
            CONCAT("+(t.t", DAY(t.sdate), ">0)") SEPARATOR ' '
          ),
          "sto_days FROM fe_dm.dm_op_sp_sal_sto_detail t 
          WHERE t.month_id = @y_m2 and @mflag2 = 1 and(0 ",
          GROUP_CONCAT(
            CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
          ),
          ")"
        ),
        ""
      )
    ) INTO @sql_str
FROM fe_dwd.dwd_pub_work_day t
WHERE t.sdate BETWEEN GREATEST(@sdate90, @month_start2)
AND @month_end2
AND @mflag2 = 1; 
SELECT 
 CONCAT(
      @sql_str,
      IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0",
          GROUP_CONCAT(
            CONCAT("+(t.t", DAY(t.sdate), ">0)") SEPARATOR ' '
          ),
          "sto_days FROM fe_dm.dm_op_sp_sal_sto_detail t
          WHERE t.month_id = @y_m3 and @mflag3 = 1 and(0 ",
          GROUP_CONCAT(
            CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
          ),
          ")"
        ),
        ""
      )
    ) INTO @sql_str
FROM fe_dwd.dwd_pub_work_day t
WHERE t.sdate BETWEEN GREATEST(@sdate90, @month_start3)
AND @month_end3
AND @mflag3 = 1;
SELECT @sql_str := CONCAT(
  "CREATE TEMPORARY TABLE fe_dm.stock_tmp (primary key(shelf_id,product_id)) 
   select s.business_name,
          t.shelf_id,
          t.product_id,
          sum(t.sto_days)sto90 
   from (",
   @sql_str,
   ")t 
   join fe_dm.shelf_tmp s on t.shelf_id = s.shelf_id
   group by t.shelf_id,t.product_id;"
  );
PREPARE sql_exe FROM @sql_str;
DROP TEMPORARY TABLE IF EXISTS fe_dm.stock_tmp;
EXECUTE sql_exe;
-- 近90天有过库存的货架数、近90天有库存货架天数之和
DROP TEMPORARY TABLE IF EXISTS fe_dm.area_product_stock_tmp;
CREATE TEMPORARY TABLE fe_dm.area_product_stock_tmp (PRIMARY KEY(business_name,product_id))
SELECT business_name,
       product_id,
       COUNT(CASE WHEN sto90 > 0 THEN shelf_id END)90sto_shelf,
       SUM(sto90)90sto_days
FROM fe_dm.stock_tmp
GROUP BY business_name,product_id;
  
delete from fe_dm.dm_op_area_product_shelf_cover_week where week_end=@week_end;
INSERT INTO fe_dm.dm_op_area_product_shelf_cover_week
(
week_end,
business_area,
product_id,
active_shelf,
good_shelf,
sto_shelf,
fill_shelf,
normal_shelf,
sto_good_shelf,
fill_good_shelf,
normal_good_shelf,
90sto_shelf,
90sto_days,
avg_fill_day,
inday,
be_normal_day
)
SELECT @week_end week_end,
       a.business_area,
       a.product_id,
       b.active_shelf,
       b.good_shelf,
       c.sto_shelf,
       c.fill_shelf,
       c.normal_shelf,
       c.sto_good_shelf,
       c.fill_good_shelf,
       c.normal_good_shelf,
       d.90sto_shelf,
       d.90sto_days,
       c.avg_fill_day,
       DATEDIFF(CURRENT_DATE,DATE(INDATE_NP)) inday,
       DATEDIFF(CURRENT_DATE,DATE(be_normal_time)) be_normal_day
FROM fe_dwd.dwd_pub_product_dim_sserp a  -- zs_product_dim_sserp
LEFT JOIN fe_dm.business_shelf_tmp b ON a.business_area = b.business_name
LEFT JOIN fe_dm.business_product_tmp c ON a.business_area = c.business_name AND a.product_id = c.product_id
LEFT JOIN fe_dm.area_product_stock_tmp d ON a.business_area = d.business_name AND a.product_id = d.product_id;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_area_product_shelf_cover_week',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_product_shelf_cover_week','dm_op_area_product_shelf_cover_week','朱星华');
  COMMIT;	
END