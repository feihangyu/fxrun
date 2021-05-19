CREATE DEFINER=`feprocess`@`%` PROCEDURE `op_shelf_week_product_stock_detail_tmp`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
set    @in_date := CURRENT_DATE,
       @week_end := SUBDATE(@in_date,DAYOFWEEK(@in_date) - 1),             -- 周最后一天
       @y_m := DATE_FORMAT(@week_end, '%Y-%m'),                            -- 周所在年月   
       @d := DAY(@week_end),                                               -- 周所在天                              
       @month_start := SUBDATE(@week_end, @d - 1),                         -- 周所在月的第一天            
       @month_start1 := SUBDATE(@month_start, INTERVAL 1 MONTH),           -- 上一个月
       @y_m1 := DATE_FORMAT(@month_start1, '%Y-%m'),                       -- 上月所在年月     
       @week_start := SUBDATE(@week_end, 6),                               -- 周开始的第一天                      
       @month_end1 := LAST_DAY(@month_start1),                             -- 上月最后一天
       @mflag1 := @month_end1 >= @week_start;                              --               
set    @last4_month := subdate(CURRENT_DATE,interval 4 month);    -- 4个月前的日期
-- 先删掉 op_shelf_week_product_stock_detail_tmp 4个月前的数据 只保留最近4个月的数据
delete from feods.op_shelf_week_product_stock_detail_tmp WHERE week_end < @last4_month;
-- 清空上周的数据
delete from feods.op_shelf_week_product_stock_detail_tmp WHERE week_end = @week_end;
  SELECT
    @sql_str1 := CONCAT(
      "SELECT t.shelf_id,t.product_id,0",
      GROUP_CONCAT(
        CONCAT("+t.s", DAY(t.sdate)) SEPARATOR ' '
      ),
      " qty_sal_week,0",
      GROUP_CONCAT(
        CONCAT("+(t.t", DAY(t.sdate), ">0)") SEPARATOR ' '
      ),
      "days_sal_week FROM feods.d_op_sp_sal_sto_detail t WHERE t.month_id = @y_m and(0 ",
      GROUP_CONCAT(
        CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
      ),
      ")"
    )
  FROM
    feods.fjr_work_days t
  WHERE t.sdate BETWEEN GREATEST(@week_start, @month_start)  
    AND @week_end;
    
	
   SELECT
    @sql_str2 := CONCAT(
      @sql_str1,
      IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0",
          GROUP_CONCAT(
            CONCAT("+t.s", DAY(t.sdate)) SEPARATOR ' '
          ),
          " qty_sal_week,0",
          GROUP_CONCAT(
            CONCAT("+(t.t", DAY(t.sdate), "> 0)") SEPARATOR ' '
          ),
          "days_sal_week FROM feods.d_op_sp_sal_sto_detail t WHERE t.month_id = @y_m1 and @mflag1 = 1 and(0 ",
          GROUP_CONCAT(
            CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
          ),
          ")"
        ),
        ""
      )
    )
  FROM
    feods.fjr_work_days t
  WHERE t.sdate BETWEEN GREATEST(@week_start, @month_start1)
    AND @month_end1
    AND @mflag1 = 1;
SELECT @sql_str3 := CONCAT(
    "insert into feods.op_shelf_week_product_stock_detail_tmp
    (
    week_end,
    shelf_id,
    product_id,
    qty_sal_week,
    days_sal_week
    ) 
    select @week_end week_end,
           t.shelf_id,
           t.product_id,
           sum(t.qty_sal_week)qty_sal_week,
           sum(t.days_sal_week)days_sal_week 
    from (",
    @sql_str2,
    ")t group by t.shelf_id,t.product_id;"
  );
PREPARE sql_exe FROM @sql_str3;
EXECUTE sql_exe;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'op_shelf_week_product_stock_detail_tmp',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
COMMIT;
END