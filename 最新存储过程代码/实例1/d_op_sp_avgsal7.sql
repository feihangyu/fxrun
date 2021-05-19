CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_op_sp_avgsal7`()
BEGIN
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
SET    @sdate := SUBDATE(CURRENT_DATE(),INTERVAL 1 DAY),  
       @y_m := DATE_FORMAT(@sdate, '%Y-%m'),                    
       @d := DAY(@sdate),                                       
       @month_start := SUBDATE(@sdate, @d - 1),                 
       @month_start1 := SUBDATE(@month_start, INTERVAL 1 MONTH),
       @y_m1 := DATE_FORMAT(@month_start1, '%Y-%m'),            
       @sdate7 := SUBDATE(@sdate, 6),                         
       @month_end1 := LAST_DAY(@month_start1),                  
       @mflag1 := @month_end1 >= @sdate7;                      
  SELECT
    @sql_str1 := CONCAT(
      "SELECT t.shelf_id,t.product_id,0",
      GROUP_CONCAT(
        CONCAT("+t.s", DAY(t.sdate)) SEPARATOR ' '
      ),
      " qty_sal7,0",
      GROUP_CONCAT(
        CONCAT("+(t.t", DAY(t.sdate), ">0)") SEPARATOR ' '
      ),
      "days_sal_sto7 FROM feods.d_op_sp_sal_sto_detail t WHERE t.month_id = @y_m and(0 ",
      GROUP_CONCAT(
        CONCAT(" OR t.t", DAY(t.sdate), "> 0 ") SEPARATOR ' '
      ),
      ")"
    )
  FROM
    feods.fjr_work_days t
  WHERE t.sdate BETWEEN GREATEST(@sdate7, @month_start)
    AND @sdate;
	
	
   SELECT
    @sql_str2 := CONCAT(
      @sql_str1,
      IFNULL(
        CONCAT(
          " union all SELECT t.shelf_id,t.product_id,0",
          GROUP_CONCAT(
            CONCAT("+t.s", DAY(t.sdate)) SEPARATOR ' '
          ),
          " qty_sal7,0",
          GROUP_CONCAT(
            CONCAT("+(t.t", DAY(t.sdate), "> 0)") SEPARATOR ' '
          ),
          "days_sal_sto7 FROM feods.d_op_sp_sal_sto_detail t WHERE t.month_id = @y_m1 and @mflag1 = 1 and(0 ",
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
  WHERE t.sdate BETWEEN GREATEST(@sdate7, @month_start1)
    AND @month_end1
    AND @mflag1 = 1;
    
-- 未对接系统澳柯玛销售(0510增加)
DROP TEMPORARY TABLE IF EXISTS feods.sale_tmp;
CREATE TEMPORARY TABLE feods.sale_tmp (PRIMARY KEY (shelf_id,product_id))
SELECT shelf_id,
       product_id,
       SUM(amount)amount
FROM fe_dwd.dwd_op_out_of_system_order_yht
WHERE pay_date >= @sdate7
AND pay_date < ADDDATE(@sdate,1)
AND refund_status = '无'
GROUP BY shelf_id,product_id;
    
	
SELECT @sql_str3 := CONCAT(
    "insert into feods.d_op_sp_avgsal7
    (
    shelf_id,
    product_id,
    qty_sal7,
    days_sal_sto7
    ) 
    select t.shelf_id,
           t.product_id,
           sum(t.qty_sal7)+ ifnull(s.amount,0) qty_sal7, -- 0510修改
           sum(t.days_sal_sto7)days_sal_sto7 
    from (",
    @sql_str2,
    ")t 
    left join feods.sale_tmp s on t.shelf_id = s.shelf_id and t.product_id = s.product_id -- 0510修改
    group by t.shelf_id,t.product_id;"
  );
  
PREPARE sql_exe FROM @sql_str3;
TRUNCATE feods.d_op_sp_avgsal7;
EXECUTE sql_exe;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'd_op_sp_avgsal7',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('唐进(朱星华)@', @user, @timestamp)
  );
COMMIT;
END