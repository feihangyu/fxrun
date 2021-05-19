CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_product_stock_temp`(IN p_startdate DATE, IN p_enddate DATE)
BEGIN
SET @run_date:= CURRENT_DATE();SET @user := CURRENT_USER();SET @timestamp := CURRENT_TIMESTAMP();
/* 该存储过程用于取数货架商品历史库存数据*/
TRUNCATE TABLE feods.d_ma_shelf_product_stock_temp;
SET @curdate0=p_startdate;
WHILE @curdate0<=p_enddate DO
    #建临时表取当月数据
    DROP TEMPORARY TABLE IF EXISTS test.temp_shelf_product_stock;
    CREATE TEMPORARY TABLE test.temp_shelf_product_stock AS
    SELECT a1.shelf_id, a1.product_id
         , d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15, d16, d17, d18, d19, d20, d21, d22, d23, d24, d25, d26, d27, d28, d29, d30, d31
    FROM feods.d_op_sp_stock_detail a1
    join fe_dwd.dwd_shelf_base_day_all a2 on a2.shelf_id=a1.shelf_id and a2.shelf_type<>9
    WHERE month_id=DATE_FORMAT(@curdate0,'%Y-%m') ;
    # 循环取每天库存
    SET @date= @curdate0; #当前取数日期
    SET @num=DAY(@date); #当前字段
    WHILE @date<=LAST_DAY(@curdate0) AND @date<=p_enddate DO
        SET @column=CONCAT('d',@num) ; #取数列
        SET @str=CONCAT(
            'insert into feods.d_ma_shelf_product_stock_temp (sdate, shelf_id, product_id, stock) '
            ,'select \'',@date,'\',shelf_id,product_id,',@column,' from test.temp_shelf_product_stock'
            ,' where ',@column,'>0'
                        );

        PREPARE str_exe FROM @str;
        EXECUTE str_exe;
        SET @date=ADDDATE(@date,1) ;
        SET @num=@num+1;
    END WHILE;
    SET @curdate0=ADDDATE(LAST_DAY(@curdate0),1) ;
END WHILE
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'prc_d_ma_shelf_product_stock_temp',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('纪伟铨@',@user,@timestamp)
);
END