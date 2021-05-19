CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_shelf_product_day_east_his_while_delete`(in in_sdate date)
BEGIN
set @sdate := in_sdate;
SELECT MIN(pid) into @start_pid FROM fe_dwd.dwd_shelf_product_day_east_his  WHERE sdate=@sdate;
SELECT MAX(pid) into @end_pid FROM fe_dwd.dwd_shelf_product_day_east_his  WHERE sdate=@sdate;
select @start_pid as start_pid,@end_pid as end_pid;
-- 用于手动分批循环删除100多万的数据，降低对开发库的影响
WHILE @start_pid <= @end_pid 
DO
	delete FROM fe_dwd.dwd_shelf_product_day_east_his WHERE pid between @start_pid and @start_pid+100000 and sdate=@sdate;
	-- select @start_pid as start_pid,@start_pid+100000 as end_pid;
	SELECT SLEEP(1);
 COMMIT;
-- 给日期+1天
set @start_pid = @start_pid+100000;
 
 
END WHILE;
 
 -- 循环结束
 
END