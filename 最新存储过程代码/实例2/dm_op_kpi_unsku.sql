CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi_unsku`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DELETE FROM fe_dm.dm_op_kpi_unsku WHERE sdate = @sdate;
  INSERT INTO fe_dm.dm_op_kpi_unsku (
    sdate, region, business_area, shelf_id, shelf_type, skus, add_user
  )
  SELECT
    @sdate, s.region_name, s.business_name, d.SHELF_ID, s.shelf_type, COUNT(*) ct, @add_user
  FROM
    fe_dwd.`dwd_shelf_product_day_all` d
    JOIN fe_dwd.`dwd_shelf_base_day_all` s
      ON d.SHELF_ID = s.SHELF_ID
      AND s.SHELF_STATUS = 2
      AND s.REVOKE_STATUS = 1
      AND s.WHETHER_CLOSE = 2
      AND s.shelf_type IN (1, 2, 3, 5)
  WHERE d.STOCK_QUANTITY > 0
  GROUP BY d.SHELF_ID
  HAVING ct < (
      CASE
        WHEN s.shelf_type IN (1, 3)
        THEN 25
        ELSE 10
      END
    );
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_kpi_unsku',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi_unsku','dm_op_kpi_unsku','宋英南');
END