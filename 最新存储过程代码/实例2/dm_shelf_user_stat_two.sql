CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_shelf_user_stat_two`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  #插入订单事实表
  DELETE
  FROM
    fe_dm.dm_shelf_user_stat
  WHERE order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) AND order_date < CURRENT_DATE;
  INSERT INTO dm_shelf_user_stat
  SELECT
  t.ORDER_ID,
  t.SHELF_ID,
  t.USER_ID,
  DATE(t.ORDER_DATE) AS ORDER_DATE,
  DATE_FORMAT(t.ORDER_DATE, '%H') AS ORDER_HOUR,
  WEEKOFYEAR(t.ORDER_DATE) AS WEEK_DAY,
  t.PRODUCT_TOTAL_AMOUNT AS AMOUNT,
  t.PRODUCT_TOTAL_AMOUNT + t.COUPON_AMOUNT + t.COMMIS_TOTAL_AMOUNT + t.DISCOUNT_AMOUNT + t.INTEGRAL_DISCOUNT AS GMV,
  t.COUPON_AMOUNT,
  b.firstOrderDay,
  IF(
    t.ORDER_DATE > b.firstOrderDay,
    1,
    0
  ) AS C_TYPE
FROM
  fe_dwd.`dwd_order_item_refund_day` t
  JOIN fe_dm.dm_user_stat b
    ON b.USER_ID = t.USER_ID
WHERE t.order_status = 2
  AND t.order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  AND t.order_date < CURRENT_DATE
  GROUP BY t.order_id;
   #更新货架数量
  DELETE
  FROM
    fe_dm.dm_shelf_stat
  WHERE ACTIVATE_DATE = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
  #插入货架数据
  INSERT INTO fe_dm.dm_shelf_stat (ACTIVATE_DATE, shelf_num)
  SELECT
    DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS statistic_date,
    COUNT(DISTINCT m.SHELF_ID) AS shelf_num
  FROM
    fe_dwd.`dwd_shelf_base_day_all` m
  WHERE m.SHELF_STATUS = 2
    AND m.SHELF_CODE <> ''
    AND m.MANAGER_NAME NOT LIKE '%作废%'
    AND LEFT(m.SHELF_CODE, 1) != 'Z';
		
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_shelf_user_stat_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_user_stat','dm_shelf_user_stat_two','蔡松林');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_shelf_stat','dm_shelf_user_stat_two','蔡松林');
END