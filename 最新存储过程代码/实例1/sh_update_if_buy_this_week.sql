CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_update_if_buy_this_week`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  UPDATE
    feods.zs_shelf_member_flag AS b
    LEFT JOIN
      (SELECT DISTINCT
        user_id
      FROM
        fe.sf_order
      WHERE WEEKOFYEAR(order_date) = WEEKOFYEAR(CURRENT_DATE())) AS a
      ON a.USER_ID = b.USER_ID SET b.if_buy_this_week = 1
  WHERE a.user_id IS NOT NULL;
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_update_if_buy_this_week',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
  COMMIT;
END