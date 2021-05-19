CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_add_shelf_damaged`()
    SQL SECURITY INVOKER
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.pj_zs_add_shelf_damaged
  WHERE pdate = DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%Y%m%d'
    )
    and notice is null;
  INSERT INTO feods.pj_zs_add_shelf_damaged (
    pdate,
    city_name,
    exploit_type,
    shelf_id,
    shelf_name,
    gmv,
    user_qty,
    shelf_status,
    daosun_value,
    daosun_lv,
    huosun,
    order_cnt,
    ma_gmv
  )
  SELECT
    t1.数据日期,
    t1.city_name,
    t1.exploit_type,
    t1.shelf_id,
    t1.shelf_name,
    t1.gmv,
    t1.user_qty,
    t1.shelf_status,
    t1.daosun_value,
    t1.daosun_lv,
    t1.huosun,
    t1.订单数量,
    t1.gmv / DATE_FORMAT(
      DATE_SUB(CURDATE(), INTERVAL 1 DAY),
      '%d'
    )
  FROM
    (SELECT
      DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        '%Y%m%d'
      ) AS "数据日期",
      a.city_name,
      a.shelf_id,
      b.shelf_name,
      a.gmv,
      a.user_qty,
      a.shelf_status,
      b.exploit_type,
      a.huosun,
      IFNULL(a.huosun, 0) + IFNULL(a.bk_money, 0) - IFNULL(a.total_error_value, 0) AS daosun_value,
      (
        IFNULL(a.huosun, 0) + IFNULL(a.bk_money, 0) - IFNULL(a.total_error_value, 0)
      ) / (
        ABS(
          IFNULL(a.huosun, 0) + IFNULL(a.bk_money, 0) - IFNULL(a.total_error_value, 0)
        ) + ABS(IFNULL(a.sale_value, 0))
      ) AS daosun_lv,
      COUNT(c.ORDER_ID) AS "订单数量"
    FROM
      feods.pj_zs_goods_damaged a
      JOIN fe.sf_shelf b
        ON a.SHELF_ID = b.SHELF_ID
      JOIN fe.sf_order c
        ON a.shelf_id = c.shelf_id
    WHERE a.smonth = DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        '%Y%m'
      )
      AND DATE_FORMAT(b.activate_time, '%Y%m%d') BETWEEN DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 30 DAY),
        '%Y%m%d'
      )
      AND DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        '%Y%m%d'
      )
    GROUP BY DATE_FORMAT(
        DATE_SUB(CURDATE(), INTERVAL 1 DAY),
        '%Y%m%d'
      ),
      a.city_name,
      a.shelf_id,
      b.shelf_name,
      a.gmv,
      a.user_qty,
      a.shelf_status,
      b.exploit_type,
      a.huosun,
      IFNULL(a.huosun, 0) + IFNULL(a.bk_money, 0) - IFNULL(a.total_error_value, 0),
      (
        IFNULL(a.huosun, 0) + IFNULL(a.bk_money, 0) - IFNULL(a.total_error_value, 0)
      ) / (
        ABS(
          IFNULL(a.huosun, 0) + IFNULL(a.bk_money, 0) - IFNULL(a.total_error_value, 0)
        ) + ABS(IFNULL(a.sale_value, 0))
      )) t1;
      
      
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sp_add_shelf_damaged',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('吴婷@', @user, @timestamp));
  COMMIT;
  
END