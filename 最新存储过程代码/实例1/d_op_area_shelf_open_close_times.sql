CREATE DEFINER=`feprocess`@`%` PROCEDURE `d_op_area_shelf_open_close_times`()
BEGIN
SET @sdate=SUBDATE(CURRENT_DATE,INTERVAL 1 DAY),
    @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
   
SET @run_date := CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();   
delete from feods.d_op_area_shelf_open_close_times where month_id=DATE_FORMAT(@sdate,'%Y-%m') ;
#关闭时长、关闭频次按月结存(货架状态为已激活,撤架状态为正常运营)
-- 增加一个最大log_id，原因：部分货架到今天未开启（update_time在11月份）
-- 1号状态为关闭，则关闭时间为当天
DROP TEMPORARY TABLE IF EXISTS feods.shelfs_tmp;
CREATE TEMPORARY TABLE feods.shelfs_tmp  AS
SELECT 0 AS log_id
      ,d.shelf_id
      ,s.shelf_type  # 添加货架类型
      ,@smonth AS update_time
      ,1 AS if_close
FROM feods.pj_area_sale_dashboard d  -- 三个月的历史数据
JOIN fe.sf_shelf s ON d.shelf_id=s.`SHELF_ID`
WHERE d.sdate=@smonth  AND d.WHETHER_CLOSE=1 AND s.SHELF_STATUS=2 AND s.REVOKE_STATUS=1 #添加货架状态为已激活，撤架状态为正在运营
UNION ALL
SELECT 100000000000 AS log_id  -- 现在状态为关闭，则默认开启时间为明天。
      ,shelf_id
      ,shelf_type
      ,ADDDATE(CURRENT_DATE,1) AS update_time
      ,0 AS if_close
FROM fe.sf_shelf WHERE WHETHER_CLOSE=1 AND data_flag=1 
AND SHELF_STATUS=2 AND REVOKE_STATUS=1 #添加撤架状态为正在运营
UNION ALL
SELECT
    t.log_id,
    t.shelf_id,
    s.shelf_type,
    t.update_time,
    t.remark LIKE ' 关%' || t.remark LIKE '系统自动  关%' if_close
  FROM
    fe.sf_shelf_log t 
    JOIN fe.sf_shelf s
    ON t.shelf_id=s.shelf_id
  WHERE t.shelf_change_type = 6
    AND (
      t.remark LIKE ' 关%'
      OR t.remark LIKE '系统自动  关%'
      OR t.remark LIKE ' 开%'
      OR t.remark LIKE '系统自动  开%'
    )
     AND s.SHELF_STATUS=2 AND s.REVOKE_STATUS=1 #添加撤架状态为正在运营
  AND t.update_time>=@smonth AND t.update_time<CURRENT_DATE;
-- 关闭货架
DROP TEMPORARY TABLE IF EXISTS feods.close_tmp;
CREATE TEMPORARY TABLE feods.close_tmp (PRIMARY KEY (shelf_id, order_num)) AS
SELECT
    @order_num := IF(
    @shelf_id = t.shelf_id,
    @order_num + 1,
1
  ) order_num,
  @shelf_id := t.shelf_id shelf_id,
  t.update_time
FROM
  feods.shelfs_tmp t,
  (SELECT
    @order_num := NULL,
    @shelf_id := NULL) xx
WHERE t.if_close = 1
ORDER BY t.shelf_id, t.log_id DESC;   -- 需要确认好升降序
-- 未关闭货架
DROP TEMPORARY TABLE IF EXISTS feods.open_tmp;
CREATE TEMPORARY TABLE feods.open_tmp (PRIMARY KEY (shelf_id, order_num)) AS
SELECT
    @order_num := IF(
    @shelf_id = t.shelf_id,
    @order_num + 1,
1
  ) order_num,
  @shelf_id := t.shelf_id shelf_id,
  t.update_time
FROM
  feods.shelfs_tmp t,
  (SELECT
    @order_num := NULL,
    @shelf_id := NULL) xx
WHERE t.if_close = 0
ORDER BY t.shelf_id, t.log_id DESC;
-- 本月变更的货架数
DROP TEMPORARY TABLE IF EXISTS feods.update_tmp;
CREATE TEMPORARY TABLE feods.update_tmp (PRIMARY KEY (shelf_id)) AS
SELECT
  t.shelf_id,
  COUNT(*) ct
FROM
  fe.sf_shelf_log t
  JOIN fe.sf_shelf s ON t.shelf_id=s.shelf_id
WHERE t.shelf_change_type = 6
  AND (
    t.remark LIKE ' 将%'
    OR t.remark LIKE '系统自动  将%'
  ) AND s.data_flag=1 AND s.shelf_status=2 AND t.update_time>=@smonth AND t.update_time<CURRENT_DATE  -- 2:已激活
GROUP BY t.shelf_id;
CREATE INDEX idx_shelf_id ON feods.update_tmp(shelf_id);
-- 已激活货架的区域信息
DROP TEMPORARY TABLE IF EXISTS feods.business_tmp;
CREATE TEMPORARY TABLE feods.business_tmp (PRIMARY KEY (shelf_id)) AS
SELECT
  s.shelf_id,
  b.business_name
FROM
  fe.sf_shelf s 
  JOIN feods.fjr_city_business b
    ON s.city = b.city
WHERE s.data_flag=1
AND s.SHELF_STATUS=2 AND s.REVOKE_STATUS=1;
CREATE INDEX idx_shelf_id1 ON feods.business_tmp(shelf_id);
INSERT INTO feods.d_op_area_shelf_open_close_times
SELECT DISTINCT
  DATE_FORMAT(@sdate,'%Y-%m') AS month_id,
  b.business_name,
  t.shelf_id,
  t.shelf_type, #添加货架类型
  co.last_close_time,
  co.last_open_time,
  co.close_times,
  co.days,
  CURRENT_TIMESTAMP AS load_time
FROM
  feods.shelfs_tmp t  -- 货架变更信息
  JOIN
    (SELECT
      t.shelf_id,
      MAX(t.close_time) last_close_time,
      MAX(t.open_time) last_open_time,
      MAX(t.order_num) close_times,
      SUM(days) AS days
    FROM
      (SELECT
        t.order_num,
        t.shelf_id,
        t.update_time close_time,
        o.update_time open_time,
        ROUND(TIMESTAMPDIFF(
          SECOND,
          t.update_time,
          o.update_time
        ) / 24 / 3600,4) days
       FROM
        feods.close_tmp t
        JOIN feods.open_tmp o
          ON t.shelf_id = o.shelf_id
          AND t.order_num = o.order_num
	   ) t
    GROUP BY t.shelf_id
	) co
    ON t.shelf_id = co.shelf_id
  LEFT JOIN feods.update_tmp u    -- 本月变更的货架数
    ON t.shelf_id = u.shelf_id
  LEFT JOIN feods.business_tmp b  -- 已激活货架的区域信息
    ON t.shelf_id = b.shelf_id;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'd_op_area_shelf_open_close_times',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('tangjin@', @user, @timestamp)
  );
COMMIT;
END