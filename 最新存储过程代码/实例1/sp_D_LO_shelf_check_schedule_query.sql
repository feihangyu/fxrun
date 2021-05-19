CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_shelf_check_schedule_query`()
begin
  -- =============================================
-- Author:	物流
-- Create date: 2019/09/24
-- Modify date: 
-- Description:	
-- 	BI报表-月度盘点进度查询(即日起每天0时开始，,每1个小时跑一次)
-- 
-- =============================================
   SET @run_date := date_format(now(), '%Y-%m-%d %H:%i:%s');
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  IF day(DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR))) >= 15
  then -- 货架月度盘点进度的更新
  delete
  from
    feods.`D_LO_shelf_check_schedule_query`
  where stat_date = DATE_FORMAT(
      DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
    );
insert into feods.`D_LO_shelf_check_schedule_query` (
stat_date,
business_area,
city_name,
shelf_id,
shelf_code,
shelf_name,
shelf_type,
min_operate_time,
max_operate_time,
if_normal_operate,
if_last_operate,
auto_nocheck_reason,
manager_id,
real_name,
SF_CODE,
BRANCH_CODE,
BRANCH_NAME,
manager_type
  )
-- 普通盘点、撤架盘点
SELECT
    DATE_FORMAT(
      DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
    ) AS '统计月份',
    t3.business_name AS '地区',
    t3.CITY_NAME AS '城市',
    t1.shelf_id AS '货架ID',
    t1.shelf_code AS '货架编码',
    t1.shelf_name AS '货架名称',
  CASE
    WHEN t1.SHELF_TYPE = 1
    THEN '四层标准货架'
    WHEN t1.SHELF_TYPE = 2
    THEN '冰箱'
    WHEN t1.SHELF_TYPE = 3
    THEN '五层防鼠货架'
    WHEN t1.SHELF_TYPE = 4
    THEN '虚拟货架'
    WHEN t1.SHELF_TYPE = 5
    THEN '冰柜'
    WHEN t1.SHELF_TYPE = 6
    THEN '智能货柜'
    WHEN t1.SHELF_TYPE = 7
    THEN '自动贩卖机'
    WHEN t1.SHELF_TYPE = 8
    THEN '校园货架'
    WHEN t1.SHELF_TYPE = 9
    THEN '前置仓'
  END AS '货架类型',
    t2.min_OPERATE_TIME AS '第一条盘点时间',
    t2.max_OPERATE_TIME AS '最后盘点时间',
    CASE
      WHEN t2.shelf_id
      THEN '已盘点'
      ELSE '未盘点'
    END AS '普通盘点',
    CASE
      WHEN (DAY(t2.max_OPERATE_TIME) BETWEEN 20 AND 31) AND t2.shelf_id IS NOT NULL
      THEN '已盘点'
      ELSE '未盘点'
    END AS '月末盘点',
    '无' AS '自贩机未盘点原因',
    t1.manager_id AS '店主ID',
    t1.real_name AS '店主姓名',
    t1.SF_CODE AS '店主工号',
    t1.BRANCH_CODE '分部编码',
    t1.BRANCH_NAME '分部名称',
    CASE
	  WHEN t1.second_user_type = 1 
	  THEN '全职店主'
	  ELSE '兼职店主'
	  END AS '店主类型'
  FROM
    (
    SELECT
      a.CITY,
      a.shelf_id,
      a.shelf_code,
      a.shelf_name,
      a.manager_id,
      a.SHELF_TYPE,
      b.real_name,
      b.BRANCH_CODE,
      b.BRANCH_NAME,
      b.SF_CODE,
      b.second_user_type
    FROM
      fe.sf_shelf a,
      fe.pub_shelf_manager b
    WHERE a.manager_id = b.manager_id
      AND a.data_flag = 1 AND b.data_flag = 1
      AND a.shelf_status = 2
      AND a.revoke_status NOT IN (6,7,9)
      AND a.SHELF_CODE <> ''
      AND a.SHELF_TYPE IN (1,2,3,5,6,8)
      AND a.shelf_id NOT IN (83290,67236,73560,73561,81538,81539,81540,85516,87318,87319,87726,87728,4183,4622,32116,32117,65748,87321,79057,79069,79643,84365,84631,89090,90026,90285,90573,91464,91509,91514,91665,92214,92481,92492,92495,92502,92530)) t1
    LEFT JOIN
      (SELECT
        b.shelf_id,
        MAX(b.OPERATE_TIME) AS max_OPERATE_TIME,
        MIN(b.OPERATE_TIME) AS min_OPERATE_TIME
      FROM
        fe.sf_shelf_check b
      WHERE b.data_flag = 1
        AND b.check_type IN (1,3)
        AND b.operate_time >= DATE_ADD(
          DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), INTERVAL - DAY(DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR))) + 1 DAY
        )
        AND b.operate_time < DATE_ADD(
          LAST_DAY(DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR))), INTERVAL 1 DAY
        )
      GROUP BY b.shelf_id) t2  
      ON t1.shelf_id = t2.shelf_id
    LEFT JOIN feods.`fjr_city_business` t3
    ON t1.CITY = t3.CITY
UNION ALL
-- 自贩机盘点(含货道、副柜盘点)
  SELECT
    DATE_FORMAT(
      DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
    ) AS '统计月份',
    t3.business_name AS '地区',
    t3.CITY_NAME AS '城市',
    t1.shelf_id AS '货架ID',
    t1.shelf_code AS '货架编码',
    t1.shelf_name AS '货架名称',
  CASE
    WHEN t1.SHELF_TYPE = 1
    THEN '四层标准货架'
    WHEN t1.SHELF_TYPE = 2
    THEN '冰箱'
    WHEN t1.SHELF_TYPE = 3
    THEN '五层防鼠货架'
    WHEN t1.SHELF_TYPE = 4
    THEN '虚拟货架'
    WHEN t1.SHELF_TYPE = 5
    THEN '冰柜'
    WHEN t1.SHELF_TYPE = 6
    THEN '智能货柜'
    WHEN t1.SHELF_TYPE = 7
    THEN '自动贩卖机'
    WHEN t1.SHELF_TYPE = 8
    THEN '校园货架'
    WHEN t1.SHELF_TYPE = 9
    THEN '前置仓'
  END AS '货架类型',
    t2.min_OPERATE_TIME AS '第一条盘点时间',
    t2.max_OPERATE_TIME AS '最后盘点时间',
    CASE
      WHEN t2.shelf_id
      THEN '已盘点'
      ELSE '未盘点'
    END AS '普通盘点',
    CASE
      WHEN (DAY(t2.hd_max_OPERATE_TIME) BETWEEN 20 AND 31) AND (DAY(t2.fg_max_OPERATE_TIME) BETWEEN 20 AND 31)
      THEN '已盘点'
      ELSE '未盘点'
    END AS '月末盘点',
    CASE
      WHEN (DAY(t2.hd_max_OPERATE_TIME) BETWEEN 20 AND 31) AND (DAY(t2.fg_max_OPERATE_TIME) BETWEEN 20 AND 31) THEN '已盘点'
      WHEN (DAY(t2.hd_max_OPERATE_TIME) BETWEEN 20 AND 31) AND (IFNULL(DAY(t2.fg_max_OPERATE_TIME),0) < 20 ) THEN '副柜未盘点'
      WHEN (IFNULL(DAY(t2.hd_max_OPERATE_TIME),0) < 20) AND (DAY(t2.fg_max_OPERATE_TIME) BETWEEN 20 AND 31) THEN '货道未盘点'
      ELSE '货道副柜均未盘点'
    END '自贩机未盘点原因',
    t1.manager_id AS '店主ID',
    t1.real_name AS '店主姓名',
    t1.SF_CODE AS '店主工号',
    t1.BRANCH_CODE '分部编码',
    t1.BRANCH_NAME '分部名称',
    CASE
	  WHEN t1.second_user_type = 1 
	  THEN '全职店主'
	  ELSE '兼职店主'
	  END AS '店主类型'
  FROM
    (
    SELECT
      a.CITY,
      a.shelf_id,
      a.shelf_code,
      a.shelf_name,
      a.manager_id,
      a.SHELF_TYPE,
      b.real_name,
      b.BRANCH_CODE,
      b.BRANCH_NAME,
      b.SF_CODE,
      b.second_user_type
    FROM
      fe.sf_shelf a,
      fe.pub_shelf_manager b
    WHERE a.manager_id = b.manager_id
      AND a.data_flag = 1 AND b.data_flag = 1
      AND a.shelf_status = 2
      AND a.revoke_status NOT IN (6,7,9)
      AND a.SHELF_CODE <> ''
      AND a.SHELF_TYPE = 7
      AND a.shelf_id NOT IN (83290,67236,73560,73561,81538,81539,81540,85516,87318,87319,87726,87728,4183,4622,32116,32117,65748,87321,79057,79069,79643,84365,84631,89090,90026,90285,90573,91464,91509,91514,91665,92214,92481,92492,92495,92502,92530)) t1
    LEFT JOIN
      (SELECT
        b.shelf_id,
        MAX(CASE WHEN b.check_type = 6 THEN b.OPERATE_TIME END) AS hd_max_OPERATE_TIME,
        MAX(CASE WHEN b.check_type = 7 THEN b.OPERATE_TIME END) AS fg_max_OPERATE_TIME,
        MAX(b.OPERATE_TIME) AS max_OPERATE_TIME,
        MIN(b.OPERATE_TIME) AS min_OPERATE_TIME
      FROM
        fe.sf_shelf_check b
      WHERE b.data_flag = 1
        AND b.check_type IN (1,3,6,7)
        AND b.operate_time >= DATE_ADD(
          DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), INTERVAL - DAY(DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR))) + 1 DAY
        )
        AND b.operate_time < DATE_ADD(
          LAST_DAY(DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR))), INTERVAL 1 DAY
        )
      GROUP BY b.shelf_id) t2  
      ON t1.shelf_id = t2.shelf_id
    LEFT JOIN feods.`fjr_city_business` t3
    ON t1.CITY = t3.CITY;
   -- 月末盘点审核标签
   SET @date_top := str_to_date(
    concat(
      DATE_FORMAT(
        DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
      ), '15'
    ), '%Y%m%d'
  );
  SET @date_end := DATE_ADD(
    LAST_DAY(DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR))), INTERVAL 1 DAY
  );
  delete
  from
    feods.D_LO_shelf_check_for_month_label
  where stat_date = DATE_FORMAT(
      DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
    );
  DROP TEMPORARY TABLE IF EXISTS feods.auto_shelf_check_temp;
  CREATE TEMPORARY TABLE feods.auto_shelf_check_temp AS
  SELECT
    MIN(
      IF(
        b.`check_type` = 6, b.`CHECK_ID`, NULL
      )
    ) AS check_id
  FROM
    fe.sf_shelf_check b
  WHERE b.data_flag = 1
    AND b.check_type IN (6, 7)
    AND b.operate_time >= @date_top
    AND b.operate_time < @date_end
  GROUP BY b.shelf_id
  HAVING MIN(
      IF(
        b.`check_type` = 6, b.`CHECK_ID`, NULL
      )
    ) IS NOT NULL
    AND MIN(
      IF(
        b.`check_type` = 7, b.`CHECK_ID`, NULL
      )
    ) IS NOT NULL
  UNION
  SELECT
    MIN(
      IF(
        b.`check_type` = 7, b.`CHECK_ID`, NULL
      )
    ) AS check_id
  FROM
    fe.sf_shelf_check b
  WHERE b.data_flag = 1
    AND b.check_type IN (6, 7)
    AND b.operate_time >= @date_top
    AND b.operate_time < @date_end
  GROUP BY b.shelf_id
  HAVING MIN(
      IF(
        b.`check_type` = 6, b.`CHECK_ID`, NULL
      )
    ) IS NOT NULL
    AND MIN(
      IF(
        b.`check_type` = 7, b.`CHECK_ID`, NULL
      )
    ) IS NOT NULL;
  insert into feods.D_LO_shelf_check_for_month_label (
    stat_date, check_id, operate_time, business_area, shelf_id, shelf_type, operator_ID, operator_name, SF_CODE, month_end_operate
  )
  SELECT
    DATE_FORMAT(
      DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
    ) as stat_date, k.check_id, k.`OPERATE_TIME`, c.`business_name`, k.`SHELF_ID`, s.`SHELF_TYPE`, k.`OPERATOR_ID`, e.real_name, e.`SF_CODE`, '月末盘点审核' as month_end_operate
  FROM
    fe.`sf_shelf_check` k, fe.`sf_shelf` s, fe.`pub_shelf_manager` e, feods.`fjr_city_business` c
  WHERE k.shelf_id = s.shelf_id
    AND k.operator_id = e.manager_id
    AND s.city = c.city
    AND k.operate_time >= @date_top
    AND k.operate_time < @date_end
    AND s.shelf_type IN (1, 2, 3, 5, 6, 7)
    AND k.data_flag = 1
    AND s.data_flag = 1
    AND e.data_flag = 1
    AND k.check_id IN
    (SELECT
      MIN(b.`CHECK_ID`) AS first_check_id
    FROM
      fe.sf_shelf_check b
    WHERE b.data_flag = 1
      AND b.check_type IN (1, 3)
      AND b.operate_time >= @date_top
      AND b.operate_time < @date_end
    GROUP BY b.shelf_id)
    union
    SELECT
      DATE_FORMAT(
        DATE(DATE_SUB(NOW(), INTERVAL 1 HOUR)), '%Y%m'
      ) AS stat_date, k.check_id, k.`OPERATE_TIME`, c.`business_name`, k.`SHELF_ID`, s.`SHELF_TYPE`, k.`OPERATOR_ID`, e.real_name, e.`SF_CODE`, '月末盘点审核' AS month_end_operate
    FROM
      fe.`sf_shelf_check` k, fe.`sf_shelf` s, fe.`pub_shelf_manager` e, feods.`fjr_city_business` c
    WHERE k.shelf_id = s.shelf_id
      AND k.operator_id = e.manager_id
      AND s.city = c.city
      AND k.operate_time >= @date_top
      AND k.operate_time < @date_end
      AND s.shelf_type IN (1, 2, 3, 5, 6, 7)
      AND k.data_flag = 1
      AND s.data_flag = 1
      AND e.data_flag = 1
      AND k.check_id IN
      (SELECT
        t.check_id
      FROM
        feods.auto_shelf_check_temp t);
        
  --   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_D_LO_shelf_check_schedule_query', @run_date, CONCAT('caisonglin@', @user, @timestamp)
  );
  
  COMMIT;
  end if;
end