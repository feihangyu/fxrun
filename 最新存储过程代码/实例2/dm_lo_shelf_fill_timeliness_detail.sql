CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_lo_shelf_fill_timeliness_detail`()
BEGIN
  -- =============================================
-- Author:	物流组
-- Create date: 
-- Modify date: 
-- Description:	
--    补货监控-次日上架率（区域补货报表的数据模型）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
 SET @date_top:= DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY);
 SET @date_end:= CURRENT_DATE;
 -- 增量插入前先清除操作
 DELETE FROM fe_dm.dm_lo_shelf_fill_timeliness_detail WHERE apply_time >= @date_top
 AND apply_time < @date_end;
 -- 增量插入昨日新增的补货订单并对这部分订单中的已上架订单计算及时性
 INSERT INTO fe_dm.dm_lo_shelf_fill_timeliness_detail(
 supplier_type        
,supplier_code                                
,SUPPLIER_NAME                                
,REGION_AREA                                        
,BUSINESS_AREA                                      
,CITY_NAME                                          
,BRANCH_CODE                                    
,BRANCH_NAME                                    
,ORDER_ID                                   
,SHELF_ID                                       
,SHELF_CODE                                     
,SHELF_TYPE                                     
,shelf_level                                    
,if_is_skewer                                   
,APPLY_TIME                                     
,FILL_TIME                                      
,FILL_TYPE                                      
,ORDER_STATUS                                   
,PRODUCT_TYPE_NUM                               
,PRODUCT_NUM                                    
,TOTAL_PRICE                                                                  
,FILL_AUDIT_STATUS         
,SF_CODE                                          
,REAL_NAME
,manager_type                                
,day_interval                     
,two_days_fill_label                    
,three_days_fill_label
,ERROR_REASON
,actual_amount                                  
,ERROR_NUM   )
SELECT
  t.SUPPLIER_TYPE,
  t.supplier_code,
  t.supplier_name,
  t.REGION_AREA,
  t.BUSINESS_AREA,
  t.CITY_NAME,
  t.BRANCH_CODE,
  t.BRANCH_NAME,
  t.order_id,
  t.shelf_id,
  t.SHELF_CODE,
  t.shelf_type,
  t.shelf_level,
  t.if_is_skewer,
  t.APPLY_TIME,
  t.FILL_TIME,
  t.FILL_TYPE,
  t.ORDER_STATUS,
  t.PRODUCT_TYPE_NUM,
  t.PRODUCT_NUM,
  t.TOTAL_PRICE,
  t.FILL_AUDIT_STATUS,
  t.sf_code,
  t.real_name,
  t.manager_type,
  t.day_diff,
  t.two_day_fill_label,
  t.three_day_fill_label,
  IFNULL(b.error_reason, 0) AS error_reason,
  SUM(
    b.PURCHASE_PRICE * b.ACTUAL_SIGN_NUM
  ) AS real_value,
  SUM(ABS(b.ERROR_NUM)) AS ERROR_NUM
FROM
  (SELECT
    a.*,
    IF(
      (SELECT
        w.`if_work_day`
      FROM
        fe_dwd.`dwd_pub_work_day` w
      WHERE w.`sdate` = DATE(a.APPLY_TIME)) = 0,
      IF(
        SUM(w.if_work_day) <= 1,
        '及时',
        IF(
          ISNULL(a.fill_time),
          NULL,
          '不及时'
        )
      ),
      IF(
        (SELECT
          w.`if_work_day`
        FROM
          fe_dwd.`dwd_pub_work_day` w
        WHERE w.`sdate` = DATE(a.fill_time)) = 0,
        IF(
          SUM(w.if_work_day) <= IF(
            ISNULL(a.task_id),
            1,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              1,
              2
            )
          ),
          '及时',
          IF(
            ISNULL(a.fill_time),
            NULL,
            '不及时'
          )
        ),
        IF(
          SUM(w.if_work_day) <= IF(
            ISNULL(a.task_id),
            2,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              2,
              3
            )
          ),
          '及时',
          IF(
            ISNULL(a.fill_time),
            NULL,
            '不及时'
          )
        )
      )
    ) AS two_day_fill_label,
    -- 次日上架及时标签
     IF(
      (SELECT
        w.`if_work_day`
      FROM
        fe_dwd.`dwd_pub_work_day` w
      WHERE w.`sdate` = DATE(a.APPLY_TIME)) = 0,
      IF(
        SUM(w.if_work_day) <= 2,
        '及时',
        IF(
          ISNULL(a.fill_time),
          NULL,
          '不及时'
        )
      ),
      IF(
        (SELECT
          w.`if_work_day`
        FROM
          fe_dwd.`dwd_pub_work_day` w
        WHERE w.`sdate` = DATE(a.fill_time)) = 0,
        IF(
          SUM(w.if_work_day) <= IF(
            ISNULL(a.task_id),
            2,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              2,
              3
            )
          ),
          '及时',
          IF(
            ISNULL(a.fill_time),
            NULL,
            '不及时'
          )
        ),
        IF(
          SUM(w.if_work_day) <= IF(
            ISNULL(a.task_id),
            3,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              3,
              4
            )
          ),
          '及时',
          IF(
            ISNULL(a.fill_time),
            NULL,
            '不及时'
          )
        )
      )
    ) AS three_day_fill_label -- 三日内上架及时标签
   FROM
    (SELECT
      a.SUPPLIER_TYPE,
      CASE
        WHEN a.supplier_type = 2
        THEN s.depot_code
        WHEN a.supplier_type IN (1, 9)
        THEN k.shelf_code
      END AS supplier_code,
      CASE
        WHEN a.supplier_type = 2
        THEN s.supplier_name
        WHEN a.supplier_type IN (1, 9)
        THEN k.shelf_name
      END AS supplier_name,
      c.region_name AS REGION_AREA,
      c.business_name AS BUSINESS_AREA,
      c.`CITY_NAME`,
      d.BRANCH_CODE,
      d.BRANCH_NAME,
      CAST(a.order_id AS CHAR) AS order_id,
      a.shelf_id,
      c.`SHELF_CODE`,
      c.shelf_type,
      c.grade AS shelf_level,
      t0.task_id,
      IF(t0.task_id IS NULL, 'no', 'yes') AS if_is_skewer,
      a.APPLY_TIME,
      a.FILL_TIME,
      a.FILL_TYPE,
      a.ORDER_STATUS,
      a.PRODUCT_TYPE_NUM,
      a.PRODUCT_NUM,
      a.TOTAL_PRICE,
      a.FILL_AUDIT_STATUS,
      d.sf_code,
      d.real_name,
      IF(
        d.second_user_type = 1,
        '全职',
        '兼职'
      ) AS manager_type,
      DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) AS day_diff
    FROM
      fe_dwd.`dwd_fill_day_inc` a
      LEFT JOIN fe_dwd.`dwd_shelf_base_day_all` c
        ON a.SHELF_ID = c.shelf_id
      LEFT JOIN fe_dwd.`dwd_pub_shelf_manager` d
        ON a.fill_user_id = d.manager_id
      LEFT JOIN fe_dwd.`dwd_sf_supplier` s
        ON a.supplier_id = s.supplier_id
      LEFT JOIN fe_dwd.`dwd_shelf_base_day_all` k
        ON a.supplier_id = k.shelf_id
      LEFT JOIN
        (SELECT
          e.order_id,
          e.task_id
        FROM
          fe_dwd.`dwd_sf_order_logistics_task_record` e
        WHERE e.add_time >= SUBDATE(CURRENT_DATE, INTERVAL 1 MONTH)
        GROUP BY e.order_id) t0
        ON a.`ORDER_ID` = t0.order_id
    WHERE a.ORDER_STATUS IN (1, 2, 3, 4)
      AND a.`FILL_TYPE` IN (1, 2, 8, 9, 10)
      AND a.`ORDER_ID` != 0
      AND a.apply_time >= @date_top
      AND a.apply_time < @date_end
    GROUP BY a.`ORDER_ID`) a
    LEFT JOIN fe_dwd.`dwd_pub_work_day` w
      ON ! ISNULL(a.apply_time)
      AND ! ISNULL(a.fill_time)
      AND DATE(a.apply_time) <= w.sdate
      AND DATE(a.fill_time) >= w.sdate
  GROUP BY a.order_id) t
  JOIN fe_dwd.`dwd_fill_day_inc` b
    ON b.`order_id` = t.order_id
GROUP BY b.`ORDER_ID`,
  b.error_reason
 ;
-- 查询计算未上架订单的待更新字段并建临时表存储
DROP TEMPORARY TABLE IF EXISTS fe_dm.fill_timeliness_temp;
CREATE TEMPORARY TABLE fe_dm.fill_timeliness_temp(KEY idx_order_reason(order_id,error_reason)) AS
SELECT
  t.order_id,
  t.new_order_id,
  t.APPLY_TIME,
  t.FILL_TIME,
  t.ORDER_STATUS,
  t.FILL_TYPE,
  t.FILL_AUDIT_STATUS,
  t.BRANCH_CODE,
  t.BRANCH_NAME,
  t.sf_code,
  t.real_name,
  t.manager_type,
  t.day_diff,
  t.two_day_fill_label,
  t.three_day_fill_label,
  IFNULL(b.error_reason, 0) AS error_reason,
  SUM(
    b.PURCHASE_PRICE * b.ACTUAL_SIGN_NUM
  ) AS real_value,
  SUM(ABS(b.ERROR_NUM)) AS ERROR_NUM
FROM
  (SELECT
    a.*,
    IF(
      (SELECT
        w.`if_work_day`
      FROM
        fe_dwd.`dwd_pub_work_day` w
      WHERE w.`sdate` = DATE(a.APPLY_TIME)) = 0,
      IF(
        SUM(w.if_work_day) <= 1,
        '及时',
        IF(
          a.fill_time IS NULL,
          NULL,
          '不及时'
        )
      ),
      IF(
        (SELECT
          w.`if_work_day`
        FROM
          fe_dwd.`dwd_pub_work_day` w
        WHERE w.`sdate` = DATE(a.fill_time)) = 0,
        IF(
          SUM(w.if_work_day) <= IF(
            a.task_id IS NULL,
            1,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              1,
              2
            )
          ),
          '及时',
          IF(
            a.fill_time IS NULL,
            NULL,
            '不及时'
          )
        ),
        IF(
          SUM(w.if_work_day) <= IF(
            a.task_id IS NULL,
            2,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              2,
              3
            )
          ),
          '及时',
          IF(
            a.fill_time IS NULL,
            NULL,
            '不及时'
          )
        )
      )
    ) AS two_day_fill_label,
    IF(
      (SELECT
        w.`if_work_day`
      FROM
        fe_dwd.`dwd_pub_work_day` w
      WHERE w.`sdate` = DATE(a.APPLY_TIME)) = 0,
      IF(
        SUM(w.if_work_day) <= 2,
        '及时',
        IF(
          a.fill_time IS NULL,
          NULL,
          '不及时'
        )
      ),
      IF(
        (SELECT
          w.`if_work_day`
        FROM
          fe_dwd.`dwd_pub_work_day` w
        WHERE w.`sdate` = DATE(a.fill_time)) = 0,
        IF(
          SUM(w.if_work_day) <= IF(
            a.task_id IS NULL,
            2,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              2,
              3
            )
          ),
          '及时',
          IF(
            a.fill_time IS NULL,
            NULL,
            '不及时'
          )
        ),
        IF(
          SUM(w.if_work_day) <= IF(
            a.task_id IS NULL,
            3,
            IF(
              (SELECT
                w.`if_work_day`
              FROM
                fe_dwd.`dwd_pub_work_day` w
              WHERE w.`sdate` = DATE_ADD(DATE(a.APPLY_TIME), INTERVAL 1 DAY)) = 0,
              3,
              4
            )
          ),
          '及时',
          IF(
            a.fill_time IS NULL,
            NULL,
            '不及时'
          )
        )
      )
    ) AS three_day_fill_label
  FROM
    (SELECT
      CAST(r.order_id AS CHAR) AS order_id,
      a.order_id AS new_order_id,
      a.APPLY_TIME,
      a.FILL_TIME,
      a.ORDER_STATUS,
      a.FILL_TYPE,
      a.FILL_AUDIT_STATUS,
      d.BRANCH_CODE,
      d.BRANCH_NAME,
      d.sf_code,
      d.real_name,
      t0.task_id,
      IF(
        d.second_user_type = 1,
        '全职',
        '兼职'
      ) AS manager_type,
      DATEDIFF(a.`FILL_TIME`, a.`APPLY_TIME`) AS day_diff
    FROM
      (SELECT
        m.`ORDER_ID`
      FROM
        fe_dm.dm_lo_shelf_fill_timeliness_detail m
      WHERE ISNULL(m.fill_time)
      GROUP BY m.`ORDER_ID`) r
      LEFT JOIN fe_dwd.`dwd_fill_day_inc` a
        ON r.order_id = a.order_id
        AND a.`ORDER_ID` != 0
      LEFT JOIN fe_dwd.dwd_shelf_base_day_all c
        ON a.SHELF_ID = c.shelf_id
      LEFT JOIN fe_dwd.dwd_pub_shelf_manager d
        ON IF(
          ISNULL(a.fill_time),
          c.manager_id,
          a.fill_user_id
        ) = d.manager_id
      LEFT JOIN
        (SELECT
          e.order_id,
          e.task_id
        FROM
          fe_dwd.dwd_sf_order_logistics_task_record e
        WHERE e.data_flag = 1
          AND e.add_time >= '2019-01-01'
        GROUP BY 1) t0
        ON a.`ORDER_ID` = t0.order_id
    GROUP BY a.`order_id`) a
    LEFT JOIN fe_dwd.`dwd_pub_work_day` w
      ON ! ISNULL(a.apply_time)
      AND ! ISNULL(a.fill_time)
      AND DATE(a.apply_time) <= w.sdate
      AND DATE(a.fill_time) >= w.sdate
  GROUP BY a.order_id) t
  JOIN fe_dwd.`dwd_fill_day_inc` b
    ON b.`order_id` = t.order_id
GROUP BY b.`ORDER_ID`,
  b.error_reason
  ;
-- 根据以上的临时表删除结果表中不符合或失效的未上架订单
DELETE t.* FROM fe_dm.dm_lo_shelf_fill_timeliness_detail t
JOIN fe_dm.fill_timeliness_temp p
ON t.order_id = p.order_id
WHERE p.ORDER_STATUS NOT IN (1,2,3,4)
  OR p.`FILL_TYPE` NOT IN (1, 2, 8, 9,10)
  OR ISNULL(p.new_order_id);
-- 根据以上的临时表更新结果表中未上架订单的待更新字段
UPDATE fe_dm.dm_lo_shelf_fill_timeliness_detail f
JOIN
fe_dm.fill_timeliness_temp c
ON f.`ORDER_ID` = c.order_id
AND f.`ERROR_REASON` = c.error_reason
SET f.`FILL_TIME`=c.fill_time, f.`ORDER_STATUS`=c.ORDER_STATUS, f.FILL_TYPE = c.FILL_TYPE, f.`FILL_AUDIT_STATUS`= c.FILL_AUDIT_STATUS,
f.`day_interval`= c.day_diff, f.`two_days_fill_label`= c.two_day_fill_label,f.`three_days_fill_label`=c.three_day_fill_label,
f.`ERROR_REASON`= c.error_reason, f.`actual_amount`= c.real_value, f.`ERROR_NUM`= c.ERROR_NUM, f.BRANCH_CODE= c.BRANCH_CODE,
f.BRANCH_NAME=c.BRANCH_NAME, f.sf_code=c.sf_code, f.real_name=c.real_name,f.manager_type = c.manager_type;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_lo_shelf_fill_timeliness_detail',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('蔡松林@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_lo_shelf_fill_timeliness_detail','dm_lo_shelf_fill_timeliness_detail','蔡松林');
END