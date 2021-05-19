CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_zs_manager_shelf_performance_label`()
BEGIN
  -- =============================================
-- Author:	物流店主
-- Create date: 2019/04/09
-- Modify date: 
-- Description:	
-- 	统计不同月份货架效能情况明细-物流店主组标签
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  
  SET @month_head := DATE_SUB(current_date,INTERVAL 1 day);
  SET @month_end := current_date;
  
  DELETE
  FROM
    feods.`zs_manager_shelf_performance_label`
  WHERE stadate >= @month_head and stadate < @month_end;
  
  INSERT INTO feods.zs_manager_shelf_performance_label(
      stadate,
      business_area,
      shelf_id,
      shelf_code,
      shelf_type,
      activate_time,
      shelf_grade,
      manager_id,
      manager_name,
      fill_num,
      operate_num,
      high_fill_num,
      transfer_num,
      shelf_gmv,
      shelf_performance
    )
  SELECT
  @month_head AS stadate,
  a.business_area,
  a.`SHELF_ID`,
  a.shelf_code,
  a.shelf_type,
  a.activate_time,
  a.shelf_level,
  a.manager_id,
  a.manager_name,
  IFNULL(f.youx_buh,0) AS '补货次数',
  IFNULL(b.operate_num,0) AS '盘点次数',
  IFNULL(f.fill_much,0) AS '高频补货次数',
  IFNULL(g.youx_diaoh,0) AS '调货次数',
  d.GMV,
  CASE
    WHEN b.operate_num IS NULL
    AND f.youx_buh IS NULL
    AND g.youx_diaoh IS NULL
    AND d.GMV IS NULL
    THEN 0
    ELSE IFNULL(b.operate_num, 0) + IFNULL(f.youx_buh, 0) + IFNULL(g.youx_diaoh,0)
  END AS shelf_performance
FROM
  (SELECT
    b.business_name AS business_area,
    a.`SHELF_ID`,
    a.`SHELF_CODE`,
    a.shelf_type,
    a.activate_time,
    p.grade AS shelf_level,
    a.`MANAGER_ID`,
    r.real_name as manager_name
  FROM
    fe.`sf_shelf` a
  JOIN feods.`fjr_city_business` b
    ON a.city = b.city
  join fe.`pub_shelf_manager` r
    on a.manager_id = r.manager_id
  LEFT JOIN feods.`d_op_shelf_grade` p    -- 新货架等级表,考虑次货架
    ON p.shelf_id = a.shelf_id
    AND p.month_id = DATE_FORMAT(DATE_SUB(ADDDATE(@month_head,INTERVAL -DAY(@month_head) + 1 DAY),INTERVAL 1 MONTH),'%Y-%m')
  WHERE a.`DATA_FLAG` = 1 and r.data_flag = 1
--     AND a.`SHELF_TYPE` IN (1, 2, 3, 5)
    AND a.`SHELF_STATUS` = 2
    ) a
  LEFT JOIN
    (SELECT
      a.`SHELF_ID`,
      COUNT(distinct a.`CHECK_ID`) AS operate_num
    FROM
      fe.`sf_shelf_check` a
    WHERE a.`DATA_FLAG` = 1
      AND a.check_type IN (1,2,4)
      AND a.`OPERATE_TIME` >= @month_head
      AND a.operate_time < @month_end
    GROUP BY a.`SHELF_ID`) b               -- 盘点次数月统计
     ON a.`SHELF_ID` = b.shelf_id
      LEFT JOIN
      (SELECT
        f.shelf_id,
        COUNT(
          DISTINCT
          CASE
            WHEN f.PRODUCT_NUM > 0
            THEN DATE_FORMAT(f.FILL_TIME, '%Y%m%d')
          END
        ) AS youx_buh,
      COUNT(
        DISTINCT IF(
          f.PRODUCT_NUM > 100,
          f.`ORDER_ID`,
          NULL
        )
      ) AS fill_much
      FROM
        fe.sf_product_fill_order f
      WHERE f.order_status IN (3, 4)
        AND f.shelf_id IN (SELECT DISTINCT h.shelf_id FROM fe.`sf_shelf` h WHERE h.data_flag = 1 AND h.shelf_type <> 9)
        AND f.supplier_type <> 1
        AND f.fill_time >= @month_head
        AND f.fill_time < @month_end
        AND f.data_flag =1 
      GROUP BY f.shelf_id) f    -- 补货次数和高频补货次数月统计
      ON a.shelf_id = f.shelf_id 
    LEFT JOIN
      (SELECT
        g.shelf_id,
        COUNT(DISTINCT DATE_FORMAT(g.FILL_TIME, '%Y%m%d')) AS total_num,
        COUNT(
          DISTINCT
          CASE
            WHEN g.PRODUCT_NUM > 0
            THEN DATE_FORMAT(g.FILL_TIME, '%Y%m%d')
          END
        ) AS youx_diaoh
      FROM
        fe.sf_product_fill_order g      
      WHERE g.order_status IN (3, 4)
        AND g.shelf_id IN (SELECT DISTINCT s.shelf_id FROM fe.`sf_shelf` s WHERE s.data_flag =1 AND s.shelf_type <> 9)
        AND g.supplier_type = 1
        AND g.fill_time >= @month_head
        AND g.fill_time < @month_end
        AND g.data_flag = 1
      GROUP BY g.shelf_id) g    -- 货架调货数据
      ON a.shelf_id = g.shelf_id  
  LEFT JOIN
    (SELECT
  a.shelf_id,
  SUM(
    IF(
      a.order_status = 6
      AND a.refund_amount > 0,
      a.quantity_act,
      a.`QUANTITY`
    ) * a.`SALE_PRICE`
  ) AS GMV
FROM
  fe_dwd.`dwd_order_item_refund_day` a
WHERE a.pay_date >= @month_head
  AND a.pay_date < @month_end
GROUP BY a.`shelf_id`) d                 -- GMV月统计
ON a.`SHELF_ID` = d.shelf_id;

-- 更新覆盖最新的货架状态以及店主信息
update feods.`zs_manager_shelf_performance_label` t
join fe.`sf_shelf` f
on t.shelf_id = f.shelf_id
join fe.`pub_shelf_manager` r
on f.manager_id = r.manager_id
set t.shelf_status = f.shelf_status, t.shelf_code = f.shelf_code,t.shelf_type = f.shelf_type,
t.sf_code = r.sf_code, t.manager_id = r.manager_id, t.manager_name = r.real_name,
t.manager_type = if(r.second_user_type=1,'全职店主','兼职店主'),
t.branch_code = r.branch_code, t.branch_name = r.branch_name
where f.data_flag = 1 and r.data_flag =1;

  CALL sh_process.`sp_sf_dw_task_log`(
  'sp_zs_manager_shelf_performance_label',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);

COMMIT;
END