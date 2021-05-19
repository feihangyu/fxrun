CREATE DEFINER=`feprocess`@`%` PROCEDURE `zs_shelf_manager_suspect_problem_label`()
BEGIN
-- =============================================
-- Author:	物流店主组
-- Create date: 
-- Modify date: 
-- Description:	
-- 	创建疑似排面问题标签
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  delete
  from
    feods.`zs_shelf_manager_suspect_problem_label`
  where smonth = date_format(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY), '%Y%m');
  set @month_head := DATE_ADD(date_sub(current_date,interval 1 day),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY);
  set @month_end := DATE_ADD(@month_head,INTERVAL 1 MONTH);
    insert into feods.`zs_shelf_manager_suspect_problem_label`(
    smonth,
    shelf_id,
    manager_id,
    manager_name,
    manager_star,
    operate_num,
    fill_num,
    shelf_gmv,
    suspect_problem_label)
    SELECT
      tab.smonth,
      tab.shelf_id,
      tab.manager_id,
      tab.manager_name,
      tab.manager_star,
      tab.operate_num,
      tab.fill_num,
      tab.shelf_GMV,
      tab.suspect_problem_label
    FROM
      (SELECT
        date_format(date_sub(current_date,interval 1 day),'%Y%m') AS smonth,
        a.`SHELF_ID`,
        a.manager_id,
        a.manager_name,
        f.manager_star,
        IFNULL(b.operate_num, 0) AS operate_num,
        IFNULL(e.fill_num, 0) AS fill_num,
        IFNULL(d.GMV, 0) AS shelf_GMV,
        CASE
          WHEN f.manager_star IN ('一星店主', '二星店主')
          AND IFNULL(b.operate_num, 0) <= 1
          AND IFNULL(e.fill_num, 0) <= 3
          AND IFNULL(d.GMV, 0) < 300
          THEN '疑似牌面问题'
          ELSE NULL
        END AS suspect_problem_label
      FROM
        fe.`sf_shelf` a
        LEFT JOIN
          (SELECT
            a.`SHELF_ID`,
            COUNT(a.`CHECK_ID`) AS operate_num
          FROM
            fe.`sf_shelf_check` a
          WHERE a.`DATA_FLAG` = 1
            and a.check_type in (1,3)
            AND a.`OPERATE_TIME` >= @month_head
            AND a.operate_time < @month_end
          GROUP BY a.`SHELF_ID`) b ###盘点次数月统计
           ON a.`SHELF_ID` = b.shelf_id
        LEFT JOIN
          (SELECT
            a.`SHELF_ID`,
            COUNT(DISTINCT a.`ORDER_ID`) AS fill_num
          FROM
            fe.`sf_product_fill_order` a
            LEFT JOIN fe.`sf_product_fill_order_item` b
              ON a.`ORDER_ID` = b.`ORDER_ID`
          WHERE a.`FILL_TYPE` IN (1, 2, 8, 9)
            AND a.`ORDER_STATUS` = 4
            AND a.fill_time >= @month_head
            AND a.fill_time < @month_end
          GROUP BY a.`SHELF_ID`) e ###补货次数月统计
           ON e.shelf_id = a.shelf_id
        LEFT JOIN
          (SELECT
            f.shelf_id,
            SUM(f.`PRODUCT_TOTAL_AMOUNT`) AS GMV
          FROM
            fe.`sf_order` f
          WHERE f.ORDER_STATUS = 2
            AND f.ORDER_DATE >= @month_head
            AND f.`ORDER_DATE` < @month_end
          GROUP BY f.SHELF_ID) d ###GMV月统计
           ON a.`SHELF_ID` = d.shelf_id
        LEFT JOIN
          (SELECT
            t.`manager_id`,
            t.`manager_type`,
            SUM(t.`score`) AS month_score,
            IF(
              SUM(t.`score`) >= 1500,
              '五星店主',
              IF(
                SUM(t.`score`) >= 1000
                AND SUM(t.`score`) < 1500,
                '四星店主',
                IF(
                  SUM(t.`score`) >= 500
                  AND SUM(t.`score`) < 1000,
                  '三星店主',
                  IF(
                    SUM(t.`score`) >= 300
                    AND SUM(t.`score`) < 500,
                    '二星店主',
                    IF(
                      SUM(t.`score`) < 300,
                      '一星店主',
                      NULL
                    )
                  )
                )
              )
            ) AS manager_star
          FROM
            fe.`sf_shelf_manager_score_detail` t
          WHERE t.`data_flag` = 1
            AND t.`score_time` >= @month_head
            AND t.`score_time` < @month_end
          GROUP BY t.`manager_id`) f
          ON a.manager_id = f.manager_id
      WHERE a.`DATA_FLAG` = 1
        AND a.`SHELF_TYPE` IN (1, 2, 3, 5)
        AND a.`SHELF_STATUS` = 2) tab
    WHERE tab.suspect_problem_label IS NOT NULL;
    
--     执行记录日志
    CALL sh_process.`sp_sf_dw_task_log`(
  'zs_shelf_manager_suspect_problem_label',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);
commit;
    END