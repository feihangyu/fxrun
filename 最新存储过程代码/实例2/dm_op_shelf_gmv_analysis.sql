CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_shelf_gmv_analysis`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @stat_date := SUBDATE(CURDATE(),1);
SET @cur_year_month := DATE_FORMAT(@stat_date,'%Y-%m');
SET @pre_year_month := DATE_FORMAT(SUBDATE(@stat_date,INTERVAL 1 MONTH),'%Y-%m');
SET @num := DAYOFMONTH(CURDATE());
SET @pre_days_2 := SUBDATE(CURDATE(),2);
SET @pre_days_8 := SUBDATE(CURDATE(),8);
SET @cur_week_01 := SUBDATE(CURDATE(),INTERVAL 1 WEEK);
SET @week_end := SUBDATE(CURDATE(),INTERVAL 1 DAY);
SET @last_week_end := SUBDATE(@week_end,7);
-- 月维度货架等级、GMV 30s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_gmv_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.shelf_gmv_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT 
        t.shelf_id,
        t.shelf_level_last,
        t.shelf_level_t,
        SUM(t.gmv_last) AS gmv_last_all,
        SUM(t.gmv_last - IFNULL(l1.gmv_large, 0)) AS gmv_last,
        SUM(t.gmv) AS gmv_all,
        SUM(t.gmv - IFNULL(l.gmv_large, 0)) AS gmv
FROM
        fe_dm.`dm_op_product_shelf_stat` t
        LEFT JOIN fe_dm.dm_op_product_shelf_sal_month_large l
                ON t.product_id = l.product_id
                AND t.shelf_id = l.shelf_id
                AND  l.month_id = @cur_year_month
        LEFT JOIN fe_dm.dm_op_product_shelf_sal_month_large l1
                ON t.product_id = l1.product_id
                AND t.shelf_id = l1.shelf_id
                AND l1.month_id = @pre_year_month
WHERE t.month_id = @cur_year_month
GROUP BY t.shelf_id
;
-- 近7天GMV 1s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_gmv_day7`;   
CREATE TEMPORARY TABLE fe_dwd.shelf_gmv_day7 (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM(IF(a.sdate = @week_end,gmv,0)) AS lw_gmv,
        SUM(IF(a.sdate = @last_week_end,gmv,0)) AS llw_gmv
FROM
        fe_dm.`dm_shelf_wgmv` a
WHERE a.sdate >= @last_week_end
GROUP BY a.shelf_id
;
-- 本月低库存数量天数 1min
-- 库存达标标准（低于该标准为低库存）：
-- 上月等级甲乙级：货架库存水平＜180，冰箱库存水平＜110，关联货架按关联数折算
-- 上月等级丙丁级：货架库存水平＜110，冰箱库存水平＜90，关联货架按关联数折算
-- 货架低库存上限 1s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`low_stock_upper_limit`;
CREATE TEMPORARY TABLE fe_dwd.low_stock_upper_limit(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT 
        a.shelf_id,
        CASE 
                WHEN a.grade IN ('丙','丁') AND a.shelf_type IN (1,3)
                        THEN IFNULL(b.shelf_stock_upper_limit,0) + 110
                WHEN a.grade IN ('丙','丁') AND a.shelf_type IN (2,5)
                        THEN IFNULL(b.shelf_stock_upper_limit,0) + 90
                WHEN a.shelf_type IN (1,3)
                        THEN IFNULL(b.shelf_stock_upper_limit,0) + 180
                WHEN a.shelf_type IN (2,5)
                        THEN IFNULL(b.shelf_stock_upper_limit,0) + 110
        END AS shelf_stock_upper_limit
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` a
        LEFT JOIN 
                (
                        SELECT
                                `MAIN_SHELF_ID` AS shelf_id,
                                SUM(
                                        CASE
                                                WHEN grade IN ('丙','丁') AND shelf_type IN (1,3) THEN 110
                                                WHEN grade IN ('丙','丁') AND shelf_type IN (2,5) THEN 90
                                                WHEN shelf_type IN (1,3) THEN 180
                                                WHEN shelf_type IN (2,5) THEN 110
                                        END
                                ) AS shelf_stock_upper_limit
                        FROM
                                fe_dwd.`dwd_shelf_base_day_all`
                        WHERE main_shelf_id IS NOT NULL
                        GROUP BY main_shelf_id
                ) b
                ON a.shelf_id = b.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_low_stock_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.shelf_low_stock_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM(b.stock_quantity < a.shelf_stock_upper_limit) AS low_shelf_days,
        a.shelf_stock_upper_limit
FROM
        fe_dwd.`low_stock_upper_limit` a
        JOIN `fe_dwd`.`dwd_shelf_day_his` b
                ON a.shelf_id = b.shelf_id
                AND b.sdate >= @cur_week_01
GROUP BY a.shelf_id
;
-- 本周补货 10s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM(IF(a.order_status = 4,a.ACTUAL_FILL_NUM,0) * b.SALE_PRICE) AS fill_value,
        SUM(a.actual_apply_num * b.SALE_PRICE) AS push_order_value,
        COUNT(DISTINCT DATE(IF(a.order_status = 4,a.apply_time,NULL))) AS fill_cnt
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` b
                ON a.apply_time >= @cur_week_01
                AND a.FILL_TYPE IN (1,2,3,4,7,8,9)
                AND a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
GROUP BY a.shelf_id
;
-- 本周盘点 2s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`check_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.check_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        COUNT(DISTINCT DATE(a.OPERATE_TIME)) AS check_cnt
FROM
        `fe_dwd`.`dwd_check_base_day_inc` a
WHERE a.OPERATE_TIME >= @cur_week_01
GROUP BY a.shelf_id
;
-- 本周取消订单 1s 
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`fill_off_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.fill_off_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        COUNT(DISTINCT DATE(a.apply_time)) AS fill_off_cnt
FROM
        `fe_dwd`.`dwd_fill_day_inc` a
WHERE a.apply_time >= @cur_week_01
        AND a.FILL_TYPE IN (1,2,3,8,9)
        AND a.order_status = 9
GROUP BY a.shelf_id
;
-- SKU数 2min
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`sku_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.sku_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM((a.SHELF_FILL_FLAG = 1)) AS allow_fill_sku,
        SUM((a.SHELF_FILL_FLAG = 1 AND ((DATEDIFF(@week_end,a.first_fill_time) < 28) OR a.first_fill_time IS NULL))) AS allow_fill_new_sku,
        SUM((a.STOCK_QUANTITY > 0 AND a.SHELF_FILL_FLAG = 2)) AS stock_stop_fill_sku,
        SUM((a.STOCK_QUANTITY > 0 AND a.SHELF_FILL_FLAG = 1)) AS stock_allow_fill_sku,
        SUM((a.STOCK_QUANTITY > 0 AND ((DATEDIFF(@week_end,a.first_fill_time) < 28) OR a.first_fill_time IS NULL))) AS stock_new_sku,
        SUM((c.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SALES_FLAG IN (1,2,3) AND a.STOCK_QUANTITY > 0)) AS stock_sale_sku,
        SUM((c.PRODUCT_TYPE IN ('新增（试运行）','原有') AND a.SALES_FLAG IN (1,2,3) AND a.STOCK_QUANTITY <= 0)) AS offstock_sale_sku,
        ROUND(SUM(IF(a.STOCK_QUANTITY > 0,a.SALE_PRICE,0)) / SUM((a.STOCK_QUANTITY > 0))) AS avg_sale_price,
        SUM((a.STOCK_QUANTITY > 0 AND a.SALE_PRICE > 7))  AS stock_price_7,
        SUM((a.STOCK_QUANTITY > 0 AND a.SALE_PRICE < 2)) AS stock_price_2
FROM
        `fe_dwd`.`dwd_shelf_product_day_all` a
        JOIN `fe_dwd`.dwd_shelf_base_day_all b
                ON a.shelf_id = b.shelf_id
        LEFT JOIN fe_dwd.`dwd_pub_product_dim_sserp` c
                ON b.business_name = c.business_area
                AND c.product_id = a.product_id
GROUP BY a.shelf_id
;
-- 折扣率 9s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`discount_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.discount_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
        a.shelf_id,
        SUM(a.discount_amount) / SUM(a.quantity * a.sale_price) AS discount_ratio,
        SUM(a.quantity * a.sale_price) / COUNT(a.order_id) AS avg_sale_value
FROM
        `fe_dwd`.`dwd_pub_order_item_recent_one_month` a
WHERE a.PAY_DATE >= @cur_week_01
GROUP BY a.shelf_id
;
-- 巡检表 2s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`inspection_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.inspection_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT
DISTINCT
  t.shelf_id ,
  ck.error_val,
  ans.*
FROM
  fe_dwd.`dwd_sf_shelf_inspection_task` t
  JOIN
    (SELECT
      t.shelf_id,
      MAX(t.id) id
    FROM
      fe_dwd.`dwd_sf_shelf_inspection_task` t
    WHERE t.data_flag = 1
      AND t.inspect_status IN (20, 80)
      AND t.update_time >= SUBDATE(CURRENT_DATE, DAY(CURRENT_DATE) - 1)
    GROUP BY t.shelf_id) mt
    ON t.id = mt.id
  LEFT JOIN
    (SELECT
      t.task_id,
      CAST(
        MIN(IF(t.id = 6, t.content, NULL)) AS CHAR(256)
      )  AS office_qty,
      CAST(
        MIN(IF(t.id = 12, t.content, NULL)) AS CHAR(256)
      ) AS compete_qty
    FROM
      (SELECT
        t.task_id,
        q.title,
        qm.id,
        GROUP_CONCAT(DISTINCT t.content) AS content
      FROM
        fe_dwd.`dwd_sf_shelf_inspection_survey_answer` t
        JOIN fe_dwd.`dwd_sf_survey_question` q
          ON t.question_id = q.id
          AND q.data_flag = 1
          AND q.survey_id IN (1, 2, 3, 4)
        JOIN
          (SELECT
            q.title,
            MIN(q.id) id
          FROM
            fe_dwd.`dwd_sf_survey_question` q
          WHERE q.data_flag = 1
            AND q.survey_id IN (1, 2, 3, 4)
          GROUP BY q.title) qm
          ON q.title = qm.title
        WHERE t.last_update_time >= SUBDATE(CURRENT_DATE, DAY(CURRENT_DATE) - 1)
      GROUP BY t.task_id,
        q.title) t
    GROUP BY t.task_id) ans
    ON t.id = ans.task_id
  LEFT JOIN
    (SELECT
        DATE(MAX(operate_time)) AS operate_time,
        MAX(check_id) AS check_id,
        shelf_id,
        SUBSTRING_INDEX(GROUP_CONCAT(error_val ORDER BY check_id DESC SEPARATOR ","),",",1)  AS error_val
FROM
        (
                SELECT 
                        operate_time operate_time,
                        check_id,
                        shelf_id,
                        SUM(error_num * sale_price) error_val
                FROM
                        fe_dwd.`dwd_check_base_day_inc`
                WHERE OPERATE_TIME >= SUBDATE(CURRENT_DATE, DAY(CURRENT_DATE) - 1)
                        AND check_type = 4
                GROUP BY check_id
        ) a
GROUP BY shelf_id) ck
    ON mt.shelf_id = ck.shelf_id
  LEFT JOIN fe_dwd.`dwd_sf_shelf_inspection_task_operation` ito
    ON t.id = ito.task_id
    AND ito.data_flag = 1
    AND ito.operate_type = 3
WHERE IFNULL(ito.update_time, t.update_time) >= @cur_week_01
;
-- 次日上架率 2s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`intime_tmp`;   
CREATE TEMPORARY TABLE fe_dwd.intime_tmp (
        KEY idx_shelf_id(shelf_id)
) AS
SELECT 
       shelf_id,
       IFNULL(COUNT(CASE WHEN two_days_fill_label = '及时' THEN order_id END),0) / COUNT(order_id) AS intime_rate -- 次日上架率
FROM fe_dm.`dm_lo_shelf_fill_timeliness_detail`
WHERE apply_time >= @pre_days_8
AND apply_time <= @pre_days_2
GROUP BY shelf_id;
-- 结果表
DELETE FROM  fe_dm.dm_op_shelf_gmv_analysis WHERE stat_date = @stat_date;
INSERT INTO fe_dm.dm_op_shelf_gmv_analysis
(
        stat_date,
        business_name,
        region_name,
        shelf_id,
        shelf_code,
        shelf_type,
        activate_time,
        REVOKE_TIME,
        SHELF_STATUS,
        REVOKE_STATUS,
        manager_type,
        is_preware_shelf,         --  是否前置仓覆盖
        bind_cnt,     -- 关联货架数
        shelf_level_last,     -- 上月货架等级
        shelf_level_t,        -- 本月货架等级
        gmv_last_all,   -- 上月GMV_全量
        gmv_last,      --  上月累计GMV
        gmv_all,        -- 当月累计GMV_全量
        gmv,    -- 当月累计GMV
        llw_gmv,        -- 上上周累计GMV
        lw_gmv,     -- 上周累计GMV
        shelf_stock_upper_limit,      -- 标准库存数量
        STOCK_QUANTITY,       -- 库存数量
        stock_value,          -- 库存金额   
        low_shelf_days,    -- 本周低库存天数
        push_order_value,     -- 本周补货推单金额
        fill_value,   -- 本周补货上架金额
        fill_cnt,     -- 本周补货次数
        office_qty,   -- 常驻人数
        compete_qty,  -- 是否有竞对
        error_val,    -- 最近一次虚库存金额
        month_lose_rate,      -- 盗损率
        manage_cnt,     -- 当周维护次数
        fill_off_cnt,         -- 本周补货订单取消次数
        intime_rate,  --  补货订单平均次日上架率
        val_sto_flag5,        -- 严重滞销金额
        high_stock_ratio,     -- 高库存金额占比
        allow_fill_sku,       -- 可补货SKU数
        allow_fill_new_sku,   --    可补货新品SKU数
        stock_stop_fill_sku,  -- 有库存停补SKU数
        stock_allow_fill_sku,         -- 有库存可补货SKU数
        stock_new_sku,        -- 有库存新品SKU数
        stock_sale_sku,       -- 有库存爆畅平SKU数
        offstock_sale_sku,    -- 缺货爆畅平SKU数
        avg_sale_price,       -- 有库存平均单价
        stock_price_7,  -- 有库存且价格>7元的SKU数
        stock_price_2,  -- 有库存且价格＜2元的SKU数
        discount_ratio,       -- 折扣率
        avg_sale_value        -- 平均订单金额
)
SELECT 
        @stat_date AS stat_date,
        a.business_name,
        a.region_name,
        a.shelf_id,
        a.shelf_code,
        a.shelf_type,
        a.activate_time,
        a.REVOKE_TIME,
        a.SHELF_STATUS,
        a.REVOKE_STATUS,
        a.manager_type,
        IF(n.shelf_id,1,2) AS is_preware_shelf,         --  是否前置仓覆盖
        a.bind_cnt,     -- 关联货架数
        b.shelf_level_last,     -- 上月货架等级
        b.shelf_level_t,        -- 本月货架等级
        b.gmv_last_all,   -- 上月GMV_全量
        b.gmv_last,      --  上月累计GMV
        b.gmv_all,        -- 当月累计GMV_全量
        b.gmv,    -- 当月累计GMV
        o.llw_gmv,        -- 上上周累计GMV
        o.lw_gmv,     -- 上周累计GMV
        d.shelf_stock_upper_limit,      -- 标准库存数量
        c.qty_sto AS STOCK_QUANTITY,       -- 库存数量
        c.val_sto AS stock_value,          -- 库存金额   
        IF(d.low_shelf_days > @num,@num,d.low_shelf_days) AS low_shelf_days,    -- 本周低库存天数
        f.push_order_value,     -- 本周补货推单金额
        f.fill_value,   -- 本周补货上架金额
        f.fill_cnt,     -- 本周补货次数
        l.office_qty,   -- 常驻人数
        l.compete_qty,  -- 是否有竞对
        l.error_val,    -- 最近一次虚库存金额
        c.month_lose_rate,      -- 盗损率
        IFNULL(f.fill_cnt,0) + IFNULL(h.check_cnt,0) AS manage_cnt,     -- 当周维护次数
        i.fill_off_cnt,         -- 本周补货订单取消次数
        m.intime_rate,  --  补货订单平均次日上架率
        c.val_sto_flag5,        -- 严重滞销金额
        e.high_stock_ratio,     -- 高库存金额占比
        j.allow_fill_sku,       -- 可补货SKU数
        j.allow_fill_new_sku,   --    可补货新品SKU数
        j.stock_stop_fill_sku,  -- 有库存停补SKU数
        j.stock_allow_fill_sku,         -- 有库存可补货SKU数
        j.stock_new_sku,        -- 有库存新品SKU数
        j.stock_sale_sku,       -- 有库存爆畅平SKU数
        j.offstock_sale_sku,    -- 缺货爆畅平SKU数
        j.avg_sale_price,       -- 有库存平均单价
        j.stock_price_7,  -- 有库存且价格>7元的SKU数
        j.stock_price_2,  -- 有库存且价格＜2元的SKU数
        ROUND(k.discount_ratio,2) AS discount_ratio,       -- 折扣率
        ROUND(k.avg_sale_value,2) AS avg_sale_value        -- 平均订单金额
FROM
        `fe_dwd`.`dwd_shelf_base_day_all` a
        LEFT JOIN fe_dwd.shelf_gmv_tmp b
                ON a.`shelf_id` = b.shelf_id
        LEFT JOIN fe_dm.dm_pub_shelf_board c
                ON a.`shelf_id` = c.shelf_id
        LEFT JOIN fe_dwd.shelf_low_stock_tmp d
                ON  a.`shelf_id` = d.shelf_id
        LEFT JOIN fe_dm.`dm_op_shelf_high_stock` e
                ON a.`shelf_id` = e.shelf_id
                AND e.month_id = @cur_year_month
        LEFT JOIN fe_dwd.fill_tmp f
                ON a.`shelf_id` = f.shelf_id
        LEFT JOIN fe_dwd.check_tmp h
                ON a.`shelf_id` = h.shelf_id
        LEFT JOIN fe_dwd.fill_off_tmp i
                ON a.`shelf_id` = i.shelf_id
        LEFT JOIN fe_dwd.sku_tmp j
                ON a.`shelf_id` = j.shelf_id
        LEFT JOIN fe_dwd.`discount_tmp` k
                ON a.`shelf_id` = k.shelf_id
        LEFT JOIN fe_dwd.inspection_tmp l
                ON a.`shelf_id` = l.shelf_id
        LEFT JOIN fe_dwd.intime_tmp m
                ON a.`shelf_id` = m.shelf_id
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` n
                ON a.`shelf_id` = n.shelf_id
        LEFT JOIN fe_dwd.shelf_gmv_day7 o
                ON a.shelf_id = o.shelf_id
WHERE a.shelf_type IN (1,2,3,5)
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_shelf_gmv_analysis',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_shelf_gmv_analysis','dm_op_shelf_gmv_analysis','宋英南');
END