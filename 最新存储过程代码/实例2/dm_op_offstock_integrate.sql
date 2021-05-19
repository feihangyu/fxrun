DELIMITER $$

USE `sh_process`$$

DROP PROCEDURE IF EXISTS `dm_op_offstock_integrate`$$

CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_offstock_integrate`()
BEGIN
-- =============================================
-- Author:	补货
-- Create date: 2020/10/12
-- Modify date: 2020/11/06
-- Description:	
-- 	缺货率 / 缺货原因（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := SUBDATE(CURRENT_DATE, 1),
@add_user := CURRENT_USER,
@timestamp := CURRENT_TIMESTAMP;
SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
SET @add_day := ADDDATE(@sdate, 1);
SET @sub_day := SUBDATE(@sdate, 1);
SET @d := DAY(@sdate);
SET @month_end_last := SUBDATE(@sdate, @d);
SET @d_lm := DAY(@month_end_last);
SET @pre_day30 := SUBDATE(@sdate,29);
SET @pre_6month := SUBDATE(@sdate,INTERVAL 6 MONTH);
SET @day_num := DAYOFWEEK(@sdate);
SET @cur_day_num := DAYOFWEEK(@add_day);
SET @pre_7day := SUBDATE(@sdate,7);
SET @pre_4week := SUBDATE(SUBDATE(@add_day,DAYOFWEEK(@add_day) - 2),INTERVAL 3 WEEK);
SET @pre_2week := SUBDATE(@sdate,INTERVAL 2 WEEK);
SET @time_1 := CURRENT_TIMESTAMP();
-- 2020-10-16 何平平要求剔除淘汰品正常补货的商品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.abnormal_tmp;
CREATE TEMPORARY TABLE fe_dwd.abnormal_tmp (
KEY idx_shelf_id_product_id(shelf_id, product_id)
) AS
SELECT 
        DISTINCT
        a.shelf_id,
        a.product_id
FROM 
        fe_dwd.`dwd_sf_shelf_product_status_log` a
        JOIN fe_dm.`dm_op_shelf_product_start_fill_label` b
                ON a.shelf_id = b.shelf_id 
                AND a.product_id = b.product_id
                AND b.stat_date = @sdate
WHERE a.add_time >= '2020-10-16'   -- 星华的开启补货逻辑从10.16开始将淘汰品爆畅平开启补货，需要将这部分剔除
        AND a.add_time < @add_day
        AND a.operate_action = 1 -- 开启补货
        AND a.data_flag = 1
        AND a.operate_enter = 1 -- 定时任务  
        AND b.label = 2         -- 爆畅平淘汰品
;

-- 货架口径
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (KEY (shelf_id)) AS
SELECT
        t.shelf_id,
        t.business_name,
        t.zone_name,
        t.zone_code,
        t.MANAGER_ID,
        t.shelf_type,
        t.whether_close = 2 whether_close2,
        t.revoke_status = 1 revoke_status1,
        t.ACTIVATE_TIME,
        CASE
                WHEN t.grade IN ('甲', '乙', '新装') THEN '甲乙新'
                WHEN t.grade IN ('丙','丁') THEN '丙丁'
        END AS shelf_level,
        CASE
                WHEN DATEDIFF(@add_day,ACTIVATE_TIME) < 30 AND t.whether_close = 2 AND t.revoke_status = 1 THEN '新装' 
                WHEN t.whether_close = 2 AND t.revoke_status = 1 THEN '正常'
                ELSE '异常'
        END AS shelf_status_classify,
        t.shelf_status,
        IF(t.manager_type = '全职店主',1,0) AS manager_type_classify,
        is_prewarehouse_cover,
        p.prewarehouse_code,
        pr.supplier_id,
        pr.product_supplier_type AS supplier_type,
        t.type_name,
        t.inner_flag,
        t.if_bind,
        CASE
                WHEN t.if_bind = 1 THEN 25
                WHEN t.shelf_type IN (1,3) THEN 35
                WHEN t.shelf_type IN (2,6,7) THEN 15
        END AS low_limit,
        CASE
                WHEN t.if_bind = 1 THEN NULL
                WHEN t.shelf_type IN (1,3) THEN 55
                WHEN t.shelf_type = 2 THEN 25
                WHEN t.shelf_type = 6 AND t.type_name LIKE '%静态%' THEN 25
                WHEN t.shelf_type = 6 THEN 35
                WHEN t.shelf_type = 7 THEN 30
        END AS up_limit
FROM
        fe_dwd.`dwd_shelf_base_day_all` t
        LEFT JOIN fe_dwd.`dwd_sf_shelf_product_supply_info` pr
                ON t.shelf_id = pr.shelf_id
                AND pr.data_flag  = 1
        LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` p
                ON t.shelf_id = p.shelf_id
WHERE t.shelf_status = 2
        AND t.shelf_type IN (1,2,3,6,7)
        AND ! ISNULL(t.shelf_id)
        AND t.shelf_name NOT LIKE '%测试%'
;
-- 商品类型划分
DROP TEMPORARY TABLE IF EXISTS fe_dwd.normal_tmp;
CREATE TEMPORARY TABLE fe_dwd.normal_tmp (
KEY idx_area_product_id(product_id, business_name)
) AS
SELECT
        t.product_id,
        t.business_name,
        t.product_type,
        CASE
                WHEN t.normal_flag
                        THEN '原有品-持续'
                WHEN t.product_type = '原有'
                        THEN '原有品-其他'
                WHEN t.product_type = '新增（试运行）'
                        THEN '新品'
                ELSE '淘汰'
        END product_type_class,
        p.INDATE_NP
FROM
        fe_dm.dm_op_dim_product_area_normal t
        LEFT JOIN fe_dwd.`dwd_pub_product_dim_sserp` p
                ON t.business_name = p.business_area
                AND t.product_id = p.product_id
WHERE t.month_id = @y_m
AND ! ISNULL(t.business_name)
AND ! ISNULL(t.product_id);
-- 补货需求
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.requirement_tmp;
  CREATE TEMPORARY TABLE fe_dwd.requirement_tmp (KEY (shelf_id, product_id))
SELECT
        a.shelf_id,
        a.product_id,
        a.stock_num,
        a.onway_num,
        a.reduce_suggest_fill_ceiling_num AS suggest_fill_num,
        a.total_fill_value AS total_price,
        a.suggest_fill_num AS start_suggest_fill_num,
        ROUND((IFNULL(MAX_QUANTITY,0) - IFNULL(a.stock_num,0) - IFNULL(a.onway_num,0) - IFNULL(a.reduce_suggest_fill_ceiling_num,0)) / (IFNULL(MAX_QUANTITY,0) / slots)) AS slot_error_num,
        a.fill_order_day
FROM
        fe_dm.`dm_op_shelf_product_fill_update2_his` a
WHERE a.cdate = @sdate
;
-- 在途
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_onway_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fill_onway_tmp (KEY (shelf_id, product_id))
  SELECT
    t.shelf_id,
    t.product_id,
    SUM(t.actual_apply_num) onway_num
  FROM
    fe_dwd.`dwd_fill_day_inc` t
  WHERE t.order_status IN (1, 2)
    AND t.apply_time >= SUBDATE(@sdate, 30)
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id,
    t.product_id;
-- 取消订单
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_cancel_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fill_cancel_tmp (KEY (shelf_id, product_id))
  SELECT
    t.shelf_id,
    t.product_id,
    SUM(t.actual_apply_num) cancel_num
  FROM
    fe_dwd.`dwd_fill_day_inc` t
  WHERE t.order_status = 9
    AND t.FILL_TYPE IN (1,2,3,4,7,8,9)
    AND t.apply_time >= @sdate
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
GROUP BY t.shelf_id,t.product_id
;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.main_tmp;
  CREATE TEMPORARY TABLE fe_dwd.main_tmp (KEY (shelf_id, product_id))
  SELECT
    shelf_id,
    product_id
  FROM
    fe_dwd.fill_onway_tmp
  UNION
  SELECT
    shelf_id,
    product_id
  FROM
    fe_dwd.requirement_tmp;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.requirement_shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.requirement_shelf_tmp (KEY (shelf_id)) AS
  SELECT
    t.shelf_id,
    SUM(IFNULL(r.onway_num, f.onway_num)) onway_num,
    IFNULL(SUM(r.suggest_fill_num), 0) suggest_fill_num,
    IFNULL(MAX(r.total_price), 0) total_price
  FROM
    fe_dwd.main_tmp t
    LEFT JOIN fe_dwd.requirement_tmp r
      ON r.shelf_id = t.shelf_id
      AND r.product_id = t.product_id
    LEFT JOIN fe_dwd.fill_onway_tmp f
      ON f.shelf_id = t.shelf_id
      AND f.product_id = t.product_id
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
-- 货架库存
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.sto_tmp;
  CREATE TEMPORARY TABLE fe_dwd.sto_tmp (KEY (shelf_id)) AS
  SELECT
    t.shelf_id,
    t.stock_quantity,
    t.stock_sum AS stock_val
  FROM
    fe_dwd.`dwd_shelf_day_his` t
  WHERE t.sdate = @sub_day
    AND ! ISNULL(t.shelf_id);
-- 昨天申请补货数量
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fill_tmp (KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id,
    t.product_id,
    SUM(t.actual_apply_num) actual_apply_num
  FROM
    fe_dwd.`dwd_fill_day_inc` t
  WHERE t.order_status != 9
    AND t.apply_time >= @sdate
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id,
    t.product_id;
-- 当天店主出单数量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_shelf_qty_tmp;
CREATE TEMPORARY TABLE fe_dwd.fill_shelf_qty_tmp (KEY (manager_id)) AS
SELECT 
        a.manager_id,
        COUNT(DISTINCT a.shelf_id) AS fill_shelf_qty
FROM
        fe_dwd.shelf_tmp a
        JOIN fe_dwd.fill_tmp b
                ON a.shelf_id = b.shelf_id
GROUP BY a.manager_id
;
-- 上个出单日逻辑
-- 全部货架上一个出单日
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_date_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fill_date_tmp(
    KEY idx_shelf_id(shelf_id)
  )
SELECT 
        shelf_id,
        fill_day_code,
        IF(@cur_day_num - 1 > INSTR(fill_day_code,1),ADDDATE(SUBDATE(@add_day,DAYOFWEEK(@add_day) - 1),INSTR(fill_day_code,1)-1),SUBDATE(@add_day,7-(INSTR(fill_day_code,1) - @cur_day_num))) AS fill_date
FROM
        fe_dwd.`dwd_sf_shelf_fill_day_config`
WHERE data_flag = 1
;
-- 去重货架上一个出单日
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.uni_fill_date_tmp;
  CREATE TEMPORARY TABLE fe_dwd.uni_fill_date_tmp(
    KEY idx_shelf_id(shelf_id)
  )
SELECT
        shelf_id,
        MAX(fill_date) AS fill_date
FROM
        fe_dwd.fill_date_tmp
GROUP BY shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.7days_fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.7days_fill_tmp(
    KEY idx_date_shelf_id_product_id(apply_date,shelf_id,product_id)
  )
SELECT
        DATE(apply_time) AS apply_date,
        a.shelf_id,
        a.product_id,
        SUM(actual_apply_num) AS actual_apply_num,
        SUM(IF(order_status IN (1,2,3,4),actual_apply_num,0)) AS normal_apply_num,
        SUM(IF(order_status = 9,actual_apply_num,0)) AS cancel_apply_num
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` a
WHERE apply_time > @pre_7day
        AND apply_time < @sdate
        AND a.FILL_TYPE IN (1,2,3,4,7,8,9)
GROUP BY DATE(apply_time),a.shelf_id,a.product_id
;
-- 上一个出单日到昨日，中间有取消订单，且未生成新的订单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.cancel_tmp;
CREATE TEMPORARY TABLE fe_dwd.cancel_tmp(
    KEY idx_shelf_id_product_id(shelf_id,product_id)
  )
SELECT
        a.shelf_id,
        a.product_id
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` a
        JOIN fe_dwd.uni_fill_date_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.apply_time >= b.fill_date
                AND a.apply_time < @add_day
                AND a.FILL_TYPE IN (1,2,3,4,7,8,9)
GROUP BY a.shelf_id,a.product_id
HAVING MAX(a.order_status) = 9
        AND MIN(a.order_status) = 9
;
-- 上个出单日到今早中间的时间段，如果没有取消，且未下单（筛选已下单的）
-- DROP TEMPORARY TABLE IF EXISTS fe_dwd.not_fill_tmp;
-- CREATE TEMPORARY TABLE fe_dwd.not_fill_tmp(
--     KEY idx_shelf_id_product_id(shelf_id,product_id)
--   )
-- SELECT
--         DISTINCT 
--         a.shelf_id,
--         a.product_id
-- FROM
--         fe_dwd.`dwd_fill_day_inc_recent_two_month` a
--         JOIN fe_dwd.uni_fill_date_tmp b
--                 ON a.shelf_id = b.shelf_id
--                 AND a.apply_time >= b.fill_date
--                 AND a.apply_time < @add_day
--                 AND a.FILL_TYPE IN (1,2,3,4,7,8,9)
-- ;
-- 上架数量不等于申请数量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.apply_fill_diff_tmp;
CREATE TEMPORARY TABLE fe_dwd.apply_fill_diff_tmp(
    KEY idx_shelf_id_product_id(shelf_id,product_id)
  )
SELECT
        DISTINCT
        shelf_id,
        product_id
FROM
        fe_dwd.`dwd_fill_day_inc`
WHERE fill_time >= @sdate
        AND fill_time < @add_day
        AND order_status = 4
        AND FILL_TYPE IN (1,2,3,4,7,8,9)
        AND actual_apply_num > ACTUAL_FILL_NUM
;
-- 新装货架初始订单生成时间
DROP TEMPORARY TABLE IF EXISTS fe_dwd.first_fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.first_fill_tmp (
KEY idx_shelf_id_product_id(shelf_id,product_id)
)
SELECT
        shelf_id,
        product_id,
        MIN(apply_time) AS create_time
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month`
WHERE FILL_TYPE = 3
        AND apply_time > @pre_day30
        AND apply_time < @add_day
        AND order_status IN (1,2,3,4)
GROUP BY shelf_id,product_id
;
-- 货架安装完成时间
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_install_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_install_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        b.shelf_id,
        MIN(a.`execute_finish_time`) AS execute_finish_time
FROM
        fe_dwd.`dwd_sf_shelf_logistics_task` a
        JOIN fe_dwd.`dwd_sf_shelf_logistics_task_install` b
                ON a.logistics_task_id = b.`logistics_task_id`
WHERE a.task_type = 1 AND execute_finish_time IS NOT NULL
        AND a.data_flag = 1
        AND b.data_flag = 1
GROUP BY b.shelf_id
;
-- 货架维度最近一次上架时间
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`last_fill_time_tmp`;
CREATE TEMPORARY TABLE fe_dwd.last_fill_time_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        shelf_id,
        MIN(fill_time) AS fill_time
FROM
        fe_dm.`dm_op_shelf_product_fill_last_time`
WHERE fill_time IS NOT NULL
GROUP BY shelf_id
;
-- 货架维度汇总表
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tot_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_tot_tmp (KEY (shelf_id)) AS
  SELECT
    t.*,
    r.onway_num,
    r.suggest_fill_num,
    r.total_price,
    sto.stock_quantity,
    sto.stock_val,
    fd.fill_day_code,
    ft.fill_shelf_qty
  FROM
    fe_dwd.shelf_tmp t
    LEFT JOIN fe_dwd.requirement_shelf_tmp r
      ON t.shelf_id = r.shelf_id
    LEFT JOIN fe_dwd.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
    LEFT JOIN fe_dwd.`dwd_sf_shelf_fill_day_config` fd
        ON t.shelf_id = fd.shelf_id
        AND 1 = SUBSTRING(fd.fill_day_code,@day_num,1) 
        AND fd.data_flag = 1
    LEFT JOIN fe_dwd.fill_shelf_qty_tmp ft
        ON t.manager_id = ft.manager_id
  WHERE ! ISNULL(t.shelf_id);
-- 货架商品维度货道库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.slot_tmp;
CREATE TEMPORARY TABLE fe_dwd.slot_tmp (KEY (shelf_id, product_id))
SELECT
        t.shelf_id, 
        t.product_id, 
        SUM(IF(t.stock_num > 0, t.stock_num, 0)) stock_num, 
        SUM(t.slot_capacity_limit) slot_capacity_limit, 
        COUNT(*) slots, 
        SUM(t.stock_num > 0) slots_sto
FROM
        fe_dwd.`dwd_shelf_machine_slot_type` t
WHERE  ! ISNULL(t.product_id)
        AND ! ISNULL(t.shelf_id)
GROUP BY t.shelf_id, t.product_id;
-- 货架商品维度汇总表
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp(
    KEY (shelf_id, product_id)
  )
SELECT
        t.detail_id,
        t.product_id,
        b.business_name,
        b.manager_id,
        t.shelf_id,
        CASE
                WHEN t.SALES_FLAG IN (1,2,3) 
                        THEN '爆畅平'
                WHEN t.SALES_FLAG IN (4,5) 
                        THEN '滞销'
                ELSE NULL
       END AS sales_flag_classify,-- 划分三类
        t.shelf_fill_flag,
        t.stock_quantity,
        t.sale_price,
        t.MAX_QUANTITY,
        b.supplier_id,
        b.prewarehouse_code,
        b.supplier_type,
        re.onway_num,
        re.suggest_fill_num,
        re.start_suggest_fill_num,
        re.slot_error_num,
        re.fill_order_day,
        q.day_sale_qty,
        p.FILL_MODEL,
        IF(t.add_time >= @pre_day30,t.add_time,NULL) AS add_time,
        f.create_time,
        st.stock_num AS slot_stock_num, 
        st.slot_capacity_limit, 
        st.slots, 
        st.slots_sto,
        sms.stock_num AS sec_stock_num,
        fl.fill_time
FROM
        fe_dwd.`dwd_shelf_product_day_all` t
        STRAIGHT_JOIN fe_dwd.shelf_tmp b
                ON t.shelf_id = b.shelf_id
        LEFT JOIN fe_dwd.requirement_tmp re 
                ON t.shelf_id = re.shelf_id 
                AND t.product_id = re.product_id 
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` q
                ON t.shelf_id = q.shelf_id 
                AND t.product_id = q.product_id 
        STRAIGHT_JOIN fe_dwd.`dwd_product_base_day_all` p
                ON t.product_id = p.product_id
        LEFT JOIN fe_dwd.first_fill_tmp f
                ON t.shelf_id = f.shelf_id 
                AND t.product_id = f.product_id 
        LEFT JOIN fe_dwd.slot_tmp st
                ON t.shelf_id = st.shelf_id 
                AND t.product_id = st.product_id 
        LEFT JOIN fe_dwd.`dwd_shelf_machine_second_info` sms
                ON t.shelf_id = sms.shelf_id 
                AND t.product_id = sms.product_id 
        LEFT JOIN fe_dm.`dm_op_shelf_product_fill_last_time` fl
                ON t.shelf_id = fl.shelf_id 
                AND t.product_id = fl.product_id 
        LEFT JOIN fe_dwd.abnormal_tmp abt
                ON t.shelf_id = abt.shelf_id 
                AND t.product_id = abt.product_id 
WHERE ((b.shelf_type != 7 AND t.SHELF_FILL_FLAG = 1) OR b.shelf_type = 7)   -- 自贩机停补和正常补的都要，其他货架只看正常补货
        AND abt.shelf_id IS NULL
;
-- 地区正常补货货架数\缺货货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`area_product_tmp`;
CREATE TEMPORARY TABLE fe_dwd.area_product_tmp(
        KEY idx_business_name_product_id(business_name,product_id)
) AS
SELECT
        business_name,
        product_id,
        SUM(SHELF_FILL_FLAG = 1) AS normal_fill_shelf_qty,
        SUM(STOCK_QUANTITY <= 0) AS offstock_shelf_qty,
        SUM(SHELF_FILL_FLAG = 1 AND STOCK_QUANTITY <= 0) AS normal_fill_offstock_shelf_qty,
        MAX(FILL_TIME) AS FILL_TIME
FROM
        fe_dwd.shelf_product_tmp
GROUP BY business_name,product_id
;
-- 货架商品异常
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_abnormal_tmp1`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_abnormal_tmp1(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        fe_dwd.`dwd_sf_shelf_product_log` a
        JOIN fe_dwd.`shelf_product_tmp` b
                ON a.`SHELF_ID` = b.shelf_id
                AND a.`PRODUCT_ID` = b.product_id
WHERE a.CREATE_TIME >= @pre_day30
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_abnormal_tmp2`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_abnormal_tmp2(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        fe_dwd.`dwd_sf_shelf_product_status_log` a
        JOIN fe_dwd.`shelf_product_tmp` b
                ON a.`SHELF_ID` = b.shelf_id
                AND a.`PRODUCT_ID` = b.product_id
WHERE a.add_time >= @pre_day30 
        AND data_flag = 1
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_abnormal_tmp3`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_abnormal_tmp3(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
        a.shelf_id,
        a.`PRODUCT_ID`
FROM
        fe_dwd. dwd_pub_import_shelf_product  a
        JOIN fe_dwd.`shelf_product_tmp` b
                ON a.`SHELF_ID` = b.shelf_id
                AND a.`PRODUCT_ID` = b.product_id
                AND SHELF_PRODUCT_IMPORT_RESULT = 9
                AND a.SHELF_FILL_FLAG IN (1,2)
WHERE a.add_time >= @pre_day30
GROUP BY a.`SHELF_ID`,a.`PRODUCT_ID`,DATE(a.add_time)    -- 同一天有变更只取一次
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_abnormal_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_abnormal_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS
SELECT
shelf_id,
product_id
FROM
(
SELECT * FROM fe_dwd.`shelf_product_abnormal_tmp1`
UNION ALL
SELECT * FROM fe_dwd.`shelf_product_abnormal_tmp2`
UNION ALL
SELECT * FROM fe_dwd.`shelf_product_abnormal_tmp3`
) a
GROUP BY shelf_id,product_id
HAVING COUNT(*) >= 3   -- 插入1次 + 变更2次
;
-- 甲乙级货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`grade12_shelf_qty_tmp`;
CREATE TEMPORARY TABLE fe_dwd.grade12_shelf_qty_tmp(
        KEY idx_business_name_product_id(business_name,product_id)
) AS
SELECT
        b.business_name,
        a.product_id,
        COUNT(*) AS grade12_shelf_qty
FROM
        fe_dwd.`dwd_shelf_product_day_all` a
        JOIN fe_dwd.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
                AND b.`SHELF_STATUS` = 2
                AND b.`shelf_type` IN (1,2,3,6,7)
                AND b.grade  IN ('甲','乙')
GROUP BY b.business_name,a.product_id
;
-- 近4周淘汰品
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`product_type_tmp`;
CREATE TEMPORARY TABLE fe_dwd.product_type_tmp(
        KEY idx_business_area_product_id(business_area,product_id)
) AS
SELECT
        DISTINCT 
        business_area,
        PRODUCT_ID
FROM
        fe_dwd.`dwd_pub_product_dim_sserp_his`
WHERE PUB_TIME > @pre_4week
        AND PRODUCT_TYPE IN  ('淘汰（替补）','退出','预淘汰','新增（免费货）')
;
-- 有出单日的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.have_fill_day_code_tmp;
CREATE TEMPORARY TABLE fe_dwd.have_fill_day_code_tmp (KEY (shelf_id)) AS
SELECT
        DISTINCT 
        shelf_id
FROM
         fe_dwd.`dwd_sf_shelf_fill_day_config` 
 WHERE data_flag = 1
 ;
-- 昨日为停补
DROP TEMPORARY TABLE IF EXISTS fe_dwd.stop_fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.stop_fill_tmp 
(KEY idx_shelf_id_product_id(shelf_id,product_id)) 
AS
SELECT
        shelf_id,
        product_id
FROM fe_dwd.`dwd_shelf_product_day_all_recent_32` 
WHERE sdate = @sdate 
        AND shelf_fill_flag = 2
;
-- 近2周，出现新近解绑货架；只统计次货架，主货架不算异动
DROP TEMPORARY TABLE IF EXISTS fe_dwd.unbind_tmp;
CREATE TEMPORARY TABLE fe_dwd.unbind_tmp 
(KEY idx_shelf_id(shelf_id)) 
AS
SELECT
        DISTINCT 
        SECONDARY_SHELF_ID AS shelf_id
FROM
        fe_dwd.`dwd_sf_shelf_relation_record`
WHERE SHELF_HANDLE_STATUS = 10
        AND UNBIND_TIME > @pre_2week
        AND DATA_FLAG = 1
;
-- 店主名下货架数
DROP TEMPORARY TABLE IF EXISTS fe_dwd.manager_shelf_qty_tmp;
CREATE TEMPORARY TABLE fe_dwd.manager_shelf_qty_tmp (KEY (MANAGER_ID)) AS
SELECT
        MANAGER_ID,
        COUNT(*) AS shelf_qty
FROM
        fe_dwd.shelf_tmp
GROUP BY manager_id
;
-- ===============================================================================================================================
-- 自贩机货道缺货
-- 当前缺货货道，排除停补，换新不少于5条货道之外，且余下能补货的，大仓覆盖货架不足150元；前置仓不足50元，直接归因停补过多
DROP TEMPORARY TABLE IF EXISTS fe_dwd.stop_fill_slot_tmp;
CREATE TEMPORARY TABLE fe_dwd.stop_fill_slot_tmp (KEY (shelf_id))
SELECT
        s.shelf_id  
FROM
        fe_dwd.`dwd_shelf_machine_slot_type` t
        JOIN fe_dwd.shelf_tmp s
                ON t.shelf_id = s.shelf_id
                AND s.shelf_type = 7
        LEFT JOIN fe_dwd.`dwd_sf_shelf_machine_product_change` pc
                ON t.slot_id = pc.slot_id
                AND pc.data_flag = 1
        LEFT JOIN fe_dwd.shelf_product_tmp d
                ON t.shelf_id = d.shelf_id
                AND t.product_id = d.product_id
GROUP BY s.shelf_id
HAVING SUM(IF((d.shelf_fill_flag = 2 OR (t.product_id = pc.product_id AND pc.change_status = 1)) OR 
((t.product_id != pc.product_id AND d.shelf_fill_flag = 1) OR (t.product_id = IFNULL(pc.product_id, t.product_id) AND d.shelf_fill_flag = 2)),1,0)) >= 5 
AND
((SUM(IF(s.supplier_type = 1 AND d.shelf_fill_flag = 1,IF(t.slot_capacity_limit -  t.stock_num > 0,t.slot_capacity_limit -  t.stock_num,0)* d.sale_price,0))  < 50) OR 
(SUM(IF(s.supplier_type != 1 AND d.shelf_fill_flag = 1,IF(t.slot_capacity_limit -  t.stock_num > 0,t.slot_capacity_limit -  t.stock_num,0)* d.sale_price,0))  < 150))
;  
-- 货架维度货道库存
DROP TEMPORARY TABLE IF EXISTS fe_dwd.slot_shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.slot_shelf_tmp (KEY (shelf_id))
SELECT
        t.shelf_id, 
        SUM(t.stock_num) stock_num, 
        SUM(t.slots) slots, 
        SUM(t.slots_sto) slots_sto
FROM
        fe_dwd.slot_tmp t
WHERE ! ISNULL(t.shelf_id)
GROUP BY t.shelf_id;
-- 货道维度缺货原因
DELETE FROM fe_dm.`dm_op_offstock_machine_slot` WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 15 DAY);
INSERT INTO fe_dm.`dm_op_offstock_machine_slot`
(
        sdate, 
        business_name, 
        shelf_id, 
        product_id, 
        slot_id, 
        sales_flag_classify, 
        shelf_fill_flag, 
        sale_price, 
        is_prewarehouse_cover, 
        shelf_status_classify,
        online_status, 
        slot_sync_status, 
        slot_status, 
        manufacturer_slot_code, 
        max_quantity, 
        slot_capacity_limit, 
        stock_num, 
        day_sale_qty, 
        reason_classify
)
SELECT 
        @sdate sdate, 
        s.business_name, 
        s.shelf_id, 
        t.product_id, 
        t.slot_id, 
        sp.sales_flag_classify, 
        IFNULL(sp.shelf_fill_flag, 2) shelf_fill_flag, 
        sp.sale_price, 
        s.is_prewarehouse_cover, 
        s.shelf_status_classify,
        IFNULL(sm.online_status, 0) AS online_status, 
        IFNULL(sm.slot_sync_status, 0) slot_sync_status, 
        t.slot_status, 
        t.manufacturer_slot_code, 
        sp.max_quantity, 
        t.slot_capacity_limit, 
        t.stock_num, 
        sp.day_sale_qty,
        CASE
                WHEN sp.onway_num > 0 || ! ISNULL(ow.shelf_id)
                        THEN '1在途订单'
                WHEN s.shelf_status_classify = '异常'
                        THEN '2货架异常' 
                WHEN IFNULL(ss.stock_num, 0) = 0 AND ISNULL(lf.shelf_id)
                        THEN '1.01无首批订单'
                WHEN sp.shelf_fill_flag = 2 OR (t.product_id = pc.product_id AND pc.change_status = 1)
                        THEN '2.3停止补货'
                WHEN sp.sec_stock_num >= sp.slot_capacity_limit 
                        THEN '2.4店主问题'
                WHEN (IFNULL(sp.onway_num, 0) > 0 && m.cancel_num IS NOT NULL && fil.shelf_id IS NULL) OR
                        (((s.supplier_type = 1 AND IFNULL(s.total_price, 0) >= 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) >= 150))  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)  && q.shelf_id IS NOT NULL)
                        THEN '4.1取消订单'
                WHEN fd.shelf_id IS NOT NULL
                        THEN '4.2店主修改订单'
                WHEN sf.shelf_id IS NOT NULL AND sp.shelf_fill_flag = 1
                        THEN '4.4新加包'
                WHEN IFNULL(sp.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num, 0) = 0 && IFNULL(sp.onway_num, 0) = 0  
                        THEN '4.5未生成补货需求' 
                WHEN sp.day_sale_qty > 2.4
                        THEN '4.6配置货道过少'
                WHEN (s.supplier_type = 1 AND IFNULL(s.total_price, 0) < 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) < 150)
                        THEN '4.7金额不足'
                WHEN hf.shelf_id IS NULL
                        THEN '5.1无出单日'
                WHEN dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req
                        THEN '5.2仓库缺货'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && sp.suggest_fill_num > p.normal_apply_num
                        THEN '5.3出单日少下了'
                WHEN sfs.shelf_id IS NOT NULL
                        THEN '5.31停补过多'
                WHEN sp.suggest_fill_num > 0 && IFNULL(fil.actual_apply_num, 0) = 0 AND sp.fill_order_day IS NOT NULL
                        THEN '5.41出单日-未下单'
                WHEN sp.suggest_fill_num > 0 && IFNULL(fil.actual_apply_num, 0) = 0 AND sp.fill_order_day IS NULL
                        THEN '5.42非出单日-未下单'
--                 判断完后再更新原因'5.5操作问题'
                ELSE '6原因不明' 
        END reason_classify
FROM
        fe_dwd.`dwd_shelf_machine_slot_type` t
        JOIN fe_dwd.shelf_tot_tmp s
                ON t.shelf_id = s.shelf_id
                AND s.shelf_type = 7
        LEFT JOIN fe_dwd.last_fill_time_tmp lf
                ON t.shelf_id = lf.shelf_id
        LEFT JOIN fe_dwd.`dwd_shelf_machine_info` sm
                ON t.shelf_id = sm.shelf_id
        LEFT JOIN fe_dwd.shelf_product_tmp sp
                ON t.shelf_id = sp.shelf_id
                AND t.product_id = sp.product_id
        LEFT JOIN fe_dwd.fill_onway_tmp ow 
                ON t.shelf_id = ow.shelf_id 
                AND t.product_id = ow.product_id 
        JOIN fe_dwd.slot_shelf_tmp ss
                ON t.shelf_id = ss.shelf_id
        LEFT JOIN fe_dwd.`dwd_sf_shelf_machine_product_change` pc
                ON t.slot_id = pc.slot_id
                AND pc.data_flag = 1
        LEFT JOIN fe_dm.dm_op_dc_reqsto dcs
                ON s.supplier_id = dcs.supplier_id
                AND t.product_id = dcs.product_id
                AND dcs.sdate = @sdate
        LEFT JOIN fe_dm.dm_op_pwh_reqsto whs
                ON s.supplier_id = whs.warehouse_id
                AND t.product_id = whs.product_id
                AND whs.sdate = @sdate
        LEFT JOIN fe_dwd.fill_tmp fil 
                ON t.shelf_id = fil.shelf_id 
                AND t.product_id = fil.product_id 
        LEFT JOIN fe_dwd.fill_cancel_tmp m
                ON t.shelf_id = m.shelf_id 
                AND t.product_id = m.product_id 
        LEFT JOIN fe_dwd.stop_fill_tmp sf
                ON t.shelf_id = sf.shelf_id
                AND t.product_id = sf.product_id
        LEFT JOIN fe_dwd.apply_fill_diff_tmp fd
                ON t.shelf_id = fd.shelf_id
                AND t.product_id = fd.product_id
        LEFT JOIN fe_dwd.cancel_tmp q
                ON t.shelf_id = q.shelf_id 
                AND t.product_id = q.product_id 
        LEFT JOIN fe_dwd.have_fill_day_code_tmp hf
                ON t.shelf_id = hf.shelf_id
        LEFT JOIN  fe_dwd.stop_fill_slot_tmp sfs
                ON t.shelf_id = sfs.shelf_id
        LEFT JOIN fe_dwd.uni_fill_date_tmp o
                ON t.shelf_id = o.shelf_id
        LEFT JOIN fe_dwd.7days_fill_tmp p
                ON o.fill_date = p.apply_date
                AND t.shelf_id = p.shelf_id
                AND t.product_id = p.product_id
WHERE t.stock_num <= 0
        AND ! ISNULL (t.product_id)
        AND ! ISNULL (t.shelf_id)
        AND ! ISNULL (t.slot_id)
;
-- 更新原因是“操作问题”的货道
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`slot_error_tmp`;
CREATE TEMPORARY TABLE fe_dwd.slot_error_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        a.product_id,
        sp.slot_error_num,
        GROUP_CONCAT(a.slot_id) AS com_slot_id
FROM
        fe_dm.`dm_op_offstock_machine_slot` a
        JOIN fe_dwd.shelf_product_tmp sp
                ON a.shelf_id = sp.shelf_id
                AND a.product_id = sp.product_id
WHERE sdate = @sdate
        AND reason_classify = '6原因不明'
        AND slot_error_num >= 1
GROUP BY a.shelf_id,a.product_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`slot_error_num_tmp`;
CREATE TEMPORARY TABLE fe_dwd.slot_error_num_tmp 
AS 
SELECT 
        DISTINCT 
        SUBSTRING_INDEX(SUBSTRING_INDEX(a.`com_slot_id`,',',b.`number`),',',-1) AS slot_id
FROM
        fe_dwd.slot_error_tmp a
        JOIN fe_dwd.`dwd_pub_number` b
                ON b.number <= IF((LENGTH(a.com_slot_id) - LENGTH(REPLACE(a.com_slot_id,',','')) + 1) <= a.slot_error_num,
                (LENGTH(a.com_slot_id) - LENGTH(REPLACE(a.com_slot_id,',','')) + 1),a.slot_error_num)
                AND b.number > 0
;
UPDATE fe_dm.`dm_op_offstock_machine_slot` a
        JOIN fe_dwd.slot_error_num_tmp b
                ON a.slot_id = b.slot_id
SET a.reason_classify = '5.5操作问题'
WHERE sdate = @sdate;
-- =========================================================================================================================
-- 货架商品维度缺货原因
DELETE FROM fe_dm.`dm_op_offstock_shelf_product` WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 7 DAY);
INSERT INTO fe_dm.`dm_op_offstock_shelf_product` (
        sdate,
        detail_id,
        business_name,
        zone_code,
        manager_id,
        shelf_id,
        product_id,
        shelf_type,
        shelf_level,  -- 货架等级分类(甲乙新、丙丁、null)
        product_type_class,
        sales_flag_classify,  --  销售标识分类(爆畅平、滞销、null)
        shelf_fill_flag,
        sale_price,
        prewarehouse_code,
        supplier_id,
        supplier_type,
        is_prewarehouse_cover,
        manager_type_classify,
        shelf_status_classify,        -- 货架状态分类(新装、正常、异常)
        is_box,
        inner_flag,
        ACTIVATE_TIME,
        product_add_time,
        create_time,
        MAX_QUANTITY,
        slot_capacity_limit_cum,
        onway_num,   
        stock_quantity,
        slot_stock_num,   -- 货道库存
        sec_stock_num,   -- 副柜库存
        offstock_slots,
        slots, 
        slot_offstock_rate,
        cank_stock_qty,
        suggest_fill_num,
        actual_apply_num,
        day_sale_qty,
        offstock_val,
        low_limit,    -- 目标值低线
        up_limit,     -- 目标值高线
        reason_classify,
        new_shelf_reason_classify,
        new_product_reason_classify
) 
SELECT 
        @sdate sdate, 
        t.detail_id,
        s.business_name,
        s.zone_code,
        s.manager_id,
        t.shelf_id,
        t.product_id,
        s.shelf_type,
        s.shelf_level,  -- 货架等级分类(甲乙新、丙丁、null)
        n.product_type_class,
        t.sales_flag_classify,  --  销售标识分类(爆畅平、滞销、null)
        t.shelf_fill_flag,
        t.sale_price,
        s.prewarehouse_code,
        s.supplier_id,
        s.supplier_type,
        s.is_prewarehouse_cover,
        s.manager_type_classify,
        s.shelf_status_classify,        -- 货架状态分类(新装、正常、异常)
        t.FILL_MODEL > 1 AS is_box,
        s.inner_flag,
        s.ACTIVATE_TIME,
        t.add_time AS product_add_time,
        t.create_time,
        t.MAX_QUANTITY,
        t.slot_capacity_limit AS slot_capacity_limit_cum,
        IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0)) onway_num,   
        IF(t.stock_quantity > 0,t.stock_quantity,0) stock_quantity,
        t.slot_stock_num,   -- 货道库存
        t.sec_stock_num,   -- 副柜库存
        IFNULL(t.slots,0) - IFNULL(t.slots_sto,0) AS offstock_slots,
        t.slots, 
        ROUND((IFNULL(t.slots,0) - IFNULL(t.slots_sto,0)) / IFNULL(t.slots,0),4) AS slot_offstock_rate,
        t.suggest_fill_num,
        IFNULL(fil.actual_apply_num, 0) actual_apply_num,
        IFNULL(whs.qty_sto,dcs.qty_sto) AS cank_stock_qty,
        t.day_sale_qty,
        ROUND(CASE
                WHEN t.stock_quantity > 0 THEN 0 
                WHEN t.stock_quantity <= 0 THEN t.day_sale_qty
                ELSE 0.06 
        END * t.sale_price,2) AS offstock_val,
        s.low_limit,    -- 目标值低线
        s.up_limit,     -- 目标值高线
        IF(s.shelf_type = 7 OR t.sales_flag_classify  = '滞销' OR t.sales_flag_classify IS NULL,NULL,
        CASE
                WHEN IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0)) > 0    -- 生成补货需求的时候有在途或者当天判定为缺货时有在途
                        THEN '1在途订单' 
                WHEN s.shelf_status_classify = '异常'
                        THEN '2货架异常' 
                WHEN n.product_type_class = '淘汰' || ISNULL(n.product_type_class) 
                        THEN '3.淘汰' 
                WHEN (IFNULL(t.onway_num, 0) > 0 && m.cancel_num IS NOT NULL && fil.shelf_id IS NULL) OR
                        (((s.supplier_type = 1 AND IFNULL(s.total_price, 0) >= 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) >= 150))  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)  && q.shelf_id IS NOT NULL)
                        THEN '4.1取消订单'
                WHEN fd.shelf_id IS NOT NULL
                        THEN '4.2店主修改订单'
                WHEN t.start_suggest_fill_num > 0 AND t.suggest_fill_num = 0
                        THEN '4.3高库存'
                WHEN sf.shelf_id IS NOT NULL AND t.shelf_fill_flag = 1
                        THEN '4.4新加包'
                WHEN IFNULL(t.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num, 0) = 0 && IFNULL(t.onway_num, 0) = 0  
                        THEN '4.5未生成补货需求' 
                WHEN (s.supplier_type = 1 AND IFNULL(s.total_price, 0) < 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) < 150)
                        THEN '4.7金额不足'
                WHEN hf.shelf_id IS NULL
                        THEN '5.1无出单日'
                WHEN dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req
                        THEN '5.2仓库缺货'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > p.normal_apply_num
                        THEN '5.3出单日少下了'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > 0 && fil.shelf_id IS NULL AND t.fill_order_day IS NOT NULL
                        THEN '5.41出单日-未下单'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > 0 && fil.shelf_id IS NULL AND t.fill_order_day IS NULL
                        THEN '5.42非出单日-未下单'
                ELSE '6原因不明' 
        END) AS reason_classify,
        IF(s.ACTIVATE_TIME <= @pre_day30 OR t.shelf_fill_flag = 2,NULL,
        CASE
                WHEN IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0)) > 0    -- 生成补货需求的时候有在途或者当天判定为缺货时有在途
                        THEN '1在途订单' 
                WHEN s.shelf_status_classify = '异常'
                        THEN '2货架异常' 
                WHEN ut.shelf_id IS NOT NULL
                        THEN '2.1货架异动'
                WHEN DATEDIFF(si.execute_finish_time,s.ACTIVATE_TIME) >= 1 AND lf.FILL_TIME IS NULL
                        THEN '2.2安装时间过长'
                WHEN IFNULL(ss.stock_num, 0) = 0 AND ISNULL(lf.shelf_id)
                        THEN '1.01无首批订单'
                WHEN t.sec_stock_num >= t.slot_capacity_limit 
                        THEN '2.4店主问题'
                WHEN n.product_type = '个性化商品'
                        THEN '3.0个性化商品'
                WHEN pt.business_area IS NOT NULL AND (dcs.qty_sto < 50 OR (t.fill_model > 1 AND whs.qty_sto < t.fill_model) OR (t.fill_model = 1 AND whs.qty_sto < 10))  
                        THEN '3.1淘汰-无库存'
                WHEN pt.business_area IS NOT NULL AND (dcs.qty_sto >= 50 OR (t.fill_model > 1 AND whs.qty_sto > t.fill_model) OR (t.fill_model = 1 AND whs.qty_sto > 10)) AND ow.shelf_id IS NULL AND t.suggest_fill_num > 0   
                        THEN '3.2淘汰-未补货'
                WHEN pt.business_area IS NOT NULL AND (dcs.qty_sto >= 50 OR (t.fill_model > 1 AND whs.qty_sto > t.fill_model) OR (t.fill_model = 1 AND whs.qty_sto > 10)) AND ow.shelf_id IS NULL 
                        THEN '3.3淘汰-未生成需求'
                WHEN pt.business_area IS NOT NULL
                        THEN '3.4淘汰-其他'
                WHEN n.INDATE_NP >= @pre_4week AND n.product_type = '新增（试运行）' AND ((t.fill_model = 1 AND  ap.normal_fill_shelf_qty >= 1.5 * IFNULL(sq.grade12_shelf_qty,0)) OR (t.fill_model > 1 AND  ap.normal_fill_shelf_qty >= 2.25 * IFNULL(sq.grade12_shelf_qty,0)))
                        THEN '4.01新引进-配置异常'
                WHEN (IFNULL(t.onway_num, 0) > 0 && m.cancel_num IS NOT NULL && fil.shelf_id IS NULL) OR
                        (((s.supplier_type = 1 AND IFNULL(s.total_price, 0) >= 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) >= 150))  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)  && q.shelf_id IS NOT NULL)
                        THEN '4.1取消订单'
                WHEN fd.shelf_id IS NOT NULL
                        THEN '4.2店主修改订单'
                WHEN t.start_suggest_fill_num > 0 AND t.suggest_fill_num = 0
                        THEN '4.3高库存'
                WHEN sf.shelf_id IS NOT NULL AND t.shelf_fill_flag = 1
                        THEN '4.4新加包'
                WHEN IFNULL(t.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num, 0) = 0 && IFNULL(t.onway_num, 0) = 0  
                        THEN '4.5未生成补货需求' 
                WHEN t.`add_time` >= @pre_day30 AND pa.shelf_id IS NOT NULL
                        THEN '4.50货架商品异常'
                WHEN t.`add_time` >= @pre_day30 AND (ps.is_sku_full = '过低' AND ((s.supplier_type = 1 AND IFNULL(s.total_price, 0) < 50) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) < 150)))
                        OR ps.is_sku_full = '超量' 
                        THEN '4.51货架SKU配置异常' 
                WHEN (s.supplier_type = 1 AND IFNULL(s.total_price, 0) < 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) < 150)
                        THEN '4.7金额不足'
                WHEN hf.shelf_id IS NULL
                        THEN '5.1无出单日'
                WHEN dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req
                        THEN '5.2仓库缺货'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > p.normal_apply_num
                        THEN '5.3出单日少下了'
                WHEN sf.shelf_id IS NOT NULL
                        THEN '5.31停补过多'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > 0 && fil.shelf_id IS NULL AND t.fill_order_day IS NOT NULL
                        THEN '5.41出单日-未下单'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > 0 && fil.shelf_id IS NULL AND t.fill_order_day IS NULL
                        THEN '5.42非出单日-未下单'
                ELSE '6原因不明' 
        END) AS new_shelf_reason_classify,
        IF(t.`add_time` IS NULL OR t.shelf_fill_flag = 2,NULL,
        CASE
                WHEN IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0)) > 0    -- 生成补货需求的时候有在途或者当天判定为缺货时有在途
                        THEN '1在途订单' 
                WHEN s.shelf_status_classify = '异常'
                        THEN '2货架异常' 
                WHEN ut.shelf_id IS NOT NULL
                        THEN '2.1货架异动'
                WHEN DATEDIFF(si.execute_finish_time,s.ACTIVATE_TIME) >= 1 AND lf.FILL_TIME IS NULL
                        THEN '2.2安装时间过长'
                WHEN s.ACTIVATE_TIME > @pre_day30 AND IFNULL(ss.stock_num, 0) = 0 AND ISNULL(lf.shelf_id)
                        THEN '1.01无首批订单'
                WHEN n.product_type = '个性化商品'
                        THEN '3.0个性化商品'
                WHEN pt.business_area IS NOT NULL AND (dcs.qty_sto < 50 OR (t.fill_model > 1 AND whs.qty_sto < t.fill_model) OR (t.fill_model = 1 AND whs.qty_sto < 10))  
                        THEN '3.1淘汰-无库存'
                WHEN pt.business_area IS NOT NULL AND (dcs.qty_sto >= 50 OR (t.fill_model > 1 AND whs.qty_sto > t.fill_model) OR (t.fill_model = 1 AND whs.qty_sto > 10)) AND ow.shelf_id IS NULL AND t.suggest_fill_num > 0   
                        THEN '3.2淘汰-未补货'
                WHEN pt.business_area IS NOT NULL AND (dcs.qty_sto >= 50 OR (t.fill_model > 1 AND whs.qty_sto > t.fill_model) OR (t.fill_model = 1 AND whs.qty_sto > 10)) AND ow.shelf_id IS NULL 
                        THEN '3.3淘汰-未生成需求'
                WHEN pt.business_area IS NOT NULL
                        THEN '3.4淘汰-其他'
                WHEN n.INDATE_NP >= @pre_4week AND n.product_type = '新增（试运行）' AND ((t.fill_model = 1 AND  ap.normal_fill_shelf_qty >= 2.25 * IFNULL(sq.grade12_shelf_qty,0)) OR (t.fill_model > 1 AND  ap.normal_fill_shelf_qty >= 1.5 * IFNULL(sq.grade12_shelf_qty,0)))
                        THEN '4.01新引进-配置异常'
                WHEN (IFNULL(t.onway_num, 0) > 0 && m.cancel_num IS NOT NULL && fil.shelf_id IS NULL) OR
                        (((s.supplier_type = 1 AND IFNULL(s.total_price, 0) >= 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) >= 150))  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)  && q.shelf_id IS NOT NULL)
                        THEN '4.1取消订单'
                WHEN fd.shelf_id IS NOT NULL
                        THEN '4.2店主修改订单'
                WHEN t.start_suggest_fill_num > 0 AND t.suggest_fill_num = 0
                        THEN '4.3高库存'
                WHEN sf.shelf_id IS NOT NULL AND t.shelf_fill_flag = 1
                        THEN '4.4新加包'
                WHEN IFNULL(t.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num, 0) = 0 && IFNULL(t.onway_num, 0) = 0  
                        THEN '4.5未生成补货需求' 
                WHEN pa.shelf_id IS NOT NULL
                        THEN '4.50货架商品异常'
                WHEN (ps.is_sku_full = '过低' AND ((s.supplier_type = 1 AND IFNULL(s.total_price, 0) < 50) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) < 150)))
                        OR ps.is_sku_full = '超量' 
                        THEN '4.51货架SKU配置异常' 
                WHEN (s.supplier_type = 1 AND IFNULL(s.total_price, 0) < 100) OR (s.supplier_type != 1 AND IFNULL(s.total_price, 0) < 150)
                        THEN '4.7金额不足'
                WHEN hf.shelf_id IS NULL
                        THEN '5.1无出单日'
                WHEN IF(n.INDATE_NP >= @pre_2week AND n.product_type = '新增（试运行）',dcs.qty_sto < t.MAX_QUANTITY * ap.normal_fill_offstock_shelf_qty,(dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req))  -- 针对新品
                        THEN '5.2仓库缺货'
                WHEN (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > p.normal_apply_num
                        THEN '5.3出单日少下了'
                WHEN sf.shelf_id IS NOT NULL
                        THEN '5.31停补过多'
                WHEN IF(n.INDATE_NP >= @pre_2week AND n.product_type = '新增（试运行）', dcs.qty_sto >= t.MAX_QUANTITY * ap.normal_fill_offstock_shelf_qty,(dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)) && t.suggest_fill_num > 0 && fil.shelf_id IS NULL AND t.fill_order_day IS NOT NULL
                        THEN '5.41出单日-未下单'
                WHEN IF(n.INDATE_NP >= @pre_2week AND n.product_type = '新增（试运行）', dcs.qty_sto >= t.MAX_QUANTITY * ap.normal_fill_offstock_shelf_qty,(dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)) && t.suggest_fill_num > 0 && fil.shelf_id IS NULL AND t.fill_order_day IS NULL
                        THEN '5.42非出单日-未下单'     
                ELSE '6原因不明' 
        END) AS new_product_reason_classify
FROM
        fe_dwd.shelf_product_tmp t 
        LEFT JOIN fe_dwd.fill_onway_tmp ow 
                ON t.shelf_id = ow.shelf_id 
                AND t.product_id = ow.product_id 
        LEFT JOIN fe_dwd.fill_tmp fil 
                ON t.shelf_id = fil.shelf_id 
                AND t.product_id = fil.product_id 
        STRAIGHT_JOIN fe_dwd.shelf_tot_tmp s 
                ON t.shelf_id = s.shelf_id 
        LEFT JOIN fe_dm.`dm_op_dc_reqsto` dcs 
                ON s.supplier_id = dcs.supplier_id 
                AND t.product_id = dcs.product_id 
                AND dcs.sdate = @sdate 
        LEFT JOIN fe_dm.`dm_op_pwh_reqsto` whs 
                ON s.supplier_id = whs.warehouse_id 
                AND t.product_id = whs.product_id 
                AND whs.sdate = @sdate 
        LEFT JOIN fe_dwd.normal_tmp n 
                ON t.product_id = n.product_id 
                AND s.business_name = n.business_name 
        LEFT JOIN fe_dwd.fill_cancel_tmp m
                ON t.shelf_id = m.shelf_id 
                AND t.product_id = m.product_id 
        LEFT JOIN fe_dwd.uni_fill_date_tmp o
                ON t.shelf_id = o.shelf_id
        LEFT JOIN fe_dwd.7days_fill_tmp p
                ON o.fill_date = p.apply_date
                AND t.shelf_id = p.shelf_id
                AND t.product_id = p.product_id
        LEFT JOIN fe_dwd.cancel_tmp q
                ON t.shelf_id = q.shelf_id 
                AND t.product_id = q.product_id 
--         LEFT JOIN  fe_dwd.not_fill_tmp r
--                 ON t.shelf_id = r.shelf_id 
--                 AND t.product_id = r.product_id 
        LEFT JOIN fe_dwd.apply_fill_diff_tmp fd
                ON t.shelf_id = fd.shelf_id
                AND t.product_id = fd.product_id
        LEFT JOIN fe_dwd.shelf_install_tmp si
                ON t.shelf_id = si.shelf_id
        LEFT JOIN fe_dwd.`last_fill_time_tmp` lf
                ON t.shelf_id = lf.shelf_id
        LEFT JOIN fe_dwd.`shelf_product_abnormal_tmp`pa
                ON t.shelf_id = pa.shelf_id
                AND t.product_id = pa.product_id
        LEFT JOIN fe_dwd.`product_type_tmp` pt
                ON t.business_name = pt.business_area
                AND t.product_id = pt.product_id
        LEFT JOIN fe_dwd.have_fill_day_code_tmp hf
                ON t.shelf_id = hf.shelf_id
        LEFT JOIN fe_dwd.stop_fill_tmp sf
                ON t.shelf_id = sf.shelf_id
                AND t.product_id = sf.product_id
        LEFT JOIN fe_dm.`dm_op_package_shelf` ps
                ON t.shelf_id = ps.shelf_id
                AND ps.stat_date = @sdate
        STRAIGHT_JOIN fe_dwd.area_product_tmp ap
                ON t.business_name = ap.business_name
                AND t.product_id = ap.product_id
        LEFT JOIN fe_dwd.`grade12_shelf_qty_tmp` sq
                ON t.business_name = sq.business_name
                AND t.product_id = sq.product_id
        LEFT JOIN fe_dwd.unbind_tmp ut
                ON t.shelf_id = ut.shelf_id
        LEFT JOIN fe_dwd.slot_shelf_tmp ss
                ON t.shelf_id = ss.shelf_id
WHERE (s.shelf_type IN (1,2,3,6) AND t.stock_quantity <= 0)       -- 无人货架正常补货，自贩机全部
        OR (s.shelf_type = 7 AND t.slot_stock_num <= 0)
;
-- 货架维度缺货原因
-- 新装货架分子分母
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`offstock_new_shelf_tmp`;
CREATE TEMPORARY TABLE fe_dwd.offstock_new_shelf_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        b.shelf_type,
        b.if_bind,
        COUNT(*) AS skus,
        SUM(a.stock_quantity <= 0) AS offstock_skus,
        SUM(a.slots) AS slots,
        SUM(IFNULL(a.slots,0) - IFNULL(a.slots_sto,0)) AS offstock_slots,
        ROUND(SUM(a.stock_quantity <= 0) / COUNT(*),2) AS offstock_parameter,
        b.low_limit,    -- 目标值低线
        b.up_limit     -- 目标值高线
FROM
        fe_dwd.shelf_product_tmp  a
        JOIN fe_dwd.shelf_tmp b
                ON a.shelf_id = b.shelf_id
WHERE b.ACTIVATE_TIME > @pre_day30
GROUP BY a.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`numerator_denominator_tmp`;
CREATE TEMPORARY TABLE fe_dwd.numerator_denominator_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        CASE
                WHEN a.shelf_type = 7 THEN offstock_slots
                WHEN a.if_bind = 1 THEN offstock_skus
                WHEN skus >= 50 AND offstock_parameter <= 0.2 THEN offstock_skus
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus > up_limit THEN up_limit - (skus - offstock_skus)
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus <= up_limit THEN offstock_skus
                WHEN skus < 50 AND offstock_parameter <= 0.15 THEN offstock_skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus > low_limit THEN offstock_skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus <= low_limit THEN low_limit - (skus - offstock_skus)
        END AS numerator,
        CASE
                WHEN a.shelf_type = 7 THEN slots
                WHEN a.if_bind = 1 THEN skus
                WHEN skus >= 50 AND offstock_parameter <= 0.2 THEN skus
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus > up_limit THEN up_limit
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus <= up_limit THEN skus
                WHEN skus < 50 AND offstock_parameter <= 0.15 THEN skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus > low_limit THEN skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus <= low_limit THEN low_limit
        END AS denominator,
        offstock_parameter
FROM
        fe_dwd.offstock_new_shelf_tmp a
;
-- 货架维度
DELETE FROM fe_dm.`dm_op_offstock_shelf` WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 45 DAY);
INSERT INTO fe_dm.`dm_op_offstock_shelf` (
        sdate,
        business_name,
        zone_code,
        manager_id,
        shelf_id,
        shelf_type,
        shelf_level, 
        product_type_class,
        sales_flag_classify,
        shelf_fill_flag,
        is_prewarehouse_cover,
        manager_type_classify,
        shelf_status_classify, 
        ACTIVATE_TIME,
        product_add_time,
        create_time,
        shelf_qty,
        MAX_QUANTITY,
        slot_capacity_limit_cum,
        onway_num,   
        stock_quantity,
        slot_stock_num,   -- 货道库存
        sec_stock_num,   -- 副柜库存
        offstock_ct,
        ct,
        offstock_slots,
        slots,         
        offstock_val,
        day_sale_expect,
        low_limit,    -- 目标值低线
        up_limit,     -- 目标值高线
        numerator,
        denominator,
        offstock_parameter,
        reason_classify,
        new_shelf_reason_classify,
        new_product_reason_classify
) 
SELECT
        @sdate AS sdate,
        t.business_name,
        s.zone_code,
        s.manager_id,
        t.shelf_id,
        s.shelf_type,
        s.shelf_level, 
        n.product_type_class,
        t.sales_flag_classify,-- 划分三类
        t.shelf_fill_flag,
        s.is_prewarehouse_cover,
        s.manager_type_classify,
        s.shelf_status_classify, 
        s.ACTIVATE_TIME,
        t.add_time AS product_add_time,
        MIN(t.create_time) AS create_time,
        m.shelf_qty,
        SUM(t.MAX_QUANTITY) AS MAX_QUANTITY,
        SUM(t.slot_capacity_limit) AS slot_capacity_limit_cum,
        SUM(IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0))) AS onway_num,   
        SUM(IF(t.stock_quantity > 0,t.stock_quantity,0)) AS stock_quantity,
        SUM(t.slot_stock_num) AS slot_stock_num,   -- 货道库存
        SUM(t.sec_stock_num) AS sec_stock_num,   -- 副柜库存
        SUM(t.stock_quantity <= 0) AS offstock_ct,
        COUNT(*) AS ct,
        SUM(IFNULL(t.slots,0) - IFNULL(t.slots_sto,0)) AS offstock_slots,
        SUM(t.slots) AS slots,         
        ROUND(CASE
                WHEN t.stock_quantity > 0 THEN 0 
                WHEN t.stock_quantity <= 0 THEN t.day_sale_qty
                ELSE 0.06 
        END * t.sale_price,2) AS offstock_val,
        ROUND(IFNULL(t.day_sale_qty,0.06) * t.sale_price,2) AS day_sale_expect,
        s.low_limit,    -- 目标值低线
        s.up_limit,     -- 目标值高线
        nd.numerator,
        nd.denominator,
        nd.offstock_parameter,
        osp.reason_classify,
        osp.new_shelf_reason_classify,
        osp.new_product_reason_classify
FROM
        fe_dwd.shelf_product_tmp  t
        STRAIGHT_JOIN fe_dwd.shelf_tot_tmp s 
                ON t.shelf_id = s.shelf_id 
        LEFT JOIN fe_dwd.normal_tmp n 
                ON t.product_id = n.product_id 
                AND s.business_name = n.business_name 
        STRAIGHT_JOIN fe_dwd.manager_shelf_qty_tmp m
                ON s.manager_id = m.manager_id
        LEFT JOIN fe_dwd.fill_onway_tmp ow 
                ON t.shelf_id = ow.shelf_id 
                AND t.product_id = ow.product_id 
        LEFT JOIN fe_dm.`dm_op_offstock_shelf_product` osp
                ON sdate = @sdate
                AND t.shelf_id = osp.shelf_id 
                AND t.product_id = osp.product_id 
        LEFT JOIN fe_dwd.numerator_denominator_tmp nd
                ON t.shelf_id = nd.shelf_id 
GROUP BY t.shelf_id,
        n.product_type_class,
        t.sales_flag_classify,
        t.shelf_fill_flag,
        t.add_time,
        osp.reason_classify,
        osp.new_shelf_reason_classify,
        osp.new_product_reason_classify
;
-- 地区商品维度缺货原因
DELETE FROM fe_dm.`dm_op_offstock_area_product` WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 45 DAY);
INSERT INTO fe_dm.`dm_op_offstock_area_product` (
        sdate,
        business_name,
        product_id,
        shelf_type,
        shelf_level, 
        product_type_class,
        sales_flag_classify,
        shelf_fill_flag,
        is_prewarehouse_cover,
        manager_type_classify,
        shelf_status_classify, 
        product_add_time,
        offstock_ct,
        ct,
        offstock_slots,
        slots,         
        offstock_val,
        day_sale_expect,
        reason_classify,
        new_shelf_reason_classify,
        new_product_reason_classify
) 
SELECT
        @sdate AS sdate,
        t.business_name,
        t.product_id,
        s.shelf_type,
        s.shelf_level, 
        n.product_type_class,
        t.sales_flag_classify,
        t.shelf_fill_flag,
        s.is_prewarehouse_cover,
        s.manager_type_classify,
        s.shelf_status_classify, 
        t.add_time AS product_add_time,
        SUM(t.stock_quantity <= 0) AS offstock_ct,
        COUNT(*) AS ct,
        SUM(IFNULL(t.slots,0) - IFNULL(t.slots_sto,0)) AS offstock_slots,
        SUM(t.slots) AS slots,         
        ROUND(CASE
                WHEN t.stock_quantity > 0 THEN 0 
                WHEN t.stock_quantity <= 0 THEN t.day_sale_qty
                ELSE 0.06 
        END * t.sale_price,2) AS offstock_val,
        ROUND(IFNULL(t.day_sale_qty,0.06) * t.sale_price,2) AS day_sale_expect,
        osp.reason_classify,
        osp.new_shelf_reason_classify,
        osp.new_product_reason_classify
FROM
        fe_dwd.shelf_product_tmp  t
        STRAIGHT_JOIN fe_dwd.shelf_tot_tmp s 
                ON t.shelf_id = s.shelf_id 
        LEFT JOIN fe_dwd.normal_tmp n 
                ON t.product_id = n.product_id 
                AND s.business_name = n.business_name 
        LEFT JOIN fe_dm.`dm_op_offstock_shelf_product` osp
                ON sdate = @sdate
                AND t.shelf_id = osp.shelf_id 
                AND t.product_id = osp.product_id 
GROUP BY t.business_name,
        t.product_id,
        s.shelf_type,
        s.shelf_level, 
        t.sales_flag_classify,
        t.shelf_fill_flag,
        s.is_prewarehouse_cover,
        s.manager_type_classify,
        s.shelf_status_classify,
        t.add_time,
        osp.reason_classify,
        osp.new_shelf_reason_classify,
        osp.new_product_reason_classify
;
-- ==================================================================================================
-- 缺货率
-- 货架维度
DELETE FROM fe_dm.`dm_op_offstock_rate_shelf` WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 1 YEAR);
INSERT INTO fe_dm.`dm_op_offstock_rate_shelf` (
        sdate,
        business_name,
        zone_code,
        manager_id,
        shelf_id,
        shelf_type,
        offstock_ct, -- 缺货记录数
        ct,  -- 总记录数
        offstock_slots,   -- 缺货货道数
        slots,    -- 总货道数
        machine_offstock_rate,     -- 自贩机缺货率
        shelf_numerator,    -- 无人货架+智能柜分子
        shelf_denominator,       -- 无人货架+智能柜分母
        shelf_offstock_rate,  -- 无人货架+智能柜缺货率
        2_4_numerator,  -- 2+4分子
        2_4_denominator,   -- 2+4分母  
        2_4_offstock_rate,  --  2+4缺货率
        total_shelf_numerator,    -- 全量缺货分子
        total_shelf_denominator,   -- 全量缺货分母
        total_shelf_offstock_rate, -- 全量缺货率
        new_shelf_numerator,  -- 新装分子
        new_shelf_denominator,      -- 新装分母
        new_shelf_offstock_rate,  -- 新装货架缺货率
        new_product_numerator,   -- 新品分子
        new_product_denominator,        -- 新品分母
        new_product_offstock_rate   -- 新品缺货率
)
SELECT
        sdate,
        business_name,
        zone_code,
        manager_id,
        shelf_id,
        shelf_type,
        SUM(offstock_ct) AS offstock_ct, -- 缺货记录数
        SUM(ct) AS ct,  -- 总记录数
        SUM(offstock_slots) AS offstock_slots,   -- 缺货货道数
        SUM(slots) AS slots,    -- 总货道数
        ROUND(SUM(offstock_slots) / SUM(slots),4) AS machine_offstock_rate,     -- 自贩机缺货率
        SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)) AS shelf_numerator,    -- 无人货架+智能柜分子
        SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)) AS shelf_denominator,       -- 无人货架+智能柜分母
        ROUND(SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)) / SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)),4) AS shelf_offstock_rate,  -- 无人货架+智能柜缺货率
        IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)),0) + IFNULL(0.2 * SUM(offstock_slots),0) AS 2_4_numerator,  -- 2+4分子
        IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)),0) + IFNULL(0.2 * SUM(slots),0) AS 2_4_denominator,    -- 2+4分母
        ROUND((IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)),0) + IFNULL(0.2 * SUM(offstock_slots),0)) / (IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)),0) + IFNULL(0.2 * SUM(slots),0)),4) AS 2_4_offstock_rate,  --  2+4缺货率
        SUM(IF(SHELF_FILL_FLAG = 1,offstock_ct,0)) AS total_shelf_numerator,    -- 全量缺货分子
        SUM(IF(SHELF_FILL_FLAG = 1,ct,0)) AS total_shelf_denominator,   -- 全量缺货分母
        ROUND(SUM(IF(SHELF_FILL_FLAG = 1,offstock_ct,0)) / SUM(IF(SHELF_FILL_FLAG = 1,ct,0)),4) AS total_shelf_offstock_rate, -- 全量缺货率
        numerator AS new_shelf_numerator,  -- 新装分子
        denominator AS new_shelf_denominator,      -- 新装分母
        ROUND(SUM(numerator) / SUM(denominator),4) AS new_shelf_offstock_rate,  -- 新装货架缺货率
        SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,offstock_ct,0)) AS new_product_numerator,   -- 新品分子
        SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,ct,0)) AS new_product_denominator,        -- 新品分母
        ROUND(SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,offstock_ct,0)) / SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,ct,0)),4) AS new_product_offstock_rate      -- 新品缺货率
FROM
        fe_dm.`dm_op_offstock_shelf`
WHERE sdate = @sdate
GROUP BY shelf_id
;
-- 地区商品维度
DELETE FROM fe_dm.`dm_op_offstock_rate_area_product` WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 45 DAY);
INSERT INTO fe_dm.`dm_op_offstock_rate_area_product` (
        sdate,
        business_name,
        product_id,
        product_type_class,
        offstock_ct, -- 缺货记录数
        ct,  -- 总记录数
        offstock_slots,   -- 缺货货道数
        slots,    -- 总货道数
        machine_offstock_rate,     -- 自贩机缺货率
        shelf_numerator,    -- 无人货架+智能柜分子
        shelf_denominator,       -- 无人货架+智能柜分母
        shelf_offstock_rate,  -- 无人货架+智能柜缺货率
        2_4_numerator,  -- 2+4分子
        2_4_denominator,    -- 2+4分母
        2_4_offstock_rate,  --  2+4缺货率
        total_shelf_numerator,    -- 全量缺货分子
        total_shelf_denominator,   -- 全量缺货分母
        total_shelf_offstock_rate, -- 全量缺货率
        new_product_numerator,   -- 新品分子
        new_product_denominator,        -- 新品分母
        new_product_offstock_rate      -- 新品缺货率
)
SELECT
        sdate,
        business_name,
        product_id,
        product_type_class,
        SUM(offstock_ct) AS offstock_ct, -- 缺货记录数
        SUM(ct) AS ct,  -- 总记录数
        SUM(offstock_slots) AS offstock_slots,   -- 缺货货道数
        SUM(slots) AS slots,    -- 总货道数
        ROUND(SUM(offstock_slots) / SUM(slots),4) AS machine_offstock_rate,     -- 自贩机缺货率
        SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)) AS shelf_numerator,    -- 无人货架+智能柜分子
        SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)) AS shelf_denominator,       -- 无人货架+智能柜分母
        ROUND(SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)) / SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)),4) AS shelf_offstock_rate,  -- 无人货架+智能柜缺货率
        IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)),0) + IFNULL(0.2 * SUM(offstock_slots),0) AS 2_4_numerator,  -- 2+4分子
        IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)),0) + IFNULL(0.2 * SUM(slots),0) AS 2_4_denominator,    -- 2+4分母
        ROUND((IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',offstock_ct,0)),0) + IFNULL(0.2 * SUM(offstock_slots),0)) / (IFNULL(0.8 * SUM(IF(shelf_type IN (1,2,3,6) AND sales_flag_classify = '爆畅平',ct,0)),0) + IFNULL(0.2 * SUM(slots),0)),4) AS 2_4_offstock_rate,  --  2+4缺货率
        SUM(IF(SHELF_FILL_FLAG = 1,offstock_ct,0)) AS total_shelf_numerator,    -- 全量缺货分子
        SUM(IF(SHELF_FILL_FLAG = 1,ct,0)) AS total_shelf_denominator,   -- 全量缺货分母
        ROUND(SUM(IF(SHELF_FILL_FLAG = 1,offstock_ct,0)) / SUM(IF(SHELF_FILL_FLAG = 1,ct,0)),4) AS total_shelf_offstock_rate, -- 全量缺货率
        SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,offstock_ct,0)) AS new_product_numerator,   -- 新品分子
        SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,ct,0)) AS new_product_denominator,        -- 新品分母
        ROUND(SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,offstock_ct,0)) / SUM(IF(`SHELF_FILL_FLAG` = 1 AND product_add_time >= @pre_day30,ct,0)),4) AS new_product_offstock_rate      -- 新品缺货率
FROM
        fe_dm.`dm_op_offstock_area_product`
WHERE sdate = @sdate
GROUP BY business_name,product_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_offstock_integrate',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
END$$

DELIMITER ;