CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_new_shelf_sp_offstock`()
BEGIN
-- =============================================
-- Author:	缺货
-- Create date: 2020/07/09
-- Modify date: 
-- Description:	
-- 	新装货架全量缺货损失（每天更新）
-- 
-- =============================================
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @sdate := SUBDATE(CURDATE(),1);
SET @pre_day30 := SUBDATE(@sdate,30);
SET @pre_day15 := SUBDATE(@sdate,15);
SET @pre_day45 := SUBDATE(@sdate,45);
SET @pre_2year := SUBDATE(@sdate,INTERVAL 2 YEAR);
SET @pre_6month := SUBDATE(@sdate,INTERVAL 6 MONTH);
SET @sub_day := SUBDATE(@sdate, 1);
SET @d := DAY(@sdate);
SET @add_day := ADDDATE(@sdate, 1);
SET @day_num := DAYOFWEEK(@sdate);
SET @cur_day_num := DAYOFWEEK(@add_day);
SET @pre_7day := SUBDATE(@sdate,7);
-- 货架口径
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (
KEY idx_shelf_id(shelf_id)
)
SELECT 
        a.business_name,
        a.shelf_id,
        b.prewarehouse_code,
        a.shelf_type,
        a.if_bind,
        a.type_name,
        a.REVOKE_STATUS,
        a.WHETHER_CLOSE,
        ! ISNULL(b.shelf_id) if_prewh,
        a.ACTIVATE_TIME,
        bdc.supplier_id
FROM
        fe_dwd.`dwd_shelf_base_day_all` a
        LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` b
                ON a.shelf_id = b.shelf_id
        LEFT JOIN
              (
                        SELECT
                                MAX(t.supplier_id) supplier_id,
                                b.business_area business_name
                        FROM
                                fe_dwd.`dwd_sf_supplier` t
                                LEFT JOIN fe_dwd.`dwd_sserp_zs_dc_business_area` b
                                ON t.depot_code = b.dc_code
                        WHERE t.status = 2
                                AND t.supplier_type = 2
                        GROUP BY business_name
              ) bdc
      ON a.business_name = bdc.business_name
WHERE a.ACTIVATE_TIME > @pre_day30
        AND a.shelf_type IN (1,2,3,6,7)
        AND a.SHELF_STATUS = 2
;
-- 初始订单生成时间
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
        AND order_status IN (1,2,3,4)
GROUP BY shelf_id,product_id
;
-- 在途
DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_onway_tmp;
CREATE TEMPORARY TABLE fe_dwd.fill_onway_tmp (
KEY idx_shelf_id_product_id(shelf_id,product_id)
)
SELECT
        a.shelf_id,
        a.product_id,
        SUM(actual_apply_num) AS onway_num
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` a
WHERE a.FILL_TYPE IN (1,2,3,4,7,8,9)
        AND a.order_status IN (1,2)
GROUP BY shelf_id,product_id
;
-- 自贩机货道容量
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`machine_info_tmp`;
CREATE TEMPORARY TABLE fe_dwd.machine_info_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        a.product_id,
        SUM(f.slot_capacity_limit) AS total_slot_capacity_limit,          -- 货道标配
        SUM(a.qty_sto_slot) AS slot_stock_num,    -- 货道库存
        SUM(a.qty_sto_sec) AS second_stock_num,      -- 副柜库存  
        SUM(a.qty_sto_slot) + SUM(a.qty_sto_sec) AS stock_num,        -- 库存数量
        COUNT(*) AS slots,            -- 货道数
        SUM(f.stock_num > 0) AS stock_slots
FROM
        fe_dm.`dm_op_sp_shelf7_stock3` a
        JOIN fe_dwd.`dwd_shelf_machine_slot_type` f
                ON a.shelf_id = f.shelf_id
                AND a.product_id = f.product_id
GROUP BY a.shelf_id,a.product_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.out_tmp;
CREATE TEMPORARY TABLE fe_dwd.out_tmp (
KEY (depot_code, product_code2)
)
SELECT
        t.warehouse_number AS depot_code,
        t.product_bar AS product_code2,
        t.fbaseqty AS cank_stock_qty
FROM
        fe_dwd.`dwd_pj_outstock2_day` t
WHERE t.fproducedate = @sub_day
        AND t.fbaseqty > 0
        AND ! ISNULL(t.warehouse_number)
        AND ! ISNULL(t.product_bar);
	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dc_tmp;
CREATE TEMPORARY TABLE fe_dwd.dc_tmp (
KEY (supplier_id, product_id)
)
SELECT
        su.supplier_id,
        t.depot_code,
        p.product_id,
        t.cank_stock_qty
FROM
        fe_dwd.out_tmp t
        JOIN fe_dwd.`dwd_product_base_day_all` p
                ON t.product_code2 = p.product_code2
        JOIN fe_dwd.`dwd_sf_supplier` su
                ON t.depot_code = su.depot_code
                AND su.data_flag = 1
WHERE ! ISNULL(su.supplier_id)
        AND ! ISNULL(p.product_id);
	
DROP  TABLE IF EXISTS test.`supplier_shelf_tmp`;
CREATE  TABLE test.`supplier_shelf_tmp` (
        shelf_id INT(8),
        supplier_id INT(8),
        depot_code VARCHAR(20),
        supplier_type TINYINT(2),
        PRIMARY KEY `idx_shelf_id` (`shelf_id`)
        ) ;
INSERT INTO test.`supplier_shelf_tmp`
SELECT
        a.shelf_id,
        a.supplier_id,
        d.depot_code,
        CASE 
                WHEN e.product_supplier_type = 1 THEN 9
                WHEN e.product_supplier_type = 2 THEN 2
                ELSE 1
        END AS supplier_type
  FROM
        fe_dwd.shelf_tmp a
        LEFT JOIN fe_dwd.`dwd_sf_shelf_product_supply_info` e
                ON a.shelf_id = e.shelf_id
        LEFT JOIN fe_dwd.`dwd_sf_supplier` d
                ON a.supplier_id = d.SUPPLIER_ID
WHERE e.data_flag = 1
;	
DROP TEMPORARY TABLE IF EXISTS fe_dwd.requirement_tmp;
CREATE TEMPORARY TABLE fe_dwd.requirement_tmp (
KEY idx_shelf_id_product_id(shelf_id, product_id)
)
SELECT
        shelf_id,
        product_id,
        supplier_id,
        depot_code,
        supplier_type,
        stock_num,
        onway_num,
        suggest_fill_num,
        cank_stock_qty,
        total_price
FROM
(
--         自贩机 + 动态柜
        SELECT
                t.shelf_id,
                t.product_id,
                MAX(t.supplier_id) supplier_id,
                MAX(dc.depot_code) depot_code,
                MAX(t.supplier_type) supplier_type,
                SUM(t.onshelf_stock) stock_num,
                SUM(t.onway_stock) onway_num,
                SUM(t.detail_suggest_fill_num) suggest_fill_num,
                SUM(IFNULL(dc.cank_stock_qty,pw.available_stock)) cank_stock_qty,
                SUM(t.total_price) total_price
        FROM
                fe_dwd.`dwd_fillorder_requirement_information_his` t
                LEFT JOIN fe_dwd.dc_tmp dc
                        ON t.supplier_id = dc.supplier_id
                        AND t.product_id = dc.product_id
                LEFT JOIN fe_dm.`dm_prewarehouse_stock_detail` pw
                        ON t.supplier_id = pw.warehouse_id
                        AND t.product_id = pw.product_id
                        AND pw.check_date = @sub_day
                JOIN fe_dwd.`dwd_shelf_base_day_all` s
                        ON t.shelf_id = s.shelf_id
                        AND s.shelf_type IN (6,7)
                        AND s.shelf_status = 2
                        AND s.type_name NOT LIKE '%静态柜%'
        WHERE t.sday = @d
                AND ! ISNULL(t.shelf_id)
                AND ! ISNULL(t.product_id)
        GROUP BY t.shelf_id,t.product_id
        UNION ALL
        -- 无人货架
        SELECT
        a.shelf_id,
        a.product_id,
        c.supplier_id,
        c.depot_code,
        c.supplier_type,
        a.stock_num,
        a.onway_num,
        a.reduce_suggest_fill_ceiling_num AS suggest_fill_num,
        a.warehouse_stock AS cank_stock_qty,
        b.total_fill_value AS total_price
        FROM
        fe_dm.`dm_op_shelf_product_fill_update_his` a
        JOIN fe_dm.`dm_op_auto_push_fill_date_his` b
                ON a.shelf_id = b.shelf_id
                AND a.cdate = @sdate
                AND b.`stat_date` = @sdate
        JOIN test.`supplier_shelf_tmp` c
                ON a.shelf_id = c.shelf_id
        UNION ALL
        -- 智能柜静态柜
        SELECT
        a.shelf_id,
        a.product_id,
        c.supplier_id,
        c.depot_code,
        c.supplier_type,
        a.stock_num,
        a.onway_num,
        a.suggest_fill_num,
        a.warehouse_stock AS cank_stock_qty,
        a.total_fill_value AS total_price
        FROM
        fe_dm.`dm_op_smart_shelf_fill_update_his` a
        JOIN test.supplier_shelf_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.cdate = @sdate
) t1
GROUP BY shelf_id, product_id
;
-- 货架商品基础信息
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_product_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        a.shelf_id,
        a.product_id,
        c.create_time,
        b.prewarehouse_code,
        CASE
                WHEN b.if_bind = 1 THEN 25
                WHEN b.shelf_type IN (1,3) THEN 35
                WHEN b.shelf_type IN (2,6,7) THEN 15
        END AS low_limit,
        CASE
                WHEN b.if_bind = 1 THEN NULL
                WHEN b.shelf_type IN (1,3) THEN 60
                WHEN b.shelf_type = 2 THEN 20
                WHEN b.shelf_type = 6 AND b.type_name LIKE '%静态%' THEN 25
                WHEN b.shelf_type = 6 AND type_name LIKE '%动态%' THEN 35
                WHEN b.shelf_type = 7 THEN 30
        END AS up_limit,
        d.total_slot_capacity_limit,
        d.slot_stock_num,
        d.second_stock_num,
        IF(shelf_type = 7,d.stock_num,a.STOCK_QUANTITY) AS STOCK_QUANTITY,
        d.stock_slots,
        d.slots,
        f.day_sale_qty,
        a.SALE_PRICE,
        b.REVOKE_STATUS,
        b.WHETHER_CLOSE,
        b.business_name,
        re.supplier_id,
        re.depot_code,
        re.supplier_type,
        re.stock_num,
        re.onway_num,
        re.suggest_fill_num,
        re.cank_stock_qty,
        b.ACTIVATE_TIME
FROM
        fe_dwd.`dwd_shelf_product_day_all` a
        JOIN fe_dwd.shelf_tmp b
                ON a.shelf_id = b.shelf_id
        LEFT JOIN fe_dwd.first_fill_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.product_id = c.product_id
        LEFT JOIN fe_dwd.`machine_info_tmp` d
                ON a.shelf_id = d.shelf_id
                AND a.product_id = d.product_id
        JOIN fe_dm.`dm_op_fill_day_sale_qty` f
                ON a.shelf_id = f.shelf_id
                AND a.product_id = f.product_id
        LEFT JOIN fe_dwd.requirement_tmp re
                ON a.shelf_id = re.shelf_id
                AND a.product_id = re.product_id
WHERE (a.SHELF_FILL_FLAG = 1 AND b.shelf_type IN (1,2,3,6)) OR b.shelf_type = 7
;
-- 当天有取消订单
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
-- 当天生成的补货订单
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
                AND r.supplier_type IN (2, 9)
        LEFT JOIN fe_dwd.fill_onway_tmp f
                ON f.shelf_id = t.shelf_id
                AND f.product_id = t.product_id
WHERE ! ISNULL(t.shelf_id)
GROUP BY t.shelf_id;
  
DROP TEMPORARY TABLE IF EXISTS fe_dwd.sto_tmp;
CREATE TEMPORARY TABLE fe_dwd.sto_tmp (KEY (shelf_id)) AS
SELECT
        t.shelf_id,
        t.stock_val_5,
        t.stock_quantity,
        t.stock_val
FROM
        fe_dm.`dm_op_flag5_shelf_stat` t
WHERE t.sdate = @sdate
        AND ! ISNULL(t.shelf_id);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.smart_shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.smart_shelf_tmp (KEY (shelf_id))
SELECT
        shelf_id,
        SUM(ALARM_QUANTITY) AS ALARM_QUANTITY
FROM
        fe_dm.`dm_op_smart_shelf_fill_update_his`
WHERE cdate = @sdate 
        AND SHELF_FILL_FLAG = 1
GROUP BY shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tot_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tot_tmp (KEY (shelf_id)) AS
SELECT
        t.*,
        r.onway_num,
        r.suggest_fill_num,
        r.total_price,
        sto.stock_val_5,
        sto.stock_quantity,
        sto.stock_val,
        sm.ALARM_QUANTITY,
        fd.fill_day_code
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
        LEFT JOIN fe_dwd.smart_shelf_tmp sm
                ON t.shelf_id = sm.shelf_id
WHERE ! ISNULL(t.shelf_id);
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
PRIMARY KEY idx_shelf_id(shelf_id)
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
    PRIMARY KEY idx_date_shelf_id_product_id(apply_date,shelf_id,product_id)
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
                AND a.FILL_TYPE IN (1,2,3,4,7,8,9)
GROUP BY a.shelf_id,a.product_id
HAVING MAX(a.order_status) = 9
        AND MIN(a.order_status) = 9
;
-- 上个出单日到今早中间的时间段，如果没有取消，且未下单
DROP TEMPORARY TABLE IF EXISTS fe_dwd.not_fill_tmp;
CREATE TEMPORARY TABLE fe_dwd.not_fill_tmp(
    KEY idx_shelf_id_product_id(shelf_id,product_id)
  )
SELECT
        DISTINCT 
        a.shelf_id,
        a.product_id
FROM
        fe_dwd.`dwd_fill_day_inc_recent_two_month` a
        JOIN fe_dwd.uni_fill_date_tmp b
                ON a.shelf_id = b.shelf_id
                AND a.apply_time >= b.fill_date
                AND a.FILL_TYPE IN (1,2,3,4,7,8,9)
;
-- 正常可补SKU数=货架商品维度正常可补SKU数+架上停补且有库存SKU数*0.5
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`shelf_sku_tmp`;
CREATE TEMPORARY TABLE fe_dwd.shelf_sku_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        SUM(a.SHELF_FILL_FLAG = 1) + SUM(a.SHELF_FILL_FLAG = 2 AND a.STOCK_QUANTITY > 0) * 0.5 AS normal_skus,
        IFNULL(SUM(MAX_QUANTITY),0) AS MAX_QUANTITY_STOCK
FROM
        fe_dwd.`dwd_shelf_product_day_all` a
        JOIN fe_dwd.shelf_tmp b
                ON a.shelf_id = b.shelf_id
GROUP BY a.shelf_id
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
GROUP BY b.shelf_id
;
-- 货架商品明细
DELETE FROM fe_dm.dm_op_new_shelf_sp_offstock WHERE sdate = @sdate OR sdate <= @pre_day15;
INSERT INTO fe_dm.dm_op_new_shelf_sp_offstock
(
        sdate,
        shelf_id,
        product_id,
        create_time,
        prewarehouse_code,
        low_limit,
        up_limit,
        total_slot_capacity_limit,
        slot_stock_num,
        second_stock_num,
        STOCK_QUANTITY,
        onway_num,
        stock_slots,
        slots,
        day_sale_qty,
        offstock_value,
        total_value,
        offstock_value_rate,
        if_stock,
        reason_classify
)
SELECT
        @sdate AS sdate,
        a.shelf_id,
        a.product_id,
        a.create_time,
        a.prewarehouse_code,
        a.low_limit,
        a.up_limit,
        a.total_slot_capacity_limit,
        a.slot_stock_num,
        a.second_stock_num,
        a.STOCK_QUANTITY,
        ow.onway_num,
        a.stock_slots,
        a.slots,
        a.day_sale_qty,
        ROUND(IF(a.STOCK_QUANTITY <= 0,a.day_sale_qty * a.SALE_PRICE,NULL),2) AS offstock_value,
        ROUND(a.day_sale_qty * a.SALE_PRICE,2) AS total_value,
        ROUND(IF(a.STOCK_QUANTITY <= 0,a.day_sale_qty * a.SALE_PRICE,NULL) / (a.day_sale_qty * a.SALE_PRICE),2) AS offstock_value_rate,
        IF(a.STOCK_QUANTITY > 0,1,0) AS if_stock,
        IF(a.STOCK_QUANTITY > 0,NULL,
        CASE
                WHEN IF(ow.onway_num > 0,ow.onway_num,IFNULL(a.onway_num, 0)) > 0 
                        THEN '在途订单'
                WHEN a.REVOKE_STATUS  != 1 OR a.WHETHER_CLOSE = 1
                        THEN '1.0货架异常'
                WHEN DATEDIFF(a.ACTIVATE_TIME,si.execute_finish_time) >= 1
                        THEN '1.2安装时间过长'
                WHEN b.PRODUCT_TYPE NOT IN ('原有','新增（试运行）')
                        THEN '1.1淘汰'
                WHEN IFNULL(a.suggest_fill_num, 0) = 0 AND IFNULL(a.onway_num, 0) > 0 AND m.cancel_num IS NOT NULL AND fil.shelf_id IS NULL
                        THEN  '取消订单'
                WHEN IFNULL(a.suggest_fill_num, 0) = 0 AND (
                        ((s.shelf_type IN (1,3) OR (s.shelf_type = 6 AND s.type_name LIKE '%动态柜%')) AND IFNULL(s.stock_quantity, 0) + IFNULL(s.onway_num, 0) >= 330) 
                        OR (s.shelf_type = 2 AND IFNULL(s.stock_quantity, 0) + IFNULL(s.onway_num, 0) >= 220)
                        AND (s.shelf_type = 6 AND s.type_name LIKE '%静态柜%' AND IFNULL(s.stock_quantity, 0) + IFNULL(s.onway_num, 0) >= s.ALARM_QUANTITY)
                        ) THEN '高库存'
                WHEN IFNULL(a.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num, 0) = 0 && IFNULL(a.onway_num, 0) = 0 
                        THEN '未生成补货需求' 
                WHEN a.suggest_fill_num > 0 && (dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req) && (s.if_prewh || IFNULL(s.total_price, 0) >= 150) 
                        THEN '仓库缺货' 
                WHEN sk.normal_skus < a.low_limit AND (sk.MAX_QUANTITY_STOCK - IFNULL(s.stock_quantity,0) -  IFNULL(s.onway_num,0)) * a.SALE_PRICE < 150
                        THEN '可补SKU不足'
                WHEN IFNULL(s.total_price, 0) < 150 && s.if_prewh = 0 
                        THEN '金额不足' 
                WHEN s.fill_day_code IS NULL
                        THEN '无出单日'
                WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && a.suggest_fill_num > p.normal_apply_num
                        THEN '上个出单日少下'
                WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req) && a.suggest_fill_num > p.normal_apply_num
                        THEN '仓库缺货'
                 WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)  && q.shelf_id IS NOT NULL
                        THEN '取消订单'
                WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && a.suggest_fill_num > 0 && r.shelf_id IS NULL
                        THEN '未下单'
                ELSE '原因不明' 
        END) AS reason_classify
FROM
        fe_dwd.shelf_product_tmp a
        JOIN fe_dwd.`dwd_pub_product_dim_sserp` b
                ON a.business_name = b.business_area
                AND a.product_id = b.product_id
        LEFT JOIN fe_dwd.fill_onway_tmp ow 
                ON a.shelf_id = ow.shelf_id 
                AND a.product_id = ow.product_id 
        LEFT JOIN fe_dwd.fill_tmp fil 
                ON a.shelf_id = fil.shelf_id 
                AND a.product_id = fil.product_id 
        JOIN fe_dwd.shelf_tot_tmp s 
                ON a.shelf_id = s.shelf_id 
        LEFT JOIN fe_dm.`dm_op_dc_reqsto` dcs 
                ON s.supplier_id = dcs.supplier_id 
                AND a.product_id = dcs.product_id 
                AND dcs.sdate = @sdate 
        LEFT JOIN fe_dm.`dm_op_pwh_reqsto` whs 
                ON s.supplier_id = whs.warehouse_id 
                AND a.product_id = whs.product_id 
                AND whs.sdate = @sdate 
        LEFT JOIN fe_dwd.fill_cancel_tmp m
                ON a.shelf_id = m.shelf_id 
                AND a.product_id = m.product_id 
        LEFT JOIN fe_dwd.uni_fill_date_tmp o
                ON a.shelf_id = o.shelf_id
        LEFT JOIN fe_dwd.7days_fill_tmp p
                ON o.fill_date = p.apply_date
                AND a.shelf_id = p.shelf_id
                AND a.product_id = p.product_id
        LEFT JOIN fe_dwd.cancel_tmp q
                ON a.shelf_id = q.shelf_id 
                AND a.product_id = q.product_id 
        LEFT JOIN  fe_dwd.not_fill_tmp r
                ON a.shelf_id = r.shelf_id 
                AND a.product_id = r.product_id 
        JOIN fe_dwd.`shelf_sku_tmp` sk
                ON a.shelf_id = sk.shelf_id
        LEFT JOIN fe_dwd.shelf_install_tmp si
                ON a.shelf_id = si.shelf_id
;
-- 货架维度
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`offstock_shelf_tmp`;
CREATE TEMPORARY TABLE fe_dwd.offstock_shelf_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        COUNT(*) AS skus,
        SUM(1 - a.if_stock) AS offstock_skus,
        ROUND(SUM(1 - a.if_stock) / COUNT(*),2) AS offstock_parameter,
        SUM(slots) - SUM(stock_slots) AS offstock_slots,
        SUM(slots) AS slots,
        low_limit,
        up_limit
FROM
        fe_dm.dm_op_new_shelf_sp_offstock  a
WHERE a.sdate = @sdate
GROUP BY a.shelf_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`numerator_denominator_tmp`;
CREATE TEMPORARY TABLE fe_dwd.numerator_denominator_tmp(
        KEY idx_shelf_id(shelf_id)
) AS 
SELECT
        a.shelf_id,
        CASE
                WHEN b.shelf_type = 7 THEN offstock_slots
                WHEN b.if_bind = 1 THEN offstock_skus
                WHEN skus >= 50 AND offstock_parameter <= 0.2 THEN offstock_skus
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus > up_limit THEN up_limit - (skus - offstock_skus)
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus <= up_limit THEN offstock_skus
                WHEN skus < 50 AND offstock_parameter <= 0.15 THEN offstock_skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus > low_limit THEN offstock_skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus <= low_limit THEN low_limit - (skus - offstock_skus)
        END AS numerator,
        CASE
                WHEN b.shelf_type = 7 THEN slots
                WHEN b.if_bind = 1 THEN skus
                WHEN skus >= 50 AND offstock_parameter <= 0.2 THEN skus
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus > up_limit THEN up_limit
                WHEN skus >= 50 AND offstock_parameter > 0.2 AND skus <= up_limit THEN skus
                WHEN skus < 50 AND offstock_parameter <= 0.15 THEN skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus > low_limit THEN skus
                WHEN skus < 50 AND offstock_parameter > 0.15 AND skus <= low_limit THEN low_limit
        END AS denominator
FROM
        fe_dwd.offstock_shelf_tmp a
        JOIN fe_dwd.shelf_tmp b
                ON a.shelf_id = b.shelf_id
;
DELETE FROM fe_dm.dm_op_new_shelf_s_offstock WHERE sdate = @sdate OR (sdate <= @pre_day45 AND ADDDATE(sdate,1) != DATE_FORMAT(ADDDATE(sdate,1),'%Y-%m-01')) OR sdate <= @pre_2year;
INSERT INTO fe_dm.dm_op_new_shelf_s_offstock
(
        sdate,
        shelf_id,
        if_stock,
        reason_classify,
        skus,
        offstock_skus,
        total_value,
        offstock_value,
        offstock_parameter,
        low_limit,
        up_limit,
        numerator,
        denominator,
        create_time,
        onway_num,
        stock_slots,
        slots
)
SELECT
        sdate,
        a.shelf_id,
        if_stock,
        reason_classify,
        COUNT(*) AS skus,
        SUM(1 - a.if_stock) AS offstock_skus,
        SUM(total_value) AS total_value,
        SUM(offstock_value) AS offstock_value,
        ROUND(SUM(1 - a.if_stock) / COUNT(*),2) AS offstock_parameter,
        low_limit,
        up_limit,
        ROUND(IF(numerator > 0,numerator,0),0) AS numerator,
        IF(denominator > 0,denominator,0) AS denominator,
        create_time,
        SUM(onway_num) AS onway_num,
        SUM(stock_slots) AS stock_slots,
        SUM(slots) AS slots
FROM
        fe_dm.dm_op_new_shelf_sp_offstock  a
        JOIN fe_dwd.numerator_denominator_tmp b
                ON a.shelf_id = b.shelf_id
GROUP BY a.shelf_id,a.if_stock,a.reason_classify
;
DELETE FROM fe_dm.dm_op_new_shelf_ap_offstock WHERE sdate = @sdate OR (sdate <= @pre_day45 AND ADDDATE(sdate,1) != DATE_FORMAT(ADDDATE(sdate,1),'%Y-%m-01')) OR sdate <= @pre_6month;
INSERT INTO fe_dm.dm_op_new_shelf_ap_offstock
(
        sdate,
        business_name,
        product_id,
        if_stock,
        reason_classify,
        skus,
        offstock_skus,
        total_value,
        offstock_value,
        offstock_parameter
)
SELECT
        sdate,
        b.business_name,
        a.product_id,
        a.if_stock,
        a.reason_classify,
        COUNT(*) AS skus,
        SUM(1 - a.if_stock) AS offstock_skus,
        SUM(total_value) AS total_value,
        SUM(offstock_value) AS offstock_value,
        ROUND(SUM(1 - a.if_stock) / COUNT(*),2) AS offstock_parameter
FROM
        fe_dm.dm_op_new_shelf_sp_offstock a
        JOIN fe_dwd.`shelf_tmp` b
                ON a.shelf_id = b.shelf_id
                AND a.sdate = @sdate
GROUP BY b.business_name,a.product_id,a.if_stock,a.reason_classify
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_new_shelf_sp_offstock',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_new_shelf_sp_offstock','dm_op_new_shelf_sp_offstock','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_new_shelf_s_offstock','dm_op_new_shelf_sp_offstock','宋英南');
CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_new_shelf_ap_offstock','dm_op_new_shelf_sp_offstock','宋英南');
END