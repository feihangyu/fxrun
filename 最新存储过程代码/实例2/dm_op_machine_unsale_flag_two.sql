CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_machine_unsale_flag_two`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @date := DATE_SUB(CURDATE(),INTERVAL 30 DAY);
SET @pre_2week_date := SUBDATE(SUBDATE(CURDATE(),INTERVAL WEEKDAY(CURDATE()) - 1 DAY ),INTERVAL 2 WEEK);
      -- 取截存的前两周二的商品类型
-- 严重滞销商品 1min44s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`d_op_shelf_product_unsale_tmp`;
SET @time_19 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.d_op_shelf_product_unsale_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT 
        t1.shelf_id,
        t1.product_id,
        t1.STOCK_QUANTITY AS unsale_stock_quantity,     -- 滞销品库存数量
        t1.STOCK_QUANTITY * t1.sale_price AS unsale_stock_value  -- 滞销品库存金额
FROM 
        fe_dwd.`dwd_shelf_product_day_all` t1
        JOIN fe_dwd.`dwd_shelf_base_day_all` t3
                ON t1.SALES_FLAG = 5 
                AND t1.NEW_FLAG = 2 
                AND t1.STOCK_QUANTITY > 0
                AND t3.SHELF_TYPE IN (1,2,3,4,5,6,8)
                AND t1.shelf_id = t3.shelf_id
;
SET @time_21 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_19--@time_21",@time_19,@time_21);
-- 最近一次补货 12s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`d_op_shelf_product_unsale_flag_tmp`;
SET @time_24 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.d_op_shelf_product_unsale_flag_tmp (
        PRIMARY KEY (`shelf_id`,`product_id`),
        KEY `idx_fill_time` (`fill_time`)
        ) AS
SELECT 
        t1.shelf_id,
        t1.product_id,
        t2.fill_type,
        t2.fill_time,
        t2.WEEK_SALE_NUM / 7 AS sale_qty_day7,
        t2.ACTUAL_FILL_NUM,
        t2.STOCK_NUM,
        t2.SUPPLIER_ID,
        t2.SUPPLIER_TYPE
FROM 
        fe_dwd.`d_op_shelf_product_unsale_tmp` t1
        JOIN fe_dm.`dm_op_shelf_product_fill_last_time` t2
                ON t1.`SHELF_ID` = t2.SHELF_ID 
                AND t1.PRODUCT_ID = t2.PRODUCT_ID 
;
SET @time_26 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_24--@time_26",@time_24,@time_26);
-- 新品选品异常 50s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`new_product_unsale_flag_tmp`;
SET @time_29 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.new_product_unsale_flag_tmp (
        KEY `idx_area_product_id` (`BUSINESS_AREA`,`product_id`)
        ) AS
SELECT 
        t3.business_name AS BUSINESS_AREA,
        t1.product_id,
        SUM(IF(t1.sales_flag = 5,1,0)) / COUNT(t1.shelf_id) AS unsale_rate
FROM 
        fe_dwd.`dwd_shelf_product_day_all` t1
        JOIN fe_dwd.`dwd_shelf_base_day_all` t3
                ON t1.`SHELF_ID` = t3.`SHELF_ID` 
WHERE t1.NEW_FLAG = 2 
        AND t1.STOCK_QUANTITY > 0
GROUP BY t3.business_name,t1.product_id
;
SET @time_31 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_29--@time_31",@time_29,@time_31);
-- 严重滞销原因标识 1s
DROP TEMPORARY TABLE IF EXISTS fe_dm.`d_op_shelf_product_unsale_res_tmp`;
SET @time_34 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dm.d_op_shelf_product_unsale_res_tmp (
        shelf_id INT(8),
        product_id INT(8),
        unsale_reason_flag TINYINT(2),
        KEY `idx_shelf_id` (`shelf_id`),
        KEY `idx_shelf_id_product_id` (`shelf_id`,product_id)
        ) ;
SET @time_36 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_34--@time_36",@time_34,@time_36);
SET @time_38 := CURRENT_TIMESTAMP();
-- 1.正常排面需求 28s
-- 库存数量<=3，且风险标识为1,2,3
-- 2.正常排面有风险
-- 库存数量<=3，且风险标识为4,5
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        a.shelf_id,   
        a.product_id,
        CASE 
                WHEN b.DANGER_FLAG IN (1,2,3)
                        THEN 1
                WHEN b.DANGER_FLAG IN (4,5)
                        THEN 2
        END AS unsale_reason_flag 
FROM 
        fe_dwd.d_op_shelf_product_unsale_tmp a
        JOIN fe_dwd.`dwd_shelf_product_day_all` b
                ON a.shelf_id = b.shelf_id
                AND a.product_id = b.product_id
                AND b.STOCK_QUANTITY <= 3
;
SET @time_40 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_38--@time_40",@time_38,@time_40);
SET @time_42 := CURRENT_TIMESTAMP();
-- 3.淘汰商品再上架 5s
-- 历史货架单品标识为淘汰，现为非淘汰
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        c.shelf_id,
        a.PRODUCT_ID,
        3 AS unsale_reason_flag
FROM 
        fe_dwd.`dwd_pub_product_dim_sserp` a
JOIN
        (
                SELECT 
                        business_area, 
                        PRODUCT_ID,
                        PRODUCT_TYPE
                FROM fe_dwd.`dwd_pub_product_dim_sserp_his` 
                WHERE PUB_TIME = @pre_2week_date
        ) b
        ON a.business_area=b.business_area AND a.PRODUCT_ID = b.PRODUCT_ID
JOIN 
        (
                SELECT 
                        t3.business_name AS `BUSINESS_AREA`,
                        t1.shelf_id,
                        t1.product_id
                FROM fe_dwd.`d_op_shelf_product_unsale_tmp` t1
                JOIN `fe_dwd`.`dwd_shelf_base_day_all` t3
                        ON t1.`SHELF_ID` = t3.`SHELF_ID` 
        ) c
        ON a.business_area=c.business_area AND a.PRODUCT_ID = c.PRODUCT_ID
WHERE b.product_type = '淘汰（替补）' AND a.product_type <> '淘汰（替补）';
SET @time_44 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_42--@time_44",@time_42,@time_44);
SET @time_46 := CURRENT_TIMESTAMP();
-- 4.选品异常 4s
-- 地区初始商品包补货，且严重滞销货架占比85%
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        DISTINCT c.shelf_id,
        a.product_id,
        4 AS unsale_reason_flag
FROM 
        fe_dwd.new_product_unsale_flag_tmp a
        JOIN
        (
                SELECT 
                        t3.business_name AS `BUSINESS_AREA`,
                        t1.shelf_id,
                        t1.product_id
                FROM fe_dwd.`d_op_shelf_product_unsale_tmp` t1
                JOIN `fe_dwd`.`dwd_shelf_base_day_all` t3
                        ON t1.`SHELF_ID` = t3.`SHELF_ID` 
        )  c     
                ON a.BUSINESS_AREA = c.BUSINESS_AREA
                AND a.product_id = c.product_id
                AND a.unsale_rate > 0.85
        JOIN fe_dwd.d_op_shelf_product_unsale_flag_tmp b
                ON c.shelf_id = b.shelf_id
                AND c.product_id = b.product_id
                AND b.fill_type = 3
;
SET @time_48 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_46--@time_48",@time_46,@time_48);
SET @time_50 := CURRENT_TIMESTAMP();
--  5：包盗损无销售 1s
-- 匹配包盗损清单（风控导入系统）
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        a.shelf_id,                                                                                                        
        b.product_id,
        5 AS unsale_reason_flag 
FROM fe_dwd.dwd_op_risk_sc_list_insert a
JOIN fe_dwd.`d_op_shelf_product_unsale_tmp` b
        ON a.shelf_id = b.shelf_id
WHERE a.version_id = 201901  
;
SET @time_52 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_50--@time_52",@time_50,@time_52);
SET @time_54 := CURRENT_TIMESTAMP();
-- 6：采购集中清理 1s
-- 采购将某些商品集中到一个货架集中清理
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        a.shelf_id,
        b.product_id,
        6 AS unsale_reason_flag 
FROM fe_dwd.dwd_op_risk_sc_list_insert  a
JOIN fe_dwd.`d_op_shelf_product_unsale_tmp` b
        ON a.shelf_id = b.shelf_id
WHERE version_id = 201902 
;
SET @time_56 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_54--@time_56",@time_54,@time_56);
SET @time_58 := CURRENT_TIMESTAMP();
-- 7.店主异常调货 1s
-- 订单类型：来源地货架
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        7 AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type = 7;
SET @time_60 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_58--@time_60",@time_58,@time_60);
SET @time_62 := CURRENT_TIMESTAMP();
-- 8.撤架    
-- 订单类型：撤架   
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        8 AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type = 4;
SET @time_64 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_62--@time_64",@time_62,@time_64);
SET @time_66 := CURRENT_TIMESTAMP();
--  9.销售下滑 4s
-- 订单类型：系统触发\人工申请\前置站调货\要货,且当前日期-最后一次下单时间>=30天
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        IF(
                t1.ACTUAL_FILL_NUM<=(
                CASE WHEN t1.sale_qty_day7 >= 0.71 THEN 17 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 >= 0.43 THEN 18 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 >= 0.14 THEN 18 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 >= 0.07 THEN 23 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 < 0.07 THEN 2 - t1.STOCK_NUM
                END
                ), 
                9,
                (
                        CASE WHEN t1.fill_type = 2 THEN 10 
                                WHEN t1.fill_type = 1 THEN 11 
                                WHEN t1.fill_type = 9 THEN 12 
                                WHEN t1.fill_type = 8 THEN 13 
                        END
                )
        ) AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type IN (1,2,8,9)
        AND t1.fill_time <= @date;
SET @time_68 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_66--@time_68",@time_66,@time_68);
SET @time_70 := CURRENT_TIMESTAMP();
-- 10.系统逻辑异常 1s
-- 订单类型：系统触发，且当前日期-最后一次下单时间<30天
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        10 AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type = 2
        AND t1.fill_time > @date;
SET @time_72 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_70--@time_72",@time_70,@time_72);
SET @time_74 := CURRENT_TIMESTAMP();
-- 11.地区补货人员补货异常 1s
-- 订单类型：人工申请
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        11 AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type =1
        AND t1.fill_time > @date;
SET @time_76 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_74--@time_76",@time_74,@time_76);
SET @time_78 := CURRENT_TIMESTAMP();
-- 12.前置站站长补货异常 1s
-- 订单类型：前置站调货
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        12 AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type = 9
        AND t1.fill_time > @date;
SET @time_80 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_78--@time_80",@time_78,@time_80);
SET @time_82 := CURRENT_TIMESTAMP();
-- 13.店主补货异常 1s
-- 订单类型：要货
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        13 AS unsale_reason_flag
FROM fe_dwd.`d_op_shelf_product_unsale_flag_tmp` t1
WHERE t1.fill_type = 8 
        AND t1.fill_time > @date;
SET @time_84 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_82--@time_84",@time_82,@time_84);
SET @time_86 := CURRENT_TIMESTAMP();
-- 14.箱规格过大 8s
-- 首次补货（只补过1次货），当前严重滞销（盒装商品补货规格>10)
INSERT INTO fe_dm.d_op_shelf_product_unsale_res_tmp
SELECT 
        a.shelf_id,
        a.product_id,
        14 AS unsale_reason_flag
FROM 
        fe_dwd.`d_op_shelf_product_unsale_tmp` a
        JOIN 
        (
                SELECT 
                        t1.shelf_id
                FROM
                (
                        SELECT 
                                MAX(sdate) AS sdate,
                                shelf_id,
                                SUBSTRING_INDEX(GROUP_CONCAT(orders_cum ORDER BY sdate DESC SEPARATOR ","),",",1)  AS orders_cum
                        FROM fe_dm.dm_op_fill_shelf_stat
                        GROUP BY shelf_id
                ) t1
                WHERE t1.orders_cum = 1
        ) b
                ON a.shelf_id = b.shelf_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` c
                ON a.product_id = c.product_id
                AND c.FILL_UNIT='盒' 
                AND c.FILL_MODEL >10 
;
SET @time_88 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_86--@time_88",@time_86,@time_88);
-- 严重滞销原因标识筛选结果 13s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`unsale_flag_tmp`;
SET @time_91 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.unsale_flag_tmp (
        KEY idx_shelf_id_product_id (shelf_id,product_id)
        ) AS
SELECT 
        c.business_name AS BUSINESS_AREA,
        p.shelf_id,
        p.product_id,
        MIN(p.unsale_reason_flag) AS unsale_reason_flag      -- 按滞销品原因选择最小优先级
FROM
        fe_dm.d_op_shelf_product_unsale_res_tmp p
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` c
                ON p.shelf_id = c.shelf_id
GROUP BY p.shelf_id,p.product_id;
SET @time_93 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_91--@time_93",@time_91,@time_93);
-- 30天销售数量和销售金额 10s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`sale_day30_tmp`;
SET @time_96 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.sale_day30_tmp (
        KEY idx_shelf_id_product_id (shelf_id,product_id)
        ) AS
SELECT 
        j.SHELF_ID,
        j.product_id,
        SUM(j.quantity) AS gmv30_qty,
        SUM(j.quantity * j.sale_price) AS gmv30
FROM `fe_dwd`.`dwd_order_item_refund_day` j
WHERE j.PAY_DATE > @date 
GROUP BY j.shelf_id,j.product_id;
SET @time_98 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_96--@time_98",@time_96,@time_98);
-- 严重滞销原因分析结果(货架\商品级) 22s
TRUNCATE fe_dm.dm_op_area_shelf_product_unsale_flag;
SET @time_101 := CURRENT_TIMESTAMP();
INSERT INTO fe_dm.dm_op_area_shelf_product_unsale_flag
(
        REGION_AREA,
        BUSINESS_AREA,
        shelf_id,
        shelf_code,
        shelf_name,
        shelf_type,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        CATEGORY_NAME,
        pre_product_type,
        product_type,
--         MANAGER_ID,
        MANAGER_NAME,
        is_full_time_manager,
        DEPT_ID,
        DEPT_NAME,
        warehouse_id,
        warehouse_name,
        gmv30_qty,
        gmv30,
        unsale_stock_quantity,
        unsale_stock_value,
        unsale_reason_flag,
        SUPPLIER_ID,
        SUPPLIER_TYPE,
        FILL_TYPE,
        ACTUAL_FILL_NUM 
)	
SELECT 
        b.region_name AS REGION_AREA,
        b.business_name AS BUSINESS_AREA,
        a.shelf_id,
        b.shelf_code,
        b.shelf_name,
        b.shelf_type,
        a.product_id,
        d.PRODUCT_CODE2 AS PRODUCT_FE,
        d.PRODUCT_NAME,
        d.CATEGORY_NAME,
        g.product_type AS pre_product_type,
        IF(g.PRODUCT_TYPE='原有','原有','非原有') AS product_type,
--         b.MANAGER_ID,
        b.real_name AS MANAGER_NAME,
        manager_type AS is_full_time_manager,
        b.branch_code AS DEPT_ID,
        b.branch_name AS DEPT_NAME,
        m.prewarehouse_id AS warehouse_id,
        m.prewarehouse_name AS warehouse_name,
        j.gmv30_qty,
        j.gmv30,
        a.unsale_stock_quantity,
        a.unsale_stock_value,
        k.unsale_reason_flag,
        f.SUPPLIER_ID,
        f.SUPPLIER_TYPE,
        f.FILL_TYPE,
        f.ACTUAL_FILL_NUM
FROM 
        fe_dwd.d_op_shelf_product_unsale_tmp a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` d
                ON a.`PRODUCT_ID` = d.`PRODUCT_ID`
        LEFT JOIN fe_dwd.d_op_shelf_product_unsale_flag_tmp f
                ON a.SHELF_ID = f.SHELF_ID 
                AND a.product_id = f.product_id
        LEFT  JOIN fe_dwd.dwd_pub_product_dim_sserp g
                ON b.`business_name` = g.business_area 
                AND a.PRODUCT_ID = g.PRODUCT_ID
        LEFT JOIN fe_dwd.sale_day30_tmp j
                ON a.shelf_id = j.shelf_id
                AND a.product_id = j.product_id
        LEFT JOIN fe_dwd.unsale_flag_tmp k
                ON k.shelf_id = a.SHELF_ID
                AND k.product_id = a.product_id
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` m
                ON a.shelf_id = m.shelf_id
;
SET @time_103 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_101--@time_103",@time_101,@time_103);
-- ============================================================================================
-- 自贩机滞销原因
-- 货道数 1s
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.slot_tmp;
SET @time_106 := CURRENT_TIMESTAMP();
  CREATE TEMPORARY TABLE fe_dwd.slot_tmp (
    PRIMARY KEY (shelf_id, product_id)
  ) AS
SELECT
        t.shelf_id,
        t.product_id,
        COUNT(*) slots,
        SUM(IF(t.slot_status IN (1,4),1,0)) AS slots_1_4,   -- 剔除故障和停用的货道数
        SUM(IF(t.slot_status = 2,1,0)) AS slots_2,      -- 故障的货道数
        SUM(IF(t.slot_status = 3,1,0)) AS slots_3        -- 停用的货道数
FROM
        fe_dwd.`dwd_shelf_machine_slot_type` t
WHERE ! ISNULL(t.shelf_id)
        AND ! ISNULL(t.product_id)
GROUP BY t.shelf_id,t.product_id
;
SET @time_108 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_106--@time_108",@time_106,@time_108);
-- 自贩机基础信息 4s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`machine_info_tmp`;
SET @time_111 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.machine_info_tmp(
        KEY idx_shelf_id_product_id(shelf_id,product_id)
) AS 
SELECT
        DISTINCT
        a.shelf_id,
        a.product_id,
        b.type_name AS machine_type,         -- 机器类型
        IFNULL(d.slots,0) AS slots,            -- 货道数
        IFNULL(d.slots_1_4,0) AS slots_1_4,
        IFNULL(d.slots_2,0) AS slots_2,
        IFNULL(d.slots_3,0) AS slots_3,
        a.qty_sto_slot + a.qty_sto_sec AS stock_num,      -- 库存数
        (a.qty_sto_slot + a.qty_sto_sec) * sale_price AS stock_value   -- 库存金额
FROM
        fe_dm.dm_op_sp_shelf7_stock3 a
        JOIN fe_dwd.`dwd_shelf_machine_info` b
                ON a.shelf_id = b.shelf_id
        LEFT JOIN  fe_dwd.slot_tmp d
                ON a.shelf_id = d.shelf_id
                AND a.product_id = d.product_id
;
SET @time_113 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_111--@time_113",@time_111,@time_113);
-- 自贩机严重滞销商品 1s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`machine_unsale_tmp`;
SET @time_116 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.machine_unsale_tmp (
        KEY idx_shelf_id_product_id(shelf_id,product_id)
        ) AS
SELECT 
        t1.shelf_id,
        t1.product_id,
        t1.machine_type,
        t1.slots,
        t1.slots_1_4,
        t1.slots_2,
        t1.slots_3,
        t1.stock_num AS unsale_stock_quantity,     -- 滞销品库存数量
        t1.stock_value AS unsale_stock_value  -- 滞销品库存金额
FROM 
        fe_dwd.machine_info_tmp t1
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` t2
                ON t2.SALES_FLAG = 5 
                AND t2.NEW_FLAG = 2 
                AND t1.stock_num > 0
                AND t1.SHELF_ID = t2.SHELF_ID 
                AND t1.PRODUCT_ID = t2.PRODUCT_ID
;
SET @time_118 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_116--@time_118",@time_116,@time_118);
-- 补货订单 5s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`machine_fill_tmp`;
SET @time_121 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.machine_fill_tmp (
        PRIMARY KEY (`shelf_id`,`product_id`),
        KEY `idx_fill_time` (`fill_time`)
        ) AS
SELECT 
        t1.shelf_id,
        t1.product_id,
        t2.fill_type,
        t2.fill_time,
        t2.WEEK_SALE_NUM / 7 AS sale_qty_day7,
        t2.ACTUAL_FILL_NUM,
        t2.STOCK_NUM,
        t2.SUPPLIER_ID,
        t2.SUPPLIER_TYPE
FROM
        fe_dwd.machine_unsale_tmp t1
        JOIN fe_dm.`dm_op_shelf_product_fill_last_time` t2
                ON t1.`SHELF_ID` = t2.SHELF_ID 
                AND t1.PRODUCT_ID = t2.PRODUCT_ID
;
SET @time_123 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_121--@time_123",@time_121,@time_123);
-- 自贩机严重滞销原因标识 1s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`d_op_machine_unsale_res_tmp`;
SET @time_126 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.d_op_machine_unsale_res_tmp (
        shelf_id INT(8),
        product_id INT(8),
        unsale_reason_flag TINYINT(2),
        KEY `idx_shelf_id` (`shelf_id`),
        KEY `idx_shelf_id_product_id` (`shelf_id`,product_id)
        ) ;
SET @time_128 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_126--@time_128",@time_126,@time_128);
SET @time_130 := CURRENT_TIMESTAMP();
-- 1.正常排面需求 1s
-- 货道数=1，且货道非停用、非故障，且库存数量<=3，且风险标识为1,2,3
-- 2.正常排面有风险
-- 货道数=1，且货道非停用、非故障，且库存数量<=3，且风险标识为4,5
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT
        a.shelf_id,                                                                                                        
        a.product_id,
        CASE 
                WHEN d.DANGER_FLAG IN (1,2,3)
                        THEN 1
                WHEN d.DANGER_FLAG IN (4,5)
                        THEN 2
        END AS unsale_reason_flag 
FROM
        fe_dwd.machine_unsale_tmp a
        JOIN `fe_dwd`.`dwd_shelf_product_day_all` d
                ON d.shelf_id = a.shelf_id
                AND d.product_id = a.product_id
WHERE a.slots_1_4 = 1 AND a.unsale_stock_quantity <= 3
;
SET @time_132 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_130--@time_132",@time_130,@time_132);
SET @time_134 := CURRENT_TIMESTAMP();
-- 3.货道全部停用 1s
-- 总货道数=停用货道数 and 货道数 >0
-- 4.货道全部故障
-- 总货道数=故障货道数 and 货道数 >0
-- 5.货道故障或停用
-- 总货道数=停用货道数+故障货道数 and 货道数 >0
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT
        a.shelf_id,                                                                                                        
        a.product_id,
        CASE
                WHEN a.slots = a.slots_3 AND a.slots > 0
                        THEN 3
                WHEN a.slots = a.slots_2 AND a.slots > 0
                        THEN 4
                WHEN a.slots = a.slots_2 + a.slots_3 AND a.slots > 0
                        THEN 5
         END AS unsale_reason_flag 
FROM
        fe_dwd.machine_unsale_tmp a
;
SET @time_136 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_134--@time_136",@time_134,@time_136);
SET @time_138 := CURRENT_TIMESTAMP();
-- 6.货道配置异常 1s
-- 总货道数>2
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT
        a.shelf_id,                                                                                                        
        a.product_id,
        6 AS unsale_reason_flag 
FROM
        fe_dwd.machine_unsale_tmp a
WHERE a.slots >= 2
;
SET @time_140 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_138--@time_140",@time_138,@time_140);
SET @time_142 := CURRENT_TIMESTAMP();
-- 7.无货道售卖，需调走 1s
-- 总货道数=0
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT
        a.shelf_id,                                                                                                        
        a.product_id,
        7 AS unsale_reason_flag 
FROM
        fe_dwd.machine_unsale_tmp a
WHERE a.slots = 0
;
SET @time_144 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_142--@time_144",@time_142,@time_144);
SET @time_146 := CURRENT_TIMESTAMP();
--  8.销售下滑 1s
-- 订单类型：系统触发\人工申请\前置站调货\要货,且当前日期-最后一次下单时间>=30天
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        IF(
                t1.ACTUAL_FILL_NUM<=(
                CASE WHEN t1.sale_qty_day7 >= 0.71 THEN 17 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 >= 0.43 THEN 18 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 >= 0.14 THEN 18 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 >= 0.07 THEN 23 * t1.sale_qty_day7 - t1.STOCK_NUM
                        WHEN t1.sale_qty_day7 < 0.07 THEN 2 - t1.STOCK_NUM
                END
                ), 
                8,
                (
                        CASE WHEN t1.fill_type = 2 THEN 9 
                                WHEN t1.fill_type = 1 THEN 10 
                                WHEN t1.fill_type = 9 THEN 11 
                                WHEN t1.fill_type = 8 THEN 12 
                        END
                )
        ) AS unsale_reason_flag
FROM fe_dwd.machine_fill_tmp t1
WHERE t1.fill_type IN (1,2,8,9)
        AND t1.fill_time <= @date;
SET @time_148 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_146--@time_148",@time_146,@time_148);
SET @time_150 := CURRENT_TIMESTAMP();
-- 9.系统逻辑异常 1s
-- 订单类型：系统触发，且当前日期-最后一次下单时间<30天
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        9 AS unsale_reason_flag
FROM fe_dwd.machine_fill_tmp t1
WHERE t1.fill_type = 2
        AND t1.fill_time > @date;
SET @time_152 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_150--@time_152",@time_150,@time_152);
SET @time_154 := CURRENT_TIMESTAMP();
-- 10.地区补货人员补货异常 1s
-- 订单类型：人工申请
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        10 AS unsale_reason_flag
FROM fe_dwd.`machine_fill_tmp` t1
WHERE t1.fill_type =1
        AND t1.fill_time > @date;
SET @time_156 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_154--@time_156",@time_154,@time_156);
SET @time_158 := CURRENT_TIMESTAMP();
-- 11.前置站站长补货异常 1s
-- 订单类型：前置站调货
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        11 AS unsale_reason_flag
FROM fe_dwd.`machine_fill_tmp` t1
WHERE t1.fill_type = 9
        AND t1.fill_time > @date;
SET @time_160 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_158--@time_160",@time_158,@time_160);
SET @time_162 := CURRENT_TIMESTAMP();
-- 12.店主补货异常 1s
-- 订单类型：要货
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT 
        t1.shelf_id,
        t1.product_id,
        12 AS unsale_reason_flag
FROM fe_dwd.`machine_fill_tmp` t1
WHERE t1.fill_type = 8 
        AND t1.fill_time > @date;
SET @time_164 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_162--@time_164",@time_162,@time_164);
SET @time_166 := CURRENT_TIMESTAMP();
-- 13.选品异常 1s
-- 地区初始商品包补货，且严重滞销货架占比85%
INSERT INTO fe_dwd.d_op_machine_unsale_res_tmp
SELECT 
        DISTINCT
        c.shelf_id,
        a.product_id,
        13 AS unsale_reason_flag
FROM 
        fe_dwd.new_product_unsale_flag_tmp a
        JOIN
        (
                SELECT 
                        t3.business_name AS BUSINESS_AREA,
                        t1.shelf_id,
                        t1.product_id
                FROM fe_dwd.`machine_unsale_tmp` t1
                JOIN `fe_dwd`.`dwd_shelf_base_day_all` t3
                        ON t1.`SHELF_ID` = t3.`SHELF_ID` 
        )  c     
                ON a.BUSINESS_AREA = c.BUSINESS_AREA
                AND a.product_id = c.product_id
                AND a.unsale_rate > 0.85
        JOIN fe_dwd.machine_fill_tmp  b
                ON c.shelf_id = b.shelf_id
                AND b.fill_type = 3
;
SET @time_168 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_166--@time_168",@time_166,@time_168);
-- 自贩机严重滞销原因标识筛选结果 1s
DROP TEMPORARY TABLE IF EXISTS fe_dwd.`machine_unsale_flag_tmp`;
SET @time_171 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dwd.machine_unsale_flag_tmp (
        KEY idx_shelf_id_product_id (shelf_id,product_id)
        ) AS
SELECT 
        b.business_name AS BUSINESS_AREA,
        p.shelf_id,
        p.product_id,
        MIN(p.unsale_reason_flag) AS unsale_reason_flag      -- 按滞销品原因选择最小优先级
FROM
        fe_dwd.d_op_machine_unsale_res_tmp p
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON p.shelf_id=b.shelf_id 
WHERE p.unsale_reason_flag IS NOT NULL
GROUP BY p.shelf_id,p.product_id;
SET @time_173 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_171--@time_173",@time_171,@time_173);
-- 自贩机严重滞销原因分析结果(货架\商品级)  1s
TRUNCATE fe_dm.dm_op_machine_unsale_flag;
SET @time_176 := CURRENT_TIMESTAMP();
INSERT INTO fe_dm.dm_op_machine_unsale_flag
(
        REGION_AREA,
        BUSINESS_AREA,
        shelf_id,
        shelf_code,
        shelf_name,
        shelf_type,
        machine_type,
        slots,
        product_id,
        PRODUCT_FE,
        PRODUCT_NAME,
        CATEGORY_NAME,
        pre_product_type,
        product_type,
--         MANAGER_ID,
        MANAGER_NAME,
        is_full_time_manager,
        DEPT_ID,
        DEPT_NAME,
        warehouse_id,
        warehouse_name,
        gmv30_qty,
        gmv30,
        unsale_stock_quantity,
        unsale_stock_value,
        unsale_reason_flag,
        SUPPLIER_ID,
        SUPPLIER_TYPE,
        FILL_TYPE,
        ACTUAL_FILL_NUM 
)	
SELECT 
        b.region_name AS REGION_AREA,
        b.business_name AS BUSINESS_AREA,
        a.shelf_id,
        b.shelf_code,
        b.shelf_name,
        b.shelf_type,
        a.machine_type,
        a.slots,
        a.product_id,
        d.PRODUCT_CODE2 AS PRODUCT_FE,
        d.PRODUCT_NAME,
        d.CATEGORY_NAME,
        g.product_type AS pre_product_type,
        IF(g.PRODUCT_TYPE='原有','原有','非原有') AS product_type,
--         b.MANAGER_ID,
        b.real_name AS MANAGER_NAME,
        b.manager_type AS is_full_time_manager,
        b.branch_code AS DEPT_ID,
        b.branch_name AS DEPT_NAME,
        m.prewarehouse_id AS warehouse_id,
        m.prewarehouse_name AS warehouse_name,
        j.gmv30_qty,
        j.gmv30,
        a.unsale_stock_quantity,
        a.unsale_stock_value,
        k.unsale_reason_flag,
        f.SUPPLIER_ID,
        f.SUPPLIER_TYPE,
        f.FILL_TYPE,
        f.ACTUAL_FILL_NUM
FROM 
        fe_dwd.machine_unsale_tmp a
        JOIN `fe_dwd`.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
        JOIN `fe_dwd`.`dwd_product_base_day_all` d
                ON a.`PRODUCT_ID` = d.`PRODUCT_ID`
        LEFT JOIN fe_dwd.machine_fill_tmp f
                ON a.SHELF_ID = f.SHELF_ID 
                AND a.product_id = f.product_id
        LEFT  JOIN fe_dwd.dwd_pub_product_dim_sserp g
                ON b.`business_name` = g.business_area 
                AND a.PRODUCT_ID = g.PRODUCT_ID
        LEFT JOIN fe_dwd.sale_day30_tmp j
                ON a.shelf_id = j.shelf_id
                AND a.product_id = j.product_id
        LEFT JOIN fe_dwd.machine_unsale_flag_tmp k
                ON k.shelf_id = a.SHELF_ID
                AND k.product_id = a.product_id
        LEFT JOIN `fe_dwd`.`dwd_relation_dc_prewarehouse_shelf_day_all` m
                ON a.shelf_id = m.shelf_id
;
SET @time_178 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_machine_unsale_flag_two","@time_176--@time_178",@time_176,@time_178);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_machine_unsale_flag_two',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_area_shelf_product_unsale_flag','dm_op_machine_unsale_flag_two','宋英南');
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_machine_unsale_flag','dm_op_machine_unsale_flag_two','宋英南');
END