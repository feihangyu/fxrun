DELIMITER $$

USE `sh_process`$$

DROP PROCEDURE IF EXISTS `dm_op_offstock_five`$$

CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_offstock_five`()
BEGIN
/*
 Author: 宋英南
 Create date: 
Modify date: 2020/11/06
 Description: 无人货架+智能柜缺货
*/
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := SUBDATE(CURRENT_DATE, 1),
  @add_user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @add_day := ADDDATE(@sdate, 1);
  SET @sub_day := SUBDATE(@sdate, 1);
  SET @d_add := DAY(@add_day);
  SET @d := DAY(@sdate);
  SET @month_end_last := SUBDATE(@sdate, @d);
  SET @y_m_last := DATE_FORMAT(@month_end_last, '%Y-%m');
  SET @ym_last := DATE_FORMAT(@month_end_last, '%Y%m');
  SET @d_lm := DAY(@month_end_last);
  SET @month_start_last := SUBDATE(@month_end_last, @d_lm - 1);
  SET @sdate_m := SUBDATE(@sdate, INTERVAL 1 MONTH);
  SET @pre_day30 := SUBDATE(@sdate,30);
  SET @pre_6month := SUBDATE(@sdate,INTERVAL 6 MONTH);
  SET @day_num := DAYOFWEEK(@sdate);
  SET @cur_day_num := DAYOFWEEK(@add_day);
  SET @pre_7day := SUBDATE(@sdate,7);
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
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.normal_tmp;
  CREATE TEMPORARY TABLE fe_dwd.normal_tmp (
    KEY idx_area_product_id(product_id, business_name)
  ) AS
  SELECT
    t.product_id,
    t.business_name,
    CASE
      WHEN t.normal_flag
      THEN '原有品-持续'
      WHEN t.product_type = '原有'
      THEN '原有品-其他'
      WHEN t.product_type = '新增（试运行）'
      THEN '新品'
      ELSE '淘汰'
    END product_type_class
  FROM
    fe_dm.dm_op_dim_product_area_normal t
  WHERE t.month_id = @y_m
    AND ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id);
	
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_1--@time_2",@time_1,@time_2);
DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (KEY (shelf_id)) AS
SELECT
        t.shelf_id,
        t.business_name,
        t.MANAGER_ID,
        t.shelf_type,
        t.whether_close = 2 whether_close2,
        t.revoke_status = 1 revoke_status1,
        CASE
                WHEN t.grade IN ('甲', '乙', '新装') THEN '甲乙新'
                WHEN t.grade IN ('丙','丁') THEN '丙丁'
        END AS shelf_level,
        IF(t.manager_type = '全职店主',1,0) AS second_user_type1,
        ! ISNULL(pr.shelf_id) if_prewh,
        IFNULL(pr.supplier_id, bdc.supplier_id) supplier_id,
        CASE
                WHEN relation_flag = 1 THEN 300
                WHEN t.grade IN ('甲', '乙', '新装') THEN 180
                ELSE 110
        END sto_min,
        t.type_name
FROM
        fe_dwd.`dwd_shelf_base_day_all` t
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
                        ON t.business_name = bdc.business_name
        LEFT JOIN
                (
                        SELECT 
                                shelf_id,
                                MAX(prewarehouse_id) AS supplier_id
                        FROM
                                fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all`
                        GROUP BY shelf_id
                ) pr
                        ON t.shelf_id = pr.shelf_id
WHERE t.shelf_status = 2
        AND ! ISNULL(t.shelf_id)
        AND (t.type_name NOT LIKE "码隆%" OR t.type_name IS NULL)       -- 2020-06码隆智能柜合作商终止合作，地区已经陆续在开始撤架
        AND  t.business_name NOT IN ('山西区','冀州区','吉林区','江西区')       -- 2020-06 杨柳要求剔除4个撤城区
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_2--@time_3",@time_2,@time_3);	
	
	
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.out_tmp;
  CREATE TEMPORARY TABLE fe_dwd.out_tmp (
    KEY (depot_code, product_code2)
  )
  SELECT
    t.warehouse_number depot_code,
    t.product_bar product_code2,
    t.fbaseqty cank_stock_qty
  FROM
    fe_dwd.`dwd_pj_outstock2_day` t
  WHERE t.fproducedate = @sub_day
    AND t.fbaseqty > 0
    AND ! ISNULL(t.warehouse_number)
    AND ! ISNULL(t.product_bar);
	
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_3--@time_4",@time_3,@time_4);	
	
	
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
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_4--@time_5",@time_4,@time_5);	
DROP TABLE IF EXISTS test.`supplier_shelf_tmp`;
CREATE TABLE test.`supplier_shelf_tmp` (
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
  CREATE TEMPORARY TABLE fe_dwd.requirement_tmp (KEY (shelf_id, product_id))
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
        a.total_fill_value AS total_price,
        a.suggest_fill_num AS start_suggest_fill_num
FROM
        fe_dm.`dm_op_shelf_product_fill_update2_his` a
        JOIN test.`supplier_shelf_tmp` c
                ON a.shelf_id = c.shelf_id
                 AND a.cdate = @sdate
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_5--@time_6",@time_5,@time_6);	
	
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
-- 智能柜静态柜标配
DROP TEMPORARY TABLE IF EXISTS fe_dwd.smart_shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.smart_shelf_tmp (KEY (shelf_id))
SELECT
        a.shelf_id,
        SUM(MAX_QUANTITY) AS ALARM_QUANTITY
FROM
        fe_dm.`dm_op_shelf_product_fill_update2_his` a
        JOIN fe_dwd.`dwd_shelf_machine_info` b
                ON a.shelf_id = b.shelf_id
                AND machine_type_code = 3
                AND cdate = @sdate 
                AND a.shelf_type = 6
GROUP BY a.shelf_id
;
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_6--@time_7",@time_6,@time_7);	
	
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
	
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_7--@time_8",@time_7,@time_8);	
SET @time_9 := CURRENT_TIMESTAMP();
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
	
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_9--@time_10",@time_9,@time_10);
	
	
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
	
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_10--@time_11",@time_10,@time_11);	
	
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
    
-- 当天 店主出单数量
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
    
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_11--@time_12",@time_11,@time_12);	
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
    fd.fill_day_code,
    ft.fill_shelf_qty
  FROM
    fe_dwd.shelf_tmp t
    LEFT JOIN fe_dwd.requirement_shelf_tmp r
      ON t.shelf_id = r.shelf_id
    LEFT JOIN fe_dwd.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
    LEFT JOIN fe_dwd.smart_shelf_tmp sm
        ON t.shelf_id = sm.shelf_id
    LEFT JOIN fe_dwd.`dwd_sf_shelf_fill_day_config` fd
        ON t.shelf_id = fd.shelf_id
        AND 1 = SUBSTRING(fd.fill_day_code,@day_num,1) 
        AND fd.data_flag = 1
    LEFT JOIN fe_dwd.fill_shelf_qty_tmp ft
        ON t.manager_id = ft.manager_id
  WHERE ! ISNULL(t.shelf_id);
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.d_op_sp_avgsal30;
  CREATE TEMPORARY TABLE fe_dwd.d_op_sp_avgsal30(
    KEY (shelf_id, product_id)
  )
SELECT
        shelf_id,
        product_id,
        sal_qty_day30 AS qty_sal30,
        stock_sal_day30 AS days_sal_sto30
FROM
        fe_dwd.`dwd_shelf_product_sto_sal_day30` 
;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_product_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_product_tmp(
    KEY (shelf_id, product_id)
  )
SELECT
        t.detail_id,
        t.product_id,
        t.shelf_id,
        t.sales_flag,
        t.shelf_fill_flag,
        t.stock_quantity,
        t.sale_price,
        sal.qty_sal30,
        sal.days_sal_sto30,
        re.supplier_id,
        re.depot_code,
        re.supplier_type,
        re.stock_num,
        re.onway_num,
        re.suggest_fill_num,
        re.start_suggest_fill_num,
        re.cank_stock_qty,
        q.day_sale_qty
FROM
        fe_dwd.`dwd_shelf_product_day_all` t
        JOIN fe_dwd.shelf_tmp b
                ON t.shelf_id = b.shelf_id
        LEFT JOIN fe_dwd.d_op_sp_avgsal30 sal 
                ON t.shelf_id = sal.shelf_id 
                AND t.product_id = sal.product_id 
        LEFT JOIN fe_dwd.requirement_tmp re 
                ON t.shelf_id = re.shelf_id 
                AND t.product_id = re.product_id 
        LEFT JOIN fe_dm.`dm_op_fill_day_sale_qty` q
                ON t.shelf_id = q.shelf_id 
                AND t.product_id = q.product_id 
        LEFT JOIN fe_dwd.abnormal_tmp abt
                ON t.shelf_id = abt.shelf_id 
                AND t.product_id = abt.product_id 
WHERE ((b.shelf_type != 7 AND t.SHELF_FILL_FLAG = 1) OR b.shelf_type = 7)
        AND abt.shelf_id IS NULL
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
 SET @time_13 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_12--@time_13",@time_12,@time_13);
TRUNCATE TABLE fe_dm.dm_op_sp_offstock;
INSERT INTO fe_dm.dm_op_sp_offstock (
        detail_id,
        product_id,
        shelf_id,
        sales_flag,
        shelf_fill_flag,
        stock_quantity,
        sto_val,
        sale_price,
        qty_sal,
        gmv,
        days_sal_sto,
        offstock_val,
        supplier_id,
        depot_code,
        supplier_type,
        stock_num,
        onway_num,
        suggest_fill_num,
        suggest_fill_val,
        cank_stock_qty,
        cank_stock_val,
        actual_apply_num,
        actual_apply_val,
        business_name,
        shelf_type,
        whether_close2,
        revoke_status1,
        shelf_level,
        second_user_type1,
        if_prewh,
        bsupplier_id,
        sup_sto_flag,
        sto_min_st,
        suggest_fill_num_st,
        total_price_st,
        package_id_st,
        min_max_quantity_st,
        stock_val_5_st,
        stock_quantity_st,
        stock_val_st,
        product_type_class,
        reason_classify,
        add_user
) 
SELECT 
        t.detail_id,
        t.product_id,
        t.shelf_id,
        t.sales_flag,
        t.shelf_fill_flag,
        IF(t.stock_quantity > 0,t.stock_quantity,0) stock_quantity,
        IF(t.stock_quantity > 0,t.stock_quantity * t.sale_price,0) sto_val,
        t.sale_price,
        IFNULL(t.qty_sal30, 0) qty_sal,
        IFNULL(t.qty_sal30 * t.sale_price, 0) gmv,
        IFNULL(t.days_sal_sto30, 0) days_sal_sto,
        CASE
                WHEN t.stock_quantity > 0 THEN 0 
                WHEN t.stock_quantity <= 0 THEN t.day_sale_qty
                ELSE 0.06 
        END * t.sale_price offstock_val,
        t.supplier_id,
        t.depot_code,
        t.supplier_type,
        IFNULL(t.stock_num, 0) stock_num,
        IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0)) onway_num,
        t.suggest_fill_num suggest_fill_num,
        t.suggest_fill_num * t.sale_price suggest_fill_val,
        IFNULL(t.cank_stock_qty, 0) cank_stock_qty,
        IFNULL(t.cank_stock_qty * t.sale_price,0) cank_stock_val,
        IFNULL(fil.actual_apply_num, 0) actual_apply_num,
        IFNULL(fil.actual_apply_num * t.sale_price,0) actual_apply_val,
        s.business_name,
        s.shelf_type,
        s.whether_close2,
        s.revoke_status1,
        s.shelf_level,
        IFNULL(s.second_user_type1, 0) second_user_type1,
        IFNULL(s.if_prewh, 0) if_prewh,
        s.supplier_id bsupplier_id,
        COALESCE(dcs.qty_sto >= dcs.qty_req,whs.qty_sto >= whs.qty_req,1) sup_sto_flag,
        s.sto_min sto_min_st,
        IFNULL(s.suggest_fill_num, 0) suggest_fill_num_st,
        IFNULL(s.total_price, 0) total_price_st,
        NULL package_id_st,
        NULL min_max_quantity_st,
        IFNULL(s.stock_val_5, 0) stock_val_5_st,
        IFNULL(s.stock_quantity, 0) stock_quantity_st,
        IFNULL(s.stock_val, 0) stock_val_st,
        n.product_type_class,
        IF(t.stock_quantity > 0,NULL,
                CASE
                        WHEN IF(ow.onway_num > 0,ow.onway_num,IFNULL(t.onway_num, 0)) > 0 
                        THEN '2在途订单' 
                        WHEN ! s.whether_close2 || ! s.revoke_status1
                        THEN '1货架异常' 
                        WHEN n.product_type_class = '淘汰' || ISNULL(n.product_type_class) 
                        THEN '淘汰' 
                        WHEN IFNULL(t.suggest_fill_num, 0) = 0 && IFNULL(t.onway_num, 0) > 0 && m.cancel_num IS NOT NULL && fil.shelf_id IS NULL
                        THEN '2.01取消订单'
--                         WHEN IFNULL(t.suggest_fill_num, 0) = 0 && (((s.shelf_type IN (1,3) OR (s.shelf_type = 6 AND s.type_name LIKE '%动态柜%')) AND IFNULL(s.stock_quantity, 0) + IFNULL(s.onway_num, 0) >= 330) 
--                                 OR (s.shelf_type IN (2,5) AND IFNULL(s.stock_quantity, 0) + IFNULL(s.onway_num, 0) >= 220)
--                                 AND (s.shelf_type = 6 AND s.type_name LIKE '%静态柜%' AND IFNULL(s.stock_quantity, 0) + IFNULL(s.onway_num, 0) >= s.ALARM_QUANTITY))
                        WHEN t.start_suggest_fill_num > 0 AND t.suggest_fill_num = 0
                        THEN '2.3高库存'
                        WHEN IFNULL(t.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num, 0) = 0 && IFNULL(t.onway_num, 0) = 0 
                        THEN '2.02未生成补货需求' 
                        ELSE 
                                CASE
                                        WHEN t.suggest_fill_num > 0 && (dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req) && (s.if_prewh || IFNULL(s.total_price, 0) >= 150) 
                                        THEN '3仓库缺货' 
                                        WHEN IFNULL(s.total_price, 0) < 150 && s.if_prewh = 0 && IFNULL(s.stock_quantity, 0) >= s.sto_min 
                                        THEN '4金额不足' 
                                        /*'4.1金额不足-低销'*/
                                        WHEN ( IFNULL(s.total_price, 0) < 150 || s.if_prewh) && IFNULL(s.stock_quantity, 0) + IFNULL(s.suggest_fill_num, 0) + IFNULL(s.onway_num, 0) < s.sto_min 
                                        THEN '4金额不足' 
                                        /*'4.2金额不足|可补SKU不足'*/
                                        WHEN IFNULL(s.total_price, 0) < 150 && s.if_prewh = 0 
                                        THEN '4金额不足' 
                                        /*'4.3金额不足-货架补货单'*/
                                        WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && IFNULL(fil.actual_apply_num, 0) = 0 && s.fill_day_code IS NULL && o.shelf_id IS NULL
                                        THEN '5无出单日地区未下单'
                                        WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > p.normal_apply_num
                                        THEN '5.3上个出单日少下'
                                        WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req) && t.suggest_fill_num > p.normal_apply_num
                                        THEN '3仓库缺货'
                                         WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req)  && q.shelf_id IS NOT NULL
                                        THEN '2.01取消订单'
                                        WHEN (IFNULL(s.total_price, 0) >= 150 || s.if_prewh)  && (dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req) && t.suggest_fill_num > 0 && r.shelf_id IS NULL
                                        THEN '5地区未下单'
                                        WHEN s.fill_shelf_qty >=
                                                        CASE
                                                                WHEN s.business_name IN ('北京区','陕西区') THEN 15
                                                                WHEN s.business_name = '湖南区' THEN 14
                                                                WHEN s.business_name = '上海区' THEN 13
                                                                WHEN s.business_name IN ('河南区','大连区','南通区') THEN 12
                                                                ELSE 10
                                                        END
                                        THEN '店主效能饱和'
                                        WHEN fd.shelf_id IS NOT NULL
                                        THEN '店主修改订单'
                                        ELSE '6原因不明' 
                                END
                END
        ) reason_classify,
        @add_user add_user 
FROM
        fe_dwd.shelf_product_tmp t 
        LEFT JOIN fe_dwd.fill_onway_tmp ow 
                ON t.shelf_id = ow.shelf_id 
                AND t.product_id = ow.product_id 
        LEFT JOIN fe_dwd.fill_tmp fil 
                ON t.shelf_id = fil.shelf_id 
                AND t.product_id = fil.product_id 
        JOIN fe_dwd.shelf_tot_tmp s 
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
        LEFT JOIN  fe_dwd.not_fill_tmp r
                ON t.shelf_id = r.shelf_id 
                AND t.product_id = r.product_id 
        LEFT JOIN fe_dwd.apply_fill_diff_tmp fd
                ON t.shelf_id = fd.shelf_id
                AND t.product_id = fd.product_id
GROUP BY t.shelf_id,t.product_id
;
  
 SET @time_14 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_13--@time_14",@time_13,@time_14); 
  
  SET @sql_str := CONCAT(
    "ALTER TABLE fe_dm.dm_op_sp_offstock_his TRUNCATE PARTITION d",
    @d
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
  SET @sql_str := CONCAT(
    "ALTER TABLE fe_dm.dm_op_sp_offstock_his TRUNCATE PARTITION d",
    DAY(SUBDATE(@sdate, 7))
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
 SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_14--@time_15",@time_14,@time_15); 
  
  INSERT INTO fe_dm.dm_op_sp_offstock_his (
    sday,
    detail_id,
    product_id,
    shelf_id,
    sales_flag,
    shelf_fill_flag,
    stock_quantity,
    sto_val,
    sale_price,
    qty_sal,
    gmv,
    days_sal_sto,
    offstock_val,
    supplier_id,
    depot_code,
    supplier_type,
    stock_num,
    onway_num,
    suggest_fill_num,
    suggest_fill_val,
    cank_stock_qty,
    cank_stock_val,
    actual_apply_num,
    actual_apply_val,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    bsupplier_id,
    sup_sto_flag,
    sto_min_st,
    suggest_fill_num_st,
    total_price_st,
    package_id_st,
    min_max_quantity_st,
    stock_val_5_st,
    stock_quantity_st,
    stock_val_st,
    product_type_class,
    reason_classify,
    add_user
  )
  SELECT
    @d sday,
    detail_id,
    product_id,
    shelf_id,
    sales_flag,
    shelf_fill_flag,
    stock_quantity,
    sto_val,
    sale_price,
    qty_sal,
    gmv,
    days_sal_sto,
    offstock_val,
    supplier_id,
    depot_code,
    supplier_type,
    stock_num,
    onway_num,
    suggest_fill_num,
    suggest_fill_val,
    cank_stock_qty,
    cank_stock_val,
    actual_apply_num,
    actual_apply_val,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    bsupplier_id,
    sup_sto_flag,
    sto_min_st,
    suggest_fill_num_st,
    total_price_st,
    package_id_st,
    min_max_quantity_st,
    stock_val_5_st,
    stock_quantity_st,
    stock_val_st,
    product_type_class,
    reason_classify,
    @add_user add_user
  FROM
    fe_dm.dm_op_sp_offstock
;
	
SET @time_16 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_15--@time_16",@time_15,@time_16);	
	
	
  DELETE
  FROM
    fe_dm.dm_op_s_offstock
  WHERE sdate = @sdate OR sdate < @pre_6month;
  INSERT INTO fe_dm.dm_op_s_offstock (
    sdate,
    sales_flag,
    shelf_fill_flag,
    shelf_id,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify,
    ct,
    gmv,
    offstock_val,
    suggest_fill_num,
    suggest_fill_val,
    cank_stock_qty,
    cank_stock_val,
    actual_apply_num,
    actual_apply_val,
    add_user
  )
  SELECT
    @sdate sdate,
    sales_flag,
    shelf_fill_flag,
    shelf_id,
    stock_quantity > 0 ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify,
    COUNT(*) ct,
    SUM(gmv) gmv,
    SUM(offstock_val) offstock_val,
    SUM(suggest_fill_num) suggest_fill_num,
    SUM(suggest_fill_val) suggest_fill_val,
    SUM(cank_stock_qty) cank_stock_qty,
    SUM(cank_stock_val) cank_stock_val,
    SUM(actual_apply_num) actual_apply_num,
    SUM(actual_apply_val) actual_apply_val,
    @add_user add_user
  FROM
    fe_dm.dm_op_sp_offstock t
WHERE t.shelf_type IN (1,2,3,5,6)
        AND sales_flag IN (1,2,3)
  GROUP BY sales_flag,
    shelf_fill_flag,
    shelf_id,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify
;
	
SET @time_17 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_16--@time_17",@time_16,@time_17);	
	
	
  DELETE
  FROM
    fe_dm.dm_op_p_offstock
  WHERE sdate = @sdate OR sdate < @pre_6month;
  INSERT INTO fe_dm.dm_op_p_offstock (
    sdate,
    sales_flag,
    shelf_fill_flag,
    product_id,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify,
    ct,
    gmv,
    offstock_val,
    suggest_fill_num,
    suggest_fill_val,
    cank_stock_qty,
    cank_stock_val,
    actual_apply_num,
    actual_apply_val,
    add_user
  )
  SELECT
    @sdate sdate,
    sales_flag,
    shelf_fill_flag,
    product_id,
    stock_quantity > 0 ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify,
    COUNT(*) ct,
    SUM(gmv) gmv,
    SUM(offstock_val) offstock_val,
    SUM(suggest_fill_num) suggest_fill_num,
    SUM(suggest_fill_val) suggest_fill_val,
    SUM(cank_stock_qty) cank_stock_qty,
    SUM(cank_stock_val) cank_stock_val,
    SUM(actual_apply_num) actual_apply_num,
    SUM(actual_apply_val) actual_apply_val,
    @add_user add_user
  FROM
    fe_dm.dm_op_sp_offstock t
WHERE t.shelf_type IN (1,2,3,5,6)
        AND sales_flag IN (1,2,3)
  GROUP BY sales_flag,
    shelf_fill_flag,
    product_id,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify;
	
SET @time_18 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_17--@time_18",@time_17,@time_18);	
	
	
  DELETE
  FROM
    fe_dm.dm_op_offstock
  WHERE sdate = @sdate  OR sdate < SUBDATE(@sdate,INTERVAL 2 YEAR);
  INSERT INTO fe_dm.dm_op_offstock (
    sdate,
    sales_flag,
    shelf_fill_flag,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify,
    ct,
    gmv,
    offstock_val,
    suggest_fill_num,
    suggest_fill_val,
    cank_stock_qty,
    cank_stock_val,
    actual_apply_num,
    actual_apply_val,
    add_user
  )
  SELECT
    @sdate sdate,
    sales_flag,
    shelf_fill_flag,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify,
    SUM(ct) ct,
    SUM(gmv) gmv,
    SUM(offstock_val) offstock_val,
    SUM(suggest_fill_num) suggest_fill_num,
    SUM(suggest_fill_val) suggest_fill_val,
    SUM(cank_stock_qty) cank_stock_qty,
    SUM(cank_stock_val) cank_stock_val,
    SUM(actual_apply_num) actual_apply_num,
    SUM(actual_apply_val) actual_apply_val,
    @add_user add_user
  FROM
    fe_dm.dm_op_p_offstock t
  WHERE t.sdate = @sdate
  GROUP BY sales_flag,
    shelf_fill_flag,
    ifsto,
    business_name,
    shelf_type,
    whether_close2,
    revoke_status1,
    shelf_level,
    second_user_type1,
    if_prewh,
    product_type_class,
    reason_classify;
	
	
SET @time_19 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("dm_op_offstock_five","@time_18--@time_19",@time_18,@time_19);	
	
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_offstock_five',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
  COMMIT;	
END$$

DELIMITER ;