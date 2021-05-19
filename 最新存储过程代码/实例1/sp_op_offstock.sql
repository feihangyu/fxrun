CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_offstock`()
BEGIN
  #run after sh_process.sp_flag5_monitor
#run after sh_process.sp_op_sf_fillorder_requirement_his
#run after sh_process.sh_outstock_day
#run after sh_process.sp_op_dc_reqsto
#run after sh_process.sp_op_sp_avgsal30
#run after sh_process.sp_erp_stock_daily
#run after sh_process.sp_op_product_shelf_stat
#run after sh_process.d_op2_shelf_grade
#run after sh_process.sp_prewarehouse_stock_detail
   CALL sh_process.sp_op_dc_reqsto ();
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
  SET @pre_6month := SUBDATE(@sdate,INTERVAL 6 MONTH);
  
  -- 20200617 杨柳要求剔除湖北撤架的货架，这部分货架现在是线下撤架，线上的撤架状态没那么快改变（一般撤架流程有几个月的考核期），但是缺货率是地区考核指标，急需剔除。
DROP TEMPORARY TABLE IF EXISTS feods.`hubei_shelf_tmp`;
CREATE TEMPORARY TABLE feods.`hubei_shelf_tmp` (
        shelf_id INT(8),
        PRIMARY KEY `idx_shelf_id` (`shelf_id`)
        ) ;  

INSERT INTO feods.`hubei_shelf_tmp`(shelf_id)
VALUES
(86810 ),
(79186 ),
(15652 ),
(79892 ),
(79891 ),
(79890 ),
(94264 ),
(29369 ),
(15655 ),
(107846),
(104810),
(87607 ),
(85253 ),
(99111 ),
(81520 ),
(78563 ),
(81523 ),
(87708 ),
(90275 ),
(81046 ),
(90230 ),
(91889 ),
(89925 ),
(59038 ),
(80530 ),
(93243 ),
(22621 ),
(64663 ),
(60523 ),
(97437 ),
(97439 ),
(77982 ),
(68538 ),
(102538),
(86869 ),
(88001 ),
(88000 ),
(78742 ),
(39325 ),
(40536 ),
(57154 ),
(57156 ),
(84388 ),
(86818 ),
(76551 ),
(43560 ),
(90981 ),
(87999 ),
(87998 ),
(45167 ),
(70105 ),
(11702 ),
(78244 ),
(86874 ),
(11234 ),
(83372 ),
(73020 ),
(22531 ),
(54307 ),
(50131 ),
(87265 ),
(91905 ),
(86805 ),
(92478 ),
(89698 ),
(65479 ),
(86904 ),
(89117 ),
(1641  ),
(83731 ),
(96793 ),
(88244 ),
(58486 ),
(47139 ),
(47141 ),
(46810 ),
(46815 ),
(47121 ),
(46823 ),
(46824 ),
(52524 ),
(46812 ),
(52522 ),
(46817 ),
(52523 ),
(52525 ),
(34863 ),
(37249 ),
(79468 ),
(103343),
(3833  ),
(79192 ),
(23491 ),
(81527 ),
(48729 ),
(79145 ),
(79146 ),
(87729 ),
(79102 ),
(40184 ),
(91093 ),
(84063 ),
(84064 ),
(63781 ),
(83412 ),
(61804 ),
(5348  ),
(69299 ),
(86884 ),
(32422 ),
(31683 ),
(32421 ),
(86885 ),
(31684 ),
(31681 ),
(89128 ),
(77774 ),
(65322 ),
(80444 ),
(57147 ),
(86809 ),
(63938 ),
(84390 ),
(88519 ),
(86910 ),
(80713 ),
(65834 ),
(86833 ),
(25148 ),
(22191 ),
(56201 ),
(49710 ),
(34862 ),
(23490 ),
(33485 ),
(51855 ),
(11673 ),
(86924 ),
(90395 ),
(80846 ),
(5397  ),
(10243 ),
(86921 ),
(9435  ),
(44338 ),
(7888  ),
(30937 ),
(54714 ),
(69292 ),
(63263 ),
(2061  ),
(74293 ),
(26405 ),
(5617  ),
(6665  ),
(13432 ),
(16176 ),
(86149 ),
(47149 ),
(54655 ),
(91095 ),
(92750 ),
(16312 ),
(12825 ),
(12824 ),
(55300 ),
(49566 ),
(97369 ),
(97368 ),
(86877 ),
(87657 ),
(87658 ),
(61677 ),
(87266 ),
(88418 ),
(81598 ),
(88716 ),
(88714 ),
(88712 ),
(88005 ),
(88715 ),
(62016 ),
(57340 ),
(85338 ),
(68067 ),
(38786 ),
(45726 ),
(45727 ),
(97653 ),
(55407 ),
(61672 ),
(91101 ),
(84904 ),
(67307 ),
(67983 ),
(51409 ),
(58829 ),
(48369 ),
(81363 ),
(99054 )
;  
  SET @time_1 := CURRENT_TIMESTAMP();
  DROP TEMPORARY TABLE IF EXISTS feods.normal_tmp;
  CREATE TEMPORARY TABLE feods.normal_tmp (
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
    feods.d_op_dim_product_area_normal t
  WHERE t.month_id = @y_m
    AND ! ISNULL(t.business_name)
    AND ! ISNULL(t.product_id);
	
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_1--@time_2",@time_1,@time_2);
	
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id,
    b.business_name,
    t.shelf_type,
    t.whether_close = 2 whether_close2,
    t.revoke_status = 1 revoke_status1,
    IF(
      ! ISNULL(sab.shelf_id),
      '甲乙新',
      '丙丁'
    ) shelf_level,
    m.second_user_type = 1 second_user_type1,
    ! ISNULL(pr.shelf_id) if_prewh,
    IFNULL(pr.supplier_id, bdc.supplier_id) supplier_id,
    CASE
      WHEN ! ISNULL(sr.shelf_id)
      THEN 300
      WHEN ! ISNULL(sab.shelf_id)
      THEN 180
      ELSE 110
    END sto_min,
    slg.last_change_time
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN
      (SELECT
        MAX(t.supplier_id) supplier_id,
        b.business_area business_name
      FROM
        fe.sf_supplier t
        LEFT JOIN sserp.ZS_DC_BUSINESS_AREA b
          ON t.depot_code = b.dc_code
      WHERE t.data_flag = 1
        AND t.status = 2
        AND t.supplier_type = 2
      GROUP BY business_name) bdc
      ON b.business_name = bdc.business_name
    LEFT JOIN feods.`d_op_shelf_grade` sab
      ON t.shelf_id = sab.shelf_id
      AND sab.month_id = @y_m_last
      AND sab.grade IN ('甲', '乙', '新装')
    LEFT JOIN fe.pub_shelf_manager m
      ON t.manager_id = m.manager_id
      AND m.data_flag = 1
    LEFT JOIN
      (SELECT
        t.shelf_id,
        MAX(t.warehouse_id) supplier_id
      FROM
        fe.sf_prewarehouse_shelf_detail t
      WHERE t.data_flag = 1
      GROUP BY t.shelf_id) pr
      ON t.shelf_id = pr.shelf_id
    LEFT JOIN
      (SELECT
        t.main_shelf_id shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9
      UNION
      SELECT
        t.secondary_shelf_id shelf_id
      FROM
        fe.sf_shelf_relation_record t
      WHERE t.data_flag = 1
        AND t.shelf_handle_status = 9) sr
      ON t.shelf_id = sr.shelf_id
    LEFT JOIN
      (SELECT
        t.shelf_id,
        MAX(t.update_time) last_change_time
      FROM
        fe.sf_shelf_log t
      WHERE t.shelf_change_type IN (2, 3, 14)
        AND t.update_time >= @sdate_m
        AND t.remark LIKE '%地址%'
      GROUP BY t.shelf_id) slg
      ON t.shelf_id = slg.shelf_id
      LEFT JOIN fe.sf_shelf_machine sm 
        ON t.shelf_id = sm.shelf_id 
        AND sm.data_flag = 1                                                      
      LEFT JOIN fe.sf_shelf_machine_type mt 
        ON sm.machine_type_id = mt.machine_type_id 
        AND t.data_flag = 1
      LEFT JOIN feods.`hubei_shelf_tmp` hs
        ON t.shelf_id = hs.shelf_id
  WHERE t.data_flag = 1
    AND t.shelf_status = 2
    AND ! ISNULL(t.shelf_id)
    AND (mt.manufacturer_code != 1 OR mt.manufacturer_code IS NULL)       -- 2020-06码隆智能柜合作商终止合作，地区已经陆续在开始撤架
    AND ISNULL(hs.shelf_id)
    AND  b.business_name NOT IN ('山西区','冀州区','吉林区','江西区')       -- 2020-06 杨柳要求剔除4个撤城区
;
	
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_2--@time_3",@time_2,@time_3);	
	
	
  DROP TEMPORARY TABLE IF EXISTS feods.out_tmp;
  CREATE TEMPORARY TABLE feods.out_tmp (
    PRIMARY KEY (depot_code, product_code2)
  )
  SELECT
    t.warehouse_number depot_code,
    t.product_bar product_code2,
    t.fbaseqty cank_stock_qty
  FROM
    feods.PJ_OUTSTOCK2_DAY t
  WHERE t.fproducedate = @sub_day
    AND t.fbaseqty > 0
    AND ! ISNULL(t.warehouse_number)
    AND ! ISNULL(t.product_bar);
	
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_3--@time_4",@time_3,@time_4);	
	
	
  DROP TEMPORARY TABLE IF EXISTS feods.dc_tmp;
  CREATE TEMPORARY TABLE feods.dc_tmp (
    PRIMARY KEY (supplier_id, product_id)
  )
  SELECT
    su.supplier_id,
    t.depot_code,
    p.product_id,
    t.cank_stock_qty
  FROM
    feods.out_tmp t
    JOIN fe.sf_product p
      ON t.product_code2 = p.product_code2
      AND p.data_flag = 1
    JOIN fe.sf_supplier su
      ON t.depot_code = su.depot_code
      AND su.data_flag = 1
  WHERE ! ISNULL(su.supplier_id)
    AND ! ISNULL(p.product_id);
	
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_4--@time_5",@time_4,@time_5);	
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
        feods.shelf_tmp a
        LEFT JOIN fe.`sf_shelf_product_supply_info` e
                ON a.shelf_id = e.shelf_id
        LEFT JOIN fe.`sf_supplier` d
                ON a.supplier_id = d.SUPPLIER_ID
WHERE e.data_flag = 1 AND d.DATA_FLAG = 1
;
  DROP TEMPORARY TABLE IF EXISTS feods.requirement_tmp;
  CREATE TEMPORARY TABLE feods.requirement_tmp (PRIMARY KEY (shelf_id, product_id))
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
  (SELECT
    t.shelf_id,
    ri.product_id,
    MAX(t.supplier_id) supplier_id,
    MAX(dc.depot_code) depot_code,
    MAX(t.supplier_type) supplier_type,
    SUM(ri.onshelf_stock) stock_num,
    SUM(ri.onway_stock) onway_num,
    SUM(ri.suggest_fill_num) suggest_fill_num,
    SUM(
      IFNULL(
        dc.cank_stock_qty,
        pw.available_stock
      )
    ) cank_stock_qty,
    SUM(t.total_price) total_price
  FROM
    feods.d_op_sf_fillorder_requirement_his t
    JOIN feods.d_op_sf_fillorder_requirement_item_his ri
      ON t.requirement_id = ri.requirement_id
      AND ri.sday = @d
    LEFT JOIN feods.dc_tmp dc
      ON t.supplier_id = dc.supplier_id
      AND ri.product_id = dc.product_id
    LEFT JOIN feods.pj_prewarehouse_stock_detail pw
      ON t.supplier_id = pw.warehouse_id
      AND ri.product_id = pw.product_id
      AND pw.check_date = @sub_day
    JOIN fe_dwd.`dwd_shelf_base_day_all` s
        ON t.shelf_id = s.shelf_id
        AND s.shelf_type IN (6,7)
        AND s.shelf_status = 2
        AND s.type_name NOT LIKE '%静态柜%'
  WHERE t.sday = @d
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(ri.product_id)
  GROUP BY t.shelf_id,
    ri.product_id
UNION
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
        feods.`d_op_shelf_product_fill_update_his` a
        JOIN feods.`d_op_auto_push_fill_date_his` b
                ON a.shelf_id = b.shelf_id
                AND a.cdate = @sdate
                AND b.`stat_date` = @sdate
        JOIN test.`supplier_shelf_tmp` c
                ON a.shelf_id = c.shelf_id
UNION
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
        feods.`d_op_smart_shelf_fill_update_his` a
        JOIN test.supplier_shelf_tmp c
                ON a.shelf_id = c.shelf_id
                AND a.cdate = @sdate) t1
GROUP BY shelf_id, product_id
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_5--@time_6",@time_5,@time_6);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.fill_onway_tmp;
  CREATE TEMPORARY TABLE feods.fill_onway_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id,
    fi.product_id,
    SUM(fi.actual_apply_num) onway_num
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
  WHERE t.data_flag = 1
    AND t.order_status IN (1, 2)
    AND t.apply_time >= SUBDATE(@sdate, 30)
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY t.shelf_id,
    fi.product_id;
	
SET @time_7 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_6--@time_7",@time_6,@time_7);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.main_tmp;
  CREATE TEMPORARY TABLE feods.main_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    shelf_id,
    product_id
  FROM
    feods.fill_onway_tmp
  UNION
  SELECT
    shelf_id,
    product_id
  FROM
    feods.requirement_tmp;
	
SET @time_8 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_7--@time_8",@time_7,@time_8);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.requirement_shelf_tmp;
  CREATE TEMPORARY TABLE feods.requirement_shelf_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id,
    SUM(IFNULL(r.onway_num, f.onway_num)) onway_num,
    IFNULL(SUM(r.suggest_fill_num), 0) suggest_fill_num,
    IFNULL(MAX(r.total_price), 0) total_price
  FROM
    feods.main_tmp t
    LEFT JOIN feods.requirement_tmp r
      ON r.shelf_id = t.shelf_id
      AND r.product_id = t.product_id
      AND r.supplier_type IN (2, 9)
    LEFT JOIN feods.fill_onway_tmp f
      ON f.shelf_id = t.shelf_id
      AND f.product_id = t.product_id
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  
SET @time_9 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_8--@time_9",@time_8,@time_9);
  
  
  DROP TEMPORARY TABLE IF EXISTS feods.sto_limit_tmp;
  CREATE TEMPORARY TABLE feods.sto_limit_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id,
    p.package_id,
    p.min_max_quantity
  FROM
    fe.sf_shelf_package_detail t
    JOIN fe.sf_package p
      ON t.package_id = p.package_id
      AND p.data_flag = 1
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id);
	
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_9--@time_10",@time_9,@time_10);
	
	
  DROP TEMPORARY TABLE IF EXISTS feods.sto_tmp;
  CREATE TEMPORARY TABLE feods.sto_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.shelf_id,
    t.stock_val_5,
    t.stock_quantity,
    t.stock_val
  FROM
    feods.fjr_flag5_shelf t
  WHERE t.sdate = @sdate
    AND ! ISNULL(t.shelf_id);
	
SET @time_11 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_10--@time_11",@time_10,@time_11);	
	
	
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id, product_id)) AS
  SELECT
    t.shelf_id,
    fi.product_id,
    SUM(fi.actual_apply_num) actual_apply_num
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
  WHERE t.data_flag = 1
    AND t.order_status != 9
    AND t.apply_time >= @sdate
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY t.shelf_id,
    fi.product_id;
	
SET @time_12 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_11--@time_12",@time_11,@time_12);	
	
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tot_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tot_tmp (PRIMARY KEY (shelf_id)) AS
  SELECT
    t.*,
    r.onway_num,
    r.suggest_fill_num,
    r.total_price,
    sl.package_id,
    sl.min_max_quantity,
    sto.stock_val_5,
    sto.stock_quantity,
    sto.stock_val
  FROM
    feods.shelf_tmp t
    LEFT JOIN feods.requirement_shelf_tmp r
      ON t.shelf_id = r.shelf_id
    LEFT JOIN feods.sto_limit_tmp sl
      ON t.shelf_id = sl.shelf_id
    LEFT JOIN feods.sto_tmp sto
      ON t.shelf_id = sto.shelf_id
  WHERE ! ISNULL(t.shelf_id);
  
 SET @time_13 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_12--@time_13",@time_12,@time_13);
 
  TRUNCATE TABLE feods.d_op_sp_offstock;
  SET @sql_str := CONCAT(
    "INSERT INTO feods.d_op_sp_offstock ( detail_id, product_id, shelf_id, sales_flag, shelf_fill_flag, stock_quantity, sto_val, sale_price, qty_sal, gmv, days_sal_sto, offstock_val, supplier_id, depot_code, supplier_type, stock_num, onway_num, suggest_fill_num, suggest_fill_val, cank_stock_qty, cank_stock_val, actual_apply_num, actual_apply_val, business_name, shelf_type, whether_close2, revoke_status1, shelf_level, second_user_type1, if_prewh, bsupplier_id, sup_sto_flag, sto_min_st, suggest_fill_num_st, total_price_st, package_id_st, min_max_quantity_st, stock_val_5_st, stock_quantity_st, stock_val_st, product_type_class, reason_classify, add_user ) SELECT t.detail_id, t.product_id, t.shelf_id, t.sales_flag, t.shelf_fill_flag, IF( t.stock_quantity > 0, t.stock_quantity, 0 ) stock_quantity, IF( t.stock_quantity > 0, t.stock_quantity * t.sale_price, 0 ) sto_val, t.sale_price, IFNULL(sal.qty_sal30, 0) qty_sal, IFNULL(sal.qty_sal30 * t.sale_price, 0) gmv, IFNULL(sal.days_sal_sto30, 0) days_sal_sto, CASE WHEN t.stock_quantity > 0 THEN 0 WHEN sal.qty_sal30 > 0 THEN sal.qty_sal30 / sal.days_sal_sto30 ELSE 0.06 END * t.sale_price offstock_val, re.supplier_id, re.depot_code, re.supplier_type, IFNULL(re.stock_num, 0) stock_num, IF( ow.onway_num > 0, ow.onway_num, IFNULL(re.onway_num, 0) ) onway_num, re.suggest_fill_num suggest_fill_num, re.suggest_fill_num * t.sale_price suggest_fill_val, IFNULL(re.cank_stock_qty, 0) cank_stock_qty, IFNULL( re.cank_stock_qty * t.sale_price, 0 ) cank_stock_val, IFNULL(fil.actual_apply_num, 0) actual_apply_num, IFNULL( fil.actual_apply_num * t.sale_price, 0 ) actual_apply_val, s.business_name, s.shelf_type, s.whether_close2, s.revoke_status1, s.shelf_level, IFNULL(s.second_user_type1, 0) second_user_type1, IFNULL(s.if_prewh, 0) if_prewh, s.supplier_id bsupplier_id, COALESCE( dcs.qty_sto >= dcs.qty_req, whs.qty_sto >= whs.qty_req, 1 ) sup_sto_flag, s.sto_min sto_min_st, IFNULL(s.suggest_fill_num, 0) suggest_fill_num_st, IFNULL(s.total_price, 0) total_price_st, s.package_id package_id_st, s.min_max_quantity min_max_quantity_st, IFNULL(s.stock_val_5, 0) stock_val_5_st, IFNULL(s.stock_quantity, 0) stock_quantity_st, IFNULL(s.stock_val, 0) stock_val_st, n.product_type_class, IF( t.stock_quantity > 0, NULL, CASE WHEN n.product_type_class = '淘汰' || ISNULL(n.product_type_class) THEN '淘汰' WHEN ! s.whether_close2 || ! s.revoke_status1 || ! ISNULL(s.last_change_time) THEN '1货架异常' WHEN IFNULL(re.suggest_fill_num, 0) = 0 && IFNULL(ow.onway_num,0)=0 && IFNULL(re.onway_num,0)=0 THEN '2未生成补货需求' ELSE CONCAT( n.product_type_class, IF( s.if_prewh, '_前置仓_', '_大仓_' ), CASE WHEN IF( ow.onway_num > 0, ow.onway_num, IFNULL(re.onway_num, 0) ) > 0 THEN '2在途订单' WHEN re.suggest_fill_num > 0 && ( dcs.qty_sto < dcs.qty_req || whs.qty_sto < whs.qty_req ) && ( s.if_prewh || IFNULL(s.total_price, 0) >= 150 ) THEN '1仓库缺货'  WHEN IFNULL(s.stock_quantity, 0) >= s.min_max_quantity && IFNULL(s.stock_val_5, 0) >= GREATEST( .3 * IFNULL(s.stock_val, 0), IFNULL(s.total_price, 0) ) THEN '3.1高库存-严重滞销过高' WHEN IFNULL(s.stock_quantity, 0) >= s.min_max_quantity && IFNULL(fil.actual_apply_num, 0) = 0 THEN '3.2高库存-高销单品多' WHEN IFNULL(s.total_price, 0) < 150 && s.if_prewh = 0 && IFNULL(s.stock_quantity, 0) >= s.sto_min THEN '4金额不足'/*'4.1金额不足-低销'*/ WHEN ( IFNULL(s.total_price, 0) < 150 || s.if_prewh ) && IFNULL(s.stock_quantity, 0) + IFNULL(s.suggest_fill_num, 0) + IFNULL(s.onway_num, 0) < s.sto_min THEN '4金额不足'/*'4.2金额不足|可补SKU不足'*/ WHEN IFNULL(s.total_price, 0) < 150 && s.if_prewh = 0 THEN '4金额不足'/*'4.3金额不足-货架补货单'*/ WHEN ( IFNULL(s.total_price, 0) >= 150 || s.if_prewh ) && s.whether_close2 && s.revoke_status1 && ( dcs.qty_sto >= dcs.qty_req || whs.qty_sto >= whs.qty_req ) && IFNULL(fil.actual_apply_num, 0) = 0 THEN '5地区未补货' ELSE '6原因不明' END ) END ) reason_classify, @add_user add_user FROM feods.d_op_shelf_product_detail_combine",
    @d_add,
    " t LEFT JOIN feods.d_op_sp_avgsal30 sal ON t.shelf_id = sal.shelf_id AND t.product_id = sal.product_id LEFT JOIN feods.requirement_tmp re ON t.shelf_id = re.shelf_id AND t.product_id = re.product_id LEFT JOIN feods.fill_onway_tmp ow ON t.shelf_id = ow.shelf_id AND t.product_id = ow.product_id LEFT JOIN feods.fill_tmp fil ON t.shelf_id = fil.shelf_id AND t.product_id = fil.product_id JOIN feods.shelf_tot_tmp s ON t.shelf_id = s.shelf_id LEFT JOIN feods.d_op_dc_reqsto dcs ON s.supplier_id = dcs.supplier_id AND t.product_id = dcs.product_id AND dcs.sdate = @sdate LEFT JOIN feods.d_op_pwh_reqsto whs ON s.supplier_id = whs.warehouse_id AND t.product_id = whs.product_id AND whs.sdate = @sdate LEFT JOIN feods.normal_tmp n ON t.product_id = n.product_id AND s.business_name = n.business_name"
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
 SET @time_14 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_13--@time_14",@time_13,@time_14); 
  
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_offstock_his TRUNCATE PARTITION d",
    @d
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
  SET @sql_str := CONCAT(
    "ALTER TABLE feods.d_op_sp_offstock_his TRUNCATE PARTITION d",
    DAY(SUBDATE(@sdate, 7))
  );
  PREPARE sql_exe FROM @sql_str;
  EXECUTE sql_exe;
  
 SET @time_15 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_14--@time_15",@time_14,@time_15); 
  
  INSERT INTO feods.d_op_sp_offstock_his (
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
    feods.d_op_sp_offstock
WHERE shelf_fill_flag = 1
;
	
SET @time_16 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_15--@time_16",@time_15,@time_16);	
	
	
  DELETE
  FROM
    feods.d_op_s_offstock
  WHERE sdate = @sdate OR sdate < @pre_6month;
  INSERT INTO feods.d_op_s_offstock (
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
    feods.d_op_sp_offstock t
WHERE t.shelf_type IN (1,2,3,5,6)
        AND sales_flag IN (1,2,3)
        AND shelf_fill_flag = 1
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
    reason_classify;
	
SET @time_17 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_op_offstock","@time_16--@time_17",@time_16,@time_17);	
	
	
  DELETE
  FROM
    feods.d_op_p_offstock
  WHERE sdate = @sdate OR sdate < @pre_6month;
  INSERT INTO feods.d_op_p_offstock (
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
    feods.d_op_sp_offstock t
WHERE t.shelf_type IN (1,2,3,5,6)
        AND sales_flag IN (1,2,3)
        AND shelf_fill_flag = 1
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
CALL sh_process.sql_log_info("sp_op_offstock","@time_17--@time_18",@time_17,@time_18);	
	
	
  DELETE
  FROM
    feods.d_op_offstock
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 2 YEAR);
  INSERT INTO feods.d_op_offstock (
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
    feods.d_op_p_offstock t
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
CALL sh_process.sql_log_info("sp_op_offstock","@time_18--@time_19",@time_18,@time_19);	
	
  CALL feods.sp_task_log (
    'sp_op_offstock',
    @sdate,
    CONCAT(
      'yingnansong_d_3062f05202437f9d4953bd740f333511',
      @timestamp,
      @add_user
    )
  );
  COMMIT;
END