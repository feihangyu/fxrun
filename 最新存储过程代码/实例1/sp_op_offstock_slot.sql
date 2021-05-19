CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_offstock_slot`()
BEGIN
  CALL sh_process.sp_op_slot_his ();
  #run after sh_process.dwd_order_item_refund_day_inc
#run after sh_process.sp_op_dc_reqsto
#run after sh_process.sp_op_slot_his
#run after sh_process.sp_op_offstock
#run after sh_process.sp_shelf_dgmv
   SET @sdate := SUBDATE(CURRENT_DATE, 1), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @add_day := ADDDATE(@sdate, 1);
  SET @d := DAY(@sdate);
  SET @month_end_last := SUBDATE(@sdate, @d);
  SET @d_lm := DAY(@month_end_last);
  SET @avg_start_day := SUBDATE(@sdate, 29);
  SET @days := 30;
  SET @y_m := DATE_FORMAT(@sdate, '%Y-%m');
  SET @y_m_last := DATE_FORMAT(@month_end_last, '%Y-%m');
  SET @month_start_last := SUBDATE(@month_end_last, @d_lm - 1);
  
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
  
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (
    PRIMARY KEY (shelf_id), KEY (shelf_code)
  )
  SELECT
    t.shelf_id, t.shelf_code, t.shelf_status, t.whether_close, t.revoke_status, ! ISNULL(pw.shelf_id) if_prewarehouse, IFNULL(
      pw.warehouse_id, bdc.supplier_id
    ) supplier_id, sm.online_status, sm.slot_sync_status, b.business_name, m.manager_id, dg.gmv
  FROM
    fe.sf_shelf t
    JOIN feods.fjr_city_business b
      ON t.city = b.city
    LEFT JOIN
      (SELECT
        MAX(t.supplier_id) supplier_id, b.business_area business_name
      FROM
        fe.sf_supplier t
        LEFT JOIN sserp.ZS_DC_BUSINESS_AREA b
          ON t.depot_code = b.dc_code
      WHERE t.data_flag = 1
        AND t.status = 2
        AND t.supplier_type = 2
      GROUP BY business_name) bdc
      ON b.business_name = bdc.business_name
    LEFT JOIN fe.pub_shelf_manager m
      ON t.manager_id = m.manager_id
      AND m.data_flag = 1
    LEFT JOIN fe.sf_prewarehouse_shelf_detail pw
      ON t.shelf_id = pw.shelf_id
      AND pw.data_flag = 1
    LEFT JOIN fe.sf_shelf_machine sm
      ON t.shelf_id = sm.shelf_id
      AND sm.data_flag = 1
    LEFT JOIN fe.sf_shelf_machine_type smt
      ON sm.machine_type_id = smt.machine_type_id
      AND smt.data_flag = 1
    LEFT JOIN feods.fjr_shelf_dgmv dg
      ON t.shelf_id = dg.shelf_id
      AND dg.sdate = @sdate
    LEFT JOIN feods.`hubei_shelf_tmp` hs
        ON t.shelf_id = hs.shelf_id
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND t.shelf_status = 2
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id)
    AND ISNULL(hs.shelf_id)
    AND b.business_name NOT IN ('山西区','冀州区','吉林区','江西区')         -- 2020-06 杨柳要求剔除4个撤城区
    ;
  DROP TEMPORARY TABLE IF EXISTS feods.mgmv_tmp;
  CREATE TEMPORARY TABLE feods.mgmv_tmp (PRIMARY KEY (shelf_id))
  SELECT DISTINCT
    t.shelf_id
  FROM
    feods.fjr_shelf_mgmv t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.month_id IN (@y_m_last, @y_m)
    AND ! ISNULL(t.shelf_id)
    UNION
    SELECT DISTINCT
      IFNULL(t.shelf_id, s.shelf_id) shelf_id
    FROM
      fe.sf_order_yht t
      LEFT JOIN fe.sf_shelf s
        ON t.asset_id = s.shelf_code
        AND s.data_flag = 1
        AND s.shelf_type = 7
        AND s.shelf_status = 2
        AND s.shelf_name NOT LIKE '%测试%'
    WHERE t.paytime >= @month_start_last
      AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id));
  DROP TEMPORARY TABLE IF EXISTS feods.pack_tmp;
  CREATE TEMPORARY TABLE feods.pack_tmp (
    PRIMARY KEY (package_id, product_id)
  )
  SELECT
    t.package_id, s.product_id, pm.alarm_quantity, pm.shelf_fill_flag
  FROM
    fe.sf_package t
    JOIN fe.sf_package_item pm
      ON t.package_id = pm.package_id
      AND pm.data_flag = 1
    JOIN fe.sf_supplier_product_detail s
      ON pm.relation_id = s.detail_id
      AND s.data_flag = 1
  WHERE t.data_flag = 1
--     AND t.statu_flag = 1
    AND ! ISNULL(s.product_id)
    AND ! ISNULL(t.package_id);
  DROP TEMPORARY TABLE IF EXISTS feods.slot_tmp;
  CREATE TEMPORARY TABLE feods.slot_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(IF(t.stock_num > 0, t.stock_num, 0)) stock_num, SUM(t.slot_capacity_limit) slot_capacity_limit, COUNT(*) slots, SUM(t.stock_num > 0) slots_sto
  FROM
    fe.sf_shelf_machine_slot t
  WHERE t.data_flag = 1
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.slot_shelf_tmp;
  CREATE TEMPORARY TABLE feods.slot_shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.stock_num) stock_num, SUM(t.slots) slots, SUM(t.slots_sto) slots_sto
  FROM
    feods.slot_tmp t
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.offstock_tmp;
  CREATE TEMPORARY TABLE feods.offstock_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.suggest_fill_val) suggest_fill_val, SUM(
      IF(
        t.supplier_type = 2, t.suggest_fill_val, 0
      )
    ) suggest_fill_val2, SUM(t.offstock_val) offstock_val
  FROM
    feods.d_op_sp_offstock t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.offstock_detail_tmp;
  CREATE TEMPORARY TABLE feods.offstock_detail_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, t.suggest_fill_num, t.cank_stock_qty, t.onway_num, IF(
      t.gmv > 0, t.gmv / t.days_sal_sto, 0.06 * t.sale_price
    ) avg_gmv, t.qty_sal / t.days_sal_sto avg_qty
  FROM
    feods.d_op_sp_offstock t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TEMPORARY TABLE IF EXISTS feods.yht_tmp;
  CREATE TEMPORARY TABLE feods.yht_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    IFNULL(t.shelf_id, s.shelf_id) shelf_id, oi.goods_id product_id, SUM(oi.product_count * oi.price) / (
      DATEDIFF(
        @sdate, GREATEST(MIN(t.payTime), @avg_start_day)
      ) + 1
    ) avg_gmv, SUM(oi.product_count) / (
      DATEDIFF(
        @sdate, GREATEST(MIN(t.payTime), @avg_start_day)
      ) + 1
    ) avg_qty
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
    LEFT JOIN feods.shelf_tmp s
      ON t.asset_id = s.shelf_code
      AND ! ISNULL(s.shelf_code)
      AND s.shelf_code != ''
  WHERE t.data_flag = 1
    AND t.pay_status = 1
    AND t.payTime >= @avg_start_day
    AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
    AND ! ISNULL(oi.goods_id)
  GROUP BY IFNULL(t.shelf_id, s.shelf_id), product_id;
  UPDATE
    feods.offstock_detail_tmp t
    JOIN feods.yht_tmp yht
      ON t.shelf_id = yht.shelf_id
      AND t.product_id = yht.product_id SET t.avg_gmv = yht.avg_gmv, t.avg_qty = yht.avg_qty;
  DROP TEMPORARY TABLE IF EXISTS feods.yht_insert_tmp;
  CREATE TEMPORARY TABLE feods.yht_insert_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.*
  FROM
    feods.yht_tmp t
    LEFT JOIN feods.offstock_detail_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
  WHERE ISNULL(d.shelf_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  INSERT INTO feods.offstock_detail_tmp (
    shelf_id, product_id, avg_gmv, avg_qty
  )
  SELECT
    shelf_id, product_id, avg_gmv, avg_qty
  FROM
    feods.yht_insert_tmp;
  DROP TEMPORARY TABLE IF EXISTS feods.offval_tmp;
  CREATE TEMPORARY TABLE feods.offval_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(o.avg_gmv) offval
  FROM
    feods.slot_tmp t
    JOIN feods.offstock_detail_tmp o
      ON t.shelf_id = o.shelf_id
      AND t.product_id = o.product_id
      AND o.avg_gmv > 0
  WHERE t.stock_num = 0
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS feods.yht_yes_tmp;
  CREATE TEMPORARY TABLE feods.yht_yes_tmp (PRIMARY KEY (shelf_id))
  SELECT
    IFNULL(t.shelf_id, s.shelf_id) shelf_id, SUM(oi.product_count * oi.price) gmv
  FROM
    fe.sf_order_yht t
    JOIN fe.sf_order_yht_item oi
      ON t.order_id = oi.order_id
    LEFT JOIN feods.shelf_tmp s
      ON t.asset_id = s.shelf_code
      AND ! ISNULL(s.shelf_code)
      AND s.shelf_code != ''
  WHERE t.data_flag = 1
    AND t.pay_status = 1
    AND t.payTime >= @sdate
    AND t.payTime < @add_day
    AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
  GROUP BY IFNULL(t.shelf_id, s.shelf_id);
  UPDATE
    feods.shelf_tmp t
    JOIN feods.yht_yes_tmp yht
      ON t.shelf_id = yht.shelf_id SET t.gmv = yht.gmv;
  DROP TEMPORARY TABLE IF EXISTS feods.onway_tmp;
  CREATE TEMPORARY TABLE feods.onway_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, fi.product_id, SUM(fi.actual_apply_num) actual_apply_num
  FROM
    fe.sf_product_fill_order t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
      AND fi.actual_apply_num > 0
  WHERE t.data_flag = 1
    AND t.order_status IN (1, 2)
    AND t.apply_time >= SUBDATE(CURRENT_DATE, INTERVAL 1 MONTH)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY t.shelf_id, fi.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.slot_detail_tmp;
  CREATE TEMPORARY TABLE feods.slot_detail_tmp (
    PRIMARY KEY (slot_id), KEY (
      shelf_id, product_id, manufacturer_slot_code
    ), KEY (
      machine_id, manufacturer_slot_code
    )
  )
  SELECT
    @slot_capacity_limit := IF(
      @shelf_id = t.shelf_id && @product_id = t.product_id, @slot_capacity_limit, 0
    ) + m.slot_capacity_limit slot_capacity_limit_cum, @shelf_id := t.shelf_id shelf_id, @product_id := t.product_id product_id, t.manufacturer_slot_code, t.slot_status, t.stock_num, t.slot_id, t.machine_id, m.slot_capacity_limit, osd.avg_qty, d.sale_price, d.max_quantity, f.sales_flag, d.shelf_fill_flag, p.product_code2, p.product_name
  FROM
    fe.sf_shelf_machine_slot t
    JOIN
      (SELECT
        @slot_capacity_limit := 0, @shelf_id := 0, @product_id := 0) s
    LEFT JOIN fe.sf_shelf_machine_slot_type m
      ON t.slot_type_id = m.slot_type_id
      AND m.data_flag = 1
    LEFT JOIN feods.offstock_detail_tmp osd
      ON t.shelf_id = osd.shelf_id
      AND t.product_id = osd.product_id
    LEFT JOIN fe.sf_shelf_product_detail_flag f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id
      AND f.data_flag = 1
    LEFT JOIN fe.sf_shelf_product_detail d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
      AND f.data_flag = 1
    LEFT JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
  WHERE t.data_flag = 1
    AND ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.slot_id)
  ORDER BY t.shelf_id, t.product_id, t.slot_id;
  DROP TEMPORARY TABLE IF EXISTS feods.pack_tmp1;
  CREATE TEMPORARY TABLE feods.pack_tmp1 (
    PRIMARY KEY (package_id, product_id)
  )
  SELECT
    package_id, product_id, alarm_quantity, shelf_fill_flag
  FROM
    feods.pack_tmp;
  DROP TEMPORARY TABLE IF EXISTS feods.sp_sal_tmp;
  CREATE TEMPORARY TABLE feods.sp_sal_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.quantity_act) quantity_act
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.yht_d1_tmp;
  CREATE TEMPORARY TABLE feods.yht_d1_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.*
  FROM
    (SELECT
      IFNULL(t.shelf_id, s.shelf_id) shelf_id, oi.goods_id product_id, SUM(oi.product_count) quantity_act
    FROM
      fe.sf_order_yht t
      JOIN fe.sf_order_yht_item oi
        ON t.order_id = oi.order_id
      LEFT JOIN feods.shelf_tmp s
        ON t.asset_id = s.shelf_code
        AND ! ISNULL(s.shelf_code)
        AND s.shelf_code != ''
    WHERE t.data_flag = 1
      AND t.pay_status = 1
      AND t.payTime >= @sdate
      AND t.payTime < @add_day
      AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
      AND ! ISNULL(oi.goods_id)
    GROUP BY IFNULL(t.shelf_id, s.shelf_id), product_id) t
    LEFT JOIN feods.sp_sal_tmp s
      ON t.shelf_id = s.shelf_id
      AND t.product_id = s.product_id
  WHERE ISNULL(s.shelf_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  INSERT INTO feods.sp_sal_tmp
  SELECT
    *
  FROM
    feods.yht_d1_tmp;
  DROP TEMPORARY TABLE IF EXISTS feods.sp_fil_tmp;
  CREATE TEMPORARY TABLE feods.sp_fil_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, fi.product_id, SUM(fi.actual_fill_num) actual_fill_num
  FROM
    fe.sf_product_fill_order t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
  WHERE t.data_flag = 1
    AND t.fill_time >= @sdate
    AND t.fill_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(fi.product_id)
  GROUP BY t.shelf_id, fi.product_id;
  DELETE
  FROM
    feods.d_op_offstock_slot
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 12 MONTH);
  INSERT INTO feods.d_op_offstock_slot (
    sdate, slot_id, business_name, shelf_id, shelf_status, revoke_status, whether_close, if_pre, online_status, slot_sync_status, manufacturer_slot_code, slot_status, product_id, max_quantity, stock_num, slot_capacity_limit, avg_qty, sale_price, sales_flag, shelf_fill_flag, reason_classify, add_user
  )
  SELECT
    @sdate sdate, t.slot_id, s.business_name, s.shelf_id, s.shelf_status, s.revoke_status, s.whether_close, s.if_prewarehouse if_pre, IFNULL(s.online_status, 0) online_status, IFNULL(s.slot_sync_status, 0) slot_sync_status, t.manufacturer_slot_code, t.slot_status, t.product_id, t.max_quantity, t.stock_num, t.slot_capacity_limit, IFNULL(t.avg_qty, 0) avg_qty, t.sale_price, t.sales_flag, IFNULL(t.shelf_fill_flag, 2) shelf_fill_flag, IF(
      t.stock_num <= 0,
      CASE
        WHEN IFNULL(ss.stock_num, 0) = 0
        AND ISNULL(sff.shelf_id)
        AND s.shelf_status = 2
        THEN '1.01激活未上架'
        WHEN s.revoke_status != 1
        OR s.whether_close = 1
        THEN '1.02非正常货架'
        WHEN (
          pt.shelf_fill_flag = 2
          AND t.product_id != pc.product_id
        )
        OR ISNULL(ptc.package_id)
        THEN '1.03停止补货'
        /*WHEN ! ISNULL(pc.product_id) && ISNULL(pt.product_id) THEN '1.商品模版不符'*/
        WHEN msd.stock_num >= t.slot_capacity_limit_cum
        THEN '2.店主问题'
        /*WHEN IFNULL(pt.alarm_quantity, 0) < IFNULL(sl.slot_capacity_limit, 0) THEN '3.标配错误'*/
        WHEN (
          t.product_id != pc.product_id
          AND ptc.shelf_fill_flag = 1
        )
        OR (
          t.product_id = IFNULL(pc.product_id, t.product_id)
          AND pt.shelf_fill_flag = 2
        )
        THEN '4.换新导致'
        WHEN osd.onway_num > 0 || ! ISNULL(ow.shelf_id)
        THEN '5.01在途'
        WHEN osd.suggest_fill_num < IFNULL(sl.slot_capacity_limit, 0) - IFNULL(sl.stock_num, 0) - IF(
          osd.onway_num > 0, osd.onway_num, IFNULL(ow.actual_apply_num, 0)
        ) - IFNULL(msd.stock_num, 0)
        THEN '5.02操作问题'
        WHEN os.suggest_fill_val2 < 150 && os.suggest_fill_val2 > 0
        THEN '5.补货金额不足'
        WHEN
        /*(
          os.suggest_fill_val < IFNULL(s.gmv, 0) && os.suggest_fill_val > 150
        )
        OR (
          IFNULL(spsal.quantity_act, 0) > IFNULL(spfil.actual_fill_num, 0)
        )*/
        t.avg_qty >= 2.4
        THEN '6.销售太快'
        WHEN dcs.qty_sto < whs.qty_req || whs.qty_sto < whs.qty_req
        THEN '7.仓库缺货'
        ELSE '8.未补货'
      END, NULL
    ) reason_classify, @add_user add_user
  FROM
    feods.slot_detail_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.d_op_shelf_firstfill sff
      ON t.shelf_id = sff.shelf_id
    LEFT JOIN feods.slot_shelf_tmp ss
      ON t.shelf_id = ss.shelf_id
    LEFT JOIN fe.sf_shelf_machine_product_change pc
      ON t.slot_id = pc.slot_id
      AND pc.data_flag = 1
    LEFT JOIN fe.sf_shelf_package_detail pd
      ON t.shelf_id = pd.shelf_id
      AND pd.data_flag = 1
    LEFT JOIN feods.pack_tmp pt
      ON pd.package_id = pt.package_id
      AND t.product_id = pt.product_id
    LEFT JOIN feods.pack_tmp1 ptc
      ON pd.package_id = ptc.package_id
      AND IFNULL(pc.product_id, t.product_id) = ptc.product_id
    LEFT JOIN fe.sf_shelf_machine_second ms
      ON t.shelf_id = ms.shelf_id
      AND ms.data_flag = 1
    LEFT JOIN fe.sf_shelf_machine_second_detail msd
      ON ms.machine_second_id = msd.machine_second_id
      AND t.product_id = msd.product_id
      AND msd.data_flag = 1
    JOIN feods.slot_tmp sl
      ON t.shelf_id = sl.shelf_id
      AND t.product_id = sl.product_id
    LEFT JOIN feods.offstock_tmp os
      ON t.shelf_id = os.shelf_id
    LEFT JOIN feods.offstock_detail_tmp osd
      ON t.shelf_id = osd.shelf_id
      AND t.product_id = osd.product_id
    LEFT JOIN feods.onway_tmp ow
      ON t.shelf_id = ow.shelf_id
      AND t.product_id = ow.product_id
    LEFT JOIN feods.sp_sal_tmp spsal
      ON t.shelf_id = spsal.shelf_id
      AND t.product_id = spsal.product_id
    LEFT JOIN feods.sp_fil_tmp spfil
      ON t.shelf_id = spfil.shelf_id
      AND t.product_id = spfil.product_id
    LEFT JOIN feods.d_op_dc_reqsto dcs
      ON s.supplier_id = dcs.supplier_id
      AND t.product_id = dcs.product_id
      AND dcs.sdate = @sdate
    LEFT JOIN feods.d_op_pwh_reqsto whs
      ON s.supplier_id = whs.warehouse_id
      AND t.product_id = whs.product_id
      AND whs.sdate = @sdate;
  DROP TEMPORARY TABLE IF EXISTS feods.unact_tmp;
  CREATE TEMPORARY TABLE feods.unact_tmp (PRIMARY KEY (shelf_id))
  SELECT DISTINCT
    shelf_id
  FROM
    feods.d_op_offstock_slot
  WHERE sdate = @sdate
    AND reason_classify = '1.01激活未上架'
    AND ! ISNULL(shelf_id);
  DELETE
  FROM
    feods.d_op_offstock_s7p
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 3 MONTH);
  INSERT INTO feods.d_op_offstock_s7p (
    sdate, business_name, shelf_id, product_id, unact_flag, package_id, base_pack_flag, max_quantity, sale_price, sales_flag, shelf_fill_flag, slots, slots_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, miss_val, add_user
  )
  SELECT
    @sdate sdate, s.business_name, t.shelf_id, t.product_id, ! ISNULL(uf.shelf_id) unact_flag, t.package_id, t.base_pack_flag, t.max_quantity, t.sale_price, t.sales_flag, t.shelf_fill_flag, t.slots, t.slots_sto, t.stock_num, t.slot_capacity_limit, t.stock_num_slot, t.stock_num_second, IFNULL(ow.actual_apply_num, 0) onway_num, IFNULL(osd.avg_qty, 0) avg_qty, IF(
      t.stock_num_slot > 0, 0, IFNULL(osd.avg_qty, 0.06) * t.sale_price
    ) miss_val, @add_user add_user
  FROM
    feods.d_op_s7p_detail t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN feods.onway_tmp ow
      ON t.shelf_id = ow.shelf_id
      AND t.product_id = ow.product_id
    LEFT JOIN feods.offstock_detail_tmp osd
      ON t.shelf_id = osd.shelf_id
      AND t.product_id = osd.product_id
    LEFT JOIN feods.unact_tmp uf
      ON t.shelf_id = uf.shelf_id;
  DELETE
  FROM
    feods.d_op_offstock_s7
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 6 MONTH);
  INSERT INTO feods.d_op_offstock_s7 (
    sdate, business_name, shelf_id, unact_flag, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, add_user
  )
  SELECT
    sdate, business_name, shelf_id, unact_flag, SUM(slots) slots, SUM(slots_sto) slots_sto, COUNT(*) skus, SUM(stock_num_slot > 0) skus_sto, SUM(stock_num) stock_num, SUM(slot_capacity_limit) slot_capacity_limit, SUM(stock_num_slot) stock_num_slot, SUM(stock_num_second) stock_num_second, SUM(onway_num) onway_num, SUM(avg_qty) avg_qty, SUM(avg_qty * sale_price) avg_val, SUM(
      IF(avg_qty > 0, avg_qty, .06) * sale_price
    ) avg_val_nmiss, SUM(miss_val) miss_val, @add_user add_user
  FROM
    feods.d_op_offstock_s7p
  WHERE sdate = @sdate
    AND slots > 0
  GROUP BY shelf_id;
  DELETE
  FROM
    feods.d_op_offstock_m7
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 3 MONTH);
  INSERT INTO feods.d_op_offstock_m7 (
    sdate, manager_id, unact_flag, shelfs, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, add_user
  )
  SELECT
    @sdate sdate, s.manager_id, t.unact_flag, COUNT(*) shelfs, SUM(t.slots) slots, SUM(t.slots_sto) slots_sto, SUM(t.skus) skus, SUM(t.skus_sto) skus_sto, SUM(t.stock_num) stock_num, SUM(t.slot_capacity_limit) slot_capacity_limit, SUM(t.stock_num_slot) stock_num_slot, SUM(t.stock_num_second) stock_num_second, SUM(t.onway_num) onway_num, SUM(t.avg_qty) avg_qty, SUM(t.avg_val) avg_val, SUM(t.avg_val_nmiss) avg_val_nmiss, SUM(t.miss_val) miss_val, @add_user add_user
  FROM
    feods.d_op_offstock_s7 t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.manager_id)
  WHERE t.sdate = @sdate
  GROUP BY s.manager_id, t.unact_flag;
  DELETE
  FROM
    feods.d_op_offstock_s7_key
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 3 MONTH);
  INSERT INTO feods.d_op_offstock_s7_key (
    sdate, business_name, shelf_id, unact_flag, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, reason_classify, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_id, t.unact_flag, t.slots, t.slots_sto, t.skus, t.skus_sto, t.stock_num, t.slot_capacity_limit, t.stock_num_slot, t.stock_num_second, t.onway_num, t.avg_qty, t.avg_val, t.avg_val_nmiss, t.miss_val, sc.reason_classify, @add_user add_user
  FROM
    feods.d_op_offstock_s7 t
    JOIN
      (SELECT
        t.shelf_id, COUNT(*) ct
      FROM
        feods.d_op_offstock_s7 t
      WHERE t.sdate BETWEEN SUBDATE(@sdate, 2)
        AND @sdate
        AND t.slots >= slots_sto + 5
      GROUP BY t.shelf_id
      HAVING ct = 3) sk
      ON t.shelf_id = sk.shelf_id
    LEFT JOIN
      (SELECT
        t.shelf_id, SUBSTRING_INDEX(
          GROUP_CONCAT(
            t.reason_classify
            ORDER BY ct DESC, miss_val DESC, reason_classify
          ), ',', 1
        ) reason_classify
      FROM
        (SELECT
          t.shelf_id, t.reason_classify, COUNT(*) ct, SUM(
            IF(avg_qty > 0, avg_qty, .06) * sale_price
          ) miss_val
        FROM
          feods.d_op_offstock_slot t
        WHERE t.sdate = @sdate
          AND ! ISNULL(t.reason_classify)
        GROUP BY t.shelf_id, t.reason_classify) t
      GROUP BY t.shelf_id) sc
      ON t.shelf_id = sc.shelf_id
  WHERE t.sdate = @sdate;
  DELETE
  FROM
    feods.d_op_offstock_area7
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 24 MONTH);
  INSERT INTO feods.d_op_offstock_area7 (
    sdate, business_name, unact_flag, shelfs, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, add_user
  )
  SELECT
    @sdate sdate, business_name, unact_flag, COUNT(*) shelfs, SUM(slots) slots, SUM(slots_sto) slots_sto, SUM(skus) skus, SUM(skus_sto) skus_sto, SUM(stock_num) stock_num, SUM(slot_capacity_limit) slot_capacity_limit, SUM(stock_num_slot) stock_num_slot, SUM(stock_num_second) stock_num_second, SUM(onway_num) avg_qty, SUM(avg_qty) avg_qty, SUM(avg_val) avg_val, SUM(avg_val_nmiss) avg_val_nmiss, SUM(miss_val) miss_val, @add_user add_user
  FROM
    feods.d_op_offstock_s7
  WHERE sdate = @sdate
  GROUP BY business_name, unact_flag;
  CALL feods.sp_task_log (
    'sp_op_offstock_slot', @sdate, CONCAT(
      'yingnansong_d_eee74f715e5dd187edc07a3f3f1f29e4', @timestamp, @add_user
    )
  );
  COMMIT;
END