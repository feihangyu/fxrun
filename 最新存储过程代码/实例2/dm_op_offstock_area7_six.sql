CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_offstock_area7_six`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
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
DROP TEMPORARY TABLE IF EXISTS fe_dm.`hubei_shelf_tmp`;
CREATE TEMPORARY TABLE fe_dm.`hubei_shelf_tmp` (
        shelf_id INT(8),
        PRIMARY KEY `idx_shelf_id` (`shelf_id`)
        ) ;  
#SELECT * FROM  fe_dm.`hubei_shelf_tmp`
INSERT INTO fe_dm.`hubei_shelf_tmp`(shelf_id)
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
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.shelf_tmp (
    PRIMARY KEY (shelf_id), KEY (shelf_code)
  )
  SELECT
    t.shelf_id, t.shelf_code, t.shelf_status, t.whether_close, t.revoke_status, ! ISNULL(pw.shelf_id) if_prewarehouse, IFNULL(
      pw.prewarehouse_id, bdc.supplier_id
    ) supplier_id, sm.online_status, sm.slot_sync_status, t.business_name, t.manager_id, dg.gmv
  FROM
    fe_dwd.`dwd_shelf_base_day_all` t
    LEFT JOIN
      (SELECT
        MAX(t.supplier_id) supplier_id, b.business_area business_name
      FROM
        fe_dwd.`dwd_sf_supplier` t
        LEFT JOIN fe_dwd.`dwd_sserp_zs_dc_business_area` b
          ON t.depot_code = b.dc_code
      WHERE t.status = 2
        AND t.supplier_type = 2
      GROUP BY business_name) bdc
      ON t.business_name = bdc.business_name
    LEFT JOIN fe_dwd.`dwd_relation_dc_prewarehouse_shelf_day_all` pw
      ON t.shelf_id = pw.shelf_id
    LEFT JOIN fe_dwd.`dwd_shelf_machine_info` sm
      ON t.shelf_id = sm.shelf_id
    LEFT JOIN fe_dwd.`dwd_shelf_day_his` dg
      ON t.shelf_id = dg.shelf_id
      AND dg.sdate = @sdate
    LEFT JOIN fe_dm.`hubei_shelf_tmp` hs
        ON t.shelf_id = hs.shelf_id
  WHERE t.shelf_type = 7
    AND t.shelf_status = 2
    AND t.shelf_name NOT LIKE '%测试%'
    AND ! ISNULL(t.shelf_id)
    AND ISNULL(hs.shelf_id)
    AND t.business_name NOT IN ('山西区','冀州区','吉林区','江西区')       -- 2020-06 杨柳要求剔除4个撤城区
;
-- 上个月到当前有GMV或补付款的货架
DROP TEMPORARY TABLE IF EXISTS fe_dwd.gmv_shelf_tmp;
CREATE TEMPORARY TABLE fe_dwd.gmv_shelf_tmp (PRIMARY KEY (shelf_id))
SELECT
        DISTINCT shelf_id
FROM
        fe_dwd.`dwd_shelf_day_his`
WHERE sdate >= @month_start_last
        AND (gmv > 0 OR AFTER_PAYMENT_MONEY > 0)
        AND ! ISNULL(shelf_id)
;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.mgmv_tmp;
  CREATE TEMPORARY TABLE fe_dwd.mgmv_tmp (PRIMARY KEY (shelf_id))
  SELECT DISTINCT
    t.shelf_id
  FROM
    fe_dwd.gmv_shelf_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
    UNION
    SELECT DISTINCT
      IFNULL(t.shelf_id, s.shelf_id) shelf_id
    FROM
      fe_dwd.`dwd_op_out_of_system_order_yht` t
      LEFT JOIN fe_dwd.`dwd_shelf_base_day_all` s
        ON t.shelf_id = s.shelf_id
        AND s.shelf_type = 7
        AND s.shelf_status = 2
        AND s.shelf_name NOT LIKE '%测试%'
    WHERE t.pay_date >= @month_start_last
      AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
      AND t.data_flag = 1;
      
--   DROP TEMPORARY TABLE IF EXISTS fe_dwd.pack_tmp;
--   CREATE TEMPORARY TABLE fe_dwd.pack_tmp (
--     PRIMARY KEY (package_id, product_id)
--   )
--   SELECT
--     t.package_id, s.product_id, t.alarm_quantity, t.shelf_fill_flag
--   FROM
--     fe_dwd.`dwd_package_information` t
--     JOIN fe_dwd.`dwd_sf_supplier_product_detail` s
--       ON t.relation_id = s.detail_id
--   WHERE  
-- --   t.statu_flag = 1
--     ! ISNULL(s.product_id)
--     AND ! ISNULL(t.package_id);
    
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.slot_tmp;
  CREATE TEMPORARY TABLE fe_dwd.slot_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(IF(t.stock_num > 0, t.stock_num, 0)) stock_num, SUM(t.slot_capacity_limit) slot_capacity_limit, COUNT(*) slots, SUM(t.stock_num > 0) slots_sto
  FROM
    fe_dwd.`dwd_shelf_machine_slot_type` t
  WHERE  ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.slot_shelf_tmp;
  CREATE TEMPORARY TABLE fe_dwd.slot_shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.stock_num) stock_num, SUM(t.slots) slots, SUM(t.slots_sto) slots_sto
  FROM
    fe_dwd.slot_tmp t
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.offstock_tmp;
  CREATE TEMPORARY TABLE fe_dwd.offstock_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(t.suggest_fill_val) suggest_fill_val, SUM(
      IF(
        t.supplier_type = 2, t.suggest_fill_val, 0
      )
    ) suggest_fill_val2, SUM(t.offstock_val) offstock_val
  FROM
    fe_dm.dm_op_sp_offstock t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.offstock_detail_tmp;
  CREATE TEMPORARY TABLE fe_dwd.offstock_detail_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, t.suggest_fill_num, t.cank_stock_qty, t.onway_num, IF(
      t.gmv > 0, t.gmv / t.days_sal_sto, 0.06 * t.sale_price
    ) avg_gmv, t.qty_sal / t.days_sal_sto avg_qty,actual_apply_num
  FROM
    fe_dm.dm_op_sp_offstock t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
    
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.yht_tmp;
  CREATE TEMPORARY TABLE fe_dwd.yht_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    IFNULL(t.shelf_id, s.shelf_id) shelf_id, t.product_id, SUM(t.amount * t.sale_price) / (
      DATEDIFF(
        @sdate, GREATEST(MIN(t.pay_date), @avg_start_day)
      ) + 1
    ) avg_gmv, SUM(t.amount) / (
      DATEDIFF(
        @sdate, GREATEST(MIN(t.pay_date), @avg_start_day)
      ) + 1
    ) avg_qty
  FROM
    fe_dwd.`dwd_op_out_of_system_order_yht` t
    LEFT JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.shelf_id)
  WHERE pay_date >= @avg_start_day
    AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
    AND ! ISNULL(t.product_id)
    AND t.data_flag = 1
  GROUP BY IFNULL(t.shelf_id, s.shelf_id), product_id;
  UPDATE
    fe_dwd.offstock_detail_tmp t
    JOIN fe_dwd.yht_tmp yht
      ON t.shelf_id = yht.shelf_id
      AND t.product_id = yht.product_id SET t.avg_gmv = yht.avg_gmv, t.avg_qty = yht.avg_qty;
      
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.yht_insert_tmp;
  CREATE TEMPORARY TABLE fe_dwd.yht_insert_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.*
  FROM
    fe_dwd.yht_tmp t
    LEFT JOIN fe_dwd.offstock_detail_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
  WHERE ISNULL(d.shelf_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
    
  INSERT INTO fe_dwd.offstock_detail_tmp (
    shelf_id, product_id, avg_gmv, avg_qty
  )
  SELECT
    shelf_id, product_id, avg_gmv, avg_qty
  FROM
    fe_dwd.yht_insert_tmp;
    
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.offval_tmp;
  CREATE TEMPORARY TABLE fe_dwd.offval_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id, SUM(o.avg_gmv) offval
  FROM
    fe_dwd.slot_tmp t
    JOIN fe_dwd.offstock_detail_tmp o
      ON t.shelf_id = o.shelf_id
      AND t.product_id = o.product_id
      AND o.avg_gmv > 0
  WHERE t.stock_num = 0
    AND ! ISNULL(t.shelf_id)
  GROUP BY t.shelf_id;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.yht_yes_tmp;
  CREATE TEMPORARY TABLE fe_dwd.yht_yes_tmp (PRIMARY KEY (shelf_id))
  SELECT
    IFNULL(t.shelf_id, s.shelf_id) shelf_id, SUM(t.amount * t.sale_price) gmv
  FROM
    fe_dwd.`dwd_op_out_of_system_order_yht` t
    LEFT JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.shelf_id)
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
  GROUP BY IFNULL(t.shelf_id, s.shelf_id);
  UPDATE
    fe_dwd.shelf_tmp t
    JOIN fe_dwd.yht_yes_tmp yht
      ON t.shelf_id = yht.shelf_id SET t.gmv = yht.gmv;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.onway_tmp;
  CREATE TEMPORARY TABLE fe_dwd.onway_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.actual_apply_num) actual_apply_num
  FROM
    fe_dwd.`dwd_fill_day_inc` t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND t.actual_apply_num > 0
  WHERE t.order_status IN (1, 2)
    AND t.apply_time >= SUBDATE(CURRENT_DATE, INTERVAL 1 MONTH)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.slot_detail_tmp;
  CREATE TEMPORARY TABLE fe_dwd.slot_detail_tmp (
    PRIMARY KEY (slot_id), KEY (
      shelf_id, product_id, manufacturer_slot_code
    ), KEY (
      machine_id, manufacturer_slot_code
    )
  )
  SELECT
    @slot_capacity_limit := IF(
      @shelf_id = t.shelf_id && @product_id = t.product_id, @slot_capacity_limit, 0
    ) + t.slot_capacity_limit slot_capacity_limit_cum, @shelf_id := t.shelf_id shelf_id, @product_id := t.product_id product_id, t.manufacturer_slot_code, t.slot_status, t.stock_num, t.slot_id, t.machine_id, t.slot_capacity_limit, osd.avg_qty, f.sale_price, f.max_quantity, f.sales_flag, f.shelf_fill_flag, p.product_code2, p.product_name
  FROM
    fe_dwd.`dwd_shelf_machine_slot_type` t
    JOIN
      (SELECT
        @slot_capacity_limit := 0, @shelf_id := 0, @product_id := 0) s
    LEFT JOIN fe_dwd.offstock_detail_tmp osd
      ON t.shelf_id = osd.shelf_id
      AND t.product_id = osd.product_id
    LEFT JOIN fe_dwd.`dwd_shelf_product_day_all` f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id
    LEFT JOIN fe_dwd.`dwd_product_base_day_all` p
      ON t.product_id = p.product_id
  WHERE  ! ISNULL(t.product_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.slot_id)
  ORDER BY t.shelf_id, t.product_id, t.slot_id;
  
--   DROP TEMPORARY TABLE IF EXISTS fe_dwd.pack_tmp1;
--   CREATE TEMPORARY TABLE fe_dwd.pack_tmp1 (
--     PRIMARY KEY (package_id, product_id)
--   )
--   SELECT
--     package_id, product_id, alarm_quantity, shelf_fill_flag
--   FROM
--     fe_dwd.pack_tmp;
--   DROP TEMPORARY TABLE IF EXISTS fe_dwd.sp_sal_tmp;
  CREATE TEMPORARY TABLE fe_dwd.sp_sal_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.quantity_act) quantity_act
  FROM
    fe_dwd.dwd_pub_order_item_recent_one_month t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.pay_date >= @sdate
    AND t.pay_date < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.yht_d1_tmp;
  CREATE TEMPORARY TABLE fe_dwd.yht_d1_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.*
  FROM
    (SELECT
      IFNULL(t.shelf_id, s.shelf_id) shelf_id, t.product_id, SUM(t.amount) quantity_act
    FROM
      fe_dwd.`dwd_op_out_of_system_order_yht` t
      LEFT JOIN fe_dwd.shelf_tmp s
        ON t.shelf_id = s.shelf_id
        AND ! ISNULL(s.shelf_id)
    WHERE t.pay_date >= @sdate
      AND t.pay_date < @add_day
      AND ! ISNULL(IFNULL(t.shelf_id, s.shelf_id))
      AND ! ISNULL(t.product_id)
    GROUP BY IFNULL(t.shelf_id, s.shelf_id), product_id) t
    LEFT JOIN fe_dwd.sp_sal_tmp s
      ON t.shelf_id = s.shelf_id
      AND t.product_id = s.product_id
  WHERE ISNULL(s.shelf_id)
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  INSERT INTO fe_dwd.sp_sal_tmp
  SELECT
    *
  FROM
    fe_dwd.yht_d1_tmp;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.sp_fil_tmp;
  CREATE TEMPORARY TABLE fe_dwd.sp_fil_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id, t.product_id, SUM(t.actual_fill_num) actual_fill_num
  FROM
    fe_dwd.`dwd_fill_day_inc` t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.fill_time >= @sdate
    AND t.fill_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
  GROUP BY t.shelf_id, t.product_id;
  
-- 所有订单
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.total_fill_tmp;
  CREATE TEMPORARY TABLE fe_dwd.total_fill_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
  DISTINCT 
    t.shelf_id,
    t.product_id,
    SUM(t.actual_apply_num) AS actual_apply_num
  FROM
    fe_dwd.`dwd_fill_day_inc` t
  WHERE t.FILL_TYPE IN (1,2,3,4,7,8,9)
    AND t.apply_time >= @sdate
    AND t.apply_time < @add_day
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id)
;  
-- 取消订单
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fill_cancel_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fill_cancel_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
  DISTINCT 
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
;  
-- 当前缺货货道，排除停补，换新不少于5条货道之外，且余下能补货的，大仓覆盖货架不足150元；前置仓不足50元，直接归因停补过多
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.stop_fill_tmp;
  CREATE TEMPORARY TABLE fe_dwd.stop_fill_tmp (PRIMARY KEY (shelf_id))
  SELECT
s.shelf_id  
  FROM
    fe_dwd.slot_detail_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dwd.`dwd_sf_shelf_machine_product_change` pc
      ON t.slot_id = pc.slot_id
      AND pc.data_flag = 1
GROUP BY s.shelf_id
HAVING SUM(IF((t.shelf_fill_flag = 2 OR (t.product_id = pc.product_id AND pc.change_status = 1)) OR 
((t.product_id != pc.product_id AND t.shelf_fill_flag = 1) OR (t.product_id = IFNULL(pc.product_id, t.product_id) AND t.shelf_fill_flag = 2)),1,0)) >= 5 
AND
((SUM(IF(s.if_prewarehouse = 1 AND t.shelf_fill_flag = 1,IF(t.slot_capacity_limit -  t.stock_num > 0,t.slot_capacity_limit -  t.stock_num,0)* t.sale_price,0))  < 50) OR 
(SUM(IF(s.if_prewarehouse = 0 AND t.shelf_fill_flag = 1,IF(t.slot_capacity_limit -  t.stock_num > 0,t.slot_capacity_limit -  t.stock_num,0)* t.sale_price,0))  < 150))
;  
  DELETE
  FROM
    fe_dm.dm_op_offstock_slot
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 12 MONTH);
  INSERT INTO fe_dm.dm_op_offstock_slot (
    sdate, slot_id, business_name, shelf_id, shelf_status, revoke_status, whether_close, if_pre, online_status, slot_sync_status, manufacturer_slot_code, slot_status, product_id, max_quantity, stock_num, slot_capacity_limit, avg_qty, sale_price, sales_flag, shelf_fill_flag, reason_classify, add_user
  )
  SELECT
    @sdate sdate, t.slot_id, s.business_name, s.shelf_id, s.shelf_status, s.revoke_status, s.whether_close, s.if_prewarehouse if_pre, IFNULL(s.online_status, 0) online_status, IFNULL(s.slot_sync_status, 0) slot_sync_status, t.manufacturer_slot_code, t.slot_status, t.product_id, t.max_quantity, t.stock_num, t.slot_capacity_limit, IFNULL(t.avg_qty, 0) avg_qty, t.sale_price, t.sales_flag, IFNULL(t.shelf_fill_flag, 2) shelf_fill_flag, IF(
      t.stock_num <= 0,
      CASE
        WHEN osd.onway_num > 0 || ! ISNULL(ow.shelf_id)
        THEN '5.01在途'
        WHEN IFNULL(ss.stock_num, 0) = 0
        AND ISNULL(sff.shelf_id)
        AND s.shelf_status = 2
        THEN '1.01激活未上架'
        WHEN s.revoke_status != 1
        OR s.whether_close = 1
        THEN '1.02非正常货架'
--         WHEN (
--           pt.shelf_fill_flag = 2
--           AND t.product_id != pc.product_id
--         )
--         OR ISNULL(ptc.package_id)
--         THEN '1.03停止补货'
        WHEN t.shelf_fill_flag = 2 OR (t.product_id = pc.product_id AND pc.change_status = 1)
                THEN '1.03停止补货'
        /*WHEN ! ISNULL(pc.product_id) && ISNULL(pt.product_id) THEN '1.商品模版不符'*/
        WHEN msd.stock_num >= t.slot_capacity_limit_cum
        THEN '2.店主问题'
        /*WHEN IFNULL(pt.alarm_quantity, 0) < IFNULL(sl.slot_capacity_limit, 0) THEN '3.标配错误'*/
--         WHEN (
--           t.product_id != pc.product_id
--           AND t.shelf_fill_flag = 1
--         )
--         OR (
--           t.product_id = IFNULL(pc.product_id, t.product_id)
--           AND t.shelf_fill_flag = 2
--         )
--         THEN '4.换新导致'
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
        THEN '6.配置货道过少'
        WHEN dcs.qty_sto < whs.qty_req || whs.qty_sto < whs.qty_req
        THEN '7.仓库缺货'
        WHEN osd.suggest_fill_num > 0 && IFNULL(tf.actual_apply_num, 0) = 0
        THEN '8.1地区未下单'
        WHEN osd.suggest_fill_num > 0 && fc.cancel_num > 0 && IFNULL(osd.actual_apply_num,0) = 0
        THEN '8.2取消订单'
        WHEN sf.shelf_id IS NOT NULL
        THEN '8.3停补过多'
        ELSE '8.1地区未下单'
      END, NULL
    ) reason_classify, @add_user add_user
  FROM
    fe_dwd.slot_detail_tmp t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dm.`dm_op_shelf_firstfill` sff
      ON t.shelf_id = sff.shelf_id
    LEFT JOIN fe_dwd.slot_shelf_tmp ss
      ON t.shelf_id = ss.shelf_id
    LEFT JOIN fe_dwd.`dwd_sf_shelf_machine_product_change` pc
      ON t.slot_id = pc.slot_id
      AND pc.data_flag = 1
--     LEFT JOIN fe_dwd.`dwd_sf_shelf_package_detail` pd
--       ON t.shelf_id = pd.shelf_id
--       AND pd.data_flag = 1
--     LEFT JOIN fe_dwd.pack_tmp pt
--       ON pd.package_id = pt.package_id
--       AND t.product_id = pt.product_id
--     LEFT JOIN fe_dwd.pack_tmp1 ptc
--       ON pd.package_id = ptc.package_id
--       AND IFNULL(pc.product_id, t.product_id) = ptc.product_id
    LEFT JOIN fe_dwd.`dwd_shelf_machine_second_info` msd
      ON t.shelf_id = msd.shelf_id
      AND t.product_id = msd.product_id
    JOIN fe_dwd.slot_tmp sl
      ON t.shelf_id = sl.shelf_id
      AND t.product_id = sl.product_id
    LEFT JOIN fe_dwd.offstock_tmp os
      ON t.shelf_id = os.shelf_id
    LEFT JOIN fe_dwd.offstock_detail_tmp osd
      ON t.shelf_id = osd.shelf_id
      AND t.product_id = osd.product_id
    LEFT JOIN fe_dwd.onway_tmp ow
      ON t.shelf_id = ow.shelf_id
      AND t.product_id = ow.product_id
    LEFT JOIN fe_dwd.sp_sal_tmp spsal
      ON t.shelf_id = spsal.shelf_id
      AND t.product_id = spsal.product_id
    LEFT JOIN fe_dwd.sp_fil_tmp spfil
      ON t.shelf_id = spfil.shelf_id
      AND t.product_id = spfil.product_id
    LEFT JOIN fe_dm.dm_op_dc_reqsto dcs
      ON s.supplier_id = dcs.supplier_id
      AND t.product_id = dcs.product_id
      AND dcs.sdate = @sdate
    LEFT JOIN fe_dm.dm_op_pwh_reqsto whs
      ON s.supplier_id = whs.warehouse_id
      AND t.product_id = whs.product_id
      AND whs.sdate = @sdate
     LEFT JOIN fe_dwd.total_fill_tmp tf
        ON t.shelf_id = tf.shelf_id
        AND t.product_id = tf.product_id
     LEFT JOIN fe_dwd.fill_cancel_tmp fc
        ON t.shelf_id = fc.shelf_id
        AND t.product_id = fc.product_id
    LEFT JOIN fe_dwd.stop_fill_tmp sf
        ON t.shelf_id = sf.shelf_id
;
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.unact_tmp;
  CREATE TEMPORARY TABLE fe_dwd.unact_tmp (PRIMARY KEY (shelf_id))
  SELECT DISTINCT
    shelf_id
  FROM
    fe_dm.dm_op_offstock_slot
  WHERE sdate = @sdate
    AND reason_classify = '1.01激活未上架'
    AND ! ISNULL(shelf_id);
  DELETE
  FROM
    fe_dm.dm_op_offstock_s7p
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 3 MONTH);
  INSERT INTO fe_dm.dm_op_offstock_s7p (
    sdate, business_name, shelf_id, product_id, unact_flag, package_id, base_pack_flag, max_quantity, sale_price, sales_flag, shelf_fill_flag, slots, slots_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, miss_val, add_user
  )
  SELECT
    @sdate sdate, s.business_name, t.shelf_id, t.product_id, ! ISNULL(uf.shelf_id) unact_flag, t.package_id, t.base_pack_flag, t.max_quantity, t.sale_price, t.sales_flag, t.shelf_fill_flag, t.slots, t.slots_sto, t.stock_num, t.slot_capacity_limit, t.stock_num_slot, t.stock_num_second, IFNULL(ow.actual_apply_num, 0) onway_num, IFNULL(osd.avg_qty, 0) avg_qty, IF(
      t.stock_num_slot > 0, 0, IFNULL(osd.avg_qty, 0.06) * t.sale_price
    ) miss_val, @add_user add_user
  FROM
    fe_dm.dm_op_s7p_detail t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    LEFT JOIN fe_dwd.onway_tmp ow
      ON t.shelf_id = ow.shelf_id
      AND t.product_id = ow.product_id
    LEFT JOIN fe_dwd.offstock_detail_tmp osd
      ON t.shelf_id = osd.shelf_id
      AND t.product_id = osd.product_id
    LEFT JOIN fe_dwd.unact_tmp uf
      ON t.shelf_id = uf.shelf_id;
  DELETE
  FROM
    fe_dm.dm_op_offstock_s7
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 6 MONTH);
  INSERT INTO fe_dm.dm_op_offstock_s7 (
    sdate, business_name, shelf_id, unact_flag, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, add_user
  )
  SELECT
    sdate, business_name, shelf_id, unact_flag, SUM(slots) slots, SUM(slots_sto) slots_sto, COUNT(*) skus, SUM(stock_num_slot > 0) skus_sto, SUM(stock_num) stock_num, SUM(slot_capacity_limit) slot_capacity_limit, SUM(stock_num_slot) stock_num_slot, SUM(stock_num_second) stock_num_second, SUM(onway_num) onway_num, SUM(avg_qty) avg_qty, SUM(avg_qty * sale_price) avg_val, SUM(
      IF(avg_qty > 0, avg_qty, .06) * sale_price
    ) avg_val_nmiss, SUM(miss_val) miss_val, @add_user add_user
  FROM
    fe_dm.dm_op_offstock_s7p
  WHERE sdate = @sdate
    AND slots > 0
  GROUP BY shelf_id;
  DELETE
  FROM
    fe_dm.dm_op_offstock_m7
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 3 MONTH);
  INSERT INTO fe_dm.dm_op_offstock_m7 (
    sdate, manager_id, unact_flag, shelfs, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, add_user
  )
  SELECT
    @sdate sdate, s.manager_id, t.unact_flag, COUNT(*) shelfs, SUM(t.slots) slots, SUM(t.slots_sto) slots_sto, SUM(t.skus) skus, SUM(t.skus_sto) skus_sto, SUM(t.stock_num) stock_num, SUM(t.slot_capacity_limit) slot_capacity_limit, SUM(t.stock_num_slot) stock_num_slot, SUM(t.stock_num_second) stock_num_second, SUM(t.onway_num) onway_num, SUM(t.avg_qty) avg_qty, SUM(t.avg_val) avg_val, SUM(t.avg_val_nmiss) avg_val_nmiss, SUM(t.miss_val) miss_val, @add_user add_user
  FROM
    fe_dm.dm_op_offstock_s7 t
    JOIN fe_dwd.shelf_tmp s
      ON t.shelf_id = s.shelf_id
      AND ! ISNULL(s.manager_id)
  WHERE t.sdate = @sdate
  GROUP BY s.manager_id, t.unact_flag;
  DELETE
  FROM
    fe_dm.dm_op_offstock_s7_key
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 3 MONTH);
  INSERT INTO fe_dm.dm_op_offstock_s7_key (
    sdate, business_name, shelf_id, unact_flag, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, reason_classify, add_user
  )
  SELECT
    @sdate sdate, t.business_name, t.shelf_id, t.unact_flag, t.slots, t.slots_sto, t.skus, t.skus_sto, t.stock_num, t.slot_capacity_limit, t.stock_num_slot, t.stock_num_second, t.onway_num, t.avg_qty, t.avg_val, t.avg_val_nmiss, t.miss_val, sc.reason_classify, @add_user add_user
  FROM
    fe_dm.dm_op_offstock_s7 t
    JOIN
      (SELECT
        t.shelf_id, COUNT(*) ct
      FROM
        fe_dm.dm_op_offstock_s7 t
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
          fe_dm.dm_op_offstock_slot t
        WHERE t.sdate = @sdate
          AND ! ISNULL(t.reason_classify)
        GROUP BY t.shelf_id, t.reason_classify) t
      GROUP BY t.shelf_id) sc
      ON t.shelf_id = sc.shelf_id
  WHERE t.sdate = @sdate;
  DELETE
  FROM
    fe_dm.dm_op_offstock_area7
  WHERE sdate = @sdate OR sdate < SUBDATE(@sdate,INTERVAL 24 MONTH);
  INSERT INTO fe_dm.dm_op_offstock_area7 (
    sdate, business_name, unact_flag, shelfs, slots, slots_sto, skus, skus_sto, stock_num, slot_capacity_limit, stock_num_slot, stock_num_second, onway_num, avg_qty, avg_val, avg_val_nmiss, miss_val, add_user
  )
  SELECT
    @sdate sdate, business_name, unact_flag, COUNT(*) shelfs, SUM(slots) slots, SUM(slots_sto) slots_sto, SUM(skus) skus, SUM(skus_sto) skus_sto, SUM(stock_num) stock_num, SUM(slot_capacity_limit) slot_capacity_limit, SUM(stock_num_slot) stock_num_slot, SUM(stock_num_second) stock_num_second, SUM(onway_num) avg_qty, SUM(avg_qty) avg_qty, SUM(avg_val) avg_val, SUM(avg_val_nmiss) avg_val_nmiss, SUM(miss_val) miss_val, @add_user add_user
  FROM
    fe_dm.dm_op_offstock_s7
  WHERE sdate = @sdate
  GROUP BY business_name, unact_flag;
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dm_op_offstock_area7_six',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('宋英南@', @user),
@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_area7','dm_op_offstock_area7_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_m7','dm_op_offstock_area7_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_s7','dm_op_offstock_area7_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_s7p','dm_op_offstock_area7_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_s7_key','dm_op_offstock_area7_six','宋英南');
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_offstock_slot','dm_op_offstock_area7_six','宋英南');
  COMMIT;	
END