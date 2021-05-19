CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_loss_value_total`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  DELETE
  FROM
    feods.zs_sale_loss_detail_day
  WHERE sdate = CURDATE();
  INSERT INTO feods.zs_sale_loss_detail_day (
    sdate,
    shelf_id,
    product_id,
    stype,
    loss_value
  )
  SELECT
    CURDATE() AS sdate,
    a.shelf_id,
    a.product_id,
    CASE
      WHEN m.product_type IN (
        '新增（试运行）',
        '新增（正式运行）'
      )
      THEN '新增'
      WHEN m.product_type NOT IN ('原有')
      THEN '淘汰品'
      WHEN n.stock_qty < o.limit_qty
      THEN '仓库缺货'
      WHEN l.shelf_id IS NOT NULL
      THEN '前置仓'
      WHEN q.ONWAY_NUM > 0
      THEN '在途'
      WHEN c.shelf_type IN (2, 5)
      AND r.stock_qty > 140
      THEN '高库存'
      WHEN c.shelf_type IN (1, 3)
      AND r.stock_qty > 180
      THEN '高库存'
      WHEN p.ACTUAL_APPLY_value < 150
      THEN '订单金额不足'
      WHEN s.shelf_id IS NULL
      THEN '未发补货订单'
      ELSE '原因不明'
    END,
    SUM(DAY_AVG_SALE_NUM * a.SALE_PRICE)
  FROM
    fe.sf_shelf_product_detail a
    LEFT JOIN fe.sf_statistics_pre_fourteen_sale_product b
      ON b.SHELF_ID = a.SHELF_ID
      AND b.PRODUCT_ID = a.PRODUCT_ID
    LEFT JOIN fe.sf_shelf c
      ON a.shelf_id = c.shelf_id
    LEFT JOIN fe.sf_shelf_product_detail_flag d
      ON a.product_id = d.product_id
      AND a.shelf_id = d.shelf_id
    LEFT JOIN fe.zs_city_business e
      ON e.CITY_NAME = SUBSTRING_INDEX(
        SUBSTRING_INDEX(c.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      )
    LEFT JOIN fe.sf_product f
      ON a.product_id = f.product_id
    LEFT JOIN feods.zs_qingdangdaoru_product_type m
      ON a.PRODUCT_ID = m.product_id
      AND e.BUSINESS_AREA = m.BUSINESS_AREA
    LEFT JOIN fe.sf_prewarehouse_shelf_detail l
      ON (
        a.shelf_id = l.shelf_id
        AND l.data_flag = 1
      )
    LEFT JOIN
      (SELECT
        f.BUSINESS_AREA,
        d.fnumber AS PRODUCT_CODE2,
        SUM(
          CASE
            WHEN g.FNAME = '正品'
            THEN a.fbaseqty
          END
        ) AS stock_qty
      FROM
        sserp.T_STK_INVENTORY a
        LEFT JOIN sserp.T_BD_STOCK_L b
          ON a.FSTOCKID = b.FSTOCKID
        LEFT JOIN sserp.T_BD_MATERIAL_L c
          ON a.FMATERIALID = c.FMATERIALID
        LEFT JOIN sserp.T_BD_MATERIAL d
          ON a.FMATERIALID = d.FMATERIALID
        LEFT JOIN sserp.T_BD_STOCK e
          ON a.FSTOCKID = e.FSTOCKID
        LEFT JOIN sserp.ZS_DC_BUSINESS_AREA f
          ON e.fnumber = f.DC_CODE
        LEFT JOIN sserp.T_BD_STOCKSTATUS_L g
          ON g.FSTOCKSTATUSID = a.FSTOCKSTATUSID
        LEFT JOIN sserp.T_BD_MATERIALGROUP_L i
          ON d.FMATERIALGROUP = i.fid
      GROUP BY f.BUSINESS_AREA,
        d.fnumber) n
      ON f.product_code2 = n.product_code2
      AND e.BUSINESS_AREA = n.BUSINESS_AREA
    LEFT JOIN feods.zs_qingdangdaoru_limit_qty o
      ON e.BUSINESS_AREA = o.BUSINESS_AREA
    LEFT JOIN
      (SELECT
        a.SHELF_ID,
        SUM(ACTUAL_APPLY_NUM * SALE_PRICE) AS ACTUAL_APPLY_value
      FROM
        fe.sf_product_fill_requirement a
        LEFT JOIN fe.sf_shelf_product_detail b
          ON b.shelf_id = a.shelf_id
          AND b.product_id = a.product_id
      GROUP BY a.SHELF_ID) p
      ON a.SHELF_ID = p.shelf_id
    LEFT JOIN
      (SELECT
        SHELF_ID,
        PRODUCT_ID,
        ONWAY_NUM
      FROM
        fe.sf_product_fill_requirement
      WHERE ONWAY_NUM > 0) q
      ON a.SHELF_ID = q.shelf_id
      AND a.product_id = q.product_id
    LEFT JOIN
      (SELECT
        a.SHELF_ID,
        SUM(STOCK_QUANTITY) AS stock_qty
      FROM
        fe.sf_shelf_product_detail a
      GROUP BY a.SHELF_ID) r
      ON a.SHELF_ID = r.shelf_id
    LEFT JOIN
      (SELECT DISTINCT
        a.shelf_id
      FROM
        fe.sf_product_fill_order a
      WHERE DATE_FORMAT(APPLY_TIME, '%Y%m%d') = DATE_FORMAT(CURDATE(), '%Y%m%d')) s
      ON a.SHELF_ID = s.shelf_id
  WHERE a.data_flag = 1
    AND a.SHELF_FILL_FLAG = 1
    AND c.REVOKE_STATUS = 1
    AND WHETHER_CLOSE = 2
    AND STOCK_DAY_NUM > 0
    AND STOCK_QUANTITY = 0
    AND d.SALES_FLAG IN (1, 2, 3, 4, 5)
  GROUP BY a.shelf_id,
    a.product_id,
    CASE
      WHEN m.product_type IN (
        '新增（试运行）',
        '新增（正式运行）'
      )
      THEN '新增'
      WHEN m.product_type NOT IN ('原有')
      THEN '淘汰品'
      WHEN n.stock_qty < o.limit_qty
      THEN '仓库缺货'
      WHEN l.shelf_id IS NOT NULL
      THEN '前置仓'
      WHEN q.ONWAY_NUM > 0
      THEN '在途'
      WHEN c.shelf_type IN (2, 5)
      AND r.stock_qty > 140
      THEN '高库存'
      WHEN c.shelf_type IN (1, 3)
      AND r.stock_qty > 180
      THEN '高库存'
      WHEN p.ACTUAL_APPLY_value < 150
      THEN '订单金额不足'
      WHEN s.shelf_id IS NULL
      THEN '未发补货订单'
      ELSE '原因不明'
    END;
  DELETE
  FROM
    feods.pj_loss_value_total
  WHERE sdate = CURDATE();
  INSERT INTO feods.pj_loss_value_total (sdate, loss_reason, loss_value)
  SELECT
    sdate,
    stype AS loss_reason,
    SUM(loss_value) AS loss_value
  FROM
    feods.zs_sale_loss_detail_day
  WHERE sdate = CURDATE()
  GROUP BY sdate,
    stype;
    
    
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_loss_value_total',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
  COMMIT;
END