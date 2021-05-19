CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_prewarehouse_manager_salary_scheme`()
BEGIN
  -- =============================================
-- Author:	物流前置仓
-- Create date: 2019/04/15
-- Modify date: 
-- Description:	
-- 	前置站盘点明细与补货订单明细宽表更新-前置站长薪资报表的中间表（每月1号的3时）
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
  SET @target_month:= DATE_SUB(CURDATE()-DAY(CURDATE())+1,INTERVAL 1 MONTH);
  
  DELETE
  FROM
    feods.csl_prewarehouse_order_detail
  WHERE fill_time>= @target_month AND fill_time < DATE_ADD(@target_month,INTERVAL 1 MONTH);
  DELETE
  FROM
    feods.`csl_prewarehouse_check_detail`
  WHERE operate_time >= @target_month AND operate_time < DATE_ADD(@target_month,INTERVAL 1 MONTH);
  
--   前置站补货订单明细中间表
   INSERT INTO feods.csl_prewarehouse_order_detail (
    APPLY_TIME,
    fill_time,
    SUPPLIER_TYPE,
    ORDER_ID,
    shelf_id,
    SHELF_CODE,
    SHELF_NAME,
    target_shelf_id,
    ORDER_STATUS,
    business_area,
    BRANCH_CODE,
    BRANCH_NAME,
    SF_CODE,
    REAL_NAME,
    FILL_TYPE,
    PRODUCT_TYPE_NUM,
    PRODUCT_NUM,
    TOTAL_PRICE,
    product_id,
    product_name,
    FILL_UNIT,
    FILL_MODEL,
    ACTUAL_FILL_NUM,
    box_fill_model,
    prod_pcs
  )
  (SELECT
    DATE_FORMAT(APPLY_TIME, '%Y%m%d') AS 申请时间,
    DATE_FORMAT(FILL_TIME, '%Y%m%d') AS 上架时间,
    a.SUPPLIER_TYPE,
    CONCAT('"', a.ORDER_ID, '"') AS 订单id,
    a.shelf_id,
    b.SHELF_CODE,
    b.SHELF_NAME,
    c.target_shelf_id,
    a.ORDER_STATUS,
    d.business_area,
    e.BRANCH_CODE,
    e.BRANCH_NAME,
    e.SF_CODE,
    e.REAL_NAME,
    a.FILL_TYPE AS '补货类型',
    a.PRODUCT_TYPE_NUM AS '商品种数',
    a.PRODUCT_NUM AS '商品总数',
    a.TOTAL_PRICE AS '商品金额',
    g.product_id AS '商品编号',
    g.product_name AS '商品名称',
    g.FILL_UNIT AS '补货单位',
    g.FILL_MODEL AS '补货规格',
    f.ACTUAL_FILL_NUM AS '实际补货量',
    g.fill_model AS '装箱规格',
    CEIL(f.ACTUAL_FILL_NUM / g.fill_model) AS 'prod_pcs'
  FROM
    fe.sf_product_fill_order a
    LEFT JOIN fe.sf_shelf b
      ON a.shelf_id = b.shelf_id
    LEFT JOIN fe.sf_shelf_goods_transfer c
      ON a.order_id = c.source_order_id
    LEFT JOIN fe.zs_city_business d
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      ) = d.CITY_NAME
    LEFT JOIN fe.pub_shelf_manager e
      ON b.manager_id = e.manager_id
    LEFT JOIN fe.sf_product_fill_order_item f
      ON a.order_id = f.order_id
    LEFT JOIN fe.sf_product g
      ON f.product_id = g.product_id
  WHERE SHELF_NAME LIKE '%前置%'
    AND a.fill_type NOT IN (8, 9, 10)
    AND a.order_status = 4
    AND a.fill_time >= @target_month AND a.fill_time< DATE_ADD(@target_month,INTERVAL 1 MONTH))
  UNION
  (SELECT
    DATE_FORMAT(APPLY_TIME, '%Y%m%d') AS 申请时间,
    DATE_FORMAT(FILL_TIME, '%Y%m%d') AS 上架时间,
    a.SUPPLIER_TYPE,
    CONCAT('"', a.ORDER_ID, '"') AS 订单id,
    a.SUPPLIER_ID,
    b.SHELF_CODE,
    b.SHELF_NAME,
    a.SHELF_ID,
    a.ORDER_STATUS,
    d.business_area,
    e.BRANCH_CODE,
    e.BRANCH_NAME,
    e.SF_CODE,
    e.REAL_NAME,
    a.FILL_TYPE,
    a.PRODUCT_TYPE_NUM,
    a.PRODUCT_NUM,
    a.TOTAL_PRICE,
    g.product_id AS '商品编号',
    g.product_name AS '商品名称',
    g.FILL_UNIT AS '补货单位',
    g.FILL_MODEL AS '补货规格',
    f.ACTUAL_FILL_NUM AS '实际补货量',
    g.fill_model AS '装箱规格',
    CEIL(f.ACTUAL_FILL_NUM / g.fill_model) AS 'prod_pcs'
  FROM
    fe.sf_product_fill_order a
    LEFT JOIN fe.sf_shelf b
      ON a.SUPPLIER_ID = b.shelf_id
    LEFT JOIN fe.zs_city_business d
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      ) = d.CITY_NAME
    LEFT JOIN fe.pub_shelf_manager e
      ON b.manager_id = e.manager_id
    LEFT JOIN fe.sf_product_fill_order_item f
      ON a.order_id = f.order_id
    LEFT JOIN fe.sf_product g
      ON f.product_id = g.product_id
  WHERE a.fill_type IN (8, 9, 10)
    AND a.order_status = 4
    AND a.fill_time >= @target_month AND a.fill_time<DATE_ADD(@target_month,INTERVAL 1 MONTH))
  UNION
  (SELECT
    DATE_FORMAT(APPLY_TIME, '%Y%m%d') AS 申请时间,
    DATE_FORMAT(FILL_TIME, '%Y%m%d') AS 上架时间,
    a.SUPPLIER_TYPE,
    CONCAT('"', a.ORDER_ID, '"') AS 订单id,
    a.SUPPLIER_ID,
    b.SHELF_CODE,
    b.SHELF_NAME,
    a.SHELF_ID,
    a.ORDER_STATUS,
    d.business_area,
    e.BRANCH_CODE,
    e.BRANCH_NAME,
    e.SF_CODE,
    e.REAL_NAME,
    a.FILL_TYPE,
    a.PRODUCT_TYPE_NUM,
    a.PRODUCT_NUM,
    a.TOTAL_PRICE,
    g.product_id AS '商品编号',
    g.product_name AS '商品名称',
    g.FILL_UNIT AS '补货单位',
    g.FILL_MODEL AS '补货规格',
    f.ACTUAL_FILL_NUM AS '实际补货量',
    g.fill_model AS '装箱规格',
    CEIL(f.ACTUAL_FILL_NUM / g.fill_model) AS 'prod_pcs'
  FROM
    fe.sf_product_fill_order a
    LEFT JOIN fe.sf_shelf b
      ON a.SUPPLIER_ID = b.shelf_id
    LEFT JOIN fe.zs_city_business d
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(b.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      ) = d.CITY_NAME
    LEFT JOIN fe.pub_shelf_manager e
      ON b.manager_id = e.manager_id
    LEFT JOIN fe.sf_product_fill_order_item f
      ON a.order_id = f.order_id
    LEFT JOIN fe.sf_product g
      ON f.product_id = g.product_id
  WHERE a.fill_type IN (1, 2)
    AND a.order_status = 4
    AND a.SUPPLIER_ID IN
    (SELECT DISTINCT
      warehouse_id
    FROM
      fe.sf_prewarehouse_shelf_detail)
    AND a.fill_time >= @target_month AND a.fill_time < DATE_ADD(@target_month,INTERVAL 1 MONTH));
    
    
    
--   前置站盘点明细中间表
   INSERT INTO feods.csl_prewarehouse_check_detail (
    shelf_id,
    shelf_code,
    SHELF_NAME,
    business_area,
    city_name,
    sf_code,
    real_name,
    CHECK_ID,
    OPERATE_TIME,
    stock_amount,
    check_amount,
    error_amount,
    audit_error_amount,
    damage_goods_amount,
    AUDIT_damage_goods_amount,
    overdue_product_amount,
    audit_overdue_product_amount,
    check_loss_amount,
    audit_check_loss_amount,
    product_quality_amount,
    audit_product_quality_amount,
    other_difference_amount,
    audit_other_difference_amount
  )
  (SELECT
    a.shelf_id AS '货架ID',
    t3.shelf_code AS '货架编码',
    t3.shelf_name AS '前置仓名称',
    t4.business_area AS '区域',
    t4.city_name AS '城市',
    t5.sf_code AS '管理员工号',
    t5.real_name AS '管理员名称',
    a.CHECK_ID AS '盘点ID',
    DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d') AS '盘点时间',
    SUM(STOCK_NUM * SALE_PRICE) AS '库存金额',
    SUM(CHECK_NUM * SALE_PRICE) AS '盘点金额',
    SUM(ERROR_NUM * SALE_PRICE) AS '差异金额',
    SUM(
      CASE
        WHEN a.AUDIT_STATUS = 2
        THEN ERROR_NUM * SALE_PRICE
      END
    ) AS '审核通过后差异金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 1
        AND a.AUDIT_STATUS = 2
        THEN ERROR_NUM * SALE_PRICE
      END
    ) AS '货物破损金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 1
        AND a.AUDIT_STATUS = 2
        THEN AUDIT_ERROR_NUM * SALE_PRICE
      END
    ) AS '货物破损审核后异常金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 2
        AND a.AUDIT_STATUS = 2
        THEN ERROR_NUM * SALE_PRICE
      END
    ) AS '商品过期金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 2
        AND a.AUDIT_STATUS = 2
        THEN AUDIT_ERROR_NUM * SALE_PRICE
      END
    ) AS '商品过期审核后异常金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 3
        AND a.AUDIT_STATUS = 2
        THEN ERROR_NUM * SALE_PRICE
      END
    ) AS '盘点盗损金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 3
        AND a.AUDIT_STATUS = 2
        THEN AUDIT_ERROR_NUM * SALE_PRICE
      END
    ) AS '盘点盗损审核后异常金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 4
        AND a.AUDIT_STATUS = 2
        THEN ERROR_NUM * SALE_PRICE
      END
    ) AS '商品质量金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 4
        AND a.AUDIT_STATUS = 2
        THEN AUDIT_ERROR_NUM * SALE_PRICE
      END
    ) AS '商品质量审核后异常金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 5
        AND a.AUDIT_STATUS = 2
        THEN ERROR_NUM * SALE_PRICE
      END
    ) AS '其他差异金额',
    SUM(
      CASE
        WHEN ERROR_REASON = 5
        AND a.AUDIT_STATUS = 2
        THEN AUDIT_ERROR_NUM * SALE_PRICE
      END
    ) AS '其他差异审核后异常金额'
  FROM
    fe.sf_shelf_check_detail a
    LEFT JOIN fe.sf_shelf_check b
      ON a.check_id = b.check_id
    LEFT JOIN fe.sf_shelf t3
      ON b.shelf_id = t3.shelf_id
    LEFT JOIN fe.zs_city_business t4
      ON SUBSTRING_INDEX(
        SUBSTRING_INDEX(t3.AREA_ADDRESS, ',', 2),
        ',',
        - 1
      ) = t4.city_name
    LEFT JOIN fe.pub_shelf_manager t5
      ON t3.manager_id = t5.manager_id
  WHERE b.shelf_id IN
    (SELECT DISTINCT
      warehouse_id AS shelf_id
    FROM
      fe.sf_prewarehouse_dept_detail)
    AND b.operate_time >= @target_month AND b.operate_time < DATE_ADD(@target_month,INTERVAL 1 MONTH)
  GROUP BY a.shelf_id,
    a.CHECK_ID,
    DATE_FORMAT(b.OPERATE_TIME, '%Y%m%d'));
    
    ### 执行记录日志
    CALL sh_process.`sp_sf_dw_task_log`(
  'sp_prewarehouse_manager_salary_scheme',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);
COMMIT;
END