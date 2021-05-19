CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_res7`()
BEGIN
SET @run_date:= CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
  DROP TEMPORARY TABLE IF EXISTS feods.shelf_tmp;
  CREATE TEMPORARY TABLE feods.shelf_tmp (PRIMARY KEY (shelf_id))
  SELECT
    t.shelf_id,
    b.business_name
  FROM
    fe.sf_shelf t
    LEFT JOIN feods.fjr_city_business b
      ON t.city = b.city
  WHERE t.data_flag = 1
    AND t.shelf_type = 7
    AND t.shelf_name NOT LIKE '%测试%';
  DROP TEMPORARY TABLE IF EXISTS feods.fill_tmp;
  CREATE TEMPORARY TABLE feods.fill_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id,
    fi.product_id,
    MAX(t.apply_time) apply_time,
    MAX(t.fill_time) fill_time
  FROM
    fe.sf_product_fill_order t
    JOIN fe.sf_product_fill_order_item fi
      ON t.order_id = fi.order_id
      AND fi.data_flag = 1
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
  WHERE t.data_flag = 1
    AND t.order_status != 9
    AND t.apply_time >= 20181101
  GROUP BY t.shelf_id,
    fi.product_id;
  DROP TEMPORARY TABLE IF EXISTS feods.detail_tmp;
  CREATE TEMPORARY TABLE feods.detail_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    t.shelf_id,
    t.product_id,
    t.stock_quantity,
    t.sale_price
  FROM
    fe_dwd.dwd_shelf_product_day_all t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id;
--   WHERE t.data_flag = 1;
  DROP TEMPORARY TABLE IF EXISTS feods.res_shelf_tmp;
  CREATE TEMPORARY TABLE feods.res_shelf_tmp (PRIMARY KEY (shelf_id, product_id))
  SELECT
    s.business_name,
    t.shelf_id,
    t.product_id,
    p.product_code2,
    p.product_name,
    t.stock_quantity,
    t.sale_price,
    f.apply_time,
    f.fill_time,
    current_timestamp ctime
  FROM
    feods.detail_tmp t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
    LEFT JOIN feods.fill_tmp f
      ON t.shelf_id = f.shelf_id
      AND t.product_id = f.product_id;
  DROP TABLE IF EXISTS feods.res7_shelf;
  CREATE TABLE feods.res7_shelf LIKE feods.res_shelf_tmp;
  INSERT INTO feods.res7_shelf
  SELECT
    *
  FROM
    feods.res_shelf_tmp;
  #地区 货架 商品 商品fe 商品名称 库存 售价 上次申请补货 上次上架
   DROP TEMPORARY TABLE IF EXISTS feods.res_slot_tmp;
  CREATE TEMPORARY TABLE feods.res_slot_tmp (
    PRIMARY KEY (
      shelf_id,
      product_id,
      manufacturer_slot_code
    )
  )
  SELECT
    s.business_name,
    t.shelf_id,
    t.manufacturer_slot_code,
    t.product_id,
    p.product_code2,
    p.product_name,
    d.sale_price,
    t.slot_capacity_limit,
    t.stock_num,
    CURRENT_TIMESTAMP ctime
  FROM
    fe.sf_shelf_machine_slot t
    JOIN feods.shelf_tmp s
      ON t.shelf_id = s.shelf_id
    JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1
    JOIN feods.detail_tmp d
      ON t.shelf_id = d.shelf_id
      AND t.product_id = d.product_id
  WHERE t.data_flag = 1
    AND ! ISNULL(t.shelf_id)
    AND ! ISNULL(t.product_id);
  DROP TABLE IF EXISTS feods.res7_slot;
  CREATE TABLE feods.res7_slot LIKE feods.res_slot_tmp;
  INSERT INTO feods.res7_slot
  SELECT
    *
  FROM
    feods.res_slot_tmp;
  #地区 货架 货道 商品 商品fe 商品名称 售价 容量 库存  
  -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'sp_op_res7',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('未知@',@user,@timestamp)
);
   COMMIT;
END