CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_shelf_product_detail`()
BEGIN
  SET @user := CURRENT_USER,
  @timestamp := CURRENT_TIMESTAMP;
SET @sdate := SUBDATE(CURDATE(),1);
SET @month_id := DATE_FORMAT(@sdate,'%Y-%m');
SET @pre_3month := DATE_FORMAT(SUBDATE(@sdate,INTERVAL 3 MONTH),'%Y-%m');
  
  TRUNCATE TABLE feods.zs_area_stock_detail;
SET @time_3 := CURRENT_TIMESTAMP();
  INSERT INTO feods.zs_area_stock_detail (
    shelf_id,
    product_id,
    product_code2,
    product_name,
    max_quantity,
    alarm_quantity,
    stock_quantity,
    sale_price,
    purchase_price,
    shelf_fill_flag,
    package_flag,
    danger_flag,
    sales_flag,
    new_flag,
    near_date,
    first_fill_time,
    near_days,
    sales_status,
    manager_fill_flag,
    city_name,
    sf_code,
    real_name,
    branch_name,
    branch_code,
    shelf_type,
    shelf_name,
    revoke_status,
    day_avg_sale_num,
    save_time_days,
    allow_shelf_days,
    allow_delay_sale_days,
    allow_activity_days,
    clear_stocks_days,
    production_date,
    sc_day,
    risk_source,
    whether_close,
    add_user,
    last_update_time
  )
  SELECT
    t.shelf_id,
    t.product_id,
    p.product_code2,
    p.product_name,
    t.max_quantity,
    t.alarm_quantity,
    t.stock_quantity,
    t.sale_price,
    t.purchase_price,
    t.shelf_fill_flag,
    t.package_flag,
    t.danger_flag,
    t.sales_flag,
    t.new_flag,
    t.near_date,
    t.first_fill_time,
    t.near_days,
    t.sales_status,
    t.manager_fill_flag,
--     SUBSTRING_INDEX(
--       SUBSTRING_INDEX(s.area_address, ',', 2),
--       ',',
--       - 1
--     ) AS city_name,
    s.CITY_NAME,
    m.sf_code,
    m.real_name,
    m.branch_name,
    m.branch_code,
    s.shelf_type,
    s.shelf_name,
    s.revoke_status,
    t.day_avg_sale_num,
    p.save_time_days,
    p.allow_shelf_days,
    p.allow_delay_sale_days,
    p.allow_activity_days,
    p.clear_stocks_days,
    DATE('2017-01-01') AS production_date,
    t.production_date AS sc_day,
    t.risk_source,
    s.whether_close,
    @user add_user,
    t.last_update_time
  FROM
    (SELECT
      t.shelf_id,
      t.product_id,
      t.max_quantity,
      t.alarm_quantity,
      t.stock_quantity,
      t.sale_price,
      t.purchase_price,
      t.shelf_fill_flag,
      t.package_flag,
      f.danger_flag,
      f.sales_flag,
      f.new_flag,
      f.near_date,
      f.first_fill_time,
      f.near_days,
      f.sales_status,
      f.manager_fill_flag,
      ft.day_avg_sale_num,
      f.production_date,
      f.risk_source,
      MAX(f1.`last_update_time`) AS last_update_time
    FROM
      fe.sf_shelf_product_detail t
      LEFT JOIN fe.sf_shelf_product_detail_flag f
        ON t.shelf_id = f.shelf_id
        AND t.product_id = f.product_id
        AND f.data_flag = 1
      LEFT JOIN fe.sf_statistics_pre_fourteen_sale_product ft
        ON t.shelf_id = ft.shelf_id
        AND t.product_id = ft.product_id
       LEFT JOIN fe.`sf_risk_production_date_source` f1
      ON t.shelf_id = f1.`shelf_id`
      AND t.`PRODUCT_ID` = f1.`product_id`
      AND f.`production_date` = f1.`production_date`
      AND f1.`data_flag` =1       
    WHERE t.data_flag = 1
      AND (
        t.stock_quantity > 0
        OR (
          t.stock_quantity = 0
          AND shelf_fill_flag = 1
        )
      )
      GROUP BY t.`SHELF_ID`,t.`PRODUCT_ID`,f1.`production_date`   
      ) t
--     JOIN fe.sf_shelf s
--       ON t.shelf_id = s.shelf_id
--       AND s.data_flag = 1
--       AND s.shelf_status = 2
    JOIN fe_dwd.`dwd_shelf_base_day_all` s
      ON t.shelf_id = s.shelf_id
      AND s.data_flag = 1
      AND s.shelf_status = 2
    LEFT JOIN fe.pub_shelf_manager m
      ON s.manager_id = m.manager_id
      AND m.data_flag = 1
    JOIN fe.sf_product p
      ON t.product_id = p.product_id
      AND p.data_flag = 1;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_shelf_product_detail","@time_3--@time_5",@time_3,@time_5);
-- 截存3个月的历史数据
DELETE FROM fe_dm.dm_op_shelf_product_danger_month WHERE month_id = @month_id OR month_id < @pre_3month;
INSERT INTO fe_dm.dm_op_shelf_product_danger_month
(
        month_id,
        region_name,
        business_name,
        zone_name,
        branch_name,
        real_name,
        shelf_id,
        shelf_code,
        shelf_type,
        product_id,
        product_code2,
        product_name,
        sc_day,
        danger_flag,
        risk_source,
        last_update_time,
        sales_flag,
        day_avg_sale_num,
        shelf_fill_flag,
        stock_quantity,
        stock_value
)
SELECT
        @month_id AS month_id,
        b.region_name,
        b.business_name,
        b.zone_name,
        a.branch_name,
        a.real_name,
        a.shelf_id,
        b.shelf_code,
        b.shelf_type,
        a.product_id,
        a.product_code2,
        a.product_name,
        a.sc_day,
        a.danger_flag,
        a.risk_source,
        a.last_update_time,
        a.sales_flag,
        a.day_avg_sale_num,
        a.shelf_fill_flag,
        a.stock_quantity,
        a.stock_quantity * a.sale_price AS stock_value
FROM
        feods.`zs_area_stock_detail` a
        JOIN fe_dwd.`dwd_shelf_base_day_all` b
                ON a.shelf_id = b.shelf_id
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("sp_shelf_product_detail","@time_5--@time_6",@time_5,@time_6);
  CALL feods.sp_task_log (
    'sp_shelf_product_detail',
    CURRENT_DATE,
    CONCAT('宋英南@',@user,@timestamp)
  );
  COMMIT;
END