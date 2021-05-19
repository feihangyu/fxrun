CREATE DEFINER=`shprocess`@`%` PROCEDURE `dwd_op_dim_date_three`()
BEGIN
  SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
  SET @sdate := CURRENT_DATE;
  SET @ddate := NULL;
  
  SELECT
    str43 INTO @load_version
  FROM
    fe_dwd.dwd_op_load_dim   
  LIMIT 1;
  
  SELECT
    COUNT(*) = 0 INTO @load_flag
  FROM
    fe_dwd.dwd_pub_product_dim_sserp_his t  
  WHERE t.version = @load_version;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.load_tmp;
  CREATE TEMPORARY TABLE fe_dwd.load_tmp LIKE fe_dwd.dwd_pub_product_dim_sserp; 
  INSERT INTO fe_dwd.load_tmp 
  (VERSION, 
   business_area, 
   product_fe, 
   product_type, 
   pub_time, 
   indate_np, 
   inqty_np, 
   replace_product_fe, 
   out_date,
   product_label, -- 20200513增加商品标签（补货组使用）
   price,          -- 20200617增加新品标准售价
   be_normal_time -- 新品转正式时间
  )
  SELECT @load_version, 
         str1,
         str2, 
         str7, 
         @sdate, 
         IF(str29 = '', NULL, str29), 
         IF(str28 = '', NULL, str28), 
         str33, 
         IF(str32 = '', NULL, str32),
         IF(str35 = '', NULL, str35), -- 20200513增加商品标签（补货组使用）
         IF(str8 = '', NULL, str8),  -- 20200617增加新品标准售价
		 IF(str31 = '',NULL,str31) -- 新品转正式时间
  FROM fe_dwd.dwd_op_load_dim  
  WHERE @load_flag;
  
  
  
  
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.fe_tmp;
  CREATE TEMPORARY TABLE fe_dwd.fe_tmp (
    product_code2 VARCHAR (100) PRIMARY KEY, product_id BIGINT, product_name VARCHAR (100)
  )
  SELECT
    product_code2, MAX(product_id) product_id, MAX(product_name) product_name
  FROM
    fe_dwd.dwd_product_base_day_all
  WHERE product_code2 != ''
  GROUP BY product_code2;
  
  SELECT
    IFNULL(
      @load_flag && COUNT(*) > 0 && COUNT(*) = COUNT(
        DISTINCT b.business_name, p.product_code2
      ), 0
    ) INTO @load_flag
  FROM
    fe_dwd.load_tmp t
    LEFT JOIN
      (SELECT DISTINCT
        business_name
      FROM
        fe_dwd.dwd_city_business) b    
      ON t.business_area = b.business_name
    LEFT JOIN fe_dwd.fe_tmp p
      ON t.product_fe = p.product_code2;
	  
	  
  UPDATE
    fe_dwd.load_tmp
  SET
    product_type = REPLACE(product_type, ' ', ''), remark = IF(
      product_type = '预淘汰', '预淘汰', 'remark'
    )
  WHERE @load_flag;
  
  
  UPDATE
    fe_dwd.load_tmp t
    JOIN fe_dwd.fe_tmp p
      ON t.product_fe = p.product_code2 SET t.product_id = p.product_id, t.product_name = p.product_name
  WHERE @load_flag;
  DELETE
  FROM
    fe_dwd.dwd_pub_product_dim_sserp     
  WHERE @load_flag;
  
  
  INSERT INTO fe_dwd.dwd_pub_product_dim_sserp
  SELECT
    *
  FROM
    fe_dwd.load_tmp
  WHERE @load_flag;
  DELETE
    t
  FROM
    fe_dwd.dwd_pub_product_dim_sserp_his t    
  WHERE t.version = @load_version
    AND @load_flag;
	
	
  INSERT INTO fe_dwd.dwd_pub_product_dim_sserp_his (
    VERSION, business_area, product_id, product_fe, product_name, product_type, remark, pub_time, indate_np, inqty_np, replace_product_fe, out_date,product_label,price,be_normal_time
  )
  SELECT
    VERSION, business_area, product_id, product_fe, product_name, product_type, remark, pub_time, indate_np, inqty_np, replace_product_fe, out_date,product_label,price,be_normal_time
  FROM
    fe_dwd.dwd_pub_product_dim_sserp    
  WHERE @load_flag;
  
  DROP TEMPORARY TABLE IF EXISTS fe_dwd.dim_tmp;
  CREATE TEMPORARY TABLE fe_dwd.dim_tmp
  SELECT DISTINCT
    t.version version_id, DATE(t.pub_time) sdate
  FROM
    fe_dwd.dwd_pub_product_dim_sserp_his t   
  ORDER BY sdate DESC;
  
  
  TRUNCATE fe_dwd.dwd_op_dim_date;        
  INSERT INTO fe_dwd.dwd_op_dim_date (edate, sdate, version_id)
  SELECT
    DATE(
      IFNULL(@ddate, ADDDATE(CURRENT_DATE, 1))
    ) edate, @ddate := t.sdate sdate, t.version_id
  FROM
    fe_dwd.dim_tmp t;
	
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'dwd_op_dim_date_three',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('朱星华@', @user),
@stime);
-- 更新任务的执行状态
UPDATE fe_dwd.dwd_project_excute_status SET execute_status=1,load_time=CURRENT_TIMESTAMP WHERE process_name='dwd_op_dim_date_three' AND sdate=CURRENT_DATE;
-- 记录表的数据量
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pub_product_dim_sserp','dwd_op_dim_date_three','朱星华');
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_pub_product_dim_sserp_his','dwd_op_dim_date_three','朱星华');
CALL sh_process.dwd_count_process_aim_table_size('fe_dwd.dwd_op_dim_date','dwd_op_dim_date_three','朱星华');
  COMMIT;
END