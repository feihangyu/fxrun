CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_op_dim_date`()
BEGIN
  SET @sdate := CURRENT_DATE, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @ddate := NULL;
  
  SELECT str43 INTO @load_version
  FROM feods.d_op_load_dim
  LIMIT 1;
  
  SELECT  COUNT(*) = 0 INTO @load_flag
  FROM feods.zs_product_dim_sserp_his t
  WHERE t.version = @load_version;
  
  DROP TEMPORARY TABLE IF EXISTS feods.load_tmp;
  CREATE TEMPORARY TABLE feods.load_tmp LIKE feods.zs_product_dim_sserp;
  INSERT INTO feods.load_tmp 
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
  FROM feods.d_op_load_dim
  WHERE @load_flag;
  
  DROP TEMPORARY TABLE IF EXISTS feods.fe_tmp;
  CREATE TEMPORARY TABLE feods.fe_tmp (product_code2 VARCHAR (100) PRIMARY KEY, product_id BIGINT, product_name VARCHAR (100))
  SELECT product_code2, 
         MAX(product_id) product_id, 
         MAX(product_name) product_name
  FROM fe.sf_product
  WHERE data_flag = 1
  AND product_code2 != ''
  GROUP BY product_code2;
  
  SELECT IFNULL( @load_flag && COUNT(*) > 0 && COUNT(*) = COUNT(DISTINCT b.business_name, p.product_code2), 0) INTO @load_flag
  FROM feods.load_tmp t
  LEFT JOIN
  (SELECT DISTINCT  business_name
   FROM feods.fjr_city_business
   ) b ON t.business_area = b.business_name
  LEFT JOIN feods.fe_tmp p ON t.product_fe = p.product_code2;
  UPDATE feods.load_tmp
  
  SET product_type = REPLACE(product_type, ' ', ''), remark = IF(product_type = '预淘汰', '预淘汰', 'remark')
  WHERE @load_flag;
  
  UPDATE feods.load_tmp t
  JOIN feods.fe_tmp p ON t.product_fe = p.product_code2 SET t.product_id = p.product_id, t.product_name = p.product_name
  WHERE @load_flag;
  
  DELETE FROM feods.zs_product_dim_sserp
  WHERE @load_flag;
  
  INSERT INTO feods.zs_product_dim_sserp
  SELECT *
  FROM feods.load_tmp
  WHERE @load_flag;
  
  DELETE t
  FROM feods.zs_product_dim_sserp_his t
  WHERE t.version = @load_version AND @load_flag;
  
  INSERT INTO feods.zs_product_dim_sserp_his (
    VERSION, business_area, product_id, product_fe, product_name, product_type, remark, pub_time, indate_np, inqty_np, replace_product_fe, out_date,product_label,price,be_normal_time
  )
  SELECT
    VERSION, business_area, product_id, product_fe, product_name, product_type, remark, pub_time, indate_np, inqty_np, replace_product_fe, out_date,product_label,price,be_normal_time
  FROM feods.zs_product_dim_sserp
  WHERE @load_flag;
  
  DROP TEMPORARY TABLE IF EXISTS feods.dim_tmp;
  CREATE TEMPORARY TABLE feods.dim_tmp
  SELECT DISTINCT t.version version_id, DATE(t.pub_time) sdate
  FROM feods.zs_product_dim_sserp_his t
  ORDER BY sdate DESC;
  
  TRUNCATE feods.d_op_dim_date;
  INSERT INTO feods.d_op_dim_date (edate, sdate, version_id)
  SELECT
    DATE(
      IFNULL(@ddate, ADDDATE(CURRENT_DATE, 1))
    ) edate, @ddate := t.sdate sdate, t.version_id
  FROM
    feods.dim_tmp t;
	
	
  CALL sh_process.`sp_sf_dw_task_log` (
    'sp_op_dim_date', @sdate, CONCAT(
      'fjr_d_d1f7da84a5665a2a384bedca60c6b5e6', @timestamp, @add_user
    )
  );
  
  COMMIT;
END