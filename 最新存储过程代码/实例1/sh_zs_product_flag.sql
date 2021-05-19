CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_zs_product_flag`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  truncate table feods.zs_product_flag;
  INSERT INTO feods.zs_product_flag (
    product_id,
    type_id,
    type_id_bin,
    DATA_FLAG,
    ADD_TIME,
    ADD_USER_ID,
    LAST_UPDATE_TIME,
    LAST_UPDATE_USER_ID
  )
  SELECT
    product_id,
    type_id,
    CASE
      WHEN type_id = 1
      THEN 1
      WHEN type_id = 2
      THEN 2
      WHEN type_id = 3
      THEN 4
      WHEN type_id = 4
      THEN 8
      WHEN type_id = 5
      THEN 16
      WHEN type_id = 11
      THEN 1024 #原来是32
    END AS type_id_bin,
    DATA_FLAG,
    CURRENT_TIMESTAMP () AS ADD_TIME,
    317 AS ADD_USER_ID,
    CURRENT_TIMESTAMP () AS LAST_UPDATE_TIME,
    317 AS LAST_UPDATE_USER_ID
  FROM
    fe.sf_product
  where DATA_FLAG=1 ;
 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_zs_product_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp)); 
 
  COMMIT;
END