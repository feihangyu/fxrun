CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_package_information`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
-- 防止异常报错
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_package_information_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_package_information_test LIKE fe_dwd.dwd_package_information;
insert into  fe_dwd.dwd_package_information_test
(
`PACKAGE_ID`
,`PACKAGE_NAME`
,`PACKAGE_TYPE`
,`PACKAGE_TYPE_ID`
,`MIN_MAX_QUANTITY`
,`PACKAGE_LAYER`
,`PROVINCE`
,`CITY`
,`DISTRICT`
,`AREA_ADDRESS`
,`STATU_FLAG`
,`DEFAULT_FLAG`
,`START_DATE`
,`END_DATE`
,`DISCOUNT_TYPE`
,`DISCOUNT_VALUE`
,`machine_type_id`
,`ITEM_ID`
,`RELATION_ID`
,`PRODUCT_POSITION`
,`POSITION_SORT`
,`SALE_PRICE`
,`OLD_SALE_PRICE`
,`QUANTITY`
,`ALARM_QUANTITY`
,`SHELF_FILL_FLAG`
,`PACKAGE_TYPE_CODE`
,`PACKAGE_MODEL`
,`PACKAGE_TYPE_NAME`
,`MAX_SKU`
,`MIN_SKU`
,`MAX_STOCK`
,`STOCK_RATE`
,`type_STATU_FLAG`
,ADD_TIME
)
select   
a.`PACKAGE_ID`
,a.`PACKAGE_NAME`
,a.`PACKAGE_TYPE`
,a.`PACKAGE_TYPE_ID`
,a.`MIN_MAX_QUANTITY`
,a.`PACKAGE_LAYER`
,a.`PROVINCE`
,a.`CITY`
,a.`DISTRICT`
,a.`AREA_ADDRESS`
,a.`STATU_FLAG`
,a.`DEFAULT_FLAG`
,a.`START_DATE`
,a.`END_DATE`
,a.`DISCOUNT_TYPE`
,a.`DISCOUNT_VALUE`
,a.`machine_type_id`
,b.`ITEM_ID`
,b.`RELATION_ID`
,b.`PRODUCT_POSITION`
,b.`POSITION_SORT`
,b.`SALE_PRICE`
,b.`OLD_SALE_PRICE`
,b.`QUANTITY`
,b.`ALARM_QUANTITY`
,b.`SHELF_FILL_FLAG`
,c.`PACKAGE_TYPE_CODE`
,c.`PACKAGE_MODEL`
,c.`PACKAGE_TYPE_NAME`
,c.`MAX_SKU`
,c.`MIN_SKU`
,c.`MAX_STOCK`
,c.`STOCK_RATE`
,c.`STATU_FLAG` as type_STATU_FLAG 
,b.ADD_TIME
from          
fe.`sf_package` a
join   
fe.sf_package_item b
on a.package_id = b.package_id
and b.`DATA_FLAG` = 1
left join
fe.sf_package_type c
on a.package_type_id = c.package_type_id
and c.`DATA_FLAG` = 1
where a.data_flag =1;
  
truncate table fe_dwd.dwd_package_information;
insert into  fe_dwd.dwd_package_information
(
`PACKAGE_ID`
,`PACKAGE_NAME`
,`PACKAGE_TYPE`
,`PACKAGE_TYPE_ID`
,`MIN_MAX_QUANTITY`
,`PACKAGE_LAYER`
,`PROVINCE`
,`CITY`
,`DISTRICT`
,`AREA_ADDRESS`
,`STATU_FLAG`
,`DEFAULT_FLAG`
,`START_DATE`
,`END_DATE`
,`DISCOUNT_TYPE`
,`DISCOUNT_VALUE`
,`machine_type_id`
,`ITEM_ID`
,`RELATION_ID`
,`PRODUCT_POSITION`
,`POSITION_SORT`
,`SALE_PRICE`
,`OLD_SALE_PRICE`
,`QUANTITY`
,`ALARM_QUANTITY`
,`SHELF_FILL_FLAG`
,`PACKAGE_TYPE_CODE`
,`PACKAGE_MODEL`
,`PACKAGE_TYPE_NAME`
,`MAX_SKU`
,`MIN_SKU`
,`MAX_STOCK`
,`STOCK_RATE`
,`type_STATU_FLAG`
,ADD_TIME
)
select   
a.`PACKAGE_ID`
,a.`PACKAGE_NAME`
,a.`PACKAGE_TYPE`
,a.`PACKAGE_TYPE_ID`
,a.`MIN_MAX_QUANTITY`
,a.`PACKAGE_LAYER`
,a.`PROVINCE`
,a.`CITY`
,a.`DISTRICT`
,a.`AREA_ADDRESS`
,a.`STATU_FLAG`
,a.`DEFAULT_FLAG`
,a.`START_DATE`
,a.`END_DATE`
,a.`DISCOUNT_TYPE`
,a.`DISCOUNT_VALUE`
,a.`machine_type_id`
,b.`ITEM_ID`
,b.`RELATION_ID`
,b.`PRODUCT_POSITION`
,b.`POSITION_SORT`
,b.`SALE_PRICE`
,b.`OLD_SALE_PRICE`
,b.`QUANTITY`
,b.`ALARM_QUANTITY`
,b.`SHELF_FILL_FLAG`
,c.`PACKAGE_TYPE_CODE`
,c.`PACKAGE_MODEL`
,c.`PACKAGE_TYPE_NAME`
,c.`MAX_SKU`
,c.`MIN_SKU`
,c.`MAX_STOCK`
,c.`STOCK_RATE`
,c.`STATU_FLAG` as type_STATU_FLAG 
,b.ADD_TIME
from          
fe.`sf_package` a
join   
fe.sf_package_item b
on a.package_id = b.package_id
and b.`DATA_FLAG` = 1
left join
fe.sf_package_type c
on a.package_type_id = c.package_type_id
and c.`DATA_FLAG` = 1
where a.data_flag =1;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_package_information',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END