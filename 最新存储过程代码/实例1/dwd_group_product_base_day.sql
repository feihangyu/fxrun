CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_group_product_base_day`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
 -- 每天开始插入数据之前删掉之前的数据
truncate table fe_dwd.dwd_group_product_base_day;
INSERT INTO fe_dwd.dwd_group_product_base_day
(
spec_id
,product_id
,product_code
,product_name
,first_category_id
,first_category_desc
,second_category_id
,second_category_desc
,category_id
,category_desc
,produce_province
,produce_city
,produce_address
,audit_status
,supply_channel
,postage
,selling_point
,product_type
,product_remark
,mall_freight_id
,group_freight_id
,packet_unit
,measurement_unit
,spec_num
,spec_desc
,supply_status
,sale_status
,purchase_price
,nofreight_purchase_price
,sale_start_time
,sale_end_time
,sale_price
,market_price
,cost_percent
,invoice_type
,tax_rate
,supply_group_id
)
select 
a.spec_id                      
,a.product_id  
,a.product_code   
,b.product_name 
,b.first_category_id      
, REPLACE(
    REPLACE(
      REPLACE(p.category_name, CHAR(10), ''),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS 'first_category_desc'
,b.second_category_id    
,  REPLACE(
    REPLACE(
      REPLACE(o.category_name, CHAR(10), ''),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS 'second_category_desc'                                                               
,b.category_id   
,REPLACE(
    REPLACE(
      REPLACE(n.category_name, CHAR(10), ''),
      CHAR(9),
      ''
    ),
    CHAR(13),
    ''
  ) AS 'category_desc' 
,b.produce_province                                                                              
,b.produce_city                                                                                  
,b.produce_address                                   
,b.audit_status        
,b.supply_channel
,b.postage  
,b.selling_point                 
,b.product_type        
,b.product_remark  
,b.mall_freight_id               
,b.group_freight_id 
,a.packet_unit                  
,a.measurement_unit               
,a.spec_num                       
 , REPLACE(
    REPLACE(
      REPLACE(a.spec_desc, CHAR(13), ''),
      CHAR(9),
      ''
    ),
    CHAR(10),
    ''
  ) AS 'spec_desc' 
,a.supply_status            
,a.sale_status              
,a.purchase_price 
,a.nofreight_purchase_price 
,a.sale_start_time            
,a.sale_end_time              
,a.sale_price                   
,a.market_price                
,a.cost_percent              
,a.invoice_type             
,a.tax_rate                                                                                           
,b.supply_group_id                                                                                             
from fe_goods.sf_group_product_spec a
join fe_goods.sf_group_product b
ON a.product_id = b.product_id
and a.data_flag =1
and b.data_flag =1
LEFT JOIN fe_goods.sf_group_product_category n
ON n.category_id = b.category_id
and b.data_flag =1
LEFT JOIN fe_goods.sf_group_product_category o
ON o.category_id = b.second_category_id
and o.data_flag =1
LEFT JOIN fe_goods.sf_group_product_category p
ON p.category_id =b.first_category_id
and p.data_flag =1;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_group_product_base_day',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;	
end