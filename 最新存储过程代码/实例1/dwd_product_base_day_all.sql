CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_product_base_day_all`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
 DROP TEMPORARY TABLE IF EXISTS fe_dwd.material_lsl_tmp;
    CREATE TEMPORARY TABLE fe_dwd.material_lsl_tmp AS
    SELECT DISTINCT             -- 对重复数据进行去重
      a.fnumber,                -- 商品FE码
      a.F_BGJ_FBOXEDSTANDARDS,  -- 装箱规格 
      a.f_bgj_poprice,          -- sserp_采购价
      a.F_BGJ_SOPRICE,          -- 建议零售价
      b.fname fname_type,       -- 类别
      c.fname                   -- 物料名称
    FROM
      sserp.T_BD_MATERIAL a 
      JOIN sserp.T_BD_MATERIALGROUP_L b 
        ON a.FMATERIALGROUP = b.FID 
      JOIN sserp.T_BD_MATERIAL_L c 
        ON a.FMATERIALID = c.FMATERIALID ;
      
    CREATE INDEX idx_material_lsl_tmp
ON fe_dwd.material_lsl_tmp (fnumber);
-- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据  
  
	DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_product_base_day_all_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_product_base_day_all_test like fe_dwd.dwd_product_base_day_all;
INSERT INTO fe_dwd.dwd_product_base_day_all_test
(
PRODUCT_ID
,PRODUCT_CODE
,PRODUCT_CODE2
,PRODUCT_NAME
,CATEGORY_NAME
,TYPE_ID
,type_name
,SECOND_TYPE_ID
,second_type_name
,SUB_TYPE_ID
,sub_type_name
,warehouse_safe_days
,SAVE_TIME_DAYS
,SALE_STATUS
,tax_rate
,tax_rate_code
,publish_status
,ALLOW_SHELF_DAYS
,ALLOW_DELAY_SALE_DAYS
,ALLOW_ACTIVITY_DAYS
,CLEAR_STOCKS_DAYS
,allow_sale
,allow_purchase
,allow_fill
,is_proxy_sale
,FILL_MODEL
,FILL_UNIT
,fill_box_gauge
,F_BGJ_FBOXEDSTANDARDS
,F_BGJ_POPRICE
,F_BGJ_SOPRICE
,fname_type
,fname
,ADD_TIME
,load_time
	)
    SELECT 
     p.PRODUCT_ID                                   -- 商品编号
	,p.PRODUCT_CODE                                 -- 商品编码
    ,p.PRODUCT_CODE2                                -- 商品FE编码
    ,p.PRODUCT_NAME                                 -- 商品名称
    ,p.CATEGORY_NAME					-- 商品类型名称
    ,p.TYPE_ID                                      -- 类型ID(1:方便面、2:饼干、3:饮料、4:零食)(DICT)
    ,pt1.ITEM_NAME                                  -- 类型名称     
    ,p.SECOND_TYPE_ID                               -- 二级分类  
    ,pt2.type_name  AS second_type_name             -- 二级分类名称
    ,p.SUB_TYPE_ID                                  -- 三级分类
    ,pt3.type_name  AS sub_type_name                -- 三级分类名称
    ,p.warehouse_safe_days                          -- 仓库安全期
    ,p.SAVE_TIME_DAYS                               -- 保质期
    ,p.SALE_STATUS                                  -- 销售状态(DICT)(1:可销售、2:不可销售、9:待发布)
    ,p.tax_rate                                     -- 税率
    ,p.tax_rate_code                                -- 税率编码
    ,p.publish_status                               -- 发布状态（1，待发布；2，已发布）
    ,p.ALLOW_SHELF_DAYS                             -- 允许在架天数
    ,p.ALLOW_DELAY_SALE_DAYS                        -- 允许延期销售天数
    ,p.ALLOW_ACTIVITY_DAYS                          -- 允许活动天数
    ,p.CLEAR_STOCKS_DAYS                            -- 清货提前天数
    ,p.allow_sale                                   -- 是否允许销售（1，是；2，否）
    ,p.allow_purchase                               -- 是否允许采购（1，是；2，否）
    ,p.allow_fill                                   -- 是否允许补货（1，是；2，否）
    ,p.is_proxy_sale                                -- 是否代销商品1是2否（缺省）
    ,p.FILL_MODEL                                     -- 补货规格
    ,p.FILL_UNIT                                     -- 补货单位
    ,p.fill_box_gauge                                  -- 补货箱规    
    ,a.F_BGJ_FBOXEDSTANDARDS                        -- 装箱规格
    ,a.f_bgj_poprice                                -- sserp_采购价
    ,a.F_BGJ_SOPRICE                                -- 建议零售价
    ,a.fname_type                                   -- 类别
    ,a.fname                                        -- 物料名称
    ,p.ADD_TIME
    ,CURRENT_TIMESTAMP AS load_time                 -- 数据加载时间
    FROM fe.sf_product p
        LEFT JOIN fe.pub_dictionary_item pt1
          ON p.TYPE_ID = pt1.ITEM_VALUE
          and pt1.DICTIONARY_ID = 10 
        LEFT JOIN fe.sf_product_type pt2
          ON p.second_type_id = pt2.type_id
         AND pt2.data_flag = 1	 
        LEFT JOIN fe.sf_product_type pt3
          ON p.sub_type_id = pt3.type_id
          AND pt3.data_flag = 1	 
    	LEFT JOIN fe_dwd.material_lsl_tmp a 
      ON a.fnumber = p.PRODUCT_CODE2
    WHERE p.data_flag = 1;	
	
UPDATE fe_dwd.dwd_product_base_day_all_test AS b
JOIN fe_dwd.`dwd_common_product_insert` a 
ON a.product_id = b.product_id 
SET b.is_common_product = 1; 
	
	
  -- 每天开始插入数据之前删掉之前的数据
TRUNCATE TABLE fe_dwd.dwd_product_base_day_all;
  INSERT INTO fe_dwd.dwd_product_base_day_all
(
PRODUCT_ID
,PRODUCT_CODE
,PRODUCT_CODE2
,PRODUCT_NAME
,CATEGORY_NAME
,TYPE_ID
,type_name
,SECOND_TYPE_ID
,second_type_name
,SUB_TYPE_ID
,sub_type_name
,warehouse_safe_days
,SAVE_TIME_DAYS
,SALE_STATUS
,tax_rate
,tax_rate_code
,publish_status
,ALLOW_SHELF_DAYS
,ALLOW_DELAY_SALE_DAYS
,ALLOW_ACTIVITY_DAYS
,CLEAR_STOCKS_DAYS
,allow_sale
,allow_purchase
,allow_fill
,is_proxy_sale
,FILL_MODEL
,FILL_UNIT
,fill_box_gauge
,F_BGJ_FBOXEDSTANDARDS
,F_BGJ_POPRICE
,F_BGJ_SOPRICE
,fname_type
,fname
,ADD_TIME
,load_time
	)
    SELECT 
     p.PRODUCT_ID                                   -- 商品编号
	,p.PRODUCT_CODE                                 -- 商品编码
    ,p.PRODUCT_CODE2                                -- 商品FE编码
    ,p.PRODUCT_NAME                                 -- 商品名称
    ,p.CATEGORY_NAME					-- 商品类型名称
    ,p.TYPE_ID                                      -- 类型ID(1:方便面、2:饼干、3:饮料、4:零食)(DICT)
    ,pt1.ITEM_NAME                                  -- 类型名称     
    ,p.SECOND_TYPE_ID                               -- 二级分类  
    ,pt2.type_name  AS second_type_name             -- 二级分类名称
    ,p.SUB_TYPE_ID                                  -- 三级分类
    ,pt3.type_name  AS sub_type_name                -- 三级分类名称
    ,p.warehouse_safe_days                          -- 仓库安全期
    ,p.SAVE_TIME_DAYS                               -- 保质期
    ,p.SALE_STATUS                                  -- 销售状态(DICT)(1:可销售、2:不可销售、9:待发布)
    ,p.tax_rate                                     -- 税率
    ,p.tax_rate_code                                -- 税率编码
    ,p.publish_status                               -- 发布状态（1，待发布；2，已发布）
    ,p.ALLOW_SHELF_DAYS                             -- 允许在架天数
    ,p.ALLOW_DELAY_SALE_DAYS                        -- 允许延期销售天数
    ,p.ALLOW_ACTIVITY_DAYS                          -- 允许活动天数
    ,p.CLEAR_STOCKS_DAYS                            -- 清货提前天数
    ,p.allow_sale                                   -- 是否允许销售（1，是；2，否）
    ,p.allow_purchase                               -- 是否允许采购（1，是；2，否）
    ,p.allow_fill                                   -- 是否允许补货（1，是；2，否）
    ,p.is_proxy_sale                                -- 是否代销商品1是2否（缺省）
    ,p.FILL_MODEL                                     -- 补货规格
    ,p.FILL_UNIT                                     -- 补货单位
    ,p.fill_box_gauge                                  -- 补货箱规    
    ,a.F_BGJ_FBOXEDSTANDARDS                        -- 装箱规格
    ,a.f_bgj_poprice                                -- sserp_采购价
    ,a.F_BGJ_SOPRICE                                -- 建议零售价
    ,a.fname_type                                   -- 类别
    ,a.fname                                        -- 物料名称
    ,p.ADD_TIME
    ,CURRENT_TIMESTAMP AS load_time                 -- 数据加载时间
    FROM fe.sf_product p
        LEFT JOIN fe.pub_dictionary_item pt1
          ON p.TYPE_ID = pt1.ITEM_VALUE
          AND pt1.DICTIONARY_ID = 10 
        LEFT JOIN fe.sf_product_type pt2
          ON p.second_type_id = pt2.type_id
         AND pt2.data_flag = 1	 
        LEFT JOIN fe.sf_product_type pt3
          ON p.sub_type_id = pt3.type_id
          AND pt3.data_flag = 1	 
    	LEFT JOIN fe_dwd.material_lsl_tmp a 
      ON a.fnumber = p.PRODUCT_CODE2
    WHERE p.data_flag = 1;
    
-- 更新一下是否大通商品 星华提供表
UPDATE fe_dwd.dwd_product_base_day_all AS b
JOIN fe_dwd.`dwd_common_product_insert` a 
ON a.product_id = b.product_id 
SET b.is_common_product = 1; 
    
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_product_base_day_all',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;	
end