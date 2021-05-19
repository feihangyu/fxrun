CREATE DEFINER=`feprocess`@`%` PROCEDURE `dm_ma_group_product_flag`()
BEGIN
-- =============================================
-- Author:	市场 拼团业务
-- Create date: 2020-4-8
-- Modify date:
-- Description: 拼团商品标签表
-- =============================================
SET @run_date := CURRENT_DATE(), @user := CURRENT_USER(), @timestamp := CURRENT_TIMESTAMP();
#临时数据
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_product; #最近三天更新的商品信息
CREATE TEMPORARY TABLE fe_dm.tmp_product AS
    SELECT a1.product_id,a1.product_name,a1.last_sale_enable_time
        ,MIN(a2.add_time) first_add_time,a1.last_update_time,a1.data_flag,a1.sale_status
    FROM fe_goods.sf_group_product a1
    JOIN fe_goods.sf_group_product_spec a2 ON a2.product_id=a1.product_id
    JOIN fe_goods.sf_mall_product_specs a3 ON a3.spec_id=a2.spec_id AND a3.platform=4
    WHERE  a1.last_update_time>=SUBDATE(CURRENT_DATE,3) AND a1.last_update_time<CURRENT_DATE
    GROUP BY a1.product_id
;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_product_sale; #最近三天商品销量
CREATE TEMPORARY TABLE fe_dm.tmp_product_sale AS
    SELECT a2.product_id,SUM(a1.quantity) quantity
    FROM fe_dwd.dwd_group_order_refound_address_day a1
    JOIN fe_goods.sf_group_product_spec a2 ON a2.spec_id=a1.product_spec_id
    JOIN fe_goods.sf_group_product a3 ON a3.product_id=a2.product_id AND a3.last_sale_enable_time<SUBDATE(CURDATE(),7)
    WHERE pay_time>=SUBDATE(CURDATE(),3) AND pay_time<CURDATE()
      AND order_type_number=10
    GROUP BY a2.product_id
    ORDER BY quantity DESC
    LIMIT 10
;#删除数据
DELETE FROM fe_dm.dm_ma_group_product_flag WHERE product_id IN (SELECT product_id FROM fe_dm.tmp_product WHERE data_flag=2 OR sale_status=2 );
#插入数据
REPLACE INTO fe_dm.dm_ma_group_product_flag
    (product_id, product_name, first_add_time)
SELECT a1.product_id,a1.product_name,a1.first_add_time
FROM fe_dm.tmp_product a1
LEFT JOIN fe_dm.dm_ma_group_product_flag a2 ON a2.product_id=a1.product_id
WHERE data_flag=1 AND sale_status=1  AND a2.product_id IS NULL
;
# 更新标签
    # 更新展示标签
UPDATE fe_dm.dm_ma_group_product_flag a1
LEFT JOIN fe_dm.tmp_product_sale a2 ON a2.product_id=a1.product_id
SET display_flag= CASE WHEN  DATEDIFF(CURDATE(),DATE(a1.first_add_time))<=7 THEN 1 WHEN a2.product_id IS NOT NULL THEN 2 ELSE 0 END
;
#记录日志
CALL sh_process.`sp_sf_dw_task_log`('dm_ma_group_product_flag',DATE_FORMAT(@run_date,'%Y-%m-%d'),CONCAT('纪伟铨@',@user,@timestamp));
END