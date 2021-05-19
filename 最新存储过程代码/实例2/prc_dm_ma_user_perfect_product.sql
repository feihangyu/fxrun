CREATE DEFINER=`shprocess`@`%` PROCEDURE `prc_dm_ma_user_perfect_product`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
SET @row=0,@row_by='';
TRUNCATE TABLE fe_dm.dm_ma_user_perfect_product;
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_user_buy1;
SET @time_1 := CURRENT_TIMESTAMP();
CREATE TEMPORARY TABLE fe_dm.temp_user_buy1(INDEX (user_id))
SELECT user_id
    ,MAX(IF(ROW=1,product_id,0)) product_id1,MAX(IF(ROW=1,PRODUCT_NAME,0)) product_name1,MAX(IF(ROW=1,sale_quantity,0)) product_quantity_sale1,MAX(IF(ROW=1,gmv,0)) product_gmv1
    ,MAX(IF(ROW=2,product_id,0)) product_id2,MAX(IF(ROW=2,PRODUCT_NAME,0)) product_name2,MAX(IF(ROW=2,sale_quantity,0)) product_quantity_sale2,MAX(IF(ROW=2,gmv,0)) product_gmv2
    ,MAX(IF(ROW=3,product_id,0)) product_id3,MAX(IF(ROW=3,PRODUCT_NAME,0)) product_name3,MAX(IF(ROW=3,sale_quantity,0)) product_quantity_sale3,MAX(IF(ROW=3,gmv,0)) product_gmv3
FROM
    (SELECT a1.*,IF(@row_by=user_id,@row:=@row+1,@row:=1) ROW,@row_by:=user_id row_by
    FROM
        (SELECT a1.user_id,a1.product_id,a2.PRODUCT_NAME
             ,SUM(a1.quantity_act) sale_quantity
             ,SUM(a1.quantity_act *a1.sale_price) gmv
            ,1 sep_type
        FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
        JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.product_id
        WHERE a1.PAY_DATE>=SUBDATE(CURDATE(),60) AND a1.PAY_DATE<CURDATE()
        GROUP BY a1.user_id,a1.product_id
        ORDER BY a1.user_id,sale_quantity DESC,gmv DESC
        )  a1
    ) tt
WHERE ROW<4
GROUP BY user_id
;
SET @time_2 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_dm_ma_user_perfect_product","@time_1--@time_2",@time_1,@time_2);  
SET @row=0,@row_by='';
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_user_buy2;
CREATE TEMPORARY TABLE fe_dm.temp_user_buy2(INDEX (user_id))
SELECT user_id
    ,MAX(IF(ROW=1,SECOND_TYPE_ID,0)) SEC_TYPE_ID1,MAX(IF(ROW=1,second_type_name,0)) SEC_TYPE_name1,MAX(IF(ROW=1,sale_quantity,0)) SEC_TYPE_quantity_sale1,MAX(IF(ROW=1,gmv,0)) SEC_TYPE_gmv1
    ,MAX(IF(ROW=2,SECOND_TYPE_ID,0)) SEC_TYPE_ID2,MAX(IF(ROW=2,second_type_name,0)) SEC_TYPE_name2,MAX(IF(ROW=2,sale_quantity,0)) SEC_TYPE_quantity_sale2,MAX(IF(ROW=2,gmv,0)) SEC_TYPE_gmv2
    ,MAX(IF(ROW=3,SECOND_TYPE_ID,0)) SEC_TYPE_ID3,MAX(IF(ROW=3,second_type_name,0)) SEC_TYPE_name3,MAX(IF(ROW=3,sale_quantity,0)) SEC_TYPE_quantity_sale3,MAX(IF(ROW=3,gmv,0)) SEC_TYPE_gmv3
FROM
    (SELECT a1.*,IF(@row_by=user_id,@row:=@row+1,@row:=1) ROW,@row_by:=user_id row_by
    FROM
        (SELECT a1.user_id,a2.SECOND_TYPE_ID,a2.second_type_name
             ,SUM(a1.quantity_act) sale_quantity
             ,SUM(a1.quantity_act* a1.sale_price) gmv
        FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
        JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.PRODUCT_ID
        WHERE a1.PAY_DATE>=SUBDATE(CURDATE(),60) AND a1.PAY_DATE<CURDATE()
        GROUP BY a1.user_id,a2.SECOND_TYPE_ID
        ORDER BY a1.user_id,sale_quantity DESC,gmv DESC
        )  a1
    ) tt
WHERE ROW<4
GROUP BY user_id
;
SET @time_3 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_dm_ma_user_perfect_product","@time_2--@time_3",@time_2,@time_3);  
SET @row=0,@row_by='';
DROP TEMPORARY TABLE IF EXISTS fe_dm.temp_user_buy3;
CREATE TEMPORARY TABLE fe_dm.temp_user_buy3(INDEX (user_id))
SELECT user_id
    ,MAX(IF(ROW=1,SUB_TYPE_ID,0)) SUB_TYPE_ID1,MAX(IF(ROW=1,sub_type_name,0)) sub_type_name1,MAX(IF(ROW=1,sale_quantity,0)) sub_type_quantity_sale1,MAX(IF(ROW=1,gmv,0)) sub_type_gmv1
    ,MAX(IF(ROW=2,SUB_TYPE_ID,0)) SUB_TYPE_ID2,MAX(IF(ROW=2,sub_type_name,0)) sub_type_name2,MAX(IF(ROW=2,sale_quantity,0)) sub_type_quantity_sale2,MAX(IF(ROW=2,gmv,0)) sub_type_gmv2
    ,MAX(IF(ROW=3,SUB_TYPE_ID,0)) SUB_TYPE_ID3,MAX(IF(ROW=3,sub_type_name,0)) sub_type_name3,MAX(IF(ROW=3,sale_quantity,0)) sub_type_quantity_sale3,MAX(IF(ROW=3,gmv,0)) sub_type_gmv3
FROM
    (SELECT a1.*,IF(@row_by=user_id,@row:=@row+1,@row:=1) ROW,@row_by:=user_id row_by
    FROM
        (SELECT a1.user_id,a2.SUB_TYPE_ID,a2.sub_type_name
             ,SUM(a1.quantity_act) sale_quantity
             ,SUM(a1.quantity_act* a1.sale_price) gmv
        FROM fe_dwd.dwd_pub_order_item_recent_two_month a1
        JOIN fe_dwd.dwd_product_base_day_all a2 ON a2.PRODUCT_ID=a1.PRODUCT_ID
        WHERE a1.PAY_DATE>=SUBDATE(CURDATE(),60) AND a1.PAY_DATE<CURDATE()
        GROUP BY a1.user_id,a2.SUB_TYPE_ID
        ORDER BY a1.user_id,sale_quantity DESC,gmv DESC
        )  a1
    ) tt
WHERE ROW<4
GROUP BY user_id
;
SET @time_4 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_dm_ma_user_perfect_product","@time_3--@time_4",@time_3,@time_4);  
INSERT INTO fe_dm.dm_ma_user_perfect_product
    (user_id, product_id1
    , product_name1, product_quantity_sale1, product_gmv1, product_id2, product_name2, product_quantity_sale2, product_gmv2, product_id3, product_name3, product_quantity_sale3, product_gmv3
    , sec_type_id1, sec_type_name1, sec_type_quantity_sale1, sec_type_gmv1, sec_type_id2, sec_type_quantity_sale2, sec_type_gmv2, sec_type_name2, sec_type_id3, sec_type_name3, sec_type_quantity_sale3, sec_type_gmv3
    , sub_type_id1, sub_type_name1, sub_type_quantity_sale1, sub_type_gmv1, sub_type_id2, sub_type_name2, sub_type_quantity_sale2, sub_type_gmv2, sub_type_id3, sub_type_name3, sub_type_quantity_sale3, sub_type_gmv3
    )
SELECT a1.*
    ,sec_type_id1, sec_type_name1, sec_type_quantity_sale1, sec_type_gmv1, sec_type_id2, sec_type_quantity_sale2, sec_type_gmv2, sec_type_name2, sec_type_id3, sec_type_name3, sec_type_quantity_sale3, sec_type_gmv3
    ,sub_type_id1, sub_type_name1, sub_type_quantity_sale1, sub_type_gmv1, sub_type_id2, sub_type_name2, sub_type_quantity_sale2, sub_type_gmv2, sub_type_id3, sub_type_name3, sub_type_quantity_sale3, sub_type_gmv3
FROM fe_dm.temp_user_buy1 a1
    JOIN fe_dm.temp_user_buy2 a2 ON a2.user_id=a1.user_id
    JOIN fe_dm.temp_user_buy3 a3 ON a3.user_id=a1.user_id
;
SET @time_5 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_dm_ma_user_perfect_product","@time_4--@time_5",@time_4,@time_5); 
UPDATE fe_dm.dm_ma_user_perfect_product
SET if_deeply_product=IF(product_quantity_sale1>=(product_quantity_sale2+product_quantity_sale3),1,0)
    ,if_deeply_sec_type=IF(sec_type_quantity_sale1>=(sec_type_quantity_sale2+sec_type_quantity_sale3),1,0)
    ,if_deeply_sub_type=IF(sub_type_quantity_sale1>=(sub_type_quantity_sale2+sub_type_quantity_sale3),1,0)
;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_dm_ma_user_perfect_product","@time_5--@time_6",@time_5,@time_6); 
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_dm_ma_user_perfect_product',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
CONCAT('纪伟铨@', @user),
@stime);
-- 记录表的数据量
call sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_user_perfect_product','prc_dm_ma_user_perfect_product','纪伟铨');
END