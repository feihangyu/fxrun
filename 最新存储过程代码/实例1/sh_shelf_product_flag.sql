CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_shelf_product_flag`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate=CURRENT_DATE;
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
SET @if_LateNight= IF(CURRENT_TIME BETWEEN '00:00:00' AND '15:00:00',0,1);
SET @datetime_start=IF(@if_LateNight=0,SUBDATE(CURDATE(),1),CURDATE())
    ,@datetime_end= IF(@if_LateNight=0,CURDATE(),ADDDATE(CURDATE(),1)) ;

TRUNCATE feods.zs_shelf_product_flag;
#更新临时基础货架商品信息表
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_product_info;
CREATE TEMPORARY TABLE feods.temp_shelf_product_info(INDEX (SHELF_ID,PRODUCT_ID)) AS #3m 45s
    SELECT a.SHELF_ID,a.PRODUCT_ID
         ,IFNULL(a.SALES_FLAG,0) sales_flag1,NEW_FLAG,a.DANGER_FLAG
         ,c.business_name,c.shelf_type,c.shelf_level,c.SHELF_STATUS
         ,b.SALE_PRICE,b.STOCK_QUANTITY
    FROM fe.sf_shelf_product_detail_flag a #force INDEX(uk_shelf_id_produce_id)
    JOIN fe.sf_shelf_product_detail b FORCE INDEX(uk_shelf_id_produce_id) ON  b.shelf_id =a.shelf_id  AND b.PRODUCT_ID=a.PRODUCT_ID AND b.DATA_FLAG=1
    JOIN fe_dwd.dwd_shelf_base_day_all c ON c.shelf_id=a.SHELF_ID  AND SHELF_STATUS IN (1,2,4,5)
    WHERE a.DATA_FLAG=1
    ORDER BY a.shelf_id,sales_flag1,STOCK_QUANTITY DESC
;
#插入基础信息 销售等级
SET @row_num=0,@row_by='';
INSERT INTO feods.zs_shelf_product_flag #1m5s
    (shelf_id,product_id
    ,danger_level,new_flag,ext1,stock_level)
SELECT SHELF_ID,PRODUCT_ID
     ,DANGER_FLAG,NEW_FLAG,sales_flag,rownum
FROM
    (SELECT
        a.shelf_id,
        a.product_id,
        a.DANGER_FLAG,
        a.NEW_FLAG,
        sales_flag1 sales_flag,
        CASE WHEN @row_by = CONCAT(a.shelf_id, '|', a.sales_flag1) AND @row_num >20 THEN @row_num := 21
            WHEN @row_by = CONCAT(a.shelf_id, '|', a.sales_flag1)  THEN @row_num := @row_num + 1
            ELSE @row_num := 1 END rownum,
        @row_by:= CONCAT(a.shelf_id, '|', a.sales_flag1) row_by
    FROM feods.temp_shelf_product_info a
    ) t1
;
# 更新标签
    # 添加购物车推荐标签 39s
UPDATE   feods.zs_shelf_product_flag  t1
JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.shelf_id=t1.shelf_id
JOIN fe_dm.dm_ma_HighProfit_list_monthly t3 ON t3.sdate=DATE_SUB(@smonth,INTERVAL 1 MONTH ) AND t3.business_area=t2.business_name AND t3.product_id=t1.product_id
SET t1.ext2=1
WHERE 1=1
;
    #商品状态(1:新增（免费货）、2:新增（试运行）、3:原有、4:预淘汰、5:淘汰（替补）、6:退出、7:个性化商品、0:其他)
DROP TEMPORARY TABLE IF EXISTS feods.temp_product_status;
CREATE TEMPORARY TABLE feods.temp_product_status(INDEX(SHELF_ID,PRODUCT_ID))  AS #42s
    SELECT t3.SHELF_ID,t2.product_id
        ,t2.product_type
        ,CASE WHEN t2.product_type='新增（免费货）' THEN 1 WHEN t2.product_type='新增（试运行）' THEN 2 WHEN t2.product_type='原有' THEN 3 WHEN t2.product_type='预淘汰' THEN 4
            WHEN t2.product_type='淘汰（替补）' THEN 5 WHEN t2.product_type='退出' THEN 6  WHEN t2.product_type='个性化商品' THEN 7
            ELSE 0 END ext8
    FROM feods.fjr_product_list_manager_week t2
    JOIN fe_dwd.dwd_shelf_base_day_all t3 ON t3.business_name=t2.business_area AND  SHELF_STATUS IN(2,4)  AND REVOKE_TIME IS NULL AND t3.DATA_FLAG=1
    WHERE t2.week_end=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+1 );
UPDATE feods.zs_shelf_product_flag t1 #1m31s
JOIN feods.temp_product_status t2 ON t2.SHELF_ID=t1.shelf_id AND t2.PRODUCT_ID=t1.product_id
SET t1.ext8=t2.ext8 WHERE 1=1
;
    # 库存周转天数，淘汰时间。 1m16s
UPDATE feods.zs_shelf_product_flag t1
JOIN feods.d_op_sp_disrate t2 ON t2.shelf_id=t1.shelf_id AND t2.product_id=t1.product_id
SET t1.clean_time=t2.out_date #淘汰时间
    , t1.ext4=(CASE WHEN t2.stock_frag='[0,30)' THEN 1 WHEN t2.stock_frag='[30,60)' THEN 2 WHEN t2.stock_frag='[60,100)' THEN 3  WHEN t2.stock_frag='[100,)' THEN 4 WHEN out_date IS NULL THEN 0 ELSE 5 END  ) #库存周转天数
WHERE 1=1
;
    #二三级商品分类: 2m16s
UPDATE feods.zs_shelf_product_flag t1
JOIN fe_dwd.dwd_product_base_day_all a1 ON a1.PRODUCT_ID=t1.product_id
SET t1.ext6=CASE WHEN a1.SUB_TYPE_ID=30 THEN a1.SUB_TYPE_ID WHEN a1.SECOND_TYPE_ID IN (1,2,4,6,7)  THEN a1.SECOND_TYPE_ID ELSE  0 END
WHERE 1=1
;
    -- 添加每日特惠标签
        #货架商品临时数据 2 m 16 s 700 ms
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_product_info1;
CREATE TEMPORARY TABLE feods.temp_shelf_product_info1  AS
    SELECT a.shelf_id,a.product_id
        ,a.SALE_PRICE,a.shelf_level
        ,a.sales_flag1,a.stock_quantity,a.shelf_type
    FROM feods.temp_shelf_product_info a
    LEFT JOIN fe.`sf_product_activity_item` f ON  f.shelf_id=a.shelf_id AND f.product_id=a.product_id  AND f.add_time>=@datetime_start AND f.add_time<@datetime_end   # 每天不重样
    WHERE a.shelf_status=2  AND a.shelf_type NOT IN (4,9)
        AND  a.shelf_id NOT IN(81921,85789) AND a.product_id NOT IN(19,67,169,29,28)  #排除特定货架还有商品
        AND a.business_name NOT IN('内蒙古区','惠州区','冀北区','烟台市') /*限制地区*/
        AND  a.stock_quantity>0 AND a.new_flag=2
        AND f.item_id IS NULL
    ORDER BY a.shelf_id ,a.sales_flag1 DESC,a.stock_quantity DESC;
        #甲乙货架 2件  销售等级 1-2   9折    2元以上 1 s 932 ms
SET @row :=0, @mid :='';
UPDATE feods.zs_shelf_product_flag t1
JOIN
    (SELECT b.shelf_id,b.product_id
     FROM
        (
        SELECT a.*
            ,CASE WHEN @mid=a.shelf_id THEN @row:=@row+1 ELSE @row:=1 END row_num
            ,@mid := a.shelf_id
        FROM feods.temp_shelf_product_info1 a
        WHERE a.SALES_FLAG1 IN(1,2) AND a.shelf_level IN(2,3)
            AND a.sale_price >= 2
        )b
     WHERE b.row_num<3
    )t2   ON t1.shelf_id=t2.SHELF_ID AND t1.product_id=t2.PRODUCT_ID
   SET t1.ext3=3 #3
    ;
        # 甲乙级货架，销售等级为3、4、5的商品，按销售等级5-3、库存数量高-低排序，每个货架选择排名最前的4款 1 s 608 ms
SET @row :=0, @mid :='';
UPDATE feods.zs_shelf_product_flag t1
JOIN
    (SELECT b.shelf_id,b.product_id
        ,b.sales_flag1,b.stock_quantity,b.row_num
     FROM
        (
        SELECT a.*
            ,CASE WHEN @mid=a.shelf_id THEN @row:=@row+1 ELSE @row:=1 END row_num
            ,@mid := a.shelf_id
        FROM feods.temp_shelf_product_info1 a
        WHERE a.SALES_FLAG1 IN(3,4,5) AND  a.shelf_level IN(2,3)
        )b
     WHERE b.row_num<5
    ) t2 ON t2.SHELF_ID=t1.shelf_id AND t2.PRODUCT_ID=t1.product_id
SET t1.ext3=CASE t2.sales_flag1 WHEN 3 THEN 3 WHEN 4 THEN 2 WHEN 5 THEN 1 END
;
        # 选择甲乙级以外的其他货架，销售等级为3、4、5的商品，按销售等级5-3、库存数量高-低排序，每个货架选择排名最前的6款，每个冰箱选择排名最前的4款。 2 s 984 ms
SET @row=0, @mid='';
UPDATE feods.zs_shelf_product_flag t1
JOIN
    (SELECT b.shelf_id,b.product_id
        ,b.sales_flag1,b.stock_quantity,b.row_num
     FROM
        (
        SELECT a.*
            ,CASE WHEN @mid=a.shelf_id THEN @row:=@row+1 ELSE @row:=1 END row_num
            ,@mid := a.shelf_id
        FROM feods.temp_shelf_product_info1 a
        WHERE a.SALES_FLAG1 IN(3,4,5) AND a.shelf_level NOT IN(2,3)
        )b
      WHERE b.row_num<IF(b.shelf_type=2,5,7)
     ) t2 ON t1.shelf_id=t2.SHELF_ID AND t1.product_id=t2.PRODUCT_ID
SET t1.ext3=CASE t2.sales_flag1 WHEN 3 THEN 3 WHEN 4 THEN 2 WHEN 5 THEN 1 END
;
# 年终大促地区严重滞销品 (1 是,0 否)
UPDATE feods.zs_shelf_product_flag t1
JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.shelf_id=t1.shelf_id
JOIN feods.d_ma_temp_shelf_product_info t4 ON t4.business_name=t2.business_name AND t4.product_id=t1.product_id
SET t1.ext7=1
;
# 主题活动标签(1:奶制品+饮料、2:速食+糖巧+休食、0:其他)
UPDATE  feods.zs_shelf_product_flag t1
JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.shelf_id=t1.shelf_id
JOIN feods.dm_ma_area_product_tag_temp t3 ON t3.business_area=t2.business_name AND t3.product_id=t1.product_id
SET t1.ext9=t3.tag
;
#是否销量下滑(1:是,0:否)
    #商品近30天销量≥15且商品近7天销量≤2且每天更新时商品库存量≥10
drop temporary table if exists feods.tmp_shelf_product_sale ; #销量下滑货架商品
create temporary table feods.tmp_shelf_product_sale(index(shelf_id,product_id)) as
    select t1.shelf_id,t1.product_id
    from feods.temp_shelf_product_info t1
    left JOIN feods.d_op_sp_avgsal7 t2 ON t2.shelf_id=t1.shelf_id AND t2.product_id=t1.product_id
    left JOIN feods.d_op_sp_avgsal30 t3 ON t3.shelf_id=t1.shelf_id AND t3.product_id=t1.product_id
    where ifnull(t2.qty_sal7,0)<=2 and ifnull(t3.days_sal_sto30,0)>=15
        and t1.STOCK_QUANTITY>=10
;
update feods.zs_shelf_product_flag t1
join feods.tmp_shelf_product_sale t2 on t2.shelf_id=t1.shelf_id and t2.product_id=t1.product_id
set t1.ext5=1
;

-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('sh_shelf_product_flag',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));

END