CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_ma_shelf_sale_2week`()
BEGIN
# 每周一一次
SET @run_date := CURDATE(),@user := CURRENT_USER(),@stime := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.dm_ma_shelf_sale_2week WHERE sdate<SUBDATE(CURDATE(),28) OR sdate=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 ) ;
#插入基础数据
INSERT INTO fe_dm.dm_ma_shelf_sale_2week
    (sdate, shelf_id
    , user_num_lw1, user_num_lw2, user_num_lw3, user_num_lw4, user_num_lw5, user_num_lw6, user_num_lw7, user_num_llw1, user_num_llw2, user_num_llw3, user_num_llw4, user_num_llw5, user_num_llw6, user_num_llw7)
SELECT
    ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 )  sdate,
    t1.shelf_id,
    SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=2,IFNULL(user_num,0),NULL))   user_num_lw1,
	SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=3,IFNULL(user_num,0),NULL))   user_num_lw2,
	SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=4,IFNULL(user_num,0),NULL))   user_num_lw3,
	SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=5,IFNULL(user_num,0),NULL))   user_num_lw4,
	SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=6,IFNULL(user_num,0),NULL))   user_num_lw5,
	SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=7,IFNULL(user_num,0),NULL))   user_num_lw6,
	SUM(IF(t3.sdate>=DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=1,IFNULL(user_num,0),NULL))   user_num_lw7,
    SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=2,IFNULL(user_num,0),NULL))   user_num_llw1,
	SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=3,IFNULL(user_num,0),NULL))   user_num_llw2,
	SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=4,IFNULL(user_num,0),NULL))   user_num_llw3,
	SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=5,IFNULL(user_num,0),NULL))   user_num_llw4,
	SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=6,IFNULL(user_num,0),NULL))   user_num_llw5,
	SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=7,IFNULL(user_num,0),NULL))   user_num_llw6,
	SUM(IF(t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-7 DAY) AND DAYOFWEEK(t3.sdate)=1,IFNULL(user_num,0),NULL))   user_num_llw7
FROM fe_dwd.dwd_shelf_base_day_all t1
JOIN fe_dwd.dwd_pub_work_day t3 ON t3.sdate>= DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2-14 DAY) AND t3.sdate<DATE_ADD(CURDATE(),INTERVAL -DAYOFWEEK(CURDATE())+2 DAY)
LEFT JOIN fe_dm.dm_ma_shelf_sale_daily t2 ON t2.sdate=t3.sdate AND t2.SHELF_ID=t1.SHELF_ID
WHERE t1.SHELF_STATUS =2  AND t1.SHELF_TYPE IN (1, 2, 3, 5, 6, 7, 8)  AND t1.DATA_FLAG = 1
GROUP BY t1.shelf_id;
# 插入库存信息
UPDATE fe_dm.dm_ma_shelf_sale_2week t1
JOIN ( SELECT SHELF_ID,SUM(STOCK_QUANTITY*SALE_PRICE) stock_value,COUNT(1) stock_sku,SUM(STOCK_QUANTITY) STOCK_QUANTITY
    FROM fe_dwd.dwd_shelf_product_day_all WHERE STOCK_QUANTITY>0 GROUP BY SHELF_ID ) t2
    ON t1.shelf_id=t2.SHELF_ID
SET t1.stock_sku=t2.stock_sku,t1.stock_quantity=t2.STOCK_QUANTITY,t1.stock_value=t2.stock_value
WHERE t1.sdate=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 )
;
#插入评分前2的两位关键客户ID
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_invite; #用户邀请
CREATE TEMPORARY TABLE fe_dm.tmp_user_invite(INDEX(user_id)) AS
     SELECT user_id,SUM(1) AS invite_qty
     FROM
         (SELECT DISTINCT a.inviter_user_id AS user_id,a.invite_id
         FROM fe_dwd.dwd_activity_invitation_information a
         WHERE a.add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND a.add_time<CURDATE()
         UNION ALL
         SELECT a.add_user_id,a.activity_id
         FROM fe_dwd.dwd_sf_friend_coupon a
         WHERE a.add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY)
         ) t1
     GROUP BY user_id;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_par; #用户参加
CREATE TEMPORARY TABLE fe_dm.tmp_user_par(INDEX(user_id)) AS
    SELECT user_id,SUM(ud) AS par_qty
    FROM
        (SELECT invitee_user_id AS user_id,1 ud
        FROM fe_dwd.dwd_activity_invitation_information
        WHERE  add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND add_time<CURDATE()
        UNION
        SELECT b.user_id,1 ud
        FROM fe_dwd.dwd_sf_friend_coupon a
        JOIN fe_dwd.dwd_sf_prize_record b ON a.activity_id=b.activity_id AND a.add_user_id<>b.user_id
        WHERE a.add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND a.add_time<CURDATE()
        ) t
    GROUP BY user_id ;
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_user_order; #用户下单
CREATE TEMPORARY TABLE fe_dm.tmp_user_order(INDEX(user_id)) AS
    SELECT USER_ID,COUNT(DISTINCT order_id) order_num
     FROM fe_dwd.dwd_order_item_refund_day t1
     WHERE ORDER_DATE>=SUBDATE(CURDATE(),90) AND ORDER_DATE<CURDATE()
        AND quantity_act>0
    GROUP BY USER_ID;
SET @rank=0,@rank_by='';
DROP TABLE IF EXISTS fe_dm.shelf_user_score;
CREATE TEMPORARY TABLE fe_dm.shelf_user_score AS
SELECT t1.*,IF(@rank_by=shelf_id,@rank:=@rank+1,@rank :=1) row_num,@rank_by:=shelf_id row_by
FROM
    (SELECT a1.shelf_id,a1.user_id,a3.invite_qty,a4.par_qty,a5.order_num,IFNULL(a3.invite_qty,0)*5+IFNULL(a4.par_qty,0)*3+IFNULL(a5.order_num,0) score
    FROM fe_dm.dm_op_su_stat a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.shelf_id AND a2.SHELF_STATUS=2 AND a2.REVOKE_STATUS IN (1,3) AND a2.SHELF_TYPE IN (1,3)
    LEFT JOIN fe_dm.tmp_user_invite a3 ON a3.user_id=a1.user_id
    LEFT JOIN fe_dm.tmp_user_par a4 ON a4.user_id=a1.user_id
    LEFT JOIN fe_dm.tmp_user_order a5 ON a5.user_id=a1.user_id
    ORDER BY shelf_id,score DESC
    ) t1
;
UPDATE fe_dm.dm_ma_shelf_sale_2week t1
JOIN
    ( SELECT shelf_id,MAX(IF(row_num=1,user_id,0)) phone1,MAX(IF(row_num=2,user_id,0)) phone2
     FROM
         (SELECT t1.shelf_id,t1.user_id,row_num
        FROM fe_dm.shelf_user_score t1
        WHERE t1.row_num<3
        ) tt
    GROUP BY shelf_id
    ) t2 ON t1.shelf_id=t2.shelf_id
SET t1.key_user_phone1=t2.phone1,t1.key_user_phone2=t2.phone2
WHERE t1.sdate=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 )
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('dm_ma_shelf_sale_2week',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user),@stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_ma_shelf_sale_2week','dm_ma_shelf_sale_2week','纪伟铨');
END