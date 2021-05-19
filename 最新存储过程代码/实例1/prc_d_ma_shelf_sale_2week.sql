CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_sale_2week`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();

DELETE FROM feods.d_ma_shelf_sale_2week WHERE sdate<SUBDATE(CURDATE(),28) OR sdate=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 ) ;
SET @time_4 := CURRENT_TIMESTAMP();
#插入基础数据
INSERT INTO feods.d_ma_shelf_sale_2week
    (sdate, shelf_id, shelf_code, city_name, activate_time, revoke_time, shelf_type, shelf_status
    , user_num_lw1, user_num_lw2, user_num_lw3, user_num_lw4, user_num_lw5, user_num_lw6, user_num_lw7, user_num_llw1, user_num_llw2, user_num_llw3, user_num_llw4, user_num_llw5, user_num_llw6, user_num_llw7)
SELECT
    ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 )  sdate,
    t1.shelf_id,
    t1.SHELF_CODE,
    t1.city_name,
    t1.ACTIVATE_TIME,
    t1.REVOKE_TIME,
    t1.SHELF_TYPE,
    t1.SHELF_STATUS,
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
LEFT JOIN feods.d_ma_shelf_sale_daily t2 ON t2.sdate=t3.sdate AND t2.SHELF_ID=t1.SHELF_ID
WHERE t1.SHELF_STATUS =2  AND t1.SHELF_TYPE IN (1, 2, 3, 5, 6, 7, 8) AND t1.DATA_FLAG = 1
GROUP BY t1.shelf_id;
SET @time_6 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_d_ma_shelf_sale_2week","@time_4--@time_6",@time_4,@time_6);
SET @time_8 := CURRENT_TIMESTAMP();
# 插入库存信息
UPDATE feods.d_ma_shelf_sale_2week t1
JOIN ( SELECT SHELF_ID,SUM(STOCK_QUANTITY*SALE_PRICE) stock_value,COUNT(1) stock_sku,SUM(STOCK_QUANTITY) STOCK_QUANTITY
    FROM fe_dwd.dwd_shelf_product_day_all WHERE STOCK_QUANTITY>0 GROUP BY SHELF_ID ) t2
    ON t1.shelf_id=t2.SHELF_ID
SET t1.stock_sku=t2.stock_sku,t1.stock_quantity=t2.STOCK_QUANTITY,t1.stock_value=t2.stock_value
WHERE t1.sdate=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 )
;
SET @time_10 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_d_ma_shelf_sale_2week","@time_8--@time_10",@time_8,@time_10);
#插入评分前2的两位关键客户ID
SET @time_14 := CURRENT_TIMESTAMP();
drop temporary table if exists feods.tmp_user_invite; #用户邀请
create temporary table feods.tmp_user_invite(index(user_id)) as
     SELECT user_id,SUM(1) AS invite_qty
     FROM
         (SELECT a.inviter_user_id AS user_id,a.invite_id
         FROM fe_activity.sf_activity_invitation a
         WHERE a.add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND a.add_time<CURDATE()
           AND a.data_flag=1
         UNION ALL
         SELECT a.add_user_id,a.activity_id
         FROM fe_activity.sf_friend_coupon a
         WHERE a.add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND a.data_flag=1
         ) t1
     GROUP BY user_id;
drop temporary table if exists feods.tmp_user_par; #用户参加
create temporary table feods.tmp_user_par(index(user_id)) as
    SELECT user_id,SUM(ud) AS par_qty
    FROM
        (SELECT invitee_user_id AS user_id,1 ud
        FROM fe_activity.sf_activity_invitation_detail
        WHERE  add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND add_time<CURDATE()
          AND data_flag=1
        UNION
        SELECT b.user_id,1 ud
        FROM fe_activity.sf_friend_coupon a
        JOIN fe_activity.sf_prize_record b ON a.activity_id=b.activity_id AND b.data_flag=1
        WHERE a.add_time>=DATE_ADD(CURDATE(),INTERVAL -90 DAY) AND a.add_time<CURDATE()
          AND a.add_user_id<>b.user_id AND a.data_flag=1
        ) t
    GROUP BY user_id ;
drop temporary table if exists feods.tmp_user_order; #用户下单
create temporary table feods.tmp_user_order(index(user_id)) as
    SELECT USER_ID,COUNT(1) order_num
     FROM fe.sf_order t1
     WHERE ORDER_DATE>=SUBDATE(CURDATE(),90) AND ORDER_DATE<CURDATE() AND ORDER_STATUS=2 AND DATA_FLAG=1
    GROUP BY USER_ID;
SET @rank=0,@rank_by='';
DROP TABLE IF EXISTS test.shelf_user_score;
CREATE TEMPORARY TABLE test.shelf_user_score AS
SELECT t1.*,IF(@rank_by=shelf_id,@rank:=@rank+1,@rank :=1) row_num,@rank_by:=shelf_id row_by
FROM
    (SELECT a1.shelf_id,a1.user_id,a3.invite_qty,a4.par_qty,a5.order_num,IFNULL(a3.invite_qty,0)*5+IFNULL(a4.par_qty,0)*3+IFNULL(a5.order_num,0) score
    FROM feods.d_op_su_stat a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a1.shelf_id=a2.shelf_id AND a2.SHELF_STATUS=2 AND a2.REVOKE_STATUS IN (1,3) AND a2.SHELF_TYPE IN (1,3)
    LEFT JOIN feods.tmp_user_invite a3 ON a3.user_id=a1.user_id
    LEFT JOIN feods.tmp_user_par a4 ON a4.user_id=a1.user_id
    LEFT JOIN feods.tmp_user_order a5 ON a5.user_id=a1.user_id
    ORDER BY shelf_id,score DESC
    ) t1
;
SET @time_16 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_d_ma_shelf_sale_2week","@time_14--@time_16",@time_14,@time_16);
SET @time_18 := CURRENT_TIMESTAMP();
UPDATE feods.d_ma_shelf_sale_2week t1
JOIN
    ( SELECT shelf_id,MAX(IF(row_num=1,user_id,0)) phone1,MAX(IF(row_num=2,user_id,0)) phone2
     FROM
         (SELECT t1.shelf_id,t1.user_id
            ,row_num
        FROM test.shelf_user_score t1
        WHERE t1.row_num<3
        ) tt
    GROUP BY shelf_id
    ) t2 ON t1.shelf_id=t2.shelf_id
SET t1.key_user_phone1=t2.phone1,t1.key_user_phone2=t2.phone2
WHERE t1.sdate=ADDDATE(CURDATE(), -IF(DAYOFWEEK(CURDATE())=1,8,DAYOFWEEK(CURDATE()))+2 )
;


SET @time_20 := CURRENT_TIMESTAMP();
CALL sh_process.sql_log_info("prc_d_ma_shelf_sale_2week","@time_18--@time_20",@time_18,@time_20);
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'prc_d_ma_shelf_sale_2week',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('纪伟铨@',@user,@timestamp)
);
END