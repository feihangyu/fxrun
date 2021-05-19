CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_member_research_crr_2`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
# 用户信息
    #8、更新前几周销量（周）最近一次消费的货架
UPDATE feods.user_research a1 #3m 32s
left join feods.d_op_su_u_stat a2 on a2.user_id=a1.user_id
SET week_order_qty =
        (CASE WHEN week_order_qty IS NOT NULL AND last_week_order_qty IS NULL THEN
            CONCAT(SUBSTRING_INDEX(week_order_qty, ',' ,- 8),',',0)
        WHEN week_order_qty IS NULL AND last_week_order_qty IS NOT NULL THEN
            CONCAT('0,0,0,0,0,0,0,0',',',last_week_order_qty)
        WHEN week_order_qty IS NOT NULL AND last_week_order_qty IS NOT NULL THEN
            CONCAT(SUBSTRING_INDEX(week_order_qty, ',' ,- 8),',',last_week_order_qty)
        ELSE NULL END)
    ,last_week_date = YEARWEEK(CURDATE(), 1)
    ,user_life_cycle_yesterday=user_life_cycle #保存昨日的用户状态
    ,a1.shelf_id=a2.last_shelf_id
;
    #9、用户类型 #3m
DROP TEMPORARY TABLE IF EXISTS feods.tmp_user_life_cycle ;
CREATE TEMPORARY TABLE feods.tmp_user_life_cycle(INDEX(user_id))  AS
	SELECT
		t1.user_id,
		CASE
        WHEN create_date >= DATE_SUB(CURDATE(), INTERVAL 28 DAY) AND CREATE_DATE<DATE_SUB(CURDATE(),INTERVAL DATE_FORMAT(CURDATE(), '%w')-1 DAY)
            AND min_order_date >= DATE_SUB(CURDATE(),INTERVAL DATE_FORMAT(CURDATE(), '%w')+6 DAY) THEN
            '当周初次购买用户'
        WHEN
        IF (a >= 2, 2, a) =
        IF (b >= 2, 2, b)
        AND
        IF (b >= 2, 2, b) =
        IF (c >= 2, 2, c)
        AND
        IF (c >= 2, 2, c) =
        IF (d >= 2, 2, d)
        AND
        IF (a >= 2, 2, a) = 2 THEN
            '持续高频'
        WHEN
        IF (a >= 2, 2, a) =
        IF (b >= 2, 2, b)
        AND
        IF (b >= 2, 2, b) =
        IF (c >= 2, 2, c)
        AND
        IF (c >= 2, 2, c) =
        IF (d >= 2, 2, d)
        AND
        IF (a >= 2, 2, a) = 1 THEN
            '持续低频'
        WHEN
        IF (a >= 2, 2, a) >=
        IF (b >= 2, 2, b)
        AND
        IF (b >= 2, 2, b) >=
        IF (c >= 2, 2, c)
        AND
        IF (c >= 2, 2, c) >=
        IF (d >= 2, 2, d)
        AND
        IF (a >= 2, 2, a) >= 1 THEN
            '单调上涨'
        WHEN
        IF (a >= 2, 2, a) <=
        IF (b >= 2, 2, b)
        AND
        IF (b >= 2, 2, b) <=
        IF (c >= 2, 2, c)
        AND
        IF (c >= 2, 2, c) <=
        IF (d >= 2, 2, d)
        AND
        IF (d >= 2, 2, d) >= 1 THEN
            '单调下降'
        WHEN a + b + c + d >= 3 THEN
            '高频波动'
        WHEN a + b + c + d < 3
        AND a + b + c + d >= 1 THEN
            '低频波动'
        WHEN create_date >= DATE_SUB(CURDATE(), INTERVAL 28 DAY) THEN
            '新用户'
        WHEN SHELF_STATUS = '已撤架货架' THEN
            '撤架流失用户'
        WHEN max_order_date < DATE_SUB(CURDATE(), INTERVAL 56 DAY) THEN
            '8周无购买流失用户'
        WHEN max_order_date < DATE_SUB(CURDATE(), INTERVAL 28 DAY) THEN
            '沉默用户'
        WHEN max_order_date >= DATE_SUB(
            CURDATE(),
            INTERVAL
        IF (
            DATE_FORMAT(CURDATE(), '%w') = 0,
            7,
            DATE_FORMAT(CURDATE(), '%w')
        ) - 1 DAY
        ) THEN
            '回流用户'
        ELSE
            '8周无购买流失用户'
        END user_life_cycle
    FROM
        (SELECT user_id,
            CREATE_DATE,SHELF_STATUS,max_order_date,min_order_date,
            SUBSTRING_INDEX(SUBSTRING_INDEX(week_order_qty, ',' ,- 4),',',1) AS d,
            SUBSTRING_INDEX(SUBSTRING_INDEX(week_order_qty, ',' ,- 3),',',1) AS c,
            SUBSTRING_INDEX(SUBSTRING_INDEX(week_order_qty, ',' ,- 2),',',1) AS b,
            SUBSTRING_INDEX(SUBSTRING_INDEX(week_order_qty, ',' ,- 1),',',1) AS a
        FROM feods.user_research
        ) t1
;
update feods.user_research a1    #1m48s
join feods.tmp_user_life_cycle a2 on a2.user_id=a1.user_id
set a1.user_life_cycle=a2.user_life_cycle
;
    #10、更新历史的用户类型(更正过) 55s
UPDATE test.user_research
SET user_life_cycle_history = CONCAT(IFNULL(user_life_cycle_history, 0),',',user_life_cycle)
    ,user_life_cycle_history_date = CONCAT(user_life_cycle_history_date,',',user_life_cycle_date)
WHERE SUBSTRING_INDEX(IFNULL(user_life_cycle_history, 0),',' ,- 1) <> user_life_cycle;

# 货架信息
    ###11、最近4周货架是否购买过 27s
drop temporary table if exists  feods.t_shelf_buy;
create temporary table feods.t_shelf_buy (index i_shelf(shelf_id) )as
	SELECT
		shelf_id,
		MAX(order_date) AS shelf_max_order_date_4_week,
		COUNT(DISTINCT order_id) AS shelf_order_qty_4_week,
		COUNT(DISTINCT user_id) AS shelf_user_qty_4_week
	FROM fe.sf_order force index (idx_order_orderdate)
	WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
		and order_status IN (2, 6, 7)
	GROUP BY shelf_id
;
    ###12、最近4周是否发生过补货 13s
drop temporary table if exists  feods.t_shelf_fill;
create temporary table feods.t_shelf_fill (index i_shelf(shelf_id) )as
	SELECT
		shelf_id,
		MAX(fill_time) AS max_fill_order_date,
		COUNT(DISTINCT order_id) AS fill_order_qty
	FROM fe_dwd.dwd_fill_day_inc_recent_two_month FORCE INDEX (idx_dwd_replenish_FILL_TIME)
	WHERE fill_time >= DATE_SUB(CURDATE(), INTERVAL 28 DAY)
		AND order_status IN (3, 4)
    	AND PRODUCT_NUM > 10
	GROUP BY shelf_id
;
drop temporary table if exists  feods.t_shelf_info; #1s
create temporary table feods.t_shelf_info (index i_shelf(shelf_id) )as
select a2.shelf_id
    ,a2.is_prewarehouse_cover,a2.manager_type
    ,CASE
        WHEN SHELF_STATUS = 2
        AND a2.REVOKE_STATUS = 1
        AND a2.WHETHER_CLOSE = 1 THEN
            '关闭未撤架'
        WHEN SHELF_STATUS = 2
        AND a2.REVOKE_STATUS <> 1
        AND a2.WHETHER_CLOSE = 1 THEN
            '关闭撤架过程中'
        WHEN SHELF_STATUS = 2
        AND a2.REVOKE_STATUS <> 1
        AND a2.WHETHER_CLOSE = 2 THEN
            '未关闭撤架过程中'
        WHEN SHELF_STATUS = 2
        AND a2.REVOKE_STATUS = 1
        AND a2.WHETHER_CLOSE = 2 THEN
            '正常货架'
        WHEN SHELF_STATUS = 3 THEN
            '已撤架货架'
        ELSE
            '其他'
        END AS SHELF_STATUS,
	    a2.grade
    ,if(a3.cooperation_type in (1,5),'内部','外部') cooperation_type
    ,a4.fill_order_qty,a4.max_fill_order_date
    ,a5.shelf_max_order_date_4_week,a5.shelf_order_qty_4_week,a5.shelf_user_qty_4_week
from  fe_dwd.dwd_shelf_base_day_all a2
left join feods.zs_shelf_flag a3 on a3.shelf_id=a2.shelf_id
left join feods.t_shelf_fill a4 on a4.shelf_id=a2.shelf_id
left join feods.t_shelf_buy a5 on a5.shelf_id=a2.shelf_id
where a2.SHELF_STATUS in (2,3,4,5)
;
update feods.user_research a1 #7m8s
join feods.t_shelf_info a2 on a2.shelf_id=a1.shelf_id
set a1.if_prewarehouse=a2.is_prewarehouse_cover
    ,a1.if_all_time_manager=a2.manager_type
    ,a1.SHELF_STATUS=a2.SHELF_STATUS
    ,a1.shelf_level=a2.grade
    ,a1.cooperation_type=a2.cooperation_type
    ,a1.fill_order_qty=a2.fill_order_qty
    ,a1.fill_order_qty=a2.fill_order_qty
    ,a1.max_fill_order_date=a2.max_fill_order_date
    ,a1.shelf_max_order_date_4_week=a2.shelf_max_order_date_4_week
    ,a1.shelf_order_qty_4_week=a1.shelf_order_qty_4_week
    ,a1.shelf_user_qty_4_week=a2.shelf_user_qty_4_week
;
# 14、 更新用户类型统计 #1m42s
DELETE FROM feods.user_research_day WHERE sdate = CURDATE();
INSERT INTO feods.user_research_day (
	sdate,
	user_life_cycle,
	user_life_cycle_yesterday,
	user_qty
)
SELECT
	CURDATE() AS sdate,
	user_life_cycle,
	user_life_cycle_yesterday,
	COUNT(DISTINCT user_id) AS user_qty
FROM feods.user_research
GROUP BY user_life_cycle,user_life_cycle_yesterday;


-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('sh_member_research_crr_2',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
COMMIT;
END