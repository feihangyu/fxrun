CREATE DEFINER=`shprocess`@`%` PROCEDURE `dm_op_kpi2_shelf_level_stat`(IN p_sdate DATE)
BEGIN
-- =============================================
-- Author:	运营
-- Create date: 2020-4-10
-- Modify date:
-- Description: 货架数统计 每天跑一次
-- =============================================
SET @run_date := SUBDATE(CURRENT_DATE, 1), @user := CURRENT_USER, @stime := CURRENT_TIMESTAMP;
SET @sdate=p_sdate;
SET @smonth=DATE_FORMAT(@sdate,'%Y-%m-01');
SET @smonth_last= LAST_DAY(DATE_SUB(@sdate,INTERVAL 1 MONTH));
#删除
DELETE FROM fe_dm.dm_op_kpi2_shelf_level_stat WHERE sdate=@sdate;
#插入数据
INSERT INTO fe_dm.dm_op_kpi2_shelf_level_stat
    (sdate, business_name, shelfs32, shelfs0, shelfs)
SELECT @sdate,business_name,SUM(IF(grade IN ('甲','已'),1,0)) shelfs32,SUM(IF(grade IN ('丁'),1,0))  shelfs0,SUM(1) shelfs
FROM fe_dwd.dwd_shelf_base_day_all
WHERE SHELF_STATUS IN (2,3,4,5) AND shelf_type<>9
  AND DATE(ACTIVATE_TIME)<=@sdate AND DATE(IFNULL(REVOKE_TIME,CURRENT_DATE)) >=@sdate
GROUP BY business_name;
# 更新上月数据
    #上月的甲乙货架,流失货架
DROP TEMPORARY TABLE IF EXISTS fe_dm.tmp_shelf_stat;
CREATE TEMPORARY TABLE fe_dm.tmp_shelf_stat AS
    SELECT t1.business_name,COUNT(1) shelfs32_lm,COUNT(shelf_id2) shelfs32_lm_loss
    FROM
        (SELECT a1.shelf_id,a21.business_name,a2.shelf_id shelf_id2
        FROM fe_dm.dm_pub_shelf_grade a1
        JOIN fe_dwd.dwd_shelf_base_day_all a21 ON a21.shelf_id=a1.shelf_id
        LEFT JOIN fe_dm.dm_pub_shelf_grade a2 ON a2.month_id=DATE_FORMAT(@smonth,'%Y-%m') AND a2.shelf_id=a1.shelf_id AND a2.grade NOT IN ('甲','已')
        WHERE a1.month_id=DATE_FORMAT(@smonth_last,'%Y-%m') AND a1.grade IN ('甲','已')
        ) t1
    GROUP BY t1.business_name;
UPDATE fe_dm.dm_op_kpi2_shelf_level_stat a1
JOIN fe_dm.tmp_shelf_stat a2 ON a2.business_name=a1.business_name
SET a1.shelfs32_lm=a2.shelfs32_lm,a1.shelfs32_lm_loss=a2.shelfs32_lm_loss
WHERE a1.sdate=@sdate;
# 更新运营kpi2
CALL sh_process.`sp_sf_dw_task_log` ('dm_op_kpi2_shelf_level_stat',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user), @stime);
-- 记录表的数据量
-- CALL sh_process.dwd_count_process_aim_table_size('fe_dm.dm_op_kpi2_shelf_level_stat','dm_op_kpi2_shelf_level_stat','纪伟铨');
COMMIT;
END