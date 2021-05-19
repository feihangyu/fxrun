CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_campus_manager_level`()
begin
-- =============================================
-- Author:	物流校园组
-- Create date: 2019/10/14
-- Modify date: 
-- Description:	
-- 	校园货架店主等级标签（每天的2时13分跑）
-- 
-- =============================================
  SET @run_date:= CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
if day(current_dATE) != 2 THEN 
DELETE FROM feods.D_LO_campus_manager_level WHERE STAT_DATE = DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY);
end if;
DELETE FROM feods.D_LO_campus_manager_level WHERE STAT_DATE = CURRENT_DATE;
insert into feods.D_LO_campus_manager_level(
 STAT_DATE                                                                                                                 
,MANAGER_ID
,manager_name                                                                                                            
,pay_amount                                                                                                              
,manager_gmv
)
 select
  CURRENT_DATE as '统计日期',
  f.manager_id,
  r.real_name,
  sum(e.PAY_TOTAL_AMOUNT) as pay_amount,
  sum(e.PAY_TOTAL_AMOUNT + e.COUPON_TOTAL_AMOUNT) as manager_gmv
 from
   fe.`sf_shelf` f
 join fe.`pub_shelf_manager` r
 on f.manager_id = r.manager_id
 join fe.sf_statistics_shelf_sale e
 on f.shelf_id = e.shelf_id
 where e.CREATE_DATE >= date_add(date_sub(current_date,interval 1 day),interval -day(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 day)
 and e.CREATE_DATE < date_add(last_day(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),interval 1 day)
 and f.shelf_type = 8
 and f.data_flag = 1
 group by f.manager_id;
 
 update feods.D_LO_campus_manager_level t
 set t.manager_level = if(t.manager_gmv < 2000,1,if(t.manager_gmv >=2000 and t.manager_gmv < 3000,2,if(t.manager_gmv>=3000
 and t.manager_gmv < 4000,3,if(t.manager_gmv>=4000,4,null)))), t.last_level_interval = if(t.manager_level=1,2000-t.manager_gmv,if(t.manager_level=2,3000-t.manager_gmv,if(t.manager_level=3,4000-t.manager_gmv,if(t.manager_level=4,0,null))))
 where t.STAT_DATE = CURRENT_DATE;
 
  SET @n:= 0;
  UPDATE feods.D_LO_campus_manager_level t
  join
  (SELECT
  t.manager_id,
  t.`manager_gmv`,
  @n := @n+1 AS top
 FROM
   feods.D_LO_campus_manager_level t
 WHERE t.STAT_DATE = CURRENT_DATE
--  and t.manager_level = 4
--  and (SELECT COUNT(e.manager_id) FROM feods.D_LO_campus_manager_level e WHERE e.manager_level=4 AND e.STAT_DATE = DATE_ADD(LAST_DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY)),INTERVAL 1 DAY))>10
 ORDER BY t.manager_gmv DESC) g
 on t.manager_id = g.manager_id
 set t.manager_level = IF(g.top <= 3,6,5)
 where t.STAT_DATE = CURRENT_DATE
 and g.top <= 10
;
update feods.`D_LO_campus_manager_level` m
left join
(SELECT
  t.manager_level,
  min(t.manager_gmv) as manager_gmv
 FROM
   feods.D_LO_campus_manager_level t
 WHERE t.STAT_DATE = CURRENT_DATE
 and t.manager_level >= 4
 group by t.manager_level) r
on m.manager_level = r.manager_level - 1
set m.last_level_interval = if(m.manager_level=6,0,r.manager_gmv - m.manager_gmv)
where m.STAT_DATE = CURRENT_DATE
and m.manager_level >= 4 ;
 
 -- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'sp_D_LO_campus_manager_level',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('caisonglin@',@user,@timestamp)
);
commit;
end