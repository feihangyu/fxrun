CREATE DEFINER=`feprocess`@`%` PROCEDURE `sc_requirement_yy_list`()
    SQL SECURITY INVOKER
BEGIN
  -- create table test.fjr_requirement_yy_list (
--   row_id bigint (20) not null auto_increment comment '行号',
--   group_name varchar (32) not null comment '组别',
--   user_name varchar (32) not null comment '用户',
--   priority tinyint not null default 6 comment '优先级',
--   priority_update tinyint not null default 0 comment '当前优先级',
--   requirement varchar (256) not null comment '需求',
--   if_done bool not null default 0 comment '是否已完成',
--   done_date date comment '实际完成时间',
--   update_time timestamp not null default current_timestamp comment '更新时间',
--   add_time timestamp not null default current_timestamp comment '添加时间',
--   primary key (`row_id`)
-- ) comment = '运营需求清单';
   #新建需求
#insert into test.fjr_requirement_yy_list (group_name,user_name,priority,requirement,update_time,add_time)
   SELECT
    t.group_name,
    t.user_name,
    MIN(n.number) priority,
    t.requirement,
    t.update_time,
    t.add_time
  FROM
    (SELECT
      'group_name' group_name,
      'user_name' user_name,
      2 priority,
      'requirement' requirement,
      @update_time := CURRENT_TIMESTAMP update_time,
      @update_time add_time) t
    JOIN feods.fjr_number n
      ON t.priority <= n.number
      AND n.number <= 6
    LEFT JOIN test.fjr_requirement_yy_list l
      ON t.group_name = l.group_name
      AND n.number = l.priority
      AND l.if_done = 0
  WHERE ISNULL(l.priority)
    OR l.priority = 6;
  #更新等级/提交时间/完成
   SELECT
    *
  FROM
    test.fjr_requirement_yy_list t
  WHERE t.group_name = 'group_name'
    AND t.if_done = 0;
  #update test.fjr_requirement_yy_list t set t.priority_update = 3 where t.row_id = 8;
#update test.fjr_requirement_yy_list t set t.update_time = t.add_time where t.row_id = 8;
#update test.fjr_requirement_yy_list t set t.if_done = 1,t.done_date = current_timestamp where t.row_id = 8;
#当前任务排序
   SELECT
    t.row_id,
    t.group_name,
    t.user_name,
    @cpriority := IF(
      t.priority_update = 0,
      t.priority,
      t.priority_update
    ) cpriority,
    t.requirement,
    t.update_time,
    t.add_time,
    @dead_line :=
    (SELECT
      MIN(ws.sdate) sdate
    FROM
      feods.fjr_work_days ws
    WHERE ws.work_day_seq = w.work_day_seq +
      CASE
        @cpriority
        WHEN 1
        THEN 1
        WHEN 2
        THEN 3
        WHEN 3
        THEN 5
        WHEN 4
        THEN 10
        WHEN 5
        THEN 15
        ELSE 20
      END) dead_line,
    DATEDIFF(@dead_line, CURRENT_DATE) remain_days
  FROM
    test.fjr_requirement_yy_list t
    JOIN feods.fjr_work_days w
      ON DATE(t.update_time) = w.sdate
  WHERE t.if_done = 0
  ORDER BY remain_days,
    cpriority,
    t.update_time;
END