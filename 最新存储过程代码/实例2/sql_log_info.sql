CREATE DEFINER=`shprocess`@`%` PROCEDURE `sql_log_info`( IN task_name  VARCHAR(1024),IN sql_str VARCHAR(128),IN stime DATETIME,IN etime DATETIME)
BEGIN
INSERT INTO fe_dwd.sql_log_info
(sdate,task_name,sql_str,stime,etime,run_time)
VALUES(current_date(),task_name,sql_str,stime,etime,ROUND(TIMESTAMPDIFF(SECOND,stime,etime)/60,1));
END