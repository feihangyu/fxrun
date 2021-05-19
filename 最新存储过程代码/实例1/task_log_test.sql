CREATE DEFINER=`feprocess`@`%` PROCEDURE `task_log_test`(IN pi_task_name VARCHAR(256),IN pi_statedate VARCHAR(64),IN start_time datetime,IN end_time datetime)
BEGIN
          DECLARE l_error_flag INT;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET l_error_flag=1; 
    INSERT INTO feods.task_log_test
      (task_name, 
       statedate,
       start_time,
	   end_time
       )
    VALUES
      (pi_task_name,
       pi_statedate,
       start_time,
	   end_time);
    COMMIT;
    END