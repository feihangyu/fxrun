CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_task_log`(
  IN pi_task_name VARCHAR (256),
  IN pi_statedate VARCHAR (64),
  IN pi_loginfo VARCHAR (8000)
)
BEGIN
  DECLARE l_error_flag INT;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET l_error_flag = 1;
  INSERT INTO feods.sf_dw_task_log (
    task_name,
    statedate,
    loginfo,
    remark
  )
  VALUES
    (
      pi_task_name,
      pi_statedate,
      pi_loginfo,
      NULL
    );
  COMMIT;
END