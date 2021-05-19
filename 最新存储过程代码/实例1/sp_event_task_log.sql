CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_event_task_log`(
  IN pi_task_name VARCHAR (64),
  IN pi_statedate VARCHAR (32),
  IN pi_status INT
)
BEGIN
  DECLARE l_error_flag INT;
  DECLARE l_row_cnt INT;
  DECLARE CODE CHAR(5) DEFAULT '00000';
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1 CODE = RETURNED_SQLSTATE,
    @x2 = MESSAGE_TEXT;
    CALL sh_process.sp_stat_err_log_info ('sp_event_task_log', @x2);
  END;
  SELECT
    COUNT(1) INTO l_row_cnt
  FROM
    feods.sf_dw_event_task_log t
  WHERE statedate = pi_statedate
    AND task_name = pi_task_name;
  IF l_row_cnt > 0
  THEN
  UPDATE
    feods.sf_dw_event_task_log t
  SET
    t.status = pi_status,
    t.moditytime = NOW()
  WHERE statedate = pi_statedate
    AND task_name = pi_task_name;
  ELSE
  INSERT INTO feods.sf_dw_event_task_log (
    statedate,
    task_name,
    STATUS,
    remark,
    createtime,
    moditytime
  )
  VALUES
    (
      IFNULL(pi_statedate, '0'),
      IFNULL(pi_task_name, '0'),
      pi_status,
      NULL,
      NOW(),
      NOW()
    );
  END IF;
  COMMIT;
END