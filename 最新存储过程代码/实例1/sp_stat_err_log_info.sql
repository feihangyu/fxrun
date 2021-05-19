CREATE DEFINER=`wuting`@`%` PROCEDURE `sp_stat_err_log_info`(in pi_procname varchar(50),in pi_loginfo varchar(8000))
BEGIN
    DECLARE l_error_flag INT;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET l_error_flag=1; 
    INSERT INTO feods.sf_dw_stat_err_log
      (logname, 
       loginfo,
       orderloginfo
       )
    VALUES
      (pi_procname,
       pi_loginfo,
       pi_loginfo);
    COMMIT;
    END