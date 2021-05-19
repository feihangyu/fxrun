CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_activity_invitation_information`()
BEGIN 
	SET @run_date := CURRENT_DATE();
    SET @user := CURRENT_USER();
    SET @timestamp := CURRENT_TIMESTAMP();
   SET @end_date = CURDATE();   
   SET @w := WEEKDAY(CURDATE());
   SET @week_flag := (@w = 6);
   SET @start_date = SUBDATE(@end_date,INTERVAL 1 DAY);
   
delete from fe_dwd.`dwd_activity_invitation_information` where add_time>= @start_date;
INSERT INTO fe_dwd.`dwd_activity_invitation_information`
(
`invite_id`
,`activity_id`
,`inviter_user_id`
,`invite_type`
,`invite_status`
,`prize_record_id`
,`is_over`
,`invite_count`
,`prize_type`
,`reward`
,`remark`
,`invitee_user_id`
,`invitee_invite_status`
,`invitee_prize_record_id`
,`invitee_prize_type`
,`rinvitee_eward`
,`invitee_remark`
,add_time
,add_time_detail
)
SELECT 
a.`invite_id`
,a.`activity_id`
,a.`inviter_user_id`
,a.`invite_type`
,a.`invite_status`
,a.`prize_record_id`
,a.`is_over`
,a.`invite_count`
,a.`prize_type`
,a.`reward`
,a.`remark`
,b.`invitee_user_id`
,b.invite_status AS `invitee_invite_status`
,b.prize_record_id AS`invitee_prize_record_id`
,b.prize_type AS `invitee_prize_type`
,b.reward AS `rinvitee_eward`
,b.remark AS `invitee_remark`
,a.add_time
,b.add_time add_time_detail
FROM
fe_activity.sf_activity_invitation a
left JOIN fe_activity.sf_activity_invitation_detail b 
ON a.invite_id = b.invite_id 
AND a.data_flag = 1 
AND b.data_flag =1
where a.add_time>= @start_date
and a.add_time <@end_date ;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_activity_invitation_information',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('lishilong@', @user, @timestamp)
  );
  COMMIT;
END