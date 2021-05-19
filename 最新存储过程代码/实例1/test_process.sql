CREATE DEFINER=`feprocess`@`%` PROCEDURE `test_process`(out now_time datetime)
begin
set now_time:= subdate(now(),interval 1 day);
-- select now_time;
-- select now() into now_time;
end