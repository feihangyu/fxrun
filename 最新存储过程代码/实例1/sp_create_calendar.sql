CREATE DEFINER=`liuyi`@`%` PROCEDURE `sp_create_calendar`(s_date DATE, e_date DATE)
BEGIN
 
	WHILE s_date <= e_date DO
		INSERT IGNORE INTO feods.ly_calendar VALUES (DATE(s_date)) ;
		SET s_date = s_date + INTERVAL 1 DAY ;
	END WHILE ; 
 
END