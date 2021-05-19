CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_ma_shelf_user_stat`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate= SUBDATE(CURDATE(),1); set @sweekend=adddate(@sdate,if(dayofweek(@sdate)=1,0,8-dayofweek(@sdate))+0);set @smonthend=last_day(@sdate);
    #激活时间100天内的用户累计数据
drop temporary table if exists  feods.temp_shelf_user_duration;
create temporary table  feods.temp_shelf_user_duration(index(SHELF_ID)) as
SELECT SHELF_ID
	,SUM(IF(min_order_date = a1.activate_time,1,0)) user_num_day1
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,1)	and ADDDATE(a1.activate_time,1) <=@sdate,1,0))   user_num_day2
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,2)	and ADDDATE(a1.activate_time,2) <=@sdate,1,0))   user_num_day3
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,3)	and ADDDATE(a1.activate_time,3) <=@sdate,1,0))   user_num_day4
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,4)	and ADDDATE(a1.activate_time,4) <=@sdate,1,0))   user_num_day5
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,5)	and ADDDATE(a1.activate_time,5) <=@sdate,1,0))   user_num_day6
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,6)	and ADDDATE(a1.activate_time,6) <=@sdate,1,0))   user_num_day7
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,7)	and ADDDATE(a1.activate_time,7) <=@sdate,1,0))   user_num_day8
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,8)	and ADDDATE(a1.activate_time,8) <=@sdate,1,0))   user_num_day9
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,9)	and ADDDATE(a1.activate_time,9) <=@sdate,1,0))   user_num_day10
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,10) and ADDDATE(a1.activate_time,10)<=@sdate,1,0))  user_num_day11
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,11) and ADDDATE(a1.activate_time,11)<=@sdate,1,0))  user_num_day12
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,12) and ADDDATE(a1.activate_time,12)<=@sdate,1,0))  user_num_day13
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,13) and ADDDATE(a1.activate_time,13)<=@sdate,1,0))  user_num_day14
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,14) and ADDDATE(a1.activate_time,14)<=@sdate,1,0))  user_num_day15
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,15) and ADDDATE(a1.activate_time,15)<=@sdate,1,0))  user_num_day16
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,16) and ADDDATE(a1.activate_time,16)<=@sdate,1,0))  user_num_day17
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,17) and ADDDATE(a1.activate_time,17)<=@sdate,1,0))  user_num_day18
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,18) and ADDDATE(a1.activate_time,18)<=@sdate,1,0))  user_num_day19
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,19) and ADDDATE(a1.activate_time,19)<=@sdate,1,0))  user_num_day20
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,20) and ADDDATE(a1.activate_time,20)<=@sdate,1,0))  user_num_day21
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,21) and ADDDATE(a1.activate_time,21)<=@sdate,1,0))  user_num_day22
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,22) and ADDDATE(a1.activate_time,22)<=@sdate,1,0))  user_num_day23
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,23) and ADDDATE(a1.activate_time,23)<=@sdate,1,0))  user_num_day24
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,24) and ADDDATE(a1.activate_time,24)<=@sdate,1,0))  user_num_day25
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,25) and ADDDATE(a1.activate_time,25)<=@sdate,1,0))  user_num_day26
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,26) and ADDDATE(a1.activate_time,26)<=@sdate,1,0))  user_num_day27
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,27) and ADDDATE(a1.activate_time,27)<=@sdate,1,0))  user_num_day28
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,28) and ADDDATE(a1.activate_time,28)<=@sdate,1,0))  user_num_day29
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,29) and ADDDATE(a1.activate_time,29)<=@sdate,1,0))  user_num_day30
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,30) and ADDDATE(a1.activate_time,30)<=@sdate,1,0))  user_num_day31
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,31) and ADDDATE(a1.activate_time,31)<=@sdate,1,0))  user_num_day32
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,32) and ADDDATE(a1.activate_time,32)<=@sdate,1,0))  user_num_day33
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,33) and ADDDATE(a1.activate_time,33)<=@sdate,1,0))  user_num_day34
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,34) and ADDDATE(a1.activate_time,34)<=@sdate,1,0))  user_num_day35
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,35) and ADDDATE(a1.activate_time,35)<=@sdate,1,0))  user_num_day36
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,36) and ADDDATE(a1.activate_time,36)<=@sdate,1,0))  user_num_day37
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,37) and ADDDATE(a1.activate_time,37)<=@sdate,1,0))  user_num_day38
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,38) and ADDDATE(a1.activate_time,38)<=@sdate,1,0))  user_num_day39
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,39) and ADDDATE(a1.activate_time,39)<=@sdate,1,0))  user_num_day40
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,40) and ADDDATE(a1.activate_time,40)<=@sdate,1,0))  user_num_day41
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,41) and ADDDATE(a1.activate_time,41)<=@sdate,1,0))  user_num_day42
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,42) and ADDDATE(a1.activate_time,42)<=@sdate,1,0))  user_num_day43
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,43) and ADDDATE(a1.activate_time,43)<=@sdate,1,0))  user_num_day44
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,44) and ADDDATE(a1.activate_time,44)<=@sdate,1,0))  user_num_day45
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,45) and ADDDATE(a1.activate_time,45)<=@sdate,1,0))  user_num_day46
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,46) and ADDDATE(a1.activate_time,46)<=@sdate,1,0))  user_num_day47
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,47) and ADDDATE(a1.activate_time,47)<=@sdate,1,0))  user_num_day48
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,48) and ADDDATE(a1.activate_time,48)<=@sdate,1,0))  user_num_day49
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,49) and ADDDATE(a1.activate_time,49)<=@sdate,1,0))  user_num_day50
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,50) and ADDDATE(a1.activate_time,50)<=@sdate,1,0))  user_num_day51
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,51) and ADDDATE(a1.activate_time,51)<=@sdate,1,0))  user_num_day52
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,52) and ADDDATE(a1.activate_time,52)<=@sdate,1,0))  user_num_day53
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,53) and ADDDATE(a1.activate_time,53)<=@sdate,1,0))  user_num_day54
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,54) and ADDDATE(a1.activate_time,54)<=@sdate,1,0))  user_num_day55
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,55) and ADDDATE(a1.activate_time,55)<=@sdate,1,0))  user_num_day56
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,56) and ADDDATE(a1.activate_time,56)<=@sdate,1,0))  user_num_day57
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,57) and ADDDATE(a1.activate_time,57)<=@sdate,1,0))  user_num_day58
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,58) and ADDDATE(a1.activate_time,58)<=@sdate,1,0))  user_num_day59
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,59) and ADDDATE(a1.activate_time,59)<=@sdate,1,0))  user_num_day60
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,60) and ADDDATE(a1.activate_time,60)<=@sdate,1,0))  user_num_day61
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,61) and ADDDATE(a1.activate_time,61)<=@sdate,1,0))  user_num_day62
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,62) and ADDDATE(a1.activate_time,62)<=@sdate,1,0))  user_num_day63
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,63) and ADDDATE(a1.activate_time,63)<=@sdate,1,0))  user_num_day64
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,64) and ADDDATE(a1.activate_time,64)<=@sdate,1,0))  user_num_day65
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,65) and ADDDATE(a1.activate_time,65)<=@sdate,1,0))  user_num_day66
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,66) and ADDDATE(a1.activate_time,66)<=@sdate,1,0))  user_num_day67
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,67) and ADDDATE(a1.activate_time,67)<=@sdate,1,0))  user_num_day68
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,68) and ADDDATE(a1.activate_time,68)<=@sdate,1,0))  user_num_day69
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,69) and ADDDATE(a1.activate_time,69)<=@sdate,1,0))  user_num_day70
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,70) and ADDDATE(a1.activate_time,70)<=@sdate,1,0))  user_num_day71
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,71) and ADDDATE(a1.activate_time,71)<=@sdate,1,0))  user_num_day72
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,72) and ADDDATE(a1.activate_time,72)<=@sdate,1,0))  user_num_day73
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,73) and ADDDATE(a1.activate_time,73)<=@sdate,1,0))  user_num_day74
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,74) and ADDDATE(a1.activate_time,74)<=@sdate,1,0))  user_num_day75
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,75) and ADDDATE(a1.activate_time,75)<=@sdate,1,0))  user_num_day76
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,76) and ADDDATE(a1.activate_time,76)<=@sdate,1,0))  user_num_day77
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,77) and ADDDATE(a1.activate_time,77)<=@sdate,1,0))  user_num_day78
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,78) and ADDDATE(a1.activate_time,78)<=@sdate,1,0))  user_num_day79
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,79) and ADDDATE(a1.activate_time,79)<=@sdate,1,0))  user_num_day80
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,80) and ADDDATE(a1.activate_time,80)<=@sdate,1,0))  user_num_day81
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,81) and ADDDATE(a1.activate_time,81)<=@sdate,1,0))  user_num_day82
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,82) and ADDDATE(a1.activate_time,82)<=@sdate,1,0))  user_num_day83
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,83) and ADDDATE(a1.activate_time,83)<=@sdate,1,0))  user_num_day84
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,84) and ADDDATE(a1.activate_time,84)<=@sdate,1,0))  user_num_day85
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,85) and ADDDATE(a1.activate_time,85)<=@sdate,1,0))  user_num_day86
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,86) and ADDDATE(a1.activate_time,86)<=@sdate,1,0))  user_num_day87
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,87) and ADDDATE(a1.activate_time,87)<=@sdate,1,0))  user_num_day88
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,88) and ADDDATE(a1.activate_time,88)<=@sdate,1,0))  user_num_day89
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,89) and ADDDATE(a1.activate_time,89)<=@sdate,1,0))  user_num_day90
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,90) and ADDDATE(a1.activate_time,90)<=@sdate,1,0))  user_num_day91
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,91) and ADDDATE(a1.activate_time,91)<=@sdate,1,0))  user_num_day92
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,92) and ADDDATE(a1.activate_time,92)<=@sdate,1,0))  user_num_day93
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,93) and ADDDATE(a1.activate_time,93)<=@sdate,1,0))  user_num_day94
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,94) and ADDDATE(a1.activate_time,94)<=@sdate,1,0))  user_num_day95
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,95) and ADDDATE(a1.activate_time,95)<=@sdate,1,0))  user_num_day96
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,96) and ADDDATE(a1.activate_time,96)<=@sdate,1,0))  user_num_day97
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,97) and ADDDATE(a1.activate_time,97)<=@sdate,1,0))  user_num_day98
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,98) and ADDDATE(a1.activate_time,98)<=@sdate,1,0))  user_num_day99
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,99) and ADDDATE(a1.activate_time,99)<=@sdate,1,0))  user_num_day100

    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,8-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,8-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week1
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,15-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,15-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week2
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,22-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,22-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week3
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,29-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,29-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week4
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,36-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,36-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week5
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,43-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,43-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week6
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,50-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,50-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week7
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,57-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,57-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week8
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,64-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,64-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week9
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,71-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,71-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week10
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,78-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,78-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week11
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,85-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,85-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week12
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,92-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,92-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week13
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND ADDDATE(a1.activate_time,99-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time))) and ADDDATE(a1.activate_time,99-IF(DAYOFWEEK(a1.activate_time)=1,8,DAYOFWEEK(a1.activate_time)))<=@sweekend ,1,0)) user_num_week14

    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND LAST_DAY(a1.activate_time) and LAST_DAY(a1.activate_time)<=@smonthend,1,0)) user_num_month1
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND LAST_DAY(DATE_ADD(a1.activate_time,INTERVAL 1 MONTH)) AND LAST_DAY(DATE_ADD(a1.activate_time,INTERVAL 1 MONTH))<=@smonthend,1,0)) user_num_month2
    ,SUM(IF(min_order_date BETWEEN a1.activate_time AND LAST_DAY(DATE_ADD(a1.activate_time,INTERVAL 2 MONTH))  AND LAST_DAY(DATE_ADD(a1.activate_time,INTERVAL 2 MONTH))<=@smonthend,1,0)) user_num_month3
FROM
    (SELECT a1.shelf_id
        ,DATE(ACTIVATE_TIME) ACTIVATE_TIME,DATE(REVOKE_TIME) REVOKE_TIME,date(min_order_date) min_order_date
    FROM fe_dwd.dwd_shelf_base_day_all a1
    JOIN feods.d_op_su_stat a2 ON a1.SHELF_ID=a2.shelf_id
    WHERE a1.SHELF_STATUS NOT IN (10,1) AND a1.SHELF_TYPE NOT IN (9)
      AND DATEDIFF(@sdate,DATE(ACTIVATE_TIME))<100
    ) a1
GROUP BY SHELF_ID;
    #激活时间100天内的30天终端用户数
drop temporary table if exists feods.temp_shelf_user_day;
create temporary table  feods.temp_shelf_user_day(index(SHELF_ID)) as
    select a1.shelf_id
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),0),a3.users,0))   users_day1
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),1),a3.users,0))   users_day2
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),2),a3.users,0))   users_day3
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),3),a3.users,0))   users_day4
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),4),a3.users,0))   users_day5
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),5),a3.users,0))   users_day6
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),6),a3.users,0))   users_day7
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),7),a3.users,0))   users_day8
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),8),a3.users,0))   users_day9
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),9),a3.users,0))   users_day10
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),10),a3.users,0))  users_day11
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),11),a3.users,0))  users_day12
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),12),a3.users,0))  users_day13
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),13),a3.users,0))  users_day14
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),14),a3.users,0))  users_day15
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),15),a3.users,0))  users_day16
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),16),a3.users,0))  users_day17
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),17),a3.users,0))  users_day18
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),18),a3.users,0))  users_day19
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),19),a3.users,0))  users_day20
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),20),a3.users,0))  users_day21
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),21),a3.users,0))  users_day22
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),22),a3.users,0))  users_day23
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),23),a3.users,0))  users_day24
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),24),a3.users,0))  users_day25
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),25),a3.users,0))  users_day26
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),26),a3.users,0))  users_day27
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),27),a3.users,0))  users_day28
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),28),a3.users,0))  users_day29
    ,sum(if(a3.sdate=adddate(date(a1.ACTIVATE_TIME),29),a3.users,0))  users_day30
from fe_dwd.dwd_shelf_base_day_all a1
join fe_dwd.dwd_pub_work_day a2 on a2.sdate>=date(a1.ACTIVATE_TIME) and a2.sdate<=ifnull(date(REVOKE_TIME),curdate())
   and  a2.sdate between date(a1.ACTIVATE_TIME) and adddate(date(a1.ACTIVATE_TIME),29)
join feods.fjr_shelf_dgmv a3 on a3.sdate=a2.sdate and a3.shelf_id=a1.shelf_id
where  SHELF_STATUS  not in (1,10)  AND  a1.shelf_type NOT IN (9)
      AND DATEDIFF(@sdate,DATE(ACTIVATE_TIME))<100
group by a1.shelf_id
;   # 插入数据
replace into feods.d_ma_shelf_user_stat
    ( shelf_id
     , user_num_day1, user_num_day2, user_num_day3, user_num_day4, user_num_day5, user_num_day6, user_num_day7, user_num_day8, user_num_day9, user_num_day10
     , user_num_day11, user_num_day12, user_num_day13, user_num_day14, user_num_day15, user_num_day16, user_num_day17, user_num_day18, user_num_day19, user_num_day20
     , user_num_day21, user_num_day22, user_num_day23, user_num_day24, user_num_day25, user_num_day26, user_num_day27, user_num_day28, user_num_day29, user_num_day30
     , user_num_day31, user_num_day32, user_num_day33, user_num_day34, user_num_day35, user_num_day36, user_num_day37, user_num_day38, user_num_day39, user_num_day40
     , user_num_day41, user_num_day42, user_num_day43, user_num_day44, user_num_day45, user_num_day46, user_num_day47, user_num_day48, user_num_day49, user_num_day50,
       user_num_day51, user_num_day52, user_num_day53, user_num_day54, user_num_day55, user_num_day56, user_num_day57, user_num_day58, user_num_day59, user_num_day60
     , user_num_day61, user_num_day62, user_num_day63, user_num_day64, user_num_day65, user_num_day66, user_num_day67, user_num_day68, user_num_day69, user_num_day70
     , user_num_day71, user_num_day72, user_num_day73, user_num_day74, user_num_day75, user_num_day76, user_num_day77, user_num_day78, user_num_day79, user_num_day80
     , user_num_day81, user_num_day82, user_num_day83, user_num_day84, user_num_day85, user_num_day86, user_num_day87, user_num_day88, user_num_day89, user_num_day90
     , user_num_day91, user_num_day92, user_num_day93, user_num_day94, user_num_day95, user_num_day96, user_num_day97, user_num_day98, user_num_day99, user_num_day100

     , users_day1, users_day2, users_day3, users_day4, users_day5, users_day6, users_day7, users_day8, users_day9, users_day10, users_day11, users_day12, users_day13
     , users_day14, users_day15, users_day16, users_day17, users_day18, users_day19, users_day20, users_day21, users_day22, users_day23, users_day24, users_day25
     , users_day26, users_day27, users_day28, users_day29, users_day30

     , user_num_week1, user_num_week2, user_num_week3, user_num_week4, user_num_week5, user_num_week6, user_num_week7, user_num_week8, user_num_week9, user_num_week10
     , user_num_week11, user_num_week12, user_num_week13
     , user_num_week14, user_num_month1, user_num_month2, user_num_month3)
select a1.shelf_id
     , user_num_day1, user_num_day2, user_num_day3, user_num_day4, user_num_day5, user_num_day6, user_num_day7, user_num_day8, user_num_day9, user_num_day10
     , user_num_day11, user_num_day12, user_num_day13, user_num_day14, user_num_day15, user_num_day16, user_num_day17, user_num_day18, user_num_day19, user_num_day20
     , user_num_day21, user_num_day22, user_num_day23, user_num_day24, user_num_day25, user_num_day26, user_num_day27, user_num_day28, user_num_day29, user_num_day30
     , user_num_day31, user_num_day32, user_num_day33, user_num_day34, user_num_day35, user_num_day36, user_num_day37, user_num_day38, user_num_day39, user_num_day40
     , user_num_day41, user_num_day42, user_num_day43, user_num_day44, user_num_day45, user_num_day46, user_num_day47, user_num_day48, user_num_day49, user_num_day50,
       user_num_day51, user_num_day52, user_num_day53, user_num_day54, user_num_day55, user_num_day56, user_num_day57, user_num_day58, user_num_day59, user_num_day60
     , user_num_day61, user_num_day62, user_num_day63, user_num_day64, user_num_day65, user_num_day66, user_num_day67, user_num_day68, user_num_day69, user_num_day70
     , user_num_day71, user_num_day72, user_num_day73, user_num_day74, user_num_day75, user_num_day76, user_num_day77, user_num_day78, user_num_day79, user_num_day80
     , user_num_day81, user_num_day82, user_num_day83, user_num_day84, user_num_day85, user_num_day86, user_num_day87, user_num_day88, user_num_day89, user_num_day90
     , user_num_day91, user_num_day92, user_num_day93, user_num_day94, user_num_day95, user_num_day96, user_num_day97, user_num_day98, user_num_day99, user_num_day100

     , a2.users_day1, a2.users_day2, a2.users_day3, a2.users_day4, a2.users_day5, a2.users_day6, a2.users_day7, a2.users_day8, a2.users_day9, a2.users_day10, a2.users_day11, a2.users_day12, a2.users_day13
     , a2.users_day14, a2.users_day15, a2.users_day16, a2.users_day17, a2.users_day18, a2.users_day19, a2.users_day20, a2.users_day21, a2.users_day22, a2.users_day23, a2.users_day24, a2.users_day25
     , a2.users_day26, a2.users_day27, a2.users_day28, a2.users_day29, a2.users_day30

     , user_num_week1, user_num_week2, user_num_week3, user_num_week4, user_num_week5, user_num_week6, user_num_week7, user_num_week8, user_num_week9, user_num_week10
     , user_num_week11, user_num_week12, user_num_week13
     , user_num_week14, user_num_month1, user_num_month2, user_num_month3
from feods.temp_shelf_user_duration a1
left join feods.temp_shelf_user_day a2 on a2.shelf_id=a1.shelf_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('prc_d_ma_shelf_user_stat',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
END