CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_d_en_org_area`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  truncate table feods.`d_en_org_area`;
INSERT INTO feods.d_en_org_area(
sdate,
user_id,
open_id,
open_type,
mobile_phone,
emp_name,
org_name,
net_code,
cancel_flag,
cancel_date,
city,
provice,
org_type,
org_name_short,
area_code,
dept_code,
business_area,
region_area
) 
SELECT 
CURDATE() AS sdate,
so.user_id,
op.open_id,
op.open_type,
CONCAT(SUBSTRING(op.mobile_phone,1,3),
             SUBSTRING(op.mobile_phone,8,1),
                 IF(SUBSTRING(op.mobile_phone,5,1)='0','9',
                 IF(SUBSTRING(op.mobile_phone,5,1)='1','5',
                 IF(SUBSTRING(op.mobile_phone,5,1)='2','4',
                 IF(SUBSTRING(op.mobile_phone,5,1)='3','0',
                 IF(SUBSTRING(op.mobile_phone,5,1)='4','3',
                 IF(SUBSTRING(op.mobile_phone,5,1)='5','8',
                 IF(SUBSTRING(op.mobile_phone,5,1)='6','1',
                 IF(SUBSTRING(op.mobile_phone,5,1)='7','7',
                 IF(SUBSTRING(op.mobile_phone,5,1)='8','2','6'))))))))),
            SUBSTRING(op.mobile_phone,10,1),
  SUBSTRING(op.mobile_phone,7,1),
             SUBSTRING(op.mobile_phone,4,1),
                 IF(SUBSTRING(op.mobile_phone,9,1)='0','9',
                 IF(SUBSTRING(op.mobile_phone,9,1)='1','5',
                 IF(SUBSTRING(op.mobile_phone,9,1)='2','4',
                 IF(SUBSTRING(op.mobile_phone,9,1)='3','0',
                 IF(SUBSTRING(op.mobile_phone,9,1)='4','3',
                 IF(SUBSTRING(op.mobile_phone,9,1)='5','8',
                 IF(SUBSTRING(op.mobile_phone,9,1)='6','1',
                 IF(SUBSTRING(op.mobile_phone,9,1)='7','7',
                 IF(SUBSTRING(op.mobile_phone,9,1)='8','2','6'))))))))),
            SUBSTRING(op.mobile_phone,6,1),
  SUBSTRING(op.mobile_phone,11,1)) 电话,
-- op.mobile_phone,
-- b.emp_num,
b.last_name, 
b.zhrzzqc,
b.net_code,
b.cancel_flag AS 离职状态,
b.cancel_date AS 离职日期,
a.city,
a.provice,
a.org_type,
a.org_name,
a.area_code,
a.dept_code,
zb.business_area,
zb.region_area
FROM
 feods.d_en_order_user so
JOIN fe.pub_user_open op ON  op.user_id = so.user_id AND op.OPEN_TYPE IN ('XMF','SFIM')
JOIN feods.`d_dv_emp_org` b ON b.emp_num = op.open_id 
JOIN `feods`.`d_en_org_address_info` a ON  a.org_code = b.net_code 
JOIN fe.`zs_city_business` zb ON zb.city_name = a.city
-- where so.`sdate` >= (SELECT MAX(a.sdate) FROM feods.d_en_org_area AS a ) 
-- AND so.user_id NOT IN (SELECT b.user_id FROM feods.d_en_org_area AS b )
ON DUPLICATE KEY UPDATE sdate = CURDATE()
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'prc_d_en_org_area',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('黎尼和@', @user, @timestamp));
 COMMIT;
END