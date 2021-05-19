CREATE DEFINER=`feprocess`@`%` PROCEDURE `dwd_order_refund_item`()
BEGIN
SET @run_date:= CURRENT_DATE(), @user := CURRENT_USER(), @timestamp :=  CURRENT_TIMESTAMP();
 
 -- 为了防止有异常发生，先测试是否跑通。跑通了就删除重跑。没有跑通就报错停止执行，保留前一天的数据
DROP TEMPORARY TABLE IF EXISTS fe_dwd.dwd_order_refund_item_test;
CREATE TEMPORARY TABLE fe_dwd.dwd_order_refund_item_test LIKE fe_dwd.dwd_order_refund_item;
 insert into fe_dwd.`dwd_order_refund_item_test`
 (
`order_id`,
`order_item_id`,
`shelf_id`,
`refund_item_id`,
`refund_order_id`,
refund_status,
 refund_finish_time,
apply_time,
`refund_amount`,
`quantity`
 )
SELECT
b.`order_id`,
b.`order_item_id`,
b.`shelf_id`,
b.`refund_item_id`,
b.`refund_order_id`,
c.refund_status,
c.finish_time AS refund_finish_time,
c.apply_time,
b.`refund_amount`,
b.`quantity`
FROM
fe.sf_order_refund_order c 
JOIN fe.sf_order_refund_item b
ON  c.`refund_order_id`= b.`refund_order_id`
AND c.order_id = b.order_id
WHERE b.data_flag = 1
AND c.data_flag = 1
AND c.refund_status IN (1,2,3,4,5)  -- 6退款失败，需管理员再次发起退款，7驳回申请,待用户处理，8超时关闭，0取消  这种剔除掉
;
 truncate table fe_dwd.`dwd_order_refund_item`;
 insert into fe_dwd.`dwd_order_refund_item`
 (
`order_id`,
`order_item_id`,
`shelf_id`,
`refund_item_id`,
`refund_order_id`,
refund_status,
 refund_finish_time,
apply_time,
`refund_amount`,
`quantity`
 )
SELECT
b.`order_id`,
b.`order_item_id`,
b.`shelf_id`,
b.`refund_item_id`,
b.`refund_order_id`,
c.refund_status,
c.finish_time AS refund_finish_time,
c.apply_time,
b.`refund_amount`,
b.`quantity`
FROM
fe.sf_order_refund_order c 
JOIN fe.sf_order_refund_item b
ON  c.`refund_order_id`= b.`refund_order_id`
AND c.order_id = b.order_id
WHERE b.data_flag = 1
AND c.data_flag = 1
and c.refund_status in (1,2,3,4,5)    -- 6退款失败，需管理员再次发起退款，7驳回申请,待用户处理，8超时关闭，0取消  这种剔除掉
;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'dwd_order_refund_item',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('李世龙@', @user, @timestamp)
  );
  
COMMIT;	
END