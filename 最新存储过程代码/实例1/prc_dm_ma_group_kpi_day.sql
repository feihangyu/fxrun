CREATE DEFINER=`feprocess`@`%` PROCEDURE `prc_dm_ma_group_kpi_day`(IN p_sdate DATE)
BEGIN
SET @run_date:= CURRENT_DATE();
SET @user := CURRENT_USER();
SET @timestamp := CURRENT_TIMESTAMP();
DELETE FROM fe_dm.dm_ma_group_kpi_day WHERE (sdate>=SUBDATE(p_sdate,9) AND sdate<ADDDATE(p_sdate,1)) OR sdate<SUBDATE(p_sdate,100)  ;
INSERT INTO fe_dm.dm_ma_group_kpi_day
    (sdate, group_link_id, gmv, amount, order_discount_amount, coupon_total_amount, quantity, refund_amount, cost, order_num, user_num,refund_order)
SELECT sdate,group_link_id,SUM(gmv) gmv,SUM(amount) amount,SUM(order_discount_amount) order_discount_amount,SUM(coupon_total_amount) coupon_total_amount,SUM(quantity) quantity
    ,IFNULL(SUM(refund_amount),0) refund_amount,SUM(cost) cost
    ,SUM(if(gmv>0,1,0)) order_num,COUNT(DISTINCT if(gmv>0,order_user_id,null)) user_num,sum(if(refund_amount>0,1,0)) refund_order
FROM
    (SELECT DATE(a1.pay_time) sdate,a2.group_link_id
        ,a1.order_id ,a2.order_user_id,a2.order_discount_amount,a2.coupon_total_amount
        ,SUM(a3.origin_sale_unit_price*(a3.quantity-IFNULL(a4.refund_quantity,0)))+a2.freight_amount gmv
        ,SUM(a3.sale_unit_price*(a3.quantity-ifnull(a4.refund_quantity,0)))+a2.freight_amount amount
        ,SUM(a3.quantity-ifnull(a4.refund_quantity,0)) quantity
        ,SUM(ifnull(a4.refund_quantity,0)*a3.sale_unit_price ) refund_amount
        ,SUM((a3.quantity-ifnull(a4.refund_quantity,0))*a3.purchase_unit_price )+a2.freight_amount cost
    FROM  fe_goods.sf_group_order_pay  a1
    JOIN fe_goods.sf_group_order a2 ON a1.order_id=a2.order_id AND a2.data_flag=1
    JOIN fe_goods.sf_group_order_item a3  ON a1.order_id=a3.order_id  AND a3.data_flag = 1
    left join
        (select b2.order_item_id ,sum(b2.quantity) refund_quantity
        from fe_goods.sf_group_order_refund b1 join fe_goods.sf_group_order_refund_item b2 on b1.refund_order_id=b2.refund_order_id and b1.data_flag=1
        where b1.add_time>=SUBDATE(p_sdate,9) and b1.add_time<ADDDATE(p_sdate,7) and b1.data_flag=1 and b1.refund_status=105 group by b2.order_item_id
        ) a4 on a3.order_item_id=a4.order_item_id
    WHERE a1.pay_time>=SUBDATE(p_sdate,9) AND a1.pay_time<ADDDATE(p_sdate,1) AND a1.pay_time>='2019-11-15' AND a1.pay_state=2 AND a1.data_flag = 1
        AND a2.order_type= 10
    GROUP BY a1.order_id
    ) t1
GROUP BY sdate,group_link_id
;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log`(
  'prc_dm_ma_group_kpi_day',
  DATE_FORMAT(@run_date,'%Y-%m-%d'),
  CONCAT('纪伟铨@',@user,@timestamp)
);
END