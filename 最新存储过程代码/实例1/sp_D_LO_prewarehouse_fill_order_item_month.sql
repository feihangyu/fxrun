CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_D_LO_prewarehouse_fill_order_item_month`()
BEGIN
-- =============================================
-- Author:	物流
-- Create date: 2019/08/13
-- Modify date: 
-- Description:	
-- 	补货出入库订单流向中间表（每天的1时17分）
-- 
-- =============================================
  SET @run_date := CURRENT_DATE();
  SET @user := CURRENT_USER();
  SET @timestamp := CURRENT_TIMESTAMP();
delete from feods.`D_LO_prewarehouse_fill_order_item_month`
where smonth= date_format(date_sub(current_date,interval 1 day),'%Y%m');
insert into feods.`D_LO_prewarehouse_fill_order_item_month` (
  smonth,
  order_id,
  SUPPLIER_ID,
  shelf_id,
  task_id,
  fill_type,
  FILL_TIME,
  PRODUCT_ID,
  QUALITY_NUM,
  PURCHASE_PRICE
)
      SELECT
        DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m') as smonth,
        a.order_id,   -- 订单
        a.supplier_id,   -- 供出方ID
        a.shelf_id,      -- 补入方ID     
        c.task_id,       -- 串点任务ID
        1 as fill_type,
        a.fill_time,
        b.product_id,    -- 补给的商品ID
        b.actual_send_num,   -- 补给商品数量
        e.purchase_price    -- 补给商品采购价
      FROM
        fe.`sf_product_fill_order` a
      LEFT JOIN
        fe.`sf_supplier` f
      ON a.supplier_id = f.supplier_id
      LEFT JOIN
        fe.sf_order_logistics_task_record c
      ON a.order_id = c.order_id
      AND c.data_flag=1
      LEFT JOIN
        fe.`sf_product_fill_order_item` b
      ON a.order_id = b.order_id
      AND b.data_flag=1
      LEFT JOIN
        fe.`sf_product` d
      ON b.product_id = d.product_id
      LEFT JOIN
        (SELECT DISTINCT
           m.fnumber,
           m.f_bgj_poprice AS purchase_price
        FROM
          sserp.`T_BD_MATERIAL` m) e
      ON d.product_code2 = e.fnumber
      where a.add_time >= date_add(date_sub(current_date,interval 1 day),interval -day(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 day)
      and a.add_time < current_date
      and a.data_flag=1
      AND a.supplier_type not in (9)
      and a.shelf_id in (select distinct k.shelf_id from fe.`sf_shelf` k where k.shelf_type=9 and k.data_flag=1)  -- 前置仓入库订单
      union
      SELECT
        DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m') AS smonth,
        a.order_id,
        a.supplier_id,
        a.shelf_id,
        c.task_id,
        2 AS fill_type,
        a.fill_time,
        b.product_id,
        b.actual_send_num,
        e.purchase_price
      FROM
        fe.`sf_product_fill_order` a
      LEFT JOIN
        fe.`sf_supplier` f
      ON a.supplier_id = f.supplier_id
      LEFT JOIN
        fe.sf_order_logistics_task_record c
      ON a.order_id = c.order_id
      AND c.data_flag=1
      LEFT JOIN
        fe.`sf_product_fill_order_item` b
      ON a.order_id = b.order_id
      AND b.data_flag=1
      LEFT JOIN
        fe.`sf_product` d
      ON b.product_id = d.product_id
      LEFT JOIN
        (SELECT DISTINCT
           m.fnumber,
           m.f_bgj_poprice AS purchase_price
        FROM
          sserp.`T_BD_MATERIAL` m) e
      ON d.product_code2 = e.fnumber
      WHERE a.add_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
      AND a.add_time < CURRENT_DATE
      AND a.data_flag=1
      AND a.supplier_type in (9)
      AND a.shelf_id NOT IN (SELECT DISTINCT k.shelf_id FROM fe.`sf_shelf` k WHERE k.shelf_type=9 AND k.data_flag=1)   -- 前置仓出库订单
      union
      SELECT
        DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),'%Y%m') AS smonth,
        a.order_id,
        a.supplier_id,
        a.shelf_id,
        c.task_id,
        3 AS fill_type,
        a.fill_time,
        b.product_id,
        b.actual_send_num,
        e.purchase_price
      FROM
        fe.`sf_product_fill_order` a
      LEFT JOIN
        fe.`sf_supplier` f
      ON a.supplier_id = f.supplier_id
      LEFT JOIN
        fe.sf_order_logistics_task_record c
      ON a.order_id = c.order_id
      AND c.data_flag=1
      LEFT JOIN
        fe.`sf_product_fill_order_item` b
      ON a.order_id = b.order_id
      AND b.data_flag=1
      LEFT JOIN
        fe.`sf_product` d
      ON b.product_id = d.product_id
      LEFT JOIN
        (SELECT DISTINCT
           m.fnumber,
           m.f_bgj_poprice AS purchase_price
        FROM
          sserp.`T_BD_MATERIAL` m) e
      ON d.product_code2 = e.fnumber
      WHERE a.add_time >= DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
      AND a.add_time < CURRENT_DATE
      AND a.data_flag=1
      AND a.supplier_type in (9)
      AND a.shelf_id IN (SELECT DISTINCT k.shelf_id FROM fe.`sf_shelf` k WHERE k.shelf_type=9 AND k.data_flag=1);    -- 前置站间调货订单
      
      
      -- 更新上月在途补货订单的上架时间
      update feods.`D_LO_prewarehouse_fill_order_item_month` a
      join fe.`sf_product_fill_order` b
      on a.order_id = b.order_id
      and b.add_time >= date_sub(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),interval 1 month)
      and b.add_time < DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY)
      set a.fill_time = b.fill_time
      where a.smonth = date_format(date_sub(DATE_ADD(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY),INTERVAL -DAY(DATE_SUB(CURRENT_DATE,INTERVAL 1 DAY))+1 DAY),interval 1 day),'%Y%m')
      and a.data_flag=1 and b.data_flag
      and a.fill_time is null
      and b.fill_time is not null;
--   执行记录日志
   CALL sh_process.`sp_sf_dw_task_log` (
    'sp_D_LO_prewarehouse_fill_order_item_month',
    DATE_FORMAT(@run_date, '%Y-%m-%d'),
    CONCAT('caisonglin@', @user, @timestamp)
  );
commit;   
END