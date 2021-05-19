CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_area_product_sale_flag`()
    SQL SECURITY INVOKER
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
  SET
    @sdate := CURRENT_DATE;
  SET
    @sdate0 := SUBDATE(@sdate, 0),
    @sdate1 := SUBDATE(@sdate, 1),
    @sdate2 := SUBDATE(@sdate, 2),
    @sdate3 := SUBDATE(@sdate, 3),
    @sdate4 := SUBDATE(@sdate, 4),
    @sdate5 := SUBDATE(@sdate, 5),
    @sdate6 := SUBDATE(@sdate, 6),
    @sdate7 := SUBDATE(@sdate, 7),
    @sdate8 := SUBDATE(@sdate, 8),
    @sdate9 := SUBDATE(@sdate, 9),
    @sdate10 := SUBDATE(@sdate, 10),
    @sdate11 := SUBDATE(@sdate, 11),
    @sdate12 := SUBDATE(@sdate, 12),
    @sdate13 := SUBDATE(@sdate, 13);
  SET
    @d0 := DAY(@sdate0),
    @d1 := DAY(@sdate1),
    @d2 := DAY(@sdate2),
    @d3 := DAY(@sdate3),
    @d4 := DAY(@sdate4),
    @d5 := DAY(@sdate5),
    @d6 := DAY(@sdate6),
    @d7 := DAY(@sdate7),
    @d8 := DAY(@sdate8),
    @d9 := DAY(@sdate9),
    @d10 := DAY(@sdate10),
    @d11 := DAY(@sdate11),
    @d12 := DAY(@sdate12),
    @d13 := DAY(@sdate13),
    @y_m0 := DATE_FORMAT(@sdate0, '%Y-%m'),
    @y_m1 := DATE_FORMAT(@sdate1, '%Y-%m'),
    @y_m2 := DATE_FORMAT(@sdate2, '%Y-%m'),
    @y_m3 := DATE_FORMAT(@sdate3, '%Y-%m'),
    @y_m4 := DATE_FORMAT(@sdate4, '%Y-%m'),
    @y_m5 := DATE_FORMAT(@sdate5, '%Y-%m'),
    @y_m6 := DATE_FORMAT(@sdate6, '%Y-%m'),
    @y_m7 := DATE_FORMAT(@sdate7, '%Y-%m'),
    @y_m8 := DATE_FORMAT(@sdate8, '%Y-%m'),
    @y_m9 := DATE_FORMAT(@sdate9, '%Y-%m'),
    @y_m10 := DATE_FORMAT(@sdate10, '%Y-%m'),
    @y_m11 := DATE_FORMAT(@sdate11, '%Y-%m'),
    @y_m12 := DATE_FORMAT(@sdate12, '%Y-%m'),
    @y_m13 := DATE_FORMAT(@sdate13, '%Y-%m');
  DELETE
  FROM
    feods.zs_area_product_sale_flag
  WHERE sdate = CURDATE();
   SET
    @str := CONCAT(
      " INSERT INTO feods.zs_area_product_sale_flag ( ",
      "   sdate, ",
      "   PRODUCT_ID, ",
      "   PRODUCT_NAME, ",
      "   business_area, ",
      "   ykc_shelf_qty, ",
      "   avg_qty, ",
      "   avg_qty_14, ",
      "   yxs_shelf_qty, ",
      "   yxshjzb, ",
      "   sale_level ",
      " ) ",
      " SELECT ",
      "   CURDATE() AS sdate, ",
      "   PRODUCT_ID AS '商品ID', ",
      "   PRODUCT_NAME AS '商品名称', ",
      "   city_name AS '城市名称', ",
      "   ykc_shelf_qty AS '有库存货架数', ",
      "   avg_qty, ",
      "   avg_qty * 14 AS '有库存货架数*14', ",
      "   yxs_shelf_qty AS '有销售货架数', ",
      "   yxshjzb AS '有销售货架占比', ",
      "   CASE ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb >= 0.75 ",
      "     AND avg_qty >= 0.49 ",
      "     THEN '热卖' ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb >= 0.75 ",
      "     AND avg_qty < 0.49 ",
      "     THEN '非常好卖' ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb >= 0.5 ",
      "     AND yxshjzb < 0.75 ",
      "     AND avg_qty >= 0.29 ",
      "     THEN '好卖' ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb >= 0.5 ",
      "     AND yxshjzb < 0.75 ",
      "     AND avg_qty < 0.29 ",
      "     THEN '一般' ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb >= 0.25 ",
      "     AND yxshjzb < 0.5 ",
      "     AND avg_qty >= 0.14 ",
      "     THEN '局部好卖' ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb >= 0.25 ",
      "     AND yxshjzb < 0.5 ",
      "     AND avg_qty < 0.14 ",
      "     THEN '非常不好卖' ",
      "     WHEN ykc_shelf_qty >= 50 ",
      "     AND yxshjzb < 0.25 ",
      "     THEN '难卖' ",
      "     else '' ",
      "   END AS '等级' ",
      " FROM ",
      "   (SELECT ",
      "     s.PRODUCT_ID, ",
      "     s.PRODUCT_NAME, ",
      "     t.business_area AS city_name, ",
      "     COUNT(DISTINCT s.shelf_id) AS ykc_shelf_qty, ",
      "     SUM(IFNULL(q.num, 0)) / SUM(s.days) AS avg_qty, ",
      "     SUM(IFNULL(q.num, 0)) / SUM(s.days) * 14 AS 'avg_qty*14', ",
      "     COUNT( ",
      "       DISTINCT ",
      "       CASE ",
      "         WHEN q.num > 0 ",
      "         THEN s.shelf_id ",
      "       END ",
      "     ) AS yxs_shelf_qty, ",
      "     COUNT( ",
      "       DISTINCT ",
      "       CASE ",
      "         WHEN q.num > 0 ",
      "         THEN s.shelf_id ",
      "       END ",
      "     ) / COUNT(DISTINCT s.shelf_id) AS yxshjzb ",
      "   FROM ",
      "     (SELECT ",
      "       t1.SHELF_ID, ",
      "       t1.PRODUCT_ID, ",
      "       t2.AREA_ADDRESS, ",
      "       t4.PRODUCT_NAME, ",
      "       SUM(t1.days) AS days ",
      "     FROM ",
	  "     feods.shelf_product_stock_14days t1 "
      "       LEFT JOIN fe.sf_shelf t2 ",
      "         ON t1.shelf_id = t2.shelf_id ",
      "       LEFT JOIN fe.sf_product t4 ",
      "         ON t1.product_id = t4.product_id ",
      "     GROUP BY t1.SHELF_ID, ",
      "       t1.PRODUCT_ID, ",
      "       t2.AREA_ADDRESS, ",
      "       t4.PRODUCT_NAME) s ",
      "     LEFT JOIN ",
 -- "       (SELECT ",
 -- "         b.SHELF_ID, ",
 -- "         a.PRODUCT_ID, ",
 -- "         SUM(a.QUANTITY) num ",
 -- "       FROM ",
 -- "         fe.sf_order_item AS a ",
 -- "         LEFT JOIN fe.sf_order AS b ",
 -- "           ON a.ORDER_ID = b.ORDER_ID ",
 -- "         LEFT JOIN fe.sf_shelf AS c ",
 -- "           ON b.SHELF_ID = c.SHELF_ID ",
 -- "       WHERE b.ORDER_STATUS = 2 ",
 -- "         AND b.ORDER_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) ",
 -- "         AND b.ORDER_DATE < CURRENT_DATE() ",
 -- "       GROUP BY b.SHELF_ID, ",
 -- "         a.PRODUCT_ID) q ",
  "       (SELECT ",
  "         a.SHELF_ID, ",
  "         a.PRODUCT_ID, ",
  "         SUM(a.QUANTITY) num ",
  "       FROM ",  
  "         fe_dwd.dwd_pub_order_item_recent_one_month AS a ",
  "         where a.pay_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) ",
  "         AND a.pay_DATE < CURRENT_DATE() ",
  "       GROUP BY a.SHELF_ID, ",
  "         a.PRODUCT_ID) q ",
  "       ON s.SHELF_ID = q.SHELF_ID ",
  "       AND s.PRODUCT_ID = q.PRODUCT_ID ",
  "     LEFT JOIN fe.zs_city_business t ",
  "       ON SUBSTRING_INDEX( ",
  "         SUBSTRING_INDEX(s.AREA_ADDRESS, ',', 2), ",
  "         ',', ",
  "         - 1 ",
  "       ) = t.city_name ",
  "   GROUP BY s.PRODUCT_ID, ",
  "     s.PRODUCT_NAME, ",
  "     t.business_area) t1; "
) ;
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  
  
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` (
'sh_area_product_sale_flag',
DATE_FORMAT(@run_date, '%Y-%m-%d'),
 CONCAT('未知@', @user, @timestamp));
 
  COMMIT;
END