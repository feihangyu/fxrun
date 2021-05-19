CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_price_sensitive_week`(in_week_end DATE)
BEGIN
  SET @week_end := SUBDATE(
    in_week_end, DAYOFWEEK(in_week_end) - 1
  ), @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  SET @week_start := SUBDATE(@week_end, 6), @add_date := ADDDATE(@week_end, 1);
  DROP TEMPORARY TABLE IF EXISTS feods.oi_last_week_tmp;
  CREATE TEMPORARY TABLE feods.oi_last_week_tmp AS
  SELECT
    b.business_name, o.order_id, DATE(o.order_date) order_date, o.user_id, o.shelf_id, oi.product_id, oi.quantity, oi.sale_price, IFNULL(
      oi.purchase_price, oi.cost_price
    ) purchase_price, IFNULL(o.product_total_amount, 0) + IFNULL(o.discount_amount, 0) + IFNULL(o.coupon_amount, 0) ogmv, ROUND(
      (
        IFNULL(o.discount_amount, 0) + IFNULL(o.coupon_amount, 0)
      ) / (
        IFNULL(o.product_total_amount, 0) + IFNULL(o.discount_amount, 0) + IFNULL(o.coupon_amount, 0)
      ), 6
    ) order_discount_rate
  FROM
    fe.sf_order o
    JOIN fe.sf_order_item oi
      ON o.order_id = oi.order_id
    JOIN fe.sf_shelf s
      ON o.shelf_id = s.shelf_id
      AND s.data_flag = 1
    JOIN feods.fjr_city_business b
      ON b.city = s.city
  WHERE o.order_status = 2
    AND o.order_date >= @week_start
    AND o.order_date < @add_date;
  CREATE INDEX idx_oi_last_week_tmp_business_name_user_id
  ON feods.oi_last_week_tmp (business_name, user_id);
  DELETE
  FROM
    feods.fjr_user_miser_stat
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_user_miser_stat (
    week_end, business_name, user_id, orders, shelfs, products, days, salqty, gmv, total_discount, orders_nmiser, products_nmiser, salqty_nmiser, gmv_nmiser, add_user
  )
  SELECT
    @week_end, t.business_name, t.user_id, COUNT(DISTINCT t.order_id) orders, COUNT(DISTINCT t.shelf_id) shelfs, COUNT(DISTINCT t.product_id) products, COUNT(DISTINCT t.order_date) days, IFNULL(SUM(t.quantity), 0) salqty, IFNULL(SUM(t.quantity * t.sale_price), 0) gmv, IFNULL(
      ROUND(
        SUM(
          t.order_discount_rate * t.quantity * t.sale_price
        ), 2
      ), 0
    ) product_discount, COUNT(
      DISTINCT
      CASE
        t.order_discount_rate
        WHEN 0
        THEN t.order_id
      END
    ) orders_nmiser, COUNT(
      DISTINCT
      CASE
        t.order_discount_rate
        WHEN 0
        THEN t.product_id
      END
    ) products_nmiser, IFNULL(
      SUM(
        CASE
          t.order_discount_rate
          WHEN 0
          THEN t.quantity
        END
      ), 0
    ) salqty_nmiser, IFNULL(
      SUM(
        CASE
          t.order_discount_rate
          WHEN 0
          THEN t.quantity * t.sale_price
        END
      ), 0
    ) gmv_nmiser, @add_user
  FROM
    feods.oi_last_week_tmp t
  GROUP BY t.business_name, t.user_id;
  DROP TEMPORARY TABLE IF EXISTS feods.area_count_tmp;
  CREATE TEMPORARY TABLE feods.area_count_tmp AS
  SELECT
    t.business_name, COUNT(*) ct
  FROM
    feods.fjr_user_miser_stat t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name;
  SET @alpha := .3;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  CREATE TEMPORARY TABLE feods.for_order_tmp AS
  SELECT
    t.business_name, t.user_id
  FROM
    (SELECT
      (
        @order_num :=
        CASE
          @order_area
          WHEN t.business_name
          THEN @order_num + 1
          ELSE 1
        END
      ) order_num, @order_area := t.business_name business_name, t.user_id
    FROM
      feods.fjr_user_miser_stat t
    WHERE t.week_end = @week_end
    ORDER BY t.business_name, t.orders DESC, orders_nmiser) t
    JOIN feods.area_count_tmp bc
      ON t.business_name = bc.business_name
      AND t.order_num <= bc.ct * @alpha;
  CREATE INDEX idx_for_order_tmp_business_name_user_id
  ON feods.for_order_tmp (business_name, user_id);
  UPDATE
    feods.fjr_user_miser_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.user_id = f.user_id SET t.orders_top_flag = 1
  WHERE t.week_end = @week_end;
  UPDATE
    feods.fjr_user_miser_stat t
  SET
    t.miser_tag = t.orders_top_flag * 2+ (t.orders_nmiser = 0)
  WHERE t.week_end = @week_end;
  DELETE
  FROM
    feods.fjr_price_sensitive_stat
  WHERE week_end = @week_end;
  INSERT INTO feods.fjr_price_sensitive_stat (
    week_end, business_name, product_id, users, salqty, gmv, total_discount, profit, users_miser, salqty_miser, gmv_miser, total_discount_miser, users_ndis, salqty_ndis, gmv_ndis, orders_related, gmv_related, add_user
  )
  SELECT
    @week_end, t.business_name, t.product_id, COUNT(DISTINCT t.user_id) users, IFNULL(SUM(t.quantity), 0) salqty, IFNULL(SUM(t.quantity * t.sale_price), 0) gmv, IFNULL(
      ROUND(
        SUM(
          t.order_discount_rate * t.quantity * t.sale_price
        ), 2
      ), 0
    ) total_discount, IFNULL(
      ROUND(
        SUM(
          (
            (1- t.order_discount_rate) * t.sale_price - t.purchase_price
          ) * t.quantity
        ), 2
      ), 0
    ) profit, COUNT(
      DISTINCT
      CASE
        um.orders_nmiser
        WHEN 0
        THEN t.user_id
      END
    ) users_miser, IFNULL(
      SUM(
        CASE
          um.orders_nmiser
          WHEN 0
          THEN t.quantity
        END
      ), 0
    ) salqty_miser, IFNULL(
      SUM(
        CASE
          um.orders_nmiser
          WHEN 0
          THEN t.quantity * t.sale_price
        END
      ), 0
    ) gmv_miser, IFNULL(
      ROUND(
        SUM(
          CASE
            um.orders_nmiser
            WHEN 0
            THEN t.order_discount_rate * t.quantity * t.sale_price
          END
        ), 2
      ), 0
    ) total_discount_miser, COUNT(
      DISTINCT
      CASE
        t.order_discount_rate
        WHEN 0
        THEN t.user_id
      END
    ) users_ndis, IFNULL(
      SUM(
        CASE
          t.order_discount_rate
          WHEN 0
          THEN t.quantity
        END
      ), 0
    ) salqty_ndis, IFNULL(
      SUM(
        CASE
          t.order_discount_rate
          WHEN 0
          THEN t.quantity * t.sale_price
        END
      ), 0
    ) gmv_ndis, SUM(
      t.ogmv > (t.quantity * t.sale_price + .1)
    ) orders_related, SUM(t.ogmv - t.quantity * t.sale_price) gmv_related, @add_user
  FROM
    feods.oi_last_week_tmp t
    JOIN feods.fjr_user_miser_stat um
      ON t.business_name = um.business_name
      AND t.user_id = um.user_id
      AND um.week_end = @week_end
  GROUP BY t.business_name, t.product_id;
  SET @alpha := .4;
  DROP TEMPORARY TABLE IF EXISTS feods.area_count_tmp;
  CREATE TEMPORARY TABLE feods.area_count_tmp AS
  SELECT
    t.business_name, COUNT(*) ct, AVG(t.gmv) agmv, AVG(t.profit) aprofit, AVG(t.gmv_related) agmv_related, STDDEV_POP(t.gmv) sgmv, STDDEV_POP(t.profit) sprofit, STDDEV_POP(t.gmv_related) sgmv_related
  FROM
    feods.fjr_price_sensitive_stat t
  WHERE t.week_end = @week_end
  GROUP BY t.business_name;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  SET @str1 := "CREATE TEMPORARY TABLE feods.for_order_tmp AS SELECT t.business_name, t.product_id,t.order_num FROM (SELECT ( @order_num := CASE @order_area WHEN t.business_name THEN @order_num + 1 ELSE 1 END ) order_num, @order_area := t.business_name business_name, t.product_id FROM feods.fjr_price_sensitive_stat t WHERE t.week_end = @week_end ORDER BY t.business_name,";
  SET @str2 := ") t JOIN feods.area_count_tmp bc ON t.business_name = bc.business_name";
  SET @str3 := " AND t.order_num <= bc.ct*@alpha";
  SET @str := CONCAT(
    @str1, "t.users DESC,t.users_miser DESC", @str2, @str3
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_for_order_tmp_business_name_product_id
  ON feods.for_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id SET t.miser_num_top_flag = 1
  WHERE t.week_end = @week_end;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  SET @str := CONCAT(
    @str1, "(CASE t.users WHEN 0 THEN 0 ELSE t.users_miser / t.users END) DESC, t.users DESC", @str2, @str3
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_for_order_tmp_business_name_product_id
  ON feods.for_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id SET t.miser_rate_top_flag = 1
  WHERE t.week_end = @week_end;
  UPDATE
    feods.fjr_price_sensitive_stat t
  SET
    t.price_sensitive_tag = t.miser_num_top_flag * 2+ t.miser_rate_top_flag
  WHERE t.week_end = @week_end;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  SET @str := CONCAT(
    @str1, "t.gmv DESC,t.profit DESC", @str2
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_for_order_tmp_business_name_product_id
  ON feods.for_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id SET t.gmv_order = f.order_num
  WHERE t.week_end = @week_end;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  SET @str := CONCAT(
    @str1, "t.profit DESC,t.gmv DESC", @str2
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_for_order_tmp_business_name_product_id
  ON feods.for_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id SET t.profit_order = f.order_num
  WHERE t.week_end = @week_end;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  SET @str := CONCAT(
    @str1, "t.gmv_related DESC,t.gmv DESC", @str2
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_for_order_tmp_business_name_product_id
  ON feods.for_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id SET t.gmv_related_order = f.order_num
  WHERE t.week_end = @week_end;
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.area_count_tmp a
      ON t.business_name = a.business_name SET t.gmv_normal = (t.gmv - a.agmv) / a.sgmv, t.profit_normal = (t.profit - a.aprofit) / a.sprofit, t.gmv_related_normal = (t.gmv_related - a.agmv_related) / a.sgmv_related
  WHERE t.week_end = @week_end;
  UPDATE
    feods.fjr_price_sensitive_stat t
  SET
    t.product_contribution_mark = .4 * t.gmv_normal + .3 * t.profit_normal + .3 * t.gmv_related_normal
  WHERE t.week_end = @week_end;
  SET @order_num := 0, @order_area := '';
  DROP TEMPORARY TABLE IF EXISTS feods.for_order_tmp;
  SET @str := CONCAT(
    @str1, "t.product_contribution_mark DESC,t.gmv DESC", @str2
  );
  PREPARE str_exe FROM @str;
  EXECUTE str_exe;
  CREATE INDEX idx_for_order_tmp_business_name_product_id
  ON feods.for_order_tmp (business_name, product_id);
  UPDATE
    feods.fjr_price_sensitive_stat t
    JOIN feods.for_order_tmp f
      ON t.business_name = f.business_name
      AND t.product_id = f.product_id SET t.product_contribution_order = f.order_num
  WHERE t.week_end = @week_end;
  SET @delta := 0.4;
  UPDATE
    feods.fjr_price_sensitive_stat t
  SET
    t.product_contribution_tag = (t.profit >= 0) * (
      4 * (t.gmv_normal > @delta) + 2 * (t.profit_normal > @delta) + (t.gmv_related_normal > @delta) + 1
    ) - 1- (t.profit < 0) * (t.gmv_normal < 0) * (t.gmv_related_normal < 0)
  WHERE t.week_end = @week_end;
  CALL feods.sp_task_log (
    'sp_price_sensitive_week', @week_end, CONCAT(
      'fjr_w_1757b8ae9401d09e8ef689661978a430', @timestamp, @add_user
    )
  );
  CALL sh_process.sp_price_sensitive_week_nation (@week_end);
  COMMIT;
END