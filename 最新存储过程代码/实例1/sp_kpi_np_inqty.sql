CREATE DEFINER=`feprocess`@`%` PROCEDURE `sp_kpi_np_inqty`(IN in_sdate DATE)
BEGIN
  SET @tmp_str := '', @sdate := in_sdate, @add_user := CURRENT_USER, @timestamp := CURRENT_TIMESTAMP;
  DELETE
  FROM
    feods.fjr_kpi_np_inqty
  WHERE sdate = @sdate;
  SELECT
    @tmp_str := CONCAT(
      "INSERT INTO feods.fjr_kpi_np_inqty(sdate,region,business_area,product_id,product_fe,product_name,inqty,salqty,stoqty,add_user) ", "SELECT @sdate,b.region_name,b.business_name,pd.product_id,pd.product_fe,pd.product_name,ifnull(pd.inqty_np,0) ", ",ifnull(SUM(sal.quantity),0) ", ",ifnull(SUM(sto.day", DAYOFMONTH(@sdate), "_quantity),0) ,@add_user ", "FROM feods.zs_product_dim_sserp_his pd ", "JOIN feods.d_op_dim_date vv ON pd.version=vv.version_id AND vv.sdate<=@sdate AND vv.edate>@sdate ", "JOIN feods.fjr_city_business b ON pd.business_area=b.business_name ", "JOIN fe.sf_shelf s ON b.city=s.city AND s.data_flag=1 AND s.shelf_type IN(1,2,3,4,5) ", "LEFT JOIN fe.sf_statistics_shelf_product_sale sal ON pd.product_id=sal.product_id AND s.shelf_id=sal.shelf_id AND sal.create_date>=SUBDATE(@sdate,30-1) AND sal.create_date<ADDDATE(@sdate,1) ", "LEFT JOIN fe.sf_shelf_product_stock_detail sto ON pd.product_id=sto.product_id AND s.shelf_id=sto.shelf_id AND sto.stat_date=DATE_FORMAT(ADDDATE(@sdate,1),'%Y-%m') ", "WHERE pd.product_type ='新增（试运行）' AND pd.indate_np =SUBDATE(@sdate,30-1) AND pd.inqty_np >0 ", "GROUP BY b.business_name,pd.product_id;"
    );
  PREPARE exe_str FROM @tmp_str;
  EXECUTE exe_str;
  CALL feods.sp_task_log (
    'sp_kpi_np_inqty', @sdate, CONCAT(
      'fjr_d_4c26c6d7402e8106aa0c27ca786a8b18', @timestamp, @add_user
    )
  );
  COMMIT;
END