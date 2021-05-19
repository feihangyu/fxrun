CREATE DEFINER=`feprocess`@`%` PROCEDURE `sh_shelf_flag`()
BEGIN
SET @run_date := CURRENT_DATE(),@user := CURRENT_USER(),@timestamp := CURRENT_TIMESTAMP();
SET @sdate=CURDATE()
    ,@sweek=SUBDATE(@sdate,IF(DAYOFWEEK(@sdate)=1,6,DAYOFWEEK(@sdate)-2)) #当周一
    ,@smonth=DATE_FORMAT(@sdate,'%Y-%m-01') #当月1号
;
# 开始插入数据
TRUNCATE TABLE feods.zs_shelf_flag;
INSERT INTO feods.zs_shelf_flag
    (shelf_id,daoshun_level,GMV_level,sale_qty_level)
SELECT
    t0.shelf_id,
    CASE
      WHEN daoshunlv < - 0.3
      THEN 1
      WHEN daoshunlv <= - 0.15
      AND daoshunlv > - 0.3
      THEN 2
      WHEN daoshunlv <= - 0.1
      AND daoshunlv > - 0.15
      THEN 3
      WHEN daoshunlv <= - 0.07
      AND daoshunlv > - 0.1
      THEN 4
      WHEN daoshunlv <= - 0.05
      AND daoshunlv > - 0.07
      THEN 5
      WHEN daoshunlv <= - 0.03
      AND daoshunlv > - 0.05
      THEN 6
      WHEN daoshunlv <= 0
      AND daoshunlv > - 0.03
      THEN 7
      WHEN daoshunlv <= 0.1
      AND daoshunlv > 0
      THEN 8
      WHEN daoshunlv <= 0.3
      AND daoshunlv > 0.1
      THEN 9
      WHEN daoshunlv >= 0.3
      THEN 10
    END AS daoshun_level,
    CASE WHEN gmv < 200 THEN 1
      WHEN gmv < 400 THEN 2
      WHEN gmv < 600 THEN 3
      WHEN gmv < 800 THEN 4
      WHEN gmv < 1000 THEN 5
      WHEN gmv >= 1000 THEN 6
    END AS GMV_level,
    CASE WHEN qty_sal < 20 THEN 1
      WHEN qty_sal < 40 AND qty_sal >= 20 THEN 2
      WHEN qty_sal < 60 AND qty_sal >= 40 THEN 3
      WHEN qty_sal < 80 AND qty_sal >= 60 THEN 4
      WHEN qty_sal < 100 AND qty_sal >= 80 THEN 5
      WHEN qty_sal >= 100 THEN 6
    END AS sale_qty_level
FROM
fe.sf_shelf t0
LEFT JOIN
  (SELECT smonth,shelf_id,
    SUM(IFNULL(huosun, 0) + IFNULL(bk_money, 0) + IFNULL(total_error_value, 0))/SUM(IFNULL(sale_value, 0)) AS daoshunlv
  FROM
    feods.pj_zs_goods_damaged a
  WHERE SHELF_STATUS = 2
    AND smonth = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL DAY(CURDATE()) DAY), '%Y%m')
  GROUP BY smonth,
    shelf_id) t1
  ON t1.shelf_id = t0.shelf_id
LEFT JOIN feods.fjr_shelf_mgmv t2
  ON t2.month_id=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 MONTH),'%Y-%m')  AND  t0.shelf_id = t2.shelf_id
WHERE t0.SHELF_STATUS IN (1,2) AND t0.DATA_FLAG = 1
;
#货架合作类型
   UPDATE
    feods.zs_shelf_flag AS b
    LEFT JOIN
      (SELECT
        a.shelf_id,
        CASE
          WHEN a.shelf_id IN (
            7852,
            18037,
            18472,
            22566,
            32035,
            35743,
            36784,
            41172,
            57467,
            57468,
            57469,
            57470,
            57471,
            57472,
            57457,
            57458,
            57459,
            57460
          )
          THEN 3
          WHEN (
            j.record_id IS NOT NULL
            OR b.COMPANY_NAME LIKE '%顺丰%'
            OR b.COMPANY_NAME LIKE '%速运%'
            OR b.COMPANY_NAME LIKE '%重货%'
            OR a.shelf_name LIKE '%顺丰%'
          )
          THEN 1
          ELSE 2
        END AS cooperation_type
      FROM
        fe.sf_shelf a
        LEFT JOIN fe.sf_company b
          ON a.COMPANY_ID = b.COMPANY_ID
        LEFT JOIN fe.sf_shelf_apply f
          ON a.shelf_id = f.SHELF_ID
        LEFT JOIN fe.sf_shelf_apply_addition_info j
          ON (
            f.record_id = j.record_id
            AND j.is_inner_shelf = 1
          )) AS a
      ON a.shelf_id = b.shelf_id SET b.cooperation_type = a.cooperation_type;
#更新货架类型及城市
   UPDATE
    feods.zs_shelf_flag AS b
    LEFT JOIN
      (SELECT
        shelf_id,
        SHELF_TYPE,
        CITY
      FROM
        fe.sf_shelf) AS a
      ON a.shelf_id = b.shelf_id SET b.SHELF_TYPE = a.SHELF_TYPE,
    b.CITY = a.CITY;
  UPDATE
   feods.zs_shelf_flag
    SET SHELF_TYPE = 4 WHERE shelf_id IN (83558,
        83557,
        83556,
        83550,
        83551,
        83554,
        83682,
        83684,
        83686,
        83688,
        83692,
        83695,
        83696,
        83699,
        83725,
        83813,
        83814,
        83816,
        83817,
        83819,
        83820,
        83868,
        83869,
        83871,
        83872,
        83968,
        84076,
        84090,
        63645
        );
#更新货架等级
UPDATE feods.zs_shelf_flag t1
LEFT JOIN feods.d_op_shelf_grade t2 ON t2.month_id=DATE_FORMAT( DATE_SUB(CURDATE(),INTERVAL 1 MONTH) ,'%Y-%m') AND t2.shelf_id=t1.SHELF_ID #上月货架等级
LEFT JOIN feods.d_op_shelf_grade t3 ON t3.month_id=DATE_FORMAT( CURDATE() ,'%Y-%m') AND t3.shelf_id=t1.SHELF_ID #本月预测货架等级
SET t1.ext1=CASE t2.grade WHEN '新装' THEN 1  WHEN '甲' THEN 2 WHEN '乙' THEN 3 WHEN '丙' THEN 4 WHEN '丁' THEN 5  WHEN '已撤架' THEN 6 ELSE 0 END
    ,t1.ext11=CASE t3.grade WHEN '新装' THEN 1  WHEN '甲' THEN 2 WHEN '乙' THEN 3 WHEN '丙' THEN 4 WHEN '丁' THEN 5  WHEN '已撤架' THEN 6 ELSE 0 END
;
#更新早餐测试货架
UPDATE feods.zs_shelf_flag t1
JOIN
    (SELECT shelf_id,1 AS ext3
    FROM feods.D_M_TEST_SHELF_BREAK
    )t2 ON t1.shelf_id=t2.shelf_id
SET t1.ext3=t2.ext3
;
# 更新撤架粗心货架标签
    UPDATE feods.zs_shelf_flag t1
    LEFT JOIN (
        SELECT a1.SHELF_ID,SUM(STOCK_QUANTITY) STOCK_QUANTITY
        FROM fe.sf_shelf_revoke a1
            JOIN  fe.sf_shelf_product_detail a2
                ON a1.SHELF_ID=a2.SHELF_ID
        WHERE a1.AUDIT_STATUS=2 AND a1.DATA_FLAG=1 AND a2.DATA_FLAG=1
            AND a2.STOCK_QUANTITY>0
        GROUP BY a1.SHELF_ID
            ) t2 ON t1.shelf_id=t2.SHELF_ID
    SET t1.ext2=IF(t2.SHELF_ID IS NULL,2,1)
;
# 定向货架分类(1:核心货架 ,2:高潜力货架,0:其他)
    #1 核心货架
SET @ROW=0
	,@MID=''
	,@tmonth=DATE_FORMAT(CURDATE(),'%Y-%m-01');
UPDATE feods.zs_shelf_flag t1
JOIN
    (SELECT SHELF_ID
    	,SUM(IF(smonth=DATE_SUB(@tmonth,INTERVAL 1 MONTH ) ,gmv,0)) GMV_lm
    	,SUM(IF(smonth=DATE_SUB(@tmonth,INTERVAL 2 MONTH ) ,gmv,0)) GMV_llm
    FROM
    	(
    	SELECT t3.smonth,t3.SHELF_ID,t3.GMV
		FROM
			(
			SELECT T2.*,CASE WHEN @MID = CONCAT(T2.smonth,T2.BUSINESS_AREA) THEN @ROW:=@ROW+1 ELSE @ROW:=1 END ROW_NUM
				,@MID:=CONCAT(T2.smonth,T2.BUSINESS_AREA)  row_by
			FROM
				(
					SELECT a.smonth,b.business_name BUSINESS_AREA,a.SHELF_ID
						,a.GMV
						,D.SHELF_NUM
					FROM feods.d_ma_shelf_sale_monthly a
					JOIN fe_dwd.dwd_shelf_base_day_all b ON b.shelf_id=a.SHELF_ID
					JOIN
						(
						SELECT DATE_FORMAT(c.sdate,'%Y-%m-01')  smonth,a.business_name,COUNT(DISTINCT a.SHELF_ID)AS SHELF_NUM
						FROM fe_dwd.dwd_shelf_base_day_all a
						JOIN fe_dwd.dwd_pub_work_day c ON c.sdate BETWEEN DATE_ADD(@tmonth,INTERVAL -2 MONTH ) AND DATE_ADD(@tmonth,INTERVAL -1 MONTH ) AND DAY(sdate)=1
						AND a.ACTIVATE_TIME<DATE_ADD(c.sdate,INTERVAL 1 MONTH)
						WHERE (a.SHELF_STATUS=2 OR a.REVOKE_TIME>DATE_ADD(@tmonth,INTERVAL -1 MONTH))
							AND a.SHELF_TYPE IN(1,2,3)
						GROUP BY a.business_name,smonth
						)D ON b.business_name=D.business_name AND D.smonth=a.smonth
					WHERE a.smonth BETWEEN DATE_SUB(@tmonth,INTERVAL 2 MONTH ) AND DATE_SUB(@tmonth,INTERVAL 1 MONTH)
						AND b.SHELF_TYPE IN(1,2,3) AND b.SHELF_STATUS=2
						AND b.business_name NOT IN ('内蒙古区','惠州区','冀北区','台州区','烟台市')
					ORDER BY a.smonth,b.business_name,a.GMV DESC
				)T2
			)t3
		WHERE t3.ROW_NUM/t3.SHELF_NUM<=0.3 /*取7月地区GMV排名前30%的货架，且当前为激活状态*/
    	)R1
    GROUP BY SHELF_ID
    HAVING GMV_lm>0 AND GMV_llm>0 AND  (GMV_lm+GMV_llm)>=1000
    /*order by BUSINESS_AREA,SHELF_ID 筛选前两月月均GMV大于等于500的货架*/
    ) t2 ON t1.shelf_id=t2.SHELF_ID
SET t1.ext4=1;
    # 2 高潜力货架
SET @tmonth=DATE_FORMAT(CURDATE(),'%Y-%m-01')
    ,@tmonth_num=DATE_FORMAT(CURDATE(),'%Y%m') ;
UPDATE feods.zs_shelf_flag t1
JOIN (
    SELECT t.SHELF_ID
         , CASE
               WHEN t.gmv_f5month_max < 200 THEN 1
               WHEN t.gmv_f5month_max >= 200 AND t.gmv_f5month_max < 400 THEN 2
               WHEN t.gmv_f5month_max >= 400 AND t.gmv_f5month_max < 600 THEN 3
               WHEN t.gmv_f5month_max >= 600 AND t.gmv_f5month_max < 800 THEN 4
               WHEN t.gmv_f5month_max >= 800 AND t.gmv_f5month_max < 1000 THEN 5
               WHEN t.gmv_f5month_max >= 1000 THEN 6
               ELSE 0 END gmv_f5month_max_flag
         , CASE
               WHEN gmv_l2month_min < 200 THEN 1
               WHEN gmv_l2month_min >= 200 AND gmv_l2month_min < 400 THEN 2
               WHEN gmv_l2month_min >= 400 AND gmv_l2month_min < 600 THEN 3
               WHEN gmv_l2month_min >= 600 AND gmv_l2month_min < 800 THEN 4
               WHEN gmv_l2month_min >= 800 AND gmv_l2month_min < 1000 THEN 5
               WHEN gmv_l2month_min >= 1000 THEN 6
               ELSE 0 END gmv_l2month_min_flag
         , CASE
               WHEN gmv_llmonth < 200 THEN 1
               WHEN gmv_llmonth >= 200 AND gmv_llmonth < 400 THEN 2
               WHEN gmv_llmonth >= 400 AND gmv_llmonth < 600 THEN 3
               WHEN gmv_llmonth >= 600 AND gmv_llmonth < 800 THEN 4
               WHEN gmv_llmonth >= 800 AND gmv_llmonth < 1000 THEN 5
               WHEN gmv_llmonth >= 1000 THEN 6
               ELSE 0 END gmv_llmonth_flag
         , CASE
               WHEN gmv_lmonth < 200 THEN 1
               WHEN gmv_lmonth >= 200 AND gmv_lmonth < 400 THEN 2
               WHEN gmv_lmonth >= 400 AND gmv_lmonth < 600 THEN 3
               WHEN gmv_lmonth >= 600 AND gmv_lmonth < 800 THEN 4
               WHEN gmv_lmonth >= 800 AND gmv_lmonth < 1000 THEN 5
               WHEN gmv_lmonth >= 1000 THEN 6
               ELSE 0 END gmv_lmonth_flag
        FROM
            (SELECT a1.SHELF_ID
                , MAX(IF(a1.smonth BETWEEN DATE_SUB(@tmonth,INTERVAL 7 MONTH) AND DATE_SUB(@tmonth,INTERVAL 3 MONTH), a1.GMV, 0))    gmv_f5month_max
                , MIN(IF(a1.smonth BETWEEN DATE_SUB(@tmonth,INTERVAL 2 MONTH) AND DATE_SUB(@tmonth,INTERVAL 1 MONTH), a1.GMV, NULL)) gmv_l2month_min
                , SUM(IF(a1.smonth = DATE_SUB(@tmonth,INTERVAL 2 MONTH), a1.GMV, 0))  gmv_llmonth
                , SUM(IF(a1.smonth = DATE_SUB(@tmonth,INTERVAL 1 MONTH), a1.GMV, 0))  gmv_lmonth
            FROM feods.d_ma_shelf_sale_monthly a1
            JOIN fe.sf_shelf a2 ON a1.SHELF_ID = a2.SHELF_ID
            JOIN feods.zs_shelf_flag a3 ON a1.SHELF_ID = a3.shelf_id AND a3.ext4 = 0 #限制不要核心货架
            WHERE a1.smonth >= DATE_SUB(@tmonth,INTERVAL 7 MONTH ) AND a1.smonth < @tmonth
                #AND MONTH(a1.smonth)<>2
            AND a2.SHELF_TYPE IN (1, 2, 3)
            AND a2.ACTIVATE_TIME <= DATE_ADD(@tmonth, INTERVAL -7 MONTH)
            GROUP BY a1.SHELF_ID
             ) t
        HAVING gmv_f5month_max_flag - gmv_l2month_min_flag >= 2  AND gmv_llmonth_flag - gmv_lmonth_flag >= -1
    ) t2 ON t1.shelf_id=t2.SHELF_ID
SET t1.ext4=2;
    #3 新终端
UPDATE feods.zs_shelf_flag t1
JOIN fe.sf_shelf t2 ON t1.shelf_id=t2.SHELF_ID
SET t1.ext4=3
WHERE t1.ext4=0 AND  DATEDIFF(CURDATE(),DATE(t2.ACTIVATE_TIME))<30
;
#二、货架库存状态：(1:库存充足,2:库存超80%,3:库存不足80% ,0:其他)
    /*l  按甲乙级库存数量货架>=180，冰箱>=110，丙丁级库存数量货架>=110，冰箱>=90作为库存阈值
    l  用“当前架上库存数量（含在途补货订单）/库存阈值”当前的库存状况n：
    n>=1，库存充足
    n>=0.8且n<1,库存超80%
    n<0.8,库存不足80%*/
UPDATE feods.zs_shelf_flag t1
JOIN
   (SELECT a1.SHELF_ID,a1.SHELF_TYPE,a1.shelf_level
         ,(IFNULL(a2.STOCK_QUANTITY,0)+IFNULL(a3.PRODUCT_NUM_onload,0))
              /(CASE WHEN a1.shelf_level IN (2,3) AND a1.SHELF_TYPE IN(1,3) THEN 180
                    WHEN  a1.shelf_level IN (2,3) AND a1.SHELF_TYPE=2   THEN 110
                    WHEN  a1.shelf_level IN (4,5) AND a1.SHELF_TYPE IN(1,3)  THEN 110
                    ELSE 90 END)
               AS stock_level
    FROM fe.sf_shelf  a1
    LEFT JOIN
        (# 当前库存
        SELECT SHELF_ID,SUM(STOCK_QUANTITY) STOCK_QUANTITY
        FROM fe_dwd.dwd_shelf_product_day_all
        WHERE STOCK_QUANTITY>0
        GROUP BY SHELF_ID
        ) a2 ON a1.SHELF_ID=a2.SHELF_ID
    LEFT JOIN
        (#在途库存
        SELECT SHELF_ID,SUM(PRODUCT_NUM) PRODUCT_NUM_onload
        FROM fe.sf_product_fill_order
            WHERE  APPLY_TIME>=DATE_ADD(CURDATE(),INTERVAL -7 DAY) AND DATA_FLAG=1 AND ORDER_STATUS=2
        GROUP BY SHELF_ID
        ) a3 ON a1.SHELF_ID=a3.SHELF_ID
    WHERE a1.SHELF_TYPE IN (1,2,3) AND a1.DATA_FLAG=1 AND a1.SHELF_STATUS=2 AND a1.shelf_level IN(2,3,4,5)
    ) t2 ON t1.shelf_id=t2.SHELF_ID
SET t1.ext5=(CASE WHEN t2.stock_level>=1 THEN 1 WHEN t2.stock_level>=0.8 THEN 2 ELSE 3 END )
;
#三、货架GMV情况(1:下滑10%-30%、2:下滑30%-50%、3:下滑50%以上、4:情况良好、0：其他 )
/*l  取“前3周货架GMV（不含补付款）/天数（见附件工作日折算表）*当月天数（见附件工作日折算表）”作为预估本月GMV
l  用"（前2月月均GMV-预估本月GMV）/前2月月均GMV"作为下滑比例n：
    n>=0.1且n<0.3,下滑10%-30%
    n>=0.3且n<0.5,下滑30%-50%
    n>=0.5,下滑50%以上
    n<0.1，情况良好*/
SET @smonth=DATE_FORMAT(CURDATE(),'%Y-%m-01');
UPDATE feods.zs_shelf_flag t1
JOIN
    (SELECT t1.shelf_id
         ,(t1.gmv_2month_avg-t1.gmv_avg*t2.workday_num_m )/t1.gmv_2month_avg  down_ratio
    FROM
        (SELECT a.shelf_id
              , SUM(IF(a.sdate >= @smonth OR a.sdate >= DATE_ADD(CURDATE(), INTERVAL -14 DAY), a.GMV, 0))
                / SUM(IF(a.sdate >= @smonth OR a.sdate >= DATE_ADD(CURDATE(), INTERVAL -14 DAY), b.workday_num, 0)) gmv_avg
              , SUM(IF(a.sdate < @smonth, GMV, 0)) / 2                                                            gmv_2month_avg
         FROM feods.d_ma_shelf_sale_daily a
                  JOIN fe_dwd.dwd_pub_work_day b ON a.sdate = b.sdate
                  JOIN fe.sf_shelf c ON a.shelf_id = c.SHELF_ID
         WHERE a.sdate BETWEEN DATE_ADD(@smonth, INTERVAL -2 MONTH) AND DATE_ADD(CURDATE(), INTERVAL -1 DAY)
           AND c.ACTIVATE_TIME <= DATE_ADD(@smonth, INTERVAL -2 MONTH)
           AND c.shelf_type IN (1, 2, 3)
         GROUP BY shelf_id
	) t1
    JOIN
        (SELECT SUM(a.workday_num) workday_num_m FROM fe_dwd.dwd_pub_work_day a WHERE a.sdate >= @smonth AND a.sdate < DATE_ADD(@smonth, INTERVAL 1 MONTH)
        ) t2  ON 1=1
    ) t2 ON t1.shelf_id=t2.shelf_id
SET t1.ext6= (CASE WHEN t2.down_ratio<0.1 THEN 4 WHEN t2.down_ratio<0.3 THEN 1 WHEN t2.down_ratio<0.5 THEN 2 ELSE 3 END)
;
# 是否新货架 激活时间小于等于14天的为新货架 因为是晚上11点多跑 所以设置为13天
UPDATE feods.zs_shelf_flag t1
JOIN fe.sf_shelf t2 ON t1.shelf_id=t2.SHELF_ID
SET t1.ext7=1
WHERE DATEDIFF(CURDATE(),DATE(t2.ACTIVATE_TIME))<=13
;
# 货架合作类型 ,是否月结用户 ,补货组自定义标签
UPDATE feods.zs_shelf_flag t1
JOIN fe_dwd.dwd_ma_shelf_info_ext t2
    ON t1.shelf_id=t2.shelf_id
    #货架合作类型（1内部货架-办公室,2-外部货架,3-商务合作货架 4:灰度 5：内部货架-分点部）
SET t1.cooperation_type=IF(t2.cooperation_type IN(1,5),t2.cooperation_type,t1.cooperation_type)
     #是否月结用户 补货组自定义标签
    ,t1.ext10=t2.if_month_settle
    ,t1.ext25= t2.fillteam_flag
;
# 终端价值分层 (0:其他,1:撤架,2:新装,3:新关联,4:低价值,5:普通价值,6:中,7:高)
#终端潜力等级(0:其他,1:中,2:高)
SET @date_st=SUBDATE(CURDATE(),1);
    #临时数据
DROP TEMPORARY TABLE IF EXISTS feods.temp_date;
CREATE TEMPORARY TABLE feods.temp_date AS #30个工作日日期
    SELECT sdate
    FROM fe_dwd.dwd_pub_work_day
    WHERE sdate>=SUBDATE(@date_st,60) AND sdate<=@date_st AND if_work_day=1
    ORDER BY sdate DESC LIMIT 30;
SET @work_date_start=(SELECT MIN(sdate) FROM feods.temp_date) ;SET @work_date_end=(SELECT MAX(sdate) FROM feods.temp_date) ;
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_bind_info;
SET @shelf_id=0,@cur_date=CURDATE(),@row=1;
CREATE TEMPORARY TABLE feods.temp_shelf_bind_info(INDEX (shelf_id)) AS #货架分段绑定日期
    SELECT shelf_id,row_num,MIN(sdate) bind_date,MAX(sdate) UNBIND_date
    FROM
        (SELECT t.*
             ,CASE WHEN @shelf_id<>shelf_id THEN @row:=1
                WHEN @shelf_id=shelf_id AND sdate >= @cur_date AND sdate<=ADDDATE(@cur_date,1) THEN @row:=@row
                ELSE @row:=@row+1 END row_num
             ,@shelf_id :=shelf_id shelf_id2,@cur_date:=sdate sdate2
        FROM
             (SELECT a1.shelf_id,a2.sdate
             FROM
                 (SELECT MAIN_SHELF_ID shelf_id,DATE(add_time) add_date
                       ,IF(SHELF_HANDLE_STATUS=10,DATE(IFNULL(UNBIND_TIME,LAST_UPDATE_TIME)),CURDATE()) UNBIND_date
                FROM fe.sf_shelf_relation_record
                WHERE SHELF_HANDLE_STATUS IN (9,10) AND IFNULL(UNBIND_TIME,CURDATE())>=@work_date_start
                     AND DATA_FLAG=1
                )a1
            JOIN fe_dwd.dwd_pub_work_day  a2 ON a2.sdate BETWEEN a1.add_date AND a1.UNBIND_date
            ORDER BY a1.shelf_id, a2.sdate
            ) t
        ) tt
    GROUP BY shelf_id,row_num
;
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_gmv_daily;
CREATE TEMPORARY TABLE feods.temp_shelf_gmv_daily(INDEX (shelf_id,sdate)) AS #货架日GMV,其他基本信息
    SELECT a1.sdate,a1.shelf_id
        ,IF(a4.shelf_id IS NOT NULL,1,0) if_bind,a4.bind_date,a4.UNBIND_date
        ,a1.gmv+a1.payment_money gmv
    FROM feods.fjr_shelf_dgmv a1
    JOIN feods.temp_date a2 ON a2.sdate=a1.sdate
    JOIN fe_dwd.dwd_shelf_base_day_all a3 ON a3.shelf_id=a1.shelf_id AND a3.shelf_type NOT IN (4)
    LEFT JOIN feods.temp_shelf_bind_info a4 ON a4.shelf_id=a1.shelf_id AND a1.sdate BETWEEN a4.bind_date AND a4.UNBIND_date
;
DROP TEMPORARY TABLE IF EXISTS feods.temp_shelf_gmv_avg;     #终端日均GMV,基本信息
CREATE TEMPORARY TABLE feods.temp_shelf_gmv_avg
    SELECT a3.shelf_id
         ,a3.shelf_type,DATE(a3.ACTIVATE_TIME) activate_date,DATE(a3.REVOKE_TIME) revoke_date
         ,a2.bind_date,a2.UNBIND_date,IF(a2.shelf_id IS NOT NULL,1,0) if_bind
         ,ROUND(SUM( IF((a2.shelf_id IS NOT NULL AND a1.if_bind=1) OR (a2.shelf_id IS NULL AND a1.if_bind=0) ,gmv,0 ) ) /30 ,2) gmv_avg
    FROM fe_dwd.dwd_shelf_base_day_all a3
    LEFT JOIN (SELECT shelf_id,MIN(bind_date) bind_date,MAX(UNBIND_date) UNBIND_date FROM feods.temp_shelf_bind_info WHERE UNBIND_date>=@date_st GROUP BY shelf_id) a2
        ON a2.shelf_id=a3.shelf_id
    LEFT JOIN feods.temp_shelf_gmv_daily a1 ON a1.shelf_id=a3.shelf_id
    WHERE a3.SHELF_STATUS IN (1,2) AND a3.shelf_type NOT IN (4,9)
    GROUP BY shelf_id
;
    #更新终端价值分层(0:其他,1:撤架,2:新装,3:新关联,4:低价值,5:普通价值,6:中,7:高)
SET @if_summer=IF(@date_st BETWEEN DATE_FORMAT(@date_st,'%Y-06-01') AND DATE_FORMAT(@date_st,'%Y-09-15'),1,0); #判断夏季,春秋冬季
UPDATE feods.zs_shelf_flag a1
JOIN feods.temp_shelf_gmv_avg a2 ON a2.shelf_id=a1.shelf_id
SET a1.ext26=
    CASE
        WHEN a1.SHELF_TYPE=4 THEN 0
        WHEN a2.ACTIVATE_date>@date_star THEN 2 #新装
        WHEN a2.if_bind=1 AND a2.bind_date>@date_star  THEN 3 #新关联
        WHEN a2.revoke_date<@date_st THEN 1   #撤架
        WHEN (a2.if_bind=1 AND a2.gmv_avg>=IF(@if_summer=1,100,90)) #关联货架
            OR (a2.shelf_type IN(2,5) AND if_bind=0 AND a2.gmv_avg>=IF(@if_summer=1,40,25)) #冰箱冰柜
            OR (a2.shelf_type=6 AND if_bind=0  AND a2.gmv_avg>=95) #智能柜
            OR (a2.shelf_type=7 AND if_bind=0  AND a2.gmv_avg>=140) #自贩机
            OR (a2.shelf_type IN(1,3,8) AND if_bind=0  AND a2.gmv_avg>=40) #除虚拟货架外的货架
            THEN 7 #高
        WHEN (a2.if_bind=1  AND a2.gmv_avg>=IF(@if_summer=1,60,50))
            OR (a2.shelf_type IN(2,5) AND if_bind=0 AND a2.gmv_avg>=IF(@if_summer=1,15,10))
            OR (a2.shelf_type=6 AND if_bind=0 AND a2.gmv_avg>=70)
            OR (a2.shelf_type=7 AND if_bind=0 AND a2.gmv_avg>=95)
            OR (a2.shelf_type IN(1,3,8) AND if_bind=0 AND a2.gmv_avg>=25)
            THEN 6 #中
        WHEN (a2.if_bind=1 AND a2.gmv_avg>=30)
            OR (a2.shelf_type IN(2,5) AND if_bind=0  AND a2.gmv_avg>=5)
            OR (a2.shelf_type=6 AND if_bind=0 AND a2.gmv_avg>=25)
            OR (a2.shelf_type=7 AND if_bind=0 AND a2.gmv_avg>=50)
            OR (a2.shelf_type IN(1,3,8) AND if_bind=0 AND a2.gmv_avg>=10)
            THEN 5 #低
        WHEN gmv_avg>=0 THEN 4
        ELSE 0 END
;
    # 终端潜力等级(0:其他,1:中,2:高)
SET @date_st=SUBDATE(CURDATE(),1) ;
UPDATE
    (SELECT a1.shelf_id,SUM(IF(ext26=7,1,0))/SUM(1) rate_7,SUM(IF(ext26=6,1,0))/SUM(1) rate_6
    FROM feods.zs_shelf_flag_his a1
    JOIN feods.temp_shelf_gmv_avg a3
        ON a3.shelf_id=a1.shelf_id AND ((a3.if_bind=1 AND a3.bind_date<=SUBDATE(@date_st,89)) OR (a3.if_bind=0 AND a3.activate_date<=SUBDATE(@date_st,89)))
    JOIN fe_dwd.dwd_pub_work_day a2 ON a2.sdate=SUBDATE(a1.sdate,1) AND a2.if_work_day=1
    WHERE a1.sdate>=SUBDATE(@date_st,89) AND a1.sdate<ADDDATE(@date_st,1)
    GROUP BY a1.shelf_id
    ) t1
JOIN feods.zs_shelf_flag t2 ON t2.shelf_id=t1.shelf_id
SET t2.ext27=CASE WHEN t1.rate_7>=0.3 THEN 2 WHEN t1.rate_6>=0.3 THEN 1 ELSE 0 END
WHERE t2.ext26 IN (4,5);
# 覆盖人数 (1: 0-30 ,2: 30-50,3: 50-100,4: 100-200,5: 200人以上)
UPDATE feods.zs_shelf_flag  t1
JOIN
    (SELECT shelf_id,scope
         ,ROUND(
             CASE WHEN scope LIKE '%-%人' THEN (SUBSTRING_INDEX(SUBSTRING_INDEX(scope,'人',1) ,'-',1)+SUBSTRING_INDEX(SUBSTRING_INDEX(scope,'人',1) ,'-',-1)) /2
             WHEN scope LIKE '%于%' THEN REPLACE(SUBSTRING_INDEX(scope,'于',-1),'人','')
             ELSE REPLACE(scope,'-',0)  END
             ) scope2
    FROM fe_dwd.dwd_shelf_base_day_all) t2 ON t2.shelf_id=t1.shelf_id
SET t1.ext28 = CASE WHEN scope2>=200 THEN 5 WHEN scope2>=100 THEN 4 WHEN scope2>=50 THEN 3 WHEN scope2>=30 THEN 2 WHEN scope2>=0 THEN 1 ELSE 0 END
;
# 是否全职店主(1:是,2:否)
UPDATE feods.zs_shelf_flag t1
JOIN fe_dwd.dwd_shelf_base_day_all t2 ON t2.shelf_id=t1.shelf_id
SET t1.ext12= 1
WHERE  t2.manager_type='全职店主'
;
# 货架商品表聚合货架标签
    #ext19 连续无新品天数 (1:[0,15）；2:[15,30）；3: [30,60）；4 : [60,∞）)
    #ext20 正常补货SKU数	(1: [0,15）；2: [15,20）；3: [20,30）；4: [30,60）；5: [60,∞）)
    #ext21 淘汰品库存金额占比	(1: [0,15%）；2: [15%,25%）；3:[25%,50%），4:[50%,100%）)
UPDATE
    (SELECT a1.SHELF_ID, DATEDIFF(ADDDATE(CURDATE(),1),MAX(DATE(FIRST_FILL_TIME)))   no_new_day
        ,SUM(IF(SHELF_FILL_FLAG=1,1,0) ) sku_fill
        ,SUM(IF(a3.shelf_id IS NOT NULL,a1.STOCK_QUANTITY,0)*SALE_PRICE)/SUM(a1.STOCK_QUANTITY*a1.SALE_PRICE) outstock_value_rate
    FROM fe.sf_shelf_product_detail a1
    JOIN fe_dwd.dwd_shelf_base_day_all a2 ON a2.shelf_id=a1.SHELF_ID AND a2.SHELF_STATUS IN (1,2)
    LEFT JOIN feods.zs_shelf_product_flag a3 ON a1.SHELF_ID=a3.shelf_id AND a1.PRODUCT_ID=a3.product_id AND ext8=5
    WHERE a1.STOCK_QUANTITY>0 AND a1.DATA_FLAG=1
    GROUP BY a1.SHELF_ID
    ) t1
JOIN feods.zs_shelf_flag t2 ON t2.shelf_id=t1.SHELF_ID
SET t2.ext19= CASE WHEN t1.no_new_day >=60 THEN 4 WHEN t1.no_new_day >=30 THEN 3 WHEN t1.no_new_day >=15
    THEN 2 WHEN t1.no_new_day >=0 THEN 1 ELSE 0 END
    ,t2.ext20 = CASE WHEN t1.sku_fill>=55 THEN 5 WHEN t1.sku_fill>=35 THEN 4 WHEN t1.sku_fill>=25 THEN 3
                WHEN t1.sku_fill>=15 THEN 2 WHEN t1.sku_fill>=0 THEN 1 END
    ,t2.ext21 = CASE WHEN t1.outstock_value_rate>=0.5 THEN 4 WHEN t1.outstock_value_rate>=0.25 THEN 3 WHEN t1.outstock_value_rate>=0.15 THEN 2
                ELSE 1 END
;
# 月度货架标签
UPDATE feods.zs_shelf_flag a1
JOIN feods.dm_ma_shelf_kpi_detail_monthly a2 ON a2.sdate=DATE_SUB(@smonth,INTERVAL 1 MONTH ) AND a2.shelf_id=a1.shelf_id
SET # 是否超甲级
    a1.ext17=IF(a2.gmv+IFNULL(a2.after_pay_amount,0)>=4000,1,0)
    # 女性员工占比区间 1:[0,30%）；2 :[30%，70%）；3 : [70%，100%]；0:-
    ,a1.ext18=CASE WHEN a2.users_women/a2.users_gender<0.3 THEN 1 WHEN a2.users_women/a2.users_gender<0.7 THEN 2
        WHEN a2.users_women/a2.users_gender>=0.7 THEN 3 ELSE 0 END
    # 假日消费占比(1:高、2:中、3:低)
    ,a1.ext22=CASE WHEN a2.gmv_holiday/a2.gmv>=0.3 THEN 1 WHEN a2.gmv_holiday/a2.gmv>=0.1 THEN 2
        WHEN a2.gmv_holiday/a2.gmv>=0 THEN 3 ELSE 0 END
    # 加班频率(1:高、2:中、3:低)
    ,a1.ext23=CASE WHEN a2.gmv_overtime/a2.gmv>=0.25 THEN 1 WHEN a2.gmv_overtime/a2.gmv>=0.1 THEN 2
        WHEN a2.gmv_overtime/a2.gmv>=0 THEN 3 ELSE 0 END
    # 高单价商品接受度(1:高、2:中、3:低)
    ,a1.ext24=CASE WHEN a2.gmv_5above/a2.gmv>=0.4 THEN 1 WHEN a2.gmv_5above/a2.gmv>=0.2 THEN 2
        WHEN a2.gmv_5above/a2.gmv>=0 THEN 3 ELSE 0 END
    # 品类偏好(二级分类) 42561
    ,a1.ext_bin_2= CASE WHEN
            IF(a2.gmv_sec_type4/a2.gmv>0.2,1,0)+IF(a2.gmv_sec_type2/a2.gmv>0.15,1,0) +IF(a2.gmv_sec_type5/a2.gmv>0.15,1,0)
                +IF(a2.gmv_sec_type6/a2.gmv>0.45,1,0)+IF(a2.gmv_sec_type1/a2.gmv>0.45,1,0) =0   THEN 1
        ELSE CONV(CONCAT( IF(a2.gmv_sec_type4/a2.gmv>0.2,1,0) ,IF(a2.gmv_sec_type2/a2.gmv>0.15,1,0) ,IF(a2.gmv_sec_type5/a2.gmv>0.15,1,0)
                    ,IF(a2.gmv_sec_type6/a2.gmv>0.45,1,0) ,IF(a2.gmv_sec_type1/a2.gmv>0.45,1,0) ,0 ),2,10) END
;
#周货架标签
UPDATE feods.zs_shelf_flag a1
JOIN fe_dm.dm_ma_shelf_derived_data_weekly a2 ON a2.sdate=@sweek AND a2.SHELF_ID=a1.shelf_id
SET a1.users_chg=a2.users_chg
    ,a1.users_active=a2.users_active
    ,a1.reorder_rate=a2.reorder_rate
    ,a1.users_quanlity=a2.users_quanlity
    ,a1.users_potential=a2.users_potential
    ,a1.orders_per_user=a2.orders_per_user
    ,a1.shelf_value=a2.shelf_value
    ,a1.users_saturability=a2.users_saturability
    ,a1.ext29=a2.users_order
    ,a1.ext30=a2.users_permeate #上周用户数渗透率
;
-- 朱星华新增：更新价格敏感度(1敏感,2一般敏感,3不敏感,4用券敏感)
UPDATE feods.zs_shelf_flag t1
JOIN 
(SELECT shelf_id,
        CASE WHEN sensitive_level = '敏感' THEN 1
             WHEN sensitive_level = '一般敏感' THEN 2
             WHEN sensitive_level = '不敏感' THEN 3
             WHEN sensitive_level = '用券敏感' THEN 4
        END AS sensitive_level
FROM fe_dm.dm_op_shelf_price_sensitivity
WHERE month_id = DATE_FORMAT(SUBDATE(CURRENT_DATE(),1),'%Y-%m')
)t2 ON t2.shelf_id=t1.shelf_id
SET t1.ext31= t2.sensitive_level;
# 货架可补sku判定(当天)
-- 朱星华新增：更新可补sku判定(1sku正常,2sku偏多,3sku偏少)
UPDATE feods.zs_shelf_flag t1
JOIN
(SELECT shelf_id,
        CASE WHEN fill_sku_situation = 'sku正常' THEN 1
             WHEN fill_sku_situation = 'sku偏多' THEN 2
             WHEN fill_sku_situation = 'sku偏少' THEN 3
        END AS fill_sku_situation
FROM fe_dm.dm_op_shelf_sku_situation
WHERE sdate = CURRENT_DATE
)t2 ON t2.shelf_id=t1.shelf_id
SET t1.ext32= t2.fill_sku_situation;
# 截存数据
DELETE FROM feods.zs_shelf_flag_his WHERE sdate=CURDATE() OR sdate<SUBDATE(CURDATE(),500) ;
INSERT INTO feods.zs_shelf_flag_his
    (sdate,shelf_id, ext1, ext4, ext5, ext26, ext27)
SELECT CURDATE() sdate,shelf_id, ext1, ext4, ext5, ext26, ext27
FROM feods.zs_shelf_flag;
-- 执行记录日志
CALL sh_process.`sp_sf_dw_task_log` ('sh_shelf_flag',DATE_FORMAT(@run_date, '%Y-%m-%d'),CONCAT('纪伟铨@', @user, @timestamp));
COMMIT;
END