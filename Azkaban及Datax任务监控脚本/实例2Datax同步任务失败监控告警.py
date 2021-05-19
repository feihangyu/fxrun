
import pymysql
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


email_list = ['fezs_it_data@sf-express.com']
def sendEmail(title,content, receivers):
    # Config
    smtp_server = "lsmtp.sf-express.com"
    smtp_port = 25
    mail_user = "fekj_monitor@sf-express.com"
    mail_pass = "xxxxx"  #替换成对应的密码
    sender = 'fekj_monitor@sf-express.com'
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = ",".join(receivers)
    message["Subject"] = title
    text = MIMEText(content, "plain", "utf-8")
    message.attach(text)
    try:
        smtp = smtplib.SMTP()
        smtp.connect(smtp_server, smtp_port)
        smtp.ehlo()
        smtp.login(mail_user, mail_pass)
        smtp.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")


def get_relation(conn,cursor,fproject_list,f_list):

    project_list=[]
    try:
        sql='''
        SELECT project FROM fe_dwd.`dwd_prc_project_relationship_detail_info` 
        WHERE  dependent_project IN ({}) AND update_frequency NOT LIKE '%已停止%' AND start_time<'12:00'
        GROUP BY project
        '''.format("'"+"','".join(fproject_list)+"'")
        cursor.execute(sql)
        res=cursor.fetchall()
        if res:
            for k in res:
                if k[0] not in sum(f_list, []):
                    project_list.append(k[0])
        f_list.append(project_list)
        #递归
        if project_list!=[]:
            get_relation(conn,cursor,project_list,f_list)
    except Exception as e:
        print(e)
        conn.rollback()
    finally:
        #返回受影响的任务列表
        return sum(f_list, [])



def datax_erp_dective(conn,cursor):

    mesg_list=[]
    try:
        sql='''
        SELECT b.name,c.table_name_one,c.table_name_two,b.description,MIN(a.trigger_time) AS trigger_time
        FROM fe_datax.`job_log` a
        JOIN fe_datax.`job_project` b
        ON CONCAT(a.job_desc,'_erp')=b.name
        JOIN fe_dwd.`dwd_datax_table_mapping_info` c
        ON b.name=c.datax_project_name AND c.delete_flag=1
        WHERE a.trigger_time>=SUBDATE(CURRENT_DATE,0) AND a.handle_code<>200  AND TIMESTAMPDIFF(MINUTE,a.handle_time,CURRENT_TIMESTAMP)<7
        GROUP BY b.name,c.table_name_one,c.table_name_two,b.description
        '''
        cursor.execute(sql)
        conn.commit()
        res=cursor.fetchall()
        if res:
            for r in res:
                mesg_list=[]
                mesg="今日datax同步任务:%s 于%s执行失败，该同步是用于：%s，对应的同步实例1的表：%s，对应的同步实例2的表：%s。"%(r[0],r[4],r[3],r[1],r[2])
                #将失败的任务写入一个表，以便于后面修复任务后，防止多个依赖的任务同时执行导致CPU资源很高，影响任务执行
                try:
                    sql3='''
                        REPLACE INTO fe_dwd.dwd_project_fail_delay_again_execute(sdate,project,dependent_project,d_start_time)
                        SELECT
                        CURRENT_DATE,project,dependent_project,start_time
                        FROM fe_dwd.`dwd_prc_project_relationship_detail_info`
                        WHERE dependent_project='{}' AND STATUS='已部署'
                    '''.format(r[0])
                    cursor.execute(sql3)
                    conn.commit()

                    #将之前失败的datax同步任务进行更新数据
                    sql4='''
                    UPDATE fe_dwd.dwd_project_fail_delay_again_execute b1
                    JOIN (
                    SELECT b.name,a.handle_time,c.rank,c.project
                    FROM fe_datax.`job_log` a
                    JOIN fe_datax.`job_project` b
                    ON CONCAT(a.job_desc,'_erp')=b.name
                    JOIN 
                    (
                    SELECT 
                    IF(@dependent_project = dependent_project  , @rankk := @rankk + 1,@rankk := 0) AS rank,
                    @project := project AS project,
                    @dependent_project := dependent_project AS dependent_project
                    FROM fe_dwd.`dwd_project_fail_delay_again_execute` WHERE  sdate=CURRENT_DATE AND project_repair_time IS NULL AND dependent_project LIKE '%_erp'
                    ORDER BY d_start_time
                      ) c
                      ON b.name=c.dependent_project
                    WHERE a.trigger_time>=CURRENT_DATE  
                    AND a.handle_code=200 
                    ) b2
                    ON b1.dependent_project=b2.name AND b1.project=b2.project
                    SET b1.project_repair_time=b2.handle_time,
                        b1.dp_will_start_time=CASE WHEN b2.rank<=2  THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 30 SECOND)
                                                WHEN b2.rank<=5  THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 90 SECOND)
                                                WHEN b2.rank<=8  THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 150 SECOND)
                                                WHEN b2.rank<=11 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 210 SECOND)
                                                WHEN b2.rank<=14 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 270 SECOND)
                                                WHEN b2.rank<=17 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 330 SECOND)
                                                WHEN b2.rank<=20 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 390 SECOND)
                                                WHEN b2.rank<=23 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 450 SECOND)
                                                WHEN b2.rank<=26 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 510 SECOND)
                                                WHEN b2.rank<=29 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 570 SECOND)
                                                WHEN b2.rank<=32 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 630 SECOND)
                                                WHEN b2.rank<=35 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 690 SECOND)
                                                WHEN b2.rank<=38 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 750 SECOND)
                                                WHEN b2.rank<=41 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 810 SECOND)
                                                WHEN b2.rank<=44 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 870 SECOND)
                                                WHEN b2.rank<=47 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 930 SECOND)
                                                WHEN b2.rank<=50 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 990 SECOND)
                                                WHEN b2.rank<=53 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1050 SECOND)
                                                WHEN b2.rank<=56 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1110 SECOND)
                                                WHEN b2.rank<=59 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1170 SECOND)
                                                WHEN b2.rank<=62 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1230 SECOND)
                                                WHEN b2.rank<=65 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1290 SECOND)
                                                WHEN b2.rank<=68 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1350 SECOND)
                                                WHEN b2.rank<=71 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1410 SECOND)
                                                WHEN b2.rank<=74 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1470 SECOND)
                                                WHEN b2.rank<=77 THEN ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1530 SECOND)
                                           ELSE  ADDDATE(CURRENT_TIMESTAMP,INTERVAL 1590 SECOND)
                                           END  #设置并发数为3
                    WHERE b1.sdate=CURRENT_DATE;
                    '''
                    cursor.execute(sql4)
                    conn.commit()

                except Exception as e:
                    print(e)
                    conn.rollback()

                mesg_list.append(mesg)
                table_name_two=r[2]

                #获取同步失败的表对网易有数任务的影响
                sql_wy='''
                SELECT * FROM fe_dwd.`dwd_wangyi_data_extract_info_base2` WHERE source_table='{}'
                '''.format(table_name_two)
                cursor.execute(sql_wy)
                res=cursor.fetchall()
                wy_list = '通过网易有数抽取sql查询，同步失败任务:%s对网易有数抽取直接影响如下：\n'%(r[0])
                if res:
                    info = '\n'.join(list(set(['将会直接影响：'+i[3] + '的'+i[1]+ '网易有数的抽取任务：'+ i[2] for i in res])))
                    wy_list+=info
                else:
                    wy_list = '通过网易有数抽取sql查询，同步失败任务:%s对网易有数抽取任务没有直接影响'%(r[0])
                mesg_list.append(wy_list)


                # 将获取直接受影响的azkaban任务
                dir_sql='''
                SELECT project,maintainer,STATUS,update_frequency,start_time FROM fe_dwd.`dwd_prc_project_relationship_detail_info`
                WHERE source_table='{}' AND STATUS='已部署'
                '''.format(table_name_two)
                cursor.execute(dir_sql)
                conn.commit()
                dir_res = cursor.fetchall()
                if dir_res:
                    for dr in dir_res:
                        mesg1 = "今日datax同步任务:%s执行失败，将直接影响：%s的%s的%s%s执行的azkaban调度任务：%s" % (r[0],dr[1], dr[2], dr[3], dr[4], dr[0])
                        mesg_list.append(mesg1)
                        fail_project=dr[0]  #直接受影响的任务
                        f_list = []
                        project_affect_list = get_relation(conn,cursor,[fail_project],f_list)  #获取依赖直接受影响的任务的其他任务列表
                        if project_affect_list != []:  #如果没有其他简介受影响的任务
                            try:
                                sql2 = '''
                                SELECT project,maintainer,STATUS,update_frequency,start_time
                                FROM fe_dwd.`dwd_prc_project_relationship_detail_info`
                                WHERE project IN ({})
                                AND STATUS='已部署'
                                GROUP BY project,maintainer,STATUS,update_frequency,start_time
                                ORDER BY start_time
                                '''.format("'" + "','".join(project_affect_list) + "'")
                                cursor.execute(sql2)
                                res2 = cursor.fetchall()
                                base_info="根据依赖关系递归查找，因datax同步任务失败直接影响的azkaban调度任务：%s 对以下azkaban调度任务产生影响："%(dr[0])
                                if res2:
                                    for r2 in res2:
                                        info ='\n'+r2[1]+r2[2]+r2[3]+r2[4]+'执行的azkaban调度任务：'+r2[0]
                                        base_info+=info
                                mesg_list.append(base_info)
                            except Exception as e:
                                print(e)
                                conn.rollback()
                        else:
                            mesg2 = "根据依赖关系递归查找，因datax同步任务失败直接影响的azkaban调度任务：%s 对其他的azkaban任务无影响！" % (dr[0])
                            mesg_list.append(mesg2)

                        #获取间接对网易有数抽取任务的影响
                        if project_affect_list != []:  # 如果没有其他间接受影响的任务
                            try:
                                wy_sql = '''
                                    SELECT * FROM fe_dwd.`dwd_wangyi_data_extract_info_base2` WHERE source_table IN (
                                    SELECT CONCAT(aim_base,'.',aim_table) AS table_name FROM fe_dwd.`dwd_prc_project_process_source_aim_table_info`  WHERE project IN ({})
                                    AND LENGTH(aim_table)>3 GROUP BY  CONCAT(aim_base,'.',aim_table)) 
                                '''.format("'" + "','".join(project_affect_list) + "'")
                                cursor.execute(wy_sql)
                                wy_sql_res2 = cursor.fetchall()
                                wy_base_info='通过网易有数抽取sql查询，同步失败任务:%s对网易有数抽取间接影响如下：\n'%(r[0])
                                if wy_sql_res2:
                                    info = '\n'.join(list(set(['将会间接影响：' + i[3] + '的' + i[1] + '网易有数的抽取任务：' + i[2] for i in wy_sql_res2])))
                                    wy_base_info += info
                                mesg_list.append(wy_base_info)
                            except Exception as e:
                                print(e)
                                conn.rollback()
                        else:
                            wy_mesg2 = "通过网易有数抽取sql查询，同步失败任务:%s对网易有数抽取任务没有间接影响"%(r[0])
                            mesg_list.append(wy_mesg2)

                else:
                    mesg1 = "今日datax同步任务:%s执行失败，对azkaban调度任务无影响！"%(r[0])
                    mesg_list.append(mesg1)

            title = 'datax同步任务失败告警及实例2azkaban任务影响告警'
            sendEmail(title, '\n\n'.join(mesg_list) + "\n以上，请知悉！", email_list)

    except Exception as e:
        print(e)

def main():
    # 创建数据库连接对象
    conn = pymysql.connect(
        host='gz-cdb-3hfr4o0x.sql.tencentcdb.com',  #需要换成内网ip
        port=60611,
        user='xxx',
        passwd='xxxx',
        db='fe_dwd',
        charset='utf8')
    cursor = conn.cursor()

    #print('开始监控************')
    datax_erp_dective(conn,cursor)
    #print('结束监控************')
    cursor.close()
    conn.close()

#每7分钟执行一次
main()